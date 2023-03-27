package processor

import (
	"context"
	"fmt"
	"sort"

	"github.com/ValerySidorin/charon/pkg/diffstore"
	"github.com/ValerySidorin/charon/pkg/processor/plugin"
	"github.com/ValerySidorin/charon/pkg/util"
	wal "github.com/ValerySidorin/charon/pkg/wal/processor"
	"github.com/ValerySidorin/charon/pkg/wal/processor/record"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/ring"
	"github.com/pkg/errors"
	"github.com/samber/lo"
	"go.uber.org/atomic"
)

type importer struct {
	cfg Config
	log log.Logger

	wal             *wal.WAL
	diffStoreReader diffstore.Reader

	executing *atomic.Bool

	processorsRing        *ring.Ring
	processorsLifecycler  *ring.BasicLifecycler
	instanceMap           *util.ConcurrentInstanceMap
	healthyInstancesCount *atomic.Uint32

	plug plugin.Plugin
}

func newImporter(ctx context.Context, ring *ring.Ring, lifecycler *ring.BasicLifecycler, instanceMap *util.ConcurrentInstanceMap, healthyCnt *atomic.Uint32, cfg Config, log log.Logger) (*importer, error) {
	reader, err := diffstore.NewReader(cfg.DiffStore, diffstore.Bucket)
	if err != nil {
		return nil, errors.Wrap(err, "importer: connect to diff store")
	}

	wal, err := wal.NewWAL(ctx, cfg.WAL, log)
	if err != nil {
		return nil, errors.Wrap(err, "importer: connect to wal")
	}

	plug, err := plugin.New(cfg.Plugin, log)
	if err != nil {
		return nil, errors.Wrap(err, "importer: init plugin")
	}

	return &importer{
		cfg: cfg,
		log: log,

		diffStoreReader: reader,
		wal:             wal,

		processorsRing:       ring,
		processorsLifecycler: lifecycler,

		instanceMap:           instanceMap,
		healthyInstancesCount: healthyCnt,

		executing: atomic.NewBool(false),

		plug: plug,
	}, nil
}

func (i *importer) notify(ctx context.Context) {
	if !i.executing.Load() {
		i.executing.Store(true)

		if err := i.handle(ctx); err != nil {
			level.Error(i.log).Log("msg", err.Error())
		}

		i.executing.Store(false)
	}
}

func (i *importer) handle(ctx context.Context) error {
	wal, err := wal.NewWAL(ctx, i.cfg.WAL, i.log)
	if err != nil {
		return errors.Wrap(err, "persister: connect to wal")
	}

	var loopErr error
	for {
		ver, loopErr := wal.GetFirstIncompleteVersion(ctx)
		if loopErr != nil {
			continue
		}
		if ver == 0 {
			break
		}

		for {
			if loopErr := wal.Lock(ctx); loopErr != nil {
				i.unlockWithRollback(ctx, wal, err)
				continue
			}

			recs, loopErr := wal.GetIncompleteRecordsByVersion(ctx, ver)
			if loopErr != nil {
				level.Error(i.log).Log("msg", loopErr.Error())
				continue
			}

			if len(recs) <= 0 {
				break
			}

			var fRec *record.Record
			fRec, found := lo.Find(recs, func(item *record.Record) bool {
				return item.ProcessorID == i.cfg.InstanceID
			})

			if !found {
				sort.Slice(recs[:], func(i, j int) bool {
					return recs[i].ObjName < recs[j].ObjName
				})

				for _, rec := range recs {
					r := rec
					if rec.ProcessorID != "" && rec.ProcessorID != i.cfg.InstanceID {
						_, ok := i.instanceMap.Get(rec.ProcessorID)
						if ok {
							healthy, loopErr := i.processorIsHealthy(rec.ProcessorID)
							if loopErr != nil {
								level.Error(i.log).Log("msg", loopErr.Error())
								return loopErr
							}

							if !healthy {
								level.Debug(i.log).Log("msg", "stealing record")
								fRec = r
								fRec.ProcessorID = i.cfg.InstanceID
								fRec.Status = record.PROCESSING
								break
							}
						}
						continue
					}
					fRec = r
					fRec.ProcessorID = i.cfg.InstanceID
					fRec.Status = record.PROCESSING
					break
				}
			}

			if fRec == nil {
				if loopErr = wal.Unlock(ctx, true); loopErr != nil {
					level.Error(i.log).Log("msg", loopErr.Error())
				}
				continue
			}

			if loopErr = wal.UpdateRecord(ctx, fRec); loopErr != nil {
				i.unlockWithRollback(ctx, wal, loopErr)
				continue
			}

			if loopErr = wal.Unlock(ctx, true); loopErr != nil {
				level.Error(i.log).Log("msg", loopErr.Error())
				continue
			}

			level.Debug(i.log).Log("msg", fmt.Sprintf("start processing: %s", fRec.ObjName))
			if err := i.plug.Exec(ctx, nil); err != nil {
				level.Error(i.log).Log("msg", fmt.Sprintf("error loading %s: %s", fRec.ObjName, err.Error()))
				continue
			}
			fRec.Status = record.COMPLETED

			if loopErr = wal.UpdateRecord(ctx, fRec); loopErr != nil {
				level.Error(i.log).Log("msg", loopErr.Error())
				continue
			}

			level.Debug(i.log).Log("msg", fmt.Sprintf("successfully processed: %s", fRec.ObjName))
			continue
		}
	}

	if err := wal.Dispose(ctx); err != nil {
		return err
	}

	return loopErr
}

func (i *importer) unlockWithRollback(ctx context.Context, wal *wal.WAL, err error) {
	level.Error(i.log).Log("msg", err.Error())
	if rbErr := i.wal.Unlock(ctx, false); rbErr != nil {
		level.Error(i.log).Log("msg", rbErr.Error())
	}
}

func (i *importer) processorIsHealthy(instanceID string) (bool, error) {
	if !i.processorsRing.HasInstance(instanceID) {
		return false, nil
	}
	state, err := i.processorsRing.GetInstanceState(instanceID)
	if err != nil {
		return false, err
	}

	if state != ring.ACTIVE && state != ring.JOINING && state != ring.LEAVING {
		return false, nil
	}

	return true, nil
}
