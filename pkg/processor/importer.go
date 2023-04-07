package processor

import (
	"context"
	"fmt"
	"time"

	cluster "github.com/ValerySidorin/charon/pkg/cluster/processor"
	"github.com/ValerySidorin/charon/pkg/cluster/processor/record"
	"github.com/ValerySidorin/charon/pkg/objstore"
	"github.com/ValerySidorin/charon/pkg/processor/plugin"
	"github.com/ValerySidorin/charon/pkg/util"
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

	clusterMonitor *cluster.Monitor
	objStore       objstore.Reader

	executing *atomic.Bool

	processorsRing        *ring.Ring
	processorsLifecycler  *ring.BasicLifecycler
	instanceMap           *util.ConcurrentInstanceMap
	healthyInstancesCount *atomic.Uint32
}

func newImporter(ctx context.Context, ring *ring.Ring, lifecycler *ring.BasicLifecycler, instanceMap *util.ConcurrentInstanceMap, healthyCnt *atomic.Uint32, cfg Config, log log.Logger) (*importer, error) {
	reader, err := objstore.NewReader(cfg.ObjStore, objstore.Bucket)
	if err != nil {
		return nil, errors.Wrap(err, "importer: connect to diff store")
	}

	monitor, err := cluster.NewMonitor(ctx, cfg.Cluster, log)
	if err != nil {
		return nil, errors.Wrap(err, "importer: connect to wal")
	}

	return &importer{
		cfg: cfg,
		log: log,

		objStore:       reader,
		clusterMonitor: monitor,

		processorsRing:       ring,
		processorsLifecycler: lifecycler,

		instanceMap:           instanceMap,
		healthyInstancesCount: healthyCnt,

		executing: atomic.NewBool(false),
	}, nil
}

func (i *importer) notify(ctx context.Context) {
	if !i.executing.Load() {
		i.executing.Store(true)

		if err := i.handle(ctx); err != nil {
			_ = level.Error(i.log).Log("msg", err.Error())
		}

		i.executing.Store(false)
	}
}

func (i *importer) handle(ctx context.Context) error {
	plug, err := plugin.New(i.cfg.Plugin, i.log)
	if err != nil {
		return errors.Wrap(err, "importer: init plugin")
	}
	defer func() {
		_ = plug.Dispose(ctx)
	}()

	monitor, err := cluster.NewMonitor(ctx, i.cfg.Cluster, i.log)
	if err != nil {
		return errors.Wrap(err, "persister: connect to wal")
	}
	defer func() {
		_ = monitor.Dispose(ctx)
	}()

	var loopErr error
	for {
		ver, loopErr := monitor.GetFirstIncompleteVersion(ctx)
		if loopErr != nil {
			_ = level.Error(i.log).Log("msg", loopErr.Error())
			time.Sleep(time.Second)
			continue
		}

		currV, loopErr := plug.GetVersion(ctx)
		if loopErr != nil {
			_ = level.Error(i.log).Log("msg", loopErr.Error())
			time.Sleep(time.Second)
			continue
		}

		if ver <= currV {
			break
		}

		for {
			if loopErr := monitor.Lock(ctx); loopErr != nil {
				i.unlockWithRollback(ctx, monitor, loopErr)
				time.Sleep(time.Second)
				continue
			}

			recs, loopErr := monitor.GetIncompleteRecordsByVersion(ctx, ver)
			if loopErr != nil {
				_ = level.Error(i.log).Log("msg", loopErr.Error())
				time.Sleep(time.Second)
				continue
			}

			if len(recs) <= 0 {
				if loopErr = monitor.Unlock(ctx, true); loopErr != nil {
					_ = level.Error(i.log).Log("msg", loopErr.Error())
					i.unlockWithRollback(ctx, monitor, loopErr)
					continue
				}
				break
			}

			var fRec *record.Record
			fRec, found := lo.Find(recs, func(item *record.Record) bool {
				return item.ProcessorID == i.cfg.InstanceID
			})

			batches := plug.Batches(recs)

			if !found {
				for _, batch := range batches {
					if !batch.IsCompleted() {
						for _, rec := range batch.Records {
							r := rec
							if rec.ProcessorID != "" && rec.ProcessorID != i.cfg.InstanceID {
								_, ok := i.instanceMap.Get(rec.ProcessorID)
								if ok {
									healthy, loopErr := i.processorIsHealthy(rec.ProcessorID)
									if loopErr != nil {
										_ = level.Error(i.log).Log("msg", loopErr.Error())
										return loopErr
									}

									if !healthy {
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

						if fRec != nil {
							break
						}
					}
				}
			}

			if fRec == nil {
				if loopErr = monitor.Unlock(ctx, true); loopErr != nil {
					_ = level.Error(i.log).Log("msg", loopErr.Error())
					i.unlockWithRollback(ctx, monitor, loopErr)
				}
				continue
			}

			if loopErr = monitor.UpdateRecord(ctx, fRec); loopErr != nil {
				i.unlockWithRollback(ctx, monitor, loopErr)
				continue
			}

			if loopErr = monitor.Unlock(ctx, true); loopErr != nil {
				_ = level.Error(i.log).Log("msg", loopErr.Error())
				i.unlockWithRollback(ctx, monitor, loopErr)
				continue
			}

			obj, loopErr := i.objStore.Retrieve(ctx, fRec.ObjName)
			if loopErr != nil {
				_ = level.Error(i.log).Log("msg", loopErr.Error())
				time.Sleep(time.Second)
				continue
			}
			defer obj.Close()

			_ = level.Debug(i.log).Log("msg", fmt.Sprintf("start processing: %s", fRec.ObjName))
			if err := plug.Exec(ctx, fRec, obj); err != nil {
				_ = level.Error(i.log).Log("msg", fmt.Sprintf("error loading %s: %s", fRec.ObjName, err.Error()))
				continue
			}
			fRec.Status = record.COMPLETED

			if loopErr = monitor.UpdateRecord(ctx, fRec); loopErr != nil {
				_ = level.Error(i.log).Log("msg", loopErr.Error())
				time.Sleep(time.Second)
				continue
			}

			_ = level.Debug(i.log).Log("msg", fmt.Sprintf("successfully processed: %s", fRec.ObjName))
			continue
		}

		if loopErr = plug.UpgradeVersion(ctx, ver); loopErr != nil {
			_ = level.Error(i.log).Log("msg", loopErr.Error())
			time.Sleep(time.Second)
			continue
		}
	}

	return loopErr
}

func (i *importer) unlockWithRollback(ctx context.Context, monitor *cluster.Monitor, err error) {
	_ = level.Error(i.log).Log("msg", err.Error())
	if rbErr := monitor.Unlock(ctx, false); rbErr != nil {
		_ = level.Error(i.log).Log("msg", rbErr.Error())
	}

	time.Sleep(1 * time.Second)
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
