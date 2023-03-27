package downloader

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/ValerySidorin/charon/pkg/downloader/fiasnalog"
	"github.com/ValerySidorin/charon/pkg/downloader/manager"
	"github.com/ValerySidorin/charon/pkg/downloader/notifier"
	"github.com/ValerySidorin/charon/pkg/objstore"
	"github.com/ValerySidorin/charon/pkg/queue"
	"github.com/ValerySidorin/charon/pkg/util"
	"github.com/ValerySidorin/charon/pkg/wal"
	walcfg "github.com/ValerySidorin/charon/pkg/wal/config"
	dwal "github.com/ValerySidorin/charon/pkg/wal/downloader"
	walrec "github.com/ValerySidorin/charon/pkg/wal/downloader/record"
	gklog "github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/samber/lo"
	"go.uber.org/atomic"
)

const (
	downloaderRingKey = "downloader"

	beginAfter                   = 1 * time.Second
	ringAutoMarkUnhealthyPeriods = 10
)

type Downloader struct {
	services.Service

	cfg Config
	log gklog.Logger

	//Lifetime services
	downloadersLifecycler *ring.BasicLifecycler
	downloadersRing       *ring.Ring
	instanceMap           *util.ConcurrentInstanceMap
	healthyInstancesCount *atomic.Uint32
	subservices           *services.Manager

	objStore        objstore.Writer
	fiasNalogClient *fiasnalog.Client
	manager         *manager.Manager

	notifier *notifier.Notifier
	wal      *dwal.WAL
	currRec  *walrec.Record

	fileTypeDownloading string
}

type Config struct {
	StartFrom       StartFromConfig `yaml:"start_from"`
	PollingInterval time.Duration   `yaml:"polling_interval"`
	TempDir         string          `yaml:"temp_dir"`
	TempDirWithID   string          `yaml:"-"`
	InstanceID      string          `yaml:"-"`

	//Manager
	BufferSize int `yaml:"bugger_size"`

	//Fias Nalog client
	Timeout  time.Duration `yaml:"timeout"`
	RetryMax int           `yaml:"retry_max"`

	DownloadersRing DownloaderRingConfig `yaml:"ring"`

	WAL      walcfg.Config   `yaml:"wal"`
	Queue    queue.Config    `yaml:"queue"`
	ObjStore objstore.Config `yaml:"obj_store"`
	Notifier notifier.Config `yaml:"notifier"`
}

type StartFromConfig struct {
	FileType string `yaml:"file_type"`
	Version  int    `yaml:"version"`
}

func New(ctx context.Context, cfg Config, reg prometheus.Registerer, log gklog.Logger) (*Downloader, error) {
	cfg.InstanceID = cfg.DownloadersRing.InstanceID
	cfg.TempDirWithID = filepath.Join(cfg.TempDir, cfg.InstanceID)

	log = gklog.With(log, "service", "downloader", "id", cfg.InstanceID)

	writer, err := objstore.NewWriter(cfg.ObjStore, objstore.Bucket)
	if err != nil {
		return nil, errors.Wrap(err, "downloader connect to obj store as writer")
	}

	wal, err := dwal.NewWAL(ctx, cfg.WAL, log)
	if err != nil {
		return nil, errors.Wrap(err, "downloader connect to WAL")
	}

	d := &Downloader{
		cfg:                   cfg,
		objStore:              writer,
		log:                   log,
		fiasNalogClient:       fiasnalog.NewClient(cfg.RetryMax, cfg.Timeout),
		manager:               manager.New(cfg.BufferSize, log),
		healthyInstancesCount: atomic.NewUint32(0),
		instanceMap:           util.NewConcurrentInstanceMap(),
		wal:                   wal,
		fileTypeDownloading:   cfg.StartFrom.FileType,
	}

	d.Service = services.NewTimerService(
		cfg.PollingInterval, d.start, d.run, nil)

	notifier, err := notifier.New(ctx, cfg.Notifier, cfg.WAL, cfg.InstanceID, log)
	if err != nil {
		return nil, errors.Wrap(err, "downloader init notifier")
	}

	downloadersRing, downloadersLifecycler, err := newRingAndLifecycler(
		cfg.DownloadersRing, d.healthyInstancesCount, d.instanceMap, log, reg)
	if err != nil {
		return nil, err
	}

	manager, err := services.NewManager(downloadersRing, downloadersLifecycler, notifier)
	if err != nil {
		return nil, errors.Wrap(err, "init service manager for downloader")
	}

	d.subservices = manager
	d.downloadersLifecycler = downloadersLifecycler
	d.downloadersRing = downloadersRing
	d.notifier = notifier

	return d, nil
}

func newRingAndLifecycler(ringCfg DownloaderRingConfig, instanceCount *atomic.Uint32, instances *util.ConcurrentInstanceMap, logger gklog.Logger, reg prometheus.Registerer) (*ring.Ring, *ring.BasicLifecycler, error) {
	reg = prometheus.WrapRegistererWithPrefix("charon_", reg)
	kvStore, err := kv.NewClient(ringCfg.KVStore, ring.GetCodec(), kv.RegistererWithKVName(reg, "downloader-lifecycler"), logger)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to initialize downloader' KV store")
	}

	lifecyclerCfg, err := ringCfg.toBasicLifecyclerConfig(logger)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to build downloader' lifecycler config")
	}

	var delegate ring.BasicLifecyclerDelegate
	delegate = ring.NewInstanceRegisterDelegate(ring.ACTIVE, lifecyclerCfg.NumTokens)
	delegate = util.NewHealthyInstanceDelegate(instanceCount, lifecyclerCfg.HeartbeatTimeout, delegate)
	delegate = util.NewInstanceListDelegate(instances, delegate)
	delegate = ring.NewLeaveOnStoppingDelegate(delegate, logger)
	delegate = util.NewAutoMarkUnhealthyDelegate(ringAutoMarkUnhealthyPeriods*lifecyclerCfg.HeartbeatTimeout, delegate, logger)

	downloadersLifecycler, err := ring.NewBasicLifecycler(lifecyclerCfg, "downloader", downloaderRingKey, kvStore, delegate, logger, reg)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to initialize downloaders' lifecycler")
	}

	downloadersRing, err := ring.New(ringCfg.toRingConfig(), "downloader", downloaderRingKey, logger, reg)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to initialize downloaders' ring client")
	}

	return downloadersRing, downloadersLifecycler, nil
}

func (d *Downloader) start(ctx context.Context) error {
	d.subservices.StartAsync(ctx)
	d.subservices.AwaitHealthy(ctx)
	time.Sleep(beginAfter)

	//Check for accidentally dropped downloads
	level.Debug(d.log).Log("msg", "restoring downloader WAL")
	if err := d.wal.Lock(ctx); err != nil {
		level.Error(d.log).Log("msg", err.Error())

		if rbErr := d.wal.Unlock(ctx, false); rbErr != nil {
			level.Error(d.log).Log("msg", rbErr.Error())
			return rbErr
		}

		return err
	}

	if d.cfg.StartFrom.FileType == wal.FileTypeFull {
		rec, found, err := d.wal.GetFirstRecord(ctx)
		if err != nil {
			level.Error(d.log).Log("msg", err.Error())

			if rbErr := d.wal.Unlock(ctx, false); rbErr != nil {
				level.Error(d.log).Log("msg", rbErr.Error())
				return rbErr
			}

			return err
		}

		if found {
			if rec.Type != wal.FileTypeFull {
				level.Warn(d.log).Log("msg", "cluster was initially configured to start from diff; picking diff")
			} else {
				level.Warn(d.log).Log("msg", "cluster has already downloaded or downloading full; picking diff")
			}

			d.fileTypeDownloading = wal.FileTypeDiff
		} else {
			if rbErr := d.wal.Unlock(ctx, false); rbErr != nil {
				level.Error(d.log).Log("msg", rbErr.Error())
				return rbErr
			}

			return nil
		}
	}

	recs, err := d.wal.GetAllRecords(ctx)
	if err != nil {
		level.Error(d.log).Log("msg", err.Error())

		if rbErr := d.wal.Unlock(ctx, false); rbErr != nil {
			level.Error(d.log).Log("msg", rbErr.Error())
			return rbErr
		}

		return err
	}

	lostRec, found := lo.Find(recs, func(item *walrec.Record) bool {
		return item.DownloaderID == d.cfg.InstanceID && item.Status == walrec.PROCESSING
	})
	if found {
		level.Debug(d.log).Log("msg", "found lost download record")
		d.currRec = lostRec
	}

	if cErr := d.wal.Unlock(ctx, true); cErr != nil {
		level.Error(d.log).Log("msg", cErr.Error())
		return cErr
	}
	return nil
}

func (d *Downloader) run(ctx context.Context) error {
	if d.currRec == nil {
		if err := d.lockWAL(ctx); err != nil {
			if rbErr := d.unlockWALWithRollback(ctx); rbErr != nil {
				return rbErr
			}

			return err
		}

		//If we don't have our failed downloads, try to steal stale downloads from another members
		if int(d.HealthyInstancesCount()) < d.downloadersRing.InstancesCount() {
			if err := d.stealWALRecord(ctx); err != nil {
				level.Error(d.log).Log("msg", err.Error())
				return nil
			}
		}

		if d.currRec != nil {
			if err := d.unlockWALWithCommit(ctx); err != nil {
				return err
			}
		}
	}

	//If there is nothing to steal, try to get new download file info from fias.nalog
	//and mark it as processing
	if d.currRec == nil {
		if err := d.leaseWALRecord(ctx); err != nil {
			level.Error(d.log).Log("msg", err.Error())
			if err := d.unlockWALWithRollback(ctx); err != nil {
				return err
			}

			return nil
		}

		if d.currRec != nil {
			if err := d.unlockWALWithCommit(ctx); err != nil {
				return err
			}
		}
	}

	//There is no diffs to process
	if d.currRec == nil {
		if err := d.unlockWALWithCommit(ctx); err != nil {
			return err
		}

		return nil
	}

	if err := d.manager.Download(d.cfg.TempDirWithID, d.currRec.Version, d.currRec.DownloadURL); err != nil {
		level.Error(d.log).Log("msg", err.Error())
		return nil
	}

	if err := d.lockWAL(ctx); err != nil {
		if rbErr := d.unlockWALWithRollback(ctx); rbErr != nil {
			return rbErr
		}

		return err
	}

	versionedDir := filepath.Join(d.cfg.TempDirWithID, strconv.Itoa(d.currRec.Version))
	fName := filepath.Join(versionedDir, manager.FileName)

	file, err := os.Open(fName)
	if err != nil {
		level.Error(d.log).Log("msg", err.Error())
		if err := d.unlockWALWithRollback(ctx); err != nil {
			return err
		}

		if rmErr := os.RemoveAll(versionedDir); rmErr != nil {
			level.Error(d.log).Log("msg", rmErr.Error())
			return nil
		}

		return nil
	}

	if err := d.objStore.Store(ctx, d.currRec.Version, d.currRec.Type, file); err != nil {
		level.Error(d.log).Log("msg", err.Error())
		if err := d.unlockWALWithRollback(ctx); err != nil {
			return err
		}
		return nil
	}
	file.Close()

	if err := os.RemoveAll(versionedDir); err != nil {
		level.Error(d.log).Log("msg", err.Error())
		if err := d.unlockWALWithRollback(ctx); err != nil {
			return err
		}

		return nil
	}

	d.currRec.Status = walrec.COMPLETED
	if err := d.wal.UpdateRecord(ctx, d.currRec); err != nil {
		level.Error(d.log).Log("msg", err.Error())
		return nil
	}

	if err := d.unlockWALWithCommit(ctx); err != nil {
		return err
	}

	d.currRec = nil
	return nil
}

// Downloader WAL operations
func (d *Downloader) stealWALRecord(ctx context.Context) error {
	level.Debug(d.log).Log("msg", "trying to steal wal record from unhealthy members")

	recs, err := d.wal.GetRecordsByStatus(ctx, walrec.PROCESSING)
	if err != nil {
		return errors.Wrap(err, "steal wal record")
	}
	for _, dID := range d.instanceMap.Keys() {
		if dID != d.cfg.InstanceID {
			_, ok := d.instanceMap.Get(dID)
			if ok {
				healthy, err := d.IsHealthy(dID)
				if err != nil {
					return err
				}

				if !healthy {
					rec, found := lo.Find(recs, func(item *walrec.Record) bool {
						return item.DownloaderID == dID
					})
					if found {
						if err := d.wal.StealRecord(ctx, rec, d.cfg.InstanceID); err != nil {
							return err
						}

						d.currRec = rec
					}
				}
			}
		}
	}

	return nil
}

func (d *Downloader) leaseWALRecord(ctx context.Context) error {
	rec, err := d.getWALRecordToProcess(ctx)
	if err != nil {
		return err
	}

	if err := d.wal.AddRecord(ctx, rec); err != nil {
		return err
	}
	d.currRec = rec

	return nil
}

func (d *Downloader) getWALRecordToProcess(ctx context.Context) (*walrec.Record, error) {
	fInfos, err := d.fiasNalogClient.GetAllDownloadFileInfo(ctx)
	if err != nil {
		return nil, err
	}

	if d.fileTypeDownloading == wal.FileTypeFull {
		_, found, err := d.wal.GetFirstRecord(ctx)
		if err != nil {
			return nil, err
		}

		if !found {
			if len(fInfos) > 0 {
				fInfo := fInfos[0]
				return walrec.New(fInfo.VersionID, d.cfg.InstanceID, fInfo.GARXMLFullURL, wal.FileTypeFull), nil
			}

			return nil, errors.New("no file infos available")
		}
	}

	recs, err := d.wal.GetAllRecords(ctx)
	if err != nil {
		return nil, err
	}

	lastSentVer := lo.Max(lo.FilterMap(recs, func(item *walrec.Record, index int) (int, bool) {
		return item.Version, item.Status == walrec.SENT
	}))

	avlRecs := lo.FilterMap(recs, func(item *walrec.Record, index int) (int, bool) {
		return item.Version, item.Version > lastSentVer
	})

	avlInfos := lo.FilterMap(fInfos, func(item fiasnalog.DownloadFileInfo, index int) (int, bool) {
		return item.VersionID, item.VersionID > lastSentVer && item.VersionID >= d.cfg.StartFrom.Version
	})

	firstAvlVer := lo.Min(lo.Without(avlInfos, avlRecs...))
	if firstAvlVer > 0 {
		info, found := lo.Find(fInfos, func(item fiasnalog.DownloadFileInfo) bool {
			return item.VersionID == firstAvlVer
		})
		if found {
			return walrec.New(info.VersionID, d.cfg.InstanceID, info.GARXMLDeltaURL, wal.FileTypeDiff), nil
		}

		return nil, errors.New(fmt.Sprintf("cannot find download info for version: %d", firstAvlVer))
	}

	return nil, errors.New("no new file infos")
}

func (d *Downloader) lockWAL(ctx context.Context) error {
	if err := d.wal.Lock(ctx); err != nil {
		level.Error(d.log).Log("msg", err.Error())
		return err
	}

	return nil
}

func (d *Downloader) unlockWALWithRollback(ctx context.Context) error {
	if err := d.wal.Unlock(ctx, false); err != nil {
		level.Error(d.log).Log("msg", err.Error())
		return err
	}

	return nil
}

func (d *Downloader) unlockWALWithCommit(ctx context.Context) error {
	if err := d.wal.Unlock(ctx, true); err != nil {
		level.Error(d.log).Log("msg", err.Error())
		return err
	}

	return nil
}

// Healthcheck operations
func (d *Downloader) HealthyInstancesCount() uint32 {
	return d.healthyInstancesCount.Load()
}

func (d *Downloader) IsHealthy(instanceID string) (bool, error) {
	if !d.downloadersRing.HasInstance(instanceID) {
		return false, nil
	}
	state, err := d.downloadersRing.GetInstanceState(instanceID)
	if err != nil {
		return false, err
	}

	if state != ring.ACTIVE && state != ring.JOINING && state != ring.LEAVING {
		return false, nil
	}

	return true, nil
}
