package downloader

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	"github.com/ValerySidorin/charon/pkg/diffstore"
	"github.com/ValerySidorin/charon/pkg/fiasnalog"
	"github.com/ValerySidorin/charon/pkg/filefetcher"
	"github.com/ValerySidorin/charon/pkg/queue"
	walconfig "github.com/ValerySidorin/charon/pkg/wal/config"
	wal "github.com/ValerySidorin/charon/pkg/wal/downloader"
	walrecord "github.com/ValerySidorin/charon/pkg/wal/downloader/record"
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
	Bucket            = "charondiffstore"
	downloaderRingKey = "downloader"

	beginAfter                   = 1 * time.Second
	ringAutoMarkUnhealthyPeriods = 10
)

type Downloader struct {
	services.Service

	cfg Config
	log gklog.Logger

	//Ring services
	downloadersLifecycler *ring.BasicLifecycler
	downloadersRing       *ring.Ring
	instanceMap           *concurrentInstanceMap
	healthyInstancesCount *atomic.Uint32
	subservices           *services.Manager

	diffStoreWriter diffstore.Writer
	fiasNalogClient *fiasnalog.Client
	fileFetcher     *filefetcher.FileFetcher

	wal     *wal.WAL
	currRec *walrecord.Record
}

type Config struct {
	StartingVersion int32         `yaml:"starting_version"`
	PollingInterval time.Duration `yaml:"polling_interval"`
	LocalDir        string        `yaml:"local_dir"`
	LocalDirWithID  string        `yaml:"-"`
	InstanceID      string        `yaml:"-"`

	DownloadersRing DownloaderRingConfig `yaml:"ring"`

	WAL         walconfig.Config   `yaml:"walstore"`
	Queue       queue.Config       `yaml:"queue"`
	DiffStore   diffstore.Config   `yaml:"diffstore"`
	FiasNalog   fiasnalog.Config   `yaml:"fias_nalog"`
	FileFetcher filefetcher.Config `yaml:"file_fetcher"`
}

func New(ctx context.Context, cfg Config, reg prometheus.Registerer, log gklog.Logger) (*Downloader, error) {
	cfg.InstanceID = cfg.DownloadersRing.InstanceID
	cfg.LocalDirWithID = filepath.Join(cfg.LocalDir, cfg.InstanceID)

	log = gklog.With(log, "service", "downloader", "id", cfg.InstanceID)

	writer, err := diffstore.NewWriter(cfg.DiffStore, Bucket)
	if err != nil {
		return nil, errors.Wrap(err, "init diffstore writer for downloader")
	}

	wal, err := wal.NewWAL(ctx, cfg.WAL, log)
	if err != nil {
		return nil, errors.Wrap(err, "init wal for downloader")
	}

	d := &Downloader{
		cfg:                   cfg,
		diffStoreWriter:       writer,
		log:                   log,
		fiasNalogClient:       fiasnalog.NewClient(cfg.FiasNalog),
		fileFetcher:           filefetcher.NewClient(cfg.FileFetcher, log),
		healthyInstancesCount: atomic.NewUint32(0),
		instanceMap:           newConcurrentInstanceMap(),
		wal:                   wal,
	}

	d.Service = services.NewTimerService(
		cfg.PollingInterval, d.start, d.run, d.stop)

	downloadersRing, downloadersLifecycler, err := newRingAndLifecycler(
		cfg.DownloadersRing, d.healthyInstancesCount, d.instanceMap, log, reg)
	if err != nil {
		return nil, err
	}

	manager, err := services.NewManager(downloadersRing, downloadersLifecycler)
	if err != nil {
		return nil, errors.Wrap(err, "init service manager for downloader")
	}

	d.subservices = manager
	d.downloadersLifecycler = downloadersLifecycler
	d.downloadersRing = downloadersRing

	return d, nil
}

func newRingAndLifecycler(ringCfg DownloaderRingConfig, instanceCount *atomic.Uint32, instances *concurrentInstanceMap, logger gklog.Logger, reg prometheus.Registerer) (*ring.Ring, *ring.BasicLifecycler, error) {
	reg = prometheus.WrapRegistererWithPrefix("charon_", reg)
	kvStore, err := kv.NewClient(ringCfg.KVStore, ring.GetCodec(), kv.RegistererWithKVName(reg, "downloader-lifecycler"), logger)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to initialize downloader' KV store")
	}

	lifecyclerCfg, err := ringCfg.ToBasicLifecyclerConfig(logger)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to build downloader' lifecycler config")
	}

	var delegate ring.BasicLifecyclerDelegate
	delegate = ring.NewInstanceRegisterDelegate(ring.ACTIVE, lifecyclerCfg.NumTokens)
	delegate = newHealthyInstanceDelegate(instanceCount, lifecyclerCfg.HeartbeatTimeout, delegate)
	delegate = newInstanceListDelegate(instances, delegate)
	delegate = ring.NewLeaveOnStoppingDelegate(delegate, logger)
	delegate = newAutoMarkUnhealthyDelegate(ringAutoMarkUnhealthyPeriods*lifecyclerCfg.HeartbeatTimeout, delegate, logger)

	distributorsLifecycler, err := ring.NewBasicLifecycler(lifecyclerCfg, "downloader", downloaderRingKey, kvStore, delegate, logger, reg)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to initialize downloaders' lifecycler")
	}

	distributorsRing, err := ring.New(ringCfg.toRingConfig(), "downloader", downloaderRingKey, logger, reg)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to initialize downloaders' ring client")
	}

	return distributorsRing, distributorsLifecycler, nil
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

	recs, err := d.wal.GetProcessingRecords(ctx)
	if err != nil {
		level.Error(d.log).Log("msg", err.Error())

		if rbErr := d.wal.Unlock(ctx, false); rbErr != nil {
			level.Error(d.log).Log("msg", rbErr.Error())
			return rbErr
		}

		return err
	}

	lostRec, found := lo.Find(recs, func(item *walrecord.Record) bool {
		return item.DownloaderID == d.cfg.InstanceID
	})
	if found {
		d.currRec = lostRec
	}

	if cErr := d.wal.Unlock(ctx, true); cErr != nil {
		level.Error(d.log).Log("msg", cErr.Error())
		return cErr
	}
	return nil
}

func (d *Downloader) stop(_ error) error {
	level.Debug(d.log).Log("msg", "finishing download")
	return nil
}

func (d *Downloader) run(ctx context.Context) error {
	if d.currRec == nil {
		if err := d.wal.Lock(ctx); err != nil {
			level.Error(d.log).Log("msg", err.Error())

			if rbErr := d.wal.Unlock(ctx, false); rbErr != nil {
				level.Error(d.log).Log("msg", rbErr.Error())
				return nil
			}

			return nil
		}

		//If we don't have our failed downloads, try to steal stale downloads from another members
		if int(d.HealthyInstancesCount()) < d.downloadersRing.InstancesCount() {
			if err := d.stealWALRecord(ctx); err != nil {
				level.Error(d.log).Log("msg", err.Error())
				return nil
			}
		}

		if d.currRec != nil {
			if err := d.wal.Unlock(ctx, true); err != nil {
				level.Error(d.log).Log("msg", err.Error())
				return nil
			}
		}
	}

	//If there is nothing to steal, try to get new download file info from fias.nalog
	//and mark it as processing
	if d.currRec == nil {
		if err := d.leaseWALRecord(ctx); err != nil {
			level.Error(d.log).Log("msg", err.Error())
			return nil
		}

		if d.currRec != nil {
			if err := d.wal.Unlock(ctx, true); err != nil {
				level.Error(d.log).Log("msg", err.Error())
				return nil
			}
		}
	}

	//There is no diffs to process
	if d.currRec == nil {
		if err := d.wal.Unlock(ctx, true); err != nil {
			level.Error(d.log).Log("msg", err.Error())
			return nil
		}

		return nil
	}

	if err := d.fileFetcher.Download(d.cfg.LocalDirWithID, d.currRec.Version, d.currRec.DownloadURL); err != nil {
		level.Error(d.log).Log("msg", err.Error())
		return nil
	}

	if err := d.wal.Lock(ctx); err != nil {
		level.Error(d.log).Log("msg", err.Error())

		if rbErr := d.wal.Unlock(ctx, false); rbErr != nil {
			level.Error(d.log).Log("msg", rbErr.Error())
			return nil
		}

		return nil
	}

	versionedDir := filepath.Join(d.cfg.LocalDirWithID, strconv.Itoa(d.currRec.Version))
	fName := filepath.Join(versionedDir, filefetcher.FileName)

	file, err := os.Open(fName)
	if err != nil {
		level.Error(d.log).Log("msg", err.Error())

		if rmErr := os.RemoveAll(versionedDir); rmErr != nil {
			level.Error(d.log).Log("msg", rmErr.Error())
			return nil
		}

		return nil
	}

	if err := d.diffStoreWriter.Store(ctx, d.currRec.Version, file); err != nil {
		level.Error(d.log).Log("msg", err.Error())
		return nil
	}
	file.Close()

	if err := os.RemoveAll(versionedDir); err != nil {
		level.Error(d.log).Log("msg", err.Error())
		return nil
	}

	if err := d.wal.CompleteRecord(ctx, d.currRec); err != nil {
		level.Error(d.log).Log("msg", err.Error())
		return nil
	}

	if err := d.wal.Unlock(ctx, true); err != nil {
		level.Error(d.log).Log("msg", err.Error())
		return nil
	}

	d.currRec = nil

	return nil
}

func (d *Downloader) stealWALRecord(ctx context.Context) error {
	level.Debug(d.log).Log("msg", "trying to steal wal record from unhealthy members")

	recs, err := d.wal.GetProcessingRecords(ctx)
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
					rec, found := lo.Find(recs, func(item *walrecord.Record) bool {
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

func (d *Downloader) getWALRecordToProcess(ctx context.Context) (*walrecord.Record, error) {
	processingRecs, err := d.wal.GetProcessingRecords(ctx)
	if err != nil {
		return nil, err
	}

	processingVers := lo.Map(processingRecs, func(item *walrecord.Record, index int) int {
		return item.Version
	})
	completedVers, err := d.diffStoreWriter.ListVersions(ctx)
	if err != nil {
		return nil, err
	}

	skipVers := lo.Union(processingVers, completedVers)
	skipVers = lo.Uniq(skipVers)

	fInfos, err := d.fiasNalogClient.GetAllDownloadFileInfo(ctx)
	if err != nil {
		return nil, err
	}

	allVers := lo.FilterMap(fInfos, func(item fiasnalog.DownloadFileInfo, index int) (int, bool) {
		return item.VersionID, item.GARXmlDeltaUrl != ""
	})

	availableVers := lo.Without(allVers, skipVers...)
	sort.Ints(availableVers)

	if len(availableVers) > 0 {
		firstAvailableVer := availableVers[0]
		info, found := lo.Find(fInfos, func(item fiasnalog.DownloadFileInfo) bool {
			return item.VersionID == firstAvailableVer
		})
		if found {
			return walrecord.New(info.VersionID, d.cfg.InstanceID, info.GARXmlDeltaUrl, walrecord.PROCESSING), nil
		}

		return nil, nil
	}

	return nil, nil
}

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
