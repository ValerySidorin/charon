package downloader

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
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

	fileTypeDownloading string
}

type Config struct {
	StartFrom       StartFromConfig `yaml:"start_from"`
	PollingInterval time.Duration   `yaml:"polling_interval"`
	LocalDir        string          `yaml:"local_dir"`
	LocalDirWithID  string          `yaml:"-"`
	InstanceID      string          `yaml:"-"`

	DownloadersRing DownloaderRingConfig `yaml:"ring"`

	WAL         walconfig.Config   `yaml:"walstore"`
	Queue       queue.Config       `yaml:"queue"`
	DiffStore   diffstore.Config   `yaml:"diffstore"`
	FiasNalog   fiasnalog.Config   `yaml:"fias_nalog"`
	FileFetcher filefetcher.Config `yaml:"file_fetcher"`
}

type StartFromConfig struct {
	FileType string `yaml:"file_type"`
	Version  int    `yaml:"version"`
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
		fileTypeDownloading:   cfg.StartFrom.FileType,
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

	lostRec, found := lo.Find(recs, func(item *walrecord.Record) bool {
		return item.DownloaderID == d.cfg.InstanceID && item.Status == walrecord.PROCESSING
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

func (d *Downloader) stop(_ error) error {
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

			if err := d.wal.Unlock(ctx, true); err != nil {
				level.Error(d.log).Log("msg", err.Error())
				return nil
			}

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

	recs, err := d.wal.GetRecordsByStatus(ctx, walrecord.PROCESSING)
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
				return walrecord.New(fInfo.VersionID, d.cfg.InstanceID, fInfo.GARXMLFullURL, wal.FileTypeFull, walrecord.PROCESSING), nil
			}

			fmt.Println(d.cfg.InstanceID, "123123")
			return nil, errors.New("no file infos available")
		}
	}

	recs, err := d.wal.GetAllRecords(ctx)
	if err != nil {
		return nil, err
	}

	lastVer := lo.Max(lo.Map(recs, func(item *walrecord.Record, index int) int {
		return item.Version
	}))

	availableVers := lo.FilterMap(fInfos, func(item fiasnalog.DownloadFileInfo, index int) (int, bool) {
		return item.VersionID, item.VersionID > lastVer
	})

	if len(availableVers) > 0 {
		firstAvailableVer := availableVers[0]
		info, found := lo.Find(fInfos, func(item fiasnalog.DownloadFileInfo) bool {
			return item.VersionID == firstAvailableVer
		})
		if found {
			return walrecord.New(info.VersionID, d.cfg.InstanceID, info.GARXMLDeltaURL, wal.FileTypeDiff, walrecord.PROCESSING), nil
		}

		return nil, errors.New(fmt.Sprintf("cannot find download info for version: %d", firstAvailableVer))
	}

	return nil, errors.New("no new file infos")
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
