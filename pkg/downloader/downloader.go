package downloader

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/ValerySidorin/charon/pkg/cluster"
	cl_cfg "github.com/ValerySidorin/charon/pkg/cluster/config"
	d_cluster "github.com/ValerySidorin/charon/pkg/cluster/downloader"
	cl_rec "github.com/ValerySidorin/charon/pkg/cluster/downloader/record"
	"github.com/ValerySidorin/charon/pkg/downloader/fiasnalog"
	"github.com/ValerySidorin/charon/pkg/downloader/manager"
	"github.com/ValerySidorin/charon/pkg/downloader/notifier"
	"github.com/ValerySidorin/charon/pkg/objstore"
	"github.com/ValerySidorin/charon/pkg/util"
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
	healthyInstancesCount *atomic.Uint32
	subservices           *services.Manager

	objStore        objstore.Writer
	fiasNalogClient *fiasnalog.Client
	manager         *manager.Manager

	notifier       *notifier.Notifier
	clusterMonitor *d_cluster.Monitor
	currRec        *cl_rec.Record

	fileTypeDownloading string
}

type Config struct {
	StartFrom       StartFromConfig `mapstructure:"start_from"`
	PollingInterval time.Duration   `mapstructure:"polling_interval"`
	TempDir         string          `mapstructure:"temp_dir"`
	TempDirWithID   string          `mapstructure:"-"`
	InstanceID      string          `mapstructure:"-"`

	//Manager
	BufferSize int `mapstructure:"buffer_size"`

	//Fias Nalog client
	Timeout  time.Duration `mapstructure:"timeout"`
	RetryMax int           `mapstructure:"retry_max"`

	DownloadersRing DownloaderRingConfig `mapstructure:"ring"`

	Cluster  cl_cfg.Config   `mapstructure:"cluster"`
	ObjStore objstore.Config `mapstructure:"objstore"`
	Notifier notifier.Config `mapstructure:"notifier"`
}

type StartFromConfig struct {
	FileType string `mapstructure:"file_type"`
	Version  int    `mapstructure:"version"`
}

func (c *StartFromConfig) RegisterFlags(f *flag.FlagSet, log gklog.Logger) {
	f.StringVar(&c.FileType, "downloader.start-from.file-type", cluster.FileTypeFull, `What type of file to start from.
	Available values: DIFF, FULL`)
	f.IntVar(&c.Version, "downloader.start-from.version", 0, "What version to start from.")
}

func (c *Config) RegisterFlags(f *flag.FlagSet, log gklog.Logger) {
	c.StartFrom.RegisterFlags(f, log)
	c.DownloadersRing.RegisterFlags(f, log)
	c.Cluster.RegisterFlags("downloader.cluster.", f)
	c.ObjStore.RegisterFlags("downloader.objstore.", f)
	c.Notifier.RegisterFlags("downloader.notifier.", f)

	f.DurationVar(&c.PollingInterval, "downloader.polling-interval", 1*time.Second, "An interval, which downloader will use to monitor new versions to download.")
	f.StringVar(&c.TempDir, "downloader.temp-dir", "", `Directory, that downloader will use as a transshipment point between source server and self object storage.`)

	f.IntVar(&c.BufferSize, "downloader.buffer-size", 4096, `The size of buffer, used to download file from source.`)

	f.DurationVar(&c.Timeout, "downloader.timeout", 30*time.Second, `Timeout for client, polling data from source server.`)
	f.IntVar(&c.RetryMax, "downloader.retry-max", 3, `Retry count for client, polling data from source`)
}

func New(ctx context.Context, cfg Config, reg prometheus.Registerer, log gklog.Logger) (*Downloader, error) {
	cfg.InstanceID = cfg.DownloadersRing.Common.InstanceID
	cfg.TempDirWithID = filepath.Join(cfg.TempDir, cfg.InstanceID)

	log = gklog.With(log, "service", "downloader", "id", cfg.InstanceID)

	writer, err := objstore.NewWriter(cfg.ObjStore, objstore.Bucket)
	if err != nil {
		return nil, errors.Wrap(err, "downloader connect to obj store as writer")
	}

	monitor, err := d_cluster.NewMonitor(ctx, cfg.Cluster, log)
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
		clusterMonitor:        monitor,
		fileTypeDownloading:   cfg.StartFrom.FileType,
	}

	d.Service = services.NewTimerService(
		cfg.PollingInterval, d.start, d.run, nil)

	notifier, err := notifier.New(ctx, cfg.Notifier, cfg.Cluster, cfg.InstanceID, log)
	if err != nil {
		return nil, errors.Wrap(err, "downloader init notifier")
	}

	downloadersRing, downloadersLifecycler, err := newRingAndLifecycler(
		cfg.DownloadersRing, d.healthyInstancesCount, log, reg)
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

func newRingAndLifecycler(ringCfg DownloaderRingConfig, instanceCount *atomic.Uint32, logger gklog.Logger, reg prometheus.Registerer) (*ring.Ring, *ring.BasicLifecycler, error) {
	reg = prometheus.WrapRegistererWithPrefix("charon_", reg)
	rCfg := ringCfg.toRingConfig()
	kvStore, err := kv.NewClient(rCfg.KVStore, ring.GetCodec(), kv.RegistererWithKVName(reg, "downloader-lifecycler"), logger)
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
	delegate = ring.NewLeaveOnStoppingDelegate(delegate, logger)
	delegate = util.NewAutoMarkUnhealthyDelegate(ringAutoMarkUnhealthyPeriods*lifecyclerCfg.HeartbeatTimeout, delegate, logger)

	downloadersLifecycler, err := ring.NewBasicLifecycler(lifecyclerCfg, ringCfg.Common.Key, downloaderRingKey, kvStore, delegate, logger, reg)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to initialize downloaders' lifecycler")
	}

	downloadersRing, err := ring.New(rCfg, ringCfg.Common.Key, downloaderRingKey, logger, reg)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to initialize downloaders' ring client")
	}

	return downloadersRing, downloadersLifecycler, nil
}

func (d *Downloader) start(ctx context.Context) error {
	if err := d.subservices.StartAsync(ctx); err != nil {
		return err
	}
	if err := d.subservices.AwaitHealthy(ctx); err != nil {
		return err
	}
	time.Sleep(beginAfter)

	//Check for accidentally dropped downloads
	_ = level.Debug(d.log).Log("msg", "restoring downloader WAL")
	if err := d.clusterMonitor.Lock(ctx); err != nil {
		_ = level.Error(d.log).Log("msg", err.Error())

		if rbErr := d.clusterMonitor.Unlock(ctx, false); rbErr != nil {
			_ = level.Error(d.log).Log("msg", rbErr.Error())
			return rbErr
		}

		return err
	}

	if d.cfg.StartFrom.FileType == cluster.FileTypeFull {
		rec, found, err := d.clusterMonitor.GetFirstRecord(ctx)
		if err != nil {
			_ = level.Error(d.log).Log("msg", err.Error())

			if rbErr := d.clusterMonitor.Unlock(ctx, false); rbErr != nil {
				_ = level.Error(d.log).Log("msg", rbErr.Error())
				return rbErr
			}

			return err
		}

		if found {
			if rec.Type != cluster.FileTypeFull {
				_ = level.Warn(d.log).Log("msg", "cluster was initially configured to start from diff; picking diff")
			} else {
				_ = level.Warn(d.log).Log("msg", "cluster has already downloaded or downloading full; picking diff")
			}

			d.fileTypeDownloading = cluster.FileTypeDiff
		} else {
			if rbErr := d.clusterMonitor.Unlock(ctx, false); rbErr != nil {
				_ = level.Error(d.log).Log("msg", rbErr.Error())
				return rbErr
			}

			return nil
		}
	}

	recs, err := d.clusterMonitor.GetAllRecords(ctx)
	if err != nil {
		_ = level.Error(d.log).Log("msg", err.Error())

		if rbErr := d.clusterMonitor.Unlock(ctx, false); rbErr != nil {
			_ = level.Error(d.log).Log("msg", rbErr.Error())
			return rbErr
		}

		return err
	}

	lostRec, found := lo.Find(recs, func(item *cl_rec.Record) bool {
		return item.DownloaderID == d.cfg.InstanceID && item.Status == cl_rec.PROCESSING
	})
	if found {
		_ = level.Debug(d.log).Log("msg", "found lost download record")
		d.currRec = lostRec
	}

	if cErr := d.clusterMonitor.Unlock(ctx, true); cErr != nil {
		_ = level.Error(d.log).Log("msg", cErr.Error())
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
				_ = level.Error(d.log).Log("msg", err.Error())
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
			_ = level.Error(d.log).Log("msg", err.Error())
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
		_ = level.Error(d.log).Log("msg", err.Error())
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
		_ = level.Error(d.log).Log("msg", err.Error())
		if err := d.unlockWALWithRollback(ctx); err != nil {
			return err
		}

		if rmErr := os.RemoveAll(versionedDir); rmErr != nil {
			_ = level.Error(d.log).Log("msg", rmErr.Error())
			return nil
		}

		return nil
	}

	if err := d.objStore.Store(ctx, d.currRec.Version, d.currRec.Type, file); err != nil {
		_ = level.Error(d.log).Log("msg", err.Error())
		if err := d.unlockWALWithRollback(ctx); err != nil {
			return err
		}
		return nil
	}
	file.Close()

	if err := os.RemoveAll(versionedDir); err != nil {
		_ = level.Error(d.log).Log("msg", err.Error())
		if err := d.unlockWALWithRollback(ctx); err != nil {
			return err
		}

		return nil
	}

	d.currRec.Status = cl_rec.COMPLETED
	if err := d.clusterMonitor.UpdateRecord(ctx, d.currRec); err != nil {
		_ = level.Error(d.log).Log("msg", err.Error())
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
	_ = level.Debug(d.log).Log("msg", "trying to steal wal record from unhealthy members")

	recs, err := d.clusterMonitor.GetRecordsByStatus(ctx, cl_rec.PROCESSING)
	if err != nil {
		return errors.Wrap(err, "steal wal record")
	}

	for _, rec := range recs {
		if rec.DownloaderID != d.cfg.InstanceID {
			healthy, err := d.IsHealthy(rec.DownloaderID)
			if err != nil {
				return err
			}

			if !healthy {
				if err := d.clusterMonitor.StealRecord(ctx, rec, d.cfg.InstanceID); err != nil {
					return err
				}

				d.currRec = rec
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

	if err := d.clusterMonitor.AddRecord(ctx, rec); err != nil {
		return err
	}
	d.currRec = rec

	return nil
}

func (d *Downloader) getWALRecordToProcess(ctx context.Context) (*cl_rec.Record, error) {
	fInfos, err := d.fiasNalogClient.GetAllDownloadFileInfo(ctx)
	if err != nil {
		return nil, err
	}

	if d.fileTypeDownloading == cluster.FileTypeFull {
		_, found, err := d.clusterMonitor.GetFirstRecord(ctx)
		if err != nil {
			return nil, err
		}

		if !found {
			if len(fInfos) > 0 {
				fInfos = lo.Filter(fInfos, func(item fiasnalog.DownloadFileInfo, index int) bool {
					return item.GARXMLFullURL != ""
				})
				fInfo := fInfos[0]
				return cl_rec.New(fInfo.VersionID, d.cfg.InstanceID, fInfo.GARXMLFullURL, cluster.FileTypeFull), nil
			}

			return nil, errors.New("no file infos available")
		}
	}

	recs, err := d.clusterMonitor.GetAllRecords(ctx)
	if err != nil {
		return nil, err
	}

	lastSentVer := lo.Max(lo.FilterMap(recs, func(item *cl_rec.Record, index int) (int, bool) {
		return item.Version, item.Status == cl_rec.SENT
	}))

	avlRecs := lo.FilterMap(recs, func(item *cl_rec.Record, index int) (int, bool) {
		return item.Version, item.Version > lastSentVer
	})

	avlInfos := lo.FilterMap(fInfos, func(item fiasnalog.DownloadFileInfo, index int) (int, bool) {
		return item.VersionID, item.VersionID > lastSentVer && item.VersionID >= d.cfg.StartFrom.Version && item.GARXMLDeltaURL != ""
	})

	firstAvlVer := lo.Min(lo.Without(avlInfos, avlRecs...))
	if firstAvlVer > 0 {
		info, found := lo.Find(fInfos, func(item fiasnalog.DownloadFileInfo) bool {
			return item.VersionID == firstAvlVer
		})
		if found {
			return cl_rec.New(info.VersionID, d.cfg.InstanceID, info.GARXMLDeltaURL, cluster.FileTypeDiff), nil
		}

		return nil, errors.New(fmt.Sprintf("cannot find download info for version: %d", firstAvlVer))
	}

	return nil, errors.New("no new file infos")
}

func (d *Downloader) lockWAL(ctx context.Context) error {
	if err := d.clusterMonitor.Lock(ctx); err != nil {
		_ = level.Error(d.log).Log("msg", err.Error())
		return err
	}

	return nil
}

func (d *Downloader) unlockWALWithRollback(ctx context.Context) error {
	if err := d.clusterMonitor.Unlock(ctx, false); err != nil {
		_ = level.Error(d.log).Log("msg", err.Error())
		return err
	}

	return nil
}

func (d *Downloader) unlockWALWithCommit(ctx context.Context) error {
	if err := d.clusterMonitor.Unlock(ctx, true); err != nil {
		_ = level.Error(d.log).Log("msg", err.Error())
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
