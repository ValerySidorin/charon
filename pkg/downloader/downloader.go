package downloader

import (
	"context"
	"strconv"
	"time"

	"github.com/ValerySidorin/charon/pkg/diffstore"
	"github.com/ValerySidorin/charon/pkg/downloader/wal"
	"github.com/ValerySidorin/charon/pkg/fiasnalog"
	"github.com/ValerySidorin/charon/pkg/queue"
	gklog "github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/atomic"
)

const (
	Bucket            = "charon_diffstore"
	downloaderRingKey = "downloader"

	waitBeforeStartPeriod = 1 * time.Second
)

type Downloader struct {
	services.Service

	cfg Config
	log gklog.Logger

	downloadersLifecycler *ring.BasicLifecycler
	downloadersRing       *ring.Ring
	instanceMap           *concurrentInstanceMap
	healthyInstancesCount *atomic.Uint32
	subservices           *services.Manager

	diffStoreWriter        diffstore.Writer
	fiasNalogClient        *fiasnalog.Client
	walClient              *wal.Client
	link                   string
	currDownloadingVersion int
}

type Config struct {
	StartingVersion int32                `yaml:"starting_version"`
	PollingInterval time.Duration        `yaml:"polling_interval"`
	DownloadersRing DownloaderRingConfig `yaml:"ring"`
	Kv              kv.Config            `yaml:"kvstore"`
	Queue           queue.Config         `yaml:"queue"`
	DiffStore       diffstore.Config     `yaml:"diffstore"`
	FiasNalogClient fiasnalog.Config     `yaml:"fias_nalog_client"`
}

func New(cfg Config, reg prometheus.Registerer, log gklog.Logger) (*Downloader, error) {
	//writer, err := diffstore.NewWriter(cfg.DiffStore, Bucket)
	//if err != nil {
	//	return nil, errors.Wrap(err, "init diffstore writer for downloader")
	//}

	walClient, err := wal.NewClient(cfg.Kv, reg, log)
	if err != nil {
		return nil, errors.Wrap(err, "init wal client for downloader")
	}

	d := &Downloader{
		cfg: cfg,
		//diffStoreWriter: writer,
		log:                   log,
		fiasNalogClient:       fiasnalog.NewClient(cfg.FiasNalogClient),
		healthyInstancesCount: atomic.NewUint32(0),
		instanceMap:           newConcurrentInstanceMap(),
		walClient:             walClient,
	}

	d.Service = services.NewTimerService(
		cfg.PollingInterval, d.start, d.run, d.stop)

	downloadersRing, downloadersLifecycler, err := newRingAndLifecycler(
		cfg.DownloadersRing, cfg.Kv, d.healthyInstancesCount, d.instanceMap, log, reg)
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

func newRingAndLifecycler(ringCfg DownloaderRingConfig, kvCfg kv.Config, instanceCount *atomic.Uint32, instances *concurrentInstanceMap, logger gklog.Logger, reg prometheus.Registerer) (*ring.Ring, *ring.BasicLifecycler, error) {
	reg = prometheus.WrapRegistererWithPrefix("charon_", reg)
	kvCfg.Prefix += "ring/"
	kvStore, err := kv.NewClient(kvCfg, ring.GetCodec(), kv.RegistererWithKVName(reg, "downloader-lifecycler"), logger)
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
	//delegate = ring.NewAutoForgetDelegate(ringAutoForgetUnhealthyPeriods*cfg.HeartbeatTimeout, delegate, logger)

	distributorsLifecycler, err := ring.NewBasicLifecycler(lifecyclerCfg, "downloader", downloaderRingKey, kvStore, delegate, logger, reg)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to initialize downloaders' lifecycler")
	}

	distributorsRing, err := ring.New(ringCfg.toRingConfig(kvCfg), "downloader", downloaderRingKey, logger, reg)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to initialize downloaders' ring client")
	}

	return distributorsRing, distributorsLifecycler, nil
}

func (d *Downloader) start(ctx context.Context) error {
	d.subservices.StartAsync(ctx)
	d.subservices.AwaitHealthy(ctx)
	time.Sleep(waitBeforeStartPeriod)

	id := d.downloadersLifecycler.GetInstanceID()
	d.log = gklog.With(d.log, "service", "downloader", "id", id)

	//Check for accidentally dropped downloads
	level.Debug(d.log).Log("msg", "restoring downloader WAL")
	v, err := d.walClient.GetVersionByDownloader(ctx, id)
	if err != nil {
		return err
	}

	if v != "" {
		intV, err := strconv.Atoi(v)
		if err != nil {
			return err
		}

		d.currDownloadingVersion = intV
	} else {
		level.Debug(d.log).Log("msg", "no stale records in WAL")
	}

	return nil
}

func (d *Downloader) stop(_ error) error {
	level.Debug(d.log).Log("msg", "finishing download")
	return nil
}

func (d *Downloader) run(ctx context.Context) error {
	//First try to steal stale downloads from other downloaders
	id := d.downloadersLifecycler.GetInstanceID()
	if int(d.HealthyInstancesCount()) < d.downloadersRing.InstancesCount() {
		for _, dID := range d.instanceMap.Keys() {
			if dID != id {
				_, ok := d.instanceMap.Get(dID)
				if ok {
					healthy, err := d.IsHealthy(dID)
					if err != nil {
						return err
					}

					if !healthy {
						v, err := d.walClient.GetVersionByDownloader(ctx, dID)
						if err != nil {
							return err
						}

						if v != "" {
							if err := d.walClient.StealRecord(ctx, v, id); err != nil {
								return err
							}

							intV, err := strconv.Atoi(v)
							if err != nil {
								return err
							}

							d.currDownloadingVersion = intV
						}
					}
				}
			}
		}
	}

	if d.currDownloadingVersion == 0 {

	}

	return nil
}

func (d *Downloader) resolveStartVersion(ctx context.Context) (int32, error) {
	cfgVersion := d.cfg.StartingVersion
	currVersion, err := d.diffStoreWriter.GetLatestVersion(ctx)
	if err != nil {
		return 0, errors.Wrap(err, "resolve start version")
	}

	if currVersion >= cfgVersion {
		return currVersion, nil
	} else {
		return cfgVersion - 1, nil
	}
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
