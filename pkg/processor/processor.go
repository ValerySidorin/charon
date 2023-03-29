package processor

import (
	"context"
	"flag"
	"sort"
	"time"

	"github.com/ValerySidorin/charon/pkg/objstore"
	"github.com/ValerySidorin/charon/pkg/processor/plugin"
	"github.com/ValerySidorin/charon/pkg/queue"
	"github.com/ValerySidorin/charon/pkg/queue/message"
	"github.com/ValerySidorin/charon/pkg/util"
	walcfg "github.com/ValerySidorin/charon/pkg/wal/config"
	wal "github.com/ValerySidorin/charon/pkg/wal/processor"
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
	channelName                  = "charon"
	Bucket                       = "charondiffstore"
	beginAfter                   = 1 * time.Second
	ringAutoMarkUnhealthyPeriods = 10
	persistMsgTryCount           = 5
)

type Config struct {
	InstanceID string `yaml:"-"`

	MsgBuffer int `yaml:"msg_buffer"`

	ProcessorsRing ProcessorRingConfig `yaml:"ring"`
	Queue          queue.Config        `yaml:"queue"`
	ObjStore       objstore.Config     `yaml:"objstore"`
	WAL            walcfg.Config       `yaml:"wal"`
	Plugin         plugin.Config       `yaml:"plugin"`
}

func (c *Config) RegisterFlags(f *flag.FlagSet, log gklog.Logger) {
	c.ProcessorsRing.RegisterFlags(f, log)
	c.Queue.RegisterFlags("processor.queue.", f)
	c.ObjStore.RegisterFlags("processor.objstore.", f)
	c.WAL.RegisterFlags("processor.wal.", f)
	c.Plugin.RegisterFlags("processor.plugin.", f)

	f.IntVar(&c.MsgBuffer, "processor.msg-buffer", 100, `Buffer, which processor will use to cache messages from downloaders.`)
}

type Processor struct {
	services.Service

	cfg Config
	log gklog.Logger

	// Lifetime services
	processorsLifecycler  *ring.BasicLifecycler
	processorsRing        *ring.Ring
	instanceMap           *util.ConcurrentInstanceMap
	healthyInstancesCount *atomic.Uint32
	subservices           *services.Manager

	persister *persister
	importer  *importer

	objStore objstore.Reader
	sub      queue.Subscriber
	wal      *wal.WAL
}

func New(ctx context.Context, cfg Config, reg prometheus.Registerer, log gklog.Logger) (*Processor, error) {
	cfg.InstanceID = cfg.ProcessorsRing.Common.InstanceID
	log = gklog.With(log, "service", "processor", "id", cfg.InstanceID)

	wal, err := wal.NewWAL(ctx, cfg.WAL, log)
	if err != nil {
		return nil, errors.Wrap(err, "processor connect to wal")
	}

	sub, err := queue.NewSubscriber(cfg.Queue, log)
	if err != nil {
		return nil, errors.Wrap(err, "processor init queue sub")
	}

	reader, err := objstore.NewReader(cfg.ObjStore, objstore.Bucket)
	if err != nil {
		return nil, errors.Wrap(err, "processor connect to diff store")
	}

	persister, err := newPersister(ctx, cfg, log)
	if err != nil {
		return nil, errors.Wrap(err, "processor: init persister")
	}

	p := Processor{
		cfg: cfg,
		log: log,

		instanceMap:           util.NewConcurrentInstanceMap(),
		healthyInstancesCount: atomic.NewUint32(0),

		objStore:  reader,
		persister: persister,
		sub:       sub,
		wal:       wal,
	}

	p.Service = services.NewIdleService(p.start, p.stop)

	processorsRing, processorsLifecycler, err := newRingAndLifecycler(
		cfg.ProcessorsRing, p.healthyInstancesCount, p.instanceMap, log, reg)
	if err != nil {
		return nil, err
	}

	manager, err := services.NewManager(processorsRing, processorsLifecycler)
	if err != nil {
		return nil, errors.Wrap(err, "init service manager for processor")
	}

	p.subservices = manager

	importer, err := newImporter(ctx, processorsRing, processorsLifecycler, p.instanceMap, p.healthyInstancesCount, cfg, log)
	if err != nil {
		return nil, errors.Wrap(err, "processor: init importer")
	}

	p.importer = importer

	return &p, nil
}

func newRingAndLifecycler(ringCfg ProcessorRingConfig, instanceCount *atomic.Uint32, instances *util.ConcurrentInstanceMap, logger gklog.Logger, reg prometheus.Registerer) (*ring.Ring, *ring.BasicLifecycler, error) {
	reg = prometheus.WrapRegistererWithPrefix("charon_", reg)
	kvStore, err := kv.NewClient(ringCfg.Common.KVStore, ring.GetCodec(), kv.RegistererWithKVName(reg, "processor-lifecycler"), logger)
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

	processorsLifecycler, err := ring.NewBasicLifecycler(lifecyclerCfg, "processor", ringCfg.Common.Key, kvStore, delegate, logger, reg)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to initialize processors' lifecycler")
	}

	processorsRing, err := ring.New(ringCfg.toRingConfig(), "processor", ringCfg.Common.Key, logger, reg)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to initialize processors' ring client")
	}

	return processorsRing, processorsLifecycler, nil
}

func (p *Processor) start(ctx context.Context) error {
	p.subservices.StartAsync(ctx)
	p.subservices.AwaitHealthy(ctx)
	time.Sleep(beginAfter)

	level.Debug(p.log).Log("msg", "restoring msg queue")
	strMsgs, err := p.objStore.ListVersionsWithTypes(ctx)
	if err != nil {
		return errors.Wrap(err, "list versions")
	}

	lastProcVer, err := p.wal.GetLastVersion(ctx)
	if err != nil {
		return errors.Wrap(err, "get last processing version")
	}

	msgs := lo.FilterMap(strMsgs, func(item string, index int) (*message.Message, bool) {
		msg, _ := message.NewMessage(item)
		return msg, msg.Version > lastProcVer
	})

	sort.Slice(msgs[:], func(i, j int) bool {
		return msgs[i].Version < msgs[j].Version
	})

	for _, m := range msgs {
		p.persister.enqueue(m)
	}

	if err := p.sub.Sub(channelName, func(msg *message.Message) {
		p.persister.enqueue(msg)
	}); err != nil {
		return errors.Wrap(err, "sub")
	}

	go p.persister.start(ctx, func() {
		p.importer.notify(ctx)
	})

	return nil
}

func (p *Processor) stop(err error) error {
	level.Error(p.log).Log("msg", err.Error())
	return nil
}
