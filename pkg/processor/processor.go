package processor

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/ValerySidorin/charon/pkg/diffstore"
	"github.com/ValerySidorin/charon/pkg/queue"
	"github.com/ValerySidorin/charon/pkg/queue/message"
	"github.com/ValerySidorin/charon/pkg/util"
	walcfg "github.com/ValerySidorin/charon/pkg/wal/config"
	wal "github.com/ValerySidorin/charon/pkg/wal/processor"
	"github.com/ValerySidorin/charon/pkg/wal/processor/record"
	gklog "github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/samber/lo"
	"github.com/sourcegraph/conc/pool"
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

	WorkerPool int `yaml:"worker_pool"`
	MsgBuffer  int `yaml:"msg_buffer"`

	ProcessorsRing ProcessorRingConfig `yaml:"ring"`
	Queue          queue.Config        `yaml:"queue"`
	DiffStore      diffstore.Config    `yaml:"diff_store"`
	WAL            walcfg.Config       `yaml:"wal"`
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

	workerPool *pool.Pool
	msgCh      chan *message.Message

	diffStoreReader diffstore.Reader
	sub             queue.Subscriber
	wal             *wal.WAL
}

func New(ctx context.Context, cfg Config, reg prometheus.Registerer, log gklog.Logger) (*Processor, error) {
	cfg.InstanceID = cfg.ProcessorsRing.InstanceID
	log = gklog.With(log, "service", "processor", "id", cfg.InstanceID, "ring_key", cfg.ProcessorsRing.Key)

	wal, err := wal.NewWAL(ctx, cfg.WAL, log)
	if err != nil {
		return nil, errors.Wrap(err, "processor connect to wal")
	}

	sub, err := queue.NewSubscriber(cfg.Queue, log)
	if err != nil {
		return nil, errors.Wrap(err, "processor init queue sub")
	}

	reader, err := diffstore.NewReader(cfg.DiffStore, diffstore.Bucket)
	if err != nil {
		return nil, errors.Wrap(err, "processor connect to diff store")
	}

	p := Processor{
		cfg: cfg,
		log: log,

		instanceMap:           util.NewConcurrentInstanceMap(),
		healthyInstancesCount: atomic.NewUint32(0),
		workerPool:            pool.New().WithMaxGoroutines(cfg.WorkerPool),

		diffStoreReader: reader,
		msgCh:           make(chan *message.Message, cfg.MsgBuffer),
		sub:             sub,
		wal:             wal,
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
	p.processorsLifecycler = processorsLifecycler
	p.processorsRing = processorsRing

	return &p, nil
}

func newRingAndLifecycler(ringCfg ProcessorRingConfig, instanceCount *atomic.Uint32, instances *util.ConcurrentInstanceMap, logger gklog.Logger, reg prometheus.Registerer) (*ring.Ring, *ring.BasicLifecycler, error) {
	reg = prometheus.WrapRegistererWithPrefix("charon_", reg)
	kvStore, err := kv.NewClient(ringCfg.KVStore, ring.GetCodec(), kv.RegistererWithKVName(reg, "processor-lifecycler"), logger)
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

	processorsLifecycler, err := ring.NewBasicLifecycler(lifecyclerCfg, "processor", ringCfg.Key, kvStore, delegate, logger, reg)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to initialize processors' lifecycler")
	}

	processorsRing, err := ring.New(ringCfg.toRingConfig(), "processor", ringCfg.Key, logger, reg)
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
	strMsgs, err := p.diffStoreReader.ListVersionsWithTypes(ctx)
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
		p.msgCh <- m
	}

	if err := p.sub.Sub(channelName, func(msg *message.Message) {
		p.msgCh <- msg
	}); err != nil {
		return errors.Wrap(err, "sub")
	}

	go p.persistMsgsToWAL(ctx)

	return nil
}

func (p *Processor) stop(err error) error {
	level.Error(p.log).Log("msg", err.Error())
	return nil
}

func (p *Processor) persistMsgsToWAL(ctx context.Context) {
	for msg := range p.msgCh {

		tryCount := 0
		processed := false

		for !processed {
			// We should crush if message can not be persisted
			// due to the fact, that all of them MUST be processed
			// sequentially and ordered
			if tryCount > persistMsgTryCount {
				panic("can't persist received message")
			}
			objs, err := p.diffStoreReader.RetrieveObjNamesByVersion(ctx, msg.Version, msg.Type)
			if err != nil {
				level.Error(p.log).Log("msg", err.Error())
				tryCount++
				continue
			}
			recs := lo.Map(objs, func(item string, index int) *record.Record {
				return record.New(msg.Version, item, msg.Type)
			})

			if err := p.wal.Lock(ctx); err != nil {
				level.Error(p.log).Log("msg", err.Error())
				if rbErr := p.wal.Unlock(ctx, false); rbErr != nil {
					level.Error(p.log).Log("msg", rbErr.Error())
					tryCount++
					continue
				}
			}

			if err := p.wal.MergeRecords(ctx, recs); err != nil {
				if rbErr := p.wal.Unlock(ctx, false); rbErr != nil {
					level.Error(p.log).Log("msg", rbErr.Error())
					tryCount++
					continue
				}
			}

			if err := p.wal.Unlock(ctx, true); err != nil {
				level.Error(p.log).Log("msg", err.Error())
				tryCount++
				continue
			}

			level.Debug(p.log).Log("msg", fmt.Sprintf("successfully persisted message: %s", msg.String()))
			processed = true
		}
	}
}
