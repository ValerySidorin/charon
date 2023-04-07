package processor

import (
	"context"
	"fmt"

	cluster "github.com/ValerySidorin/charon/pkg/cluster/processor"
	"github.com/ValerySidorin/charon/pkg/cluster/processor/record"
	"github.com/ValerySidorin/charon/pkg/objstore"
	"github.com/ValerySidorin/charon/pkg/processor/plugin"
	"github.com/ValerySidorin/charon/pkg/queue/message"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/samber/lo"
)

type persister struct {
	cfg Config
	log log.Logger

	objStore       objstore.Reader
	clusterMonitor *cluster.Monitor
	msgCh          chan *message.Message

	plug plugin.Plugin
}

func newPersister(ctx context.Context, cfg Config, log log.Logger) (*persister, error) {
	reader, err := objstore.NewReader(cfg.ObjStore, objstore.Bucket)
	if err != nil {
		return nil, errors.Wrap(err, "persister: connect to diff store")
	}

	monitor, err := cluster.NewMonitor(ctx, cfg.Cluster, log)
	if err != nil {
		return nil, errors.Wrap(err, "persister: connect to processor wal")
	}

	plug, err := plugin.New(cfg.Plugin, log)
	if err != nil {
		return nil, errors.Wrap(err, "persister: init plugin")
	}

	return &persister{
		cfg:            cfg,
		log:            log,
		objStore:       reader,
		clusterMonitor: monitor,
		msgCh:          make(chan *message.Message, cfg.MsgBuffer),
		plug:           plug,
	}, nil
}

func (p *persister) enqueue(msg *message.Message) {
	p.msgCh <- msg
}

func (p *persister) start(ctx context.Context, callback func()) {
	callback()

	for msg := range p.msgCh {

		tryCount := 0
		processed := false

		for !processed {
			// We should crush if message can not be persisted
			// due to the fact, that all of them MUST be processed
			// sequentially and ordered
			if tryCount > persistMsgTryCount {
				panic("persister: can't persist received message")
			}

			v, err := p.plug.GetVersion(ctx)
			if err != nil {
				_ = level.Error(p.log).Log("msg", err.Error())
				tryCount++
				continue
			}

			if v >= msg.Version {
				break
			}

			objs, err := p.objStore.RetrieveObjNamesByVersion(ctx, msg.Version, msg.Type)
			if err != nil {
				_ = level.Error(p.log).Log("msg", err.Error())
				tryCount++
				continue
			}
			recs := lo.Map(objs, func(item string, index int) *record.Record {
				return record.New(msg.Version, item, msg.Type)
			})
			recs = p.plug.Filter(recs)

			if err := p.clusterMonitor.Lock(ctx); err != nil {
				_ = level.Error(p.log).Log("msg", err.Error())
				if rbErr := p.clusterMonitor.Unlock(ctx, false); rbErr != nil {
					_ = level.Error(p.log).Log("msg", rbErr.Error())
					tryCount++
					continue
				}
			}

			if err := p.clusterMonitor.MergeRecords(ctx, recs); err != nil {
				if rbErr := p.clusterMonitor.Unlock(ctx, false); rbErr != nil {
					_ = level.Error(p.log).Log("msg", rbErr.Error())
					tryCount++
					continue
				}
			}

			if err := p.clusterMonitor.Unlock(ctx, true); err != nil {
				_ = level.Error(p.log).Log("msg", err.Error())
				tryCount++
				continue
			}

			_ = level.Debug(p.log).Log("msg", fmt.Sprintf("persister: successfully persisted message: %s", msg.String()))
			processed = true
		}

		if processed {
			callback()
		}
	}
}
