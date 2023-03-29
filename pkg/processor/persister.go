package processor

import (
	"context"
	"fmt"

	"github.com/ValerySidorin/charon/pkg/objstore"
	"github.com/ValerySidorin/charon/pkg/processor/plugin"
	"github.com/ValerySidorin/charon/pkg/queue/message"
	wal "github.com/ValerySidorin/charon/pkg/wal/processor"
	"github.com/ValerySidorin/charon/pkg/wal/processor/record"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/samber/lo"
)

type persister struct {
	cfg Config
	log log.Logger

	objStore objstore.Reader
	wal      *wal.WAL
	msgCh    chan *message.Message

	plug plugin.Plugin
}

func newPersister(ctx context.Context, cfg Config, log log.Logger) (*persister, error) {
	reader, err := objstore.NewReader(cfg.ObjStore, objstore.Bucket)
	if err != nil {
		return nil, errors.Wrap(err, "persister: connect to diff store")
	}

	wal, err := wal.NewWAL(ctx, cfg.WAL, log)
	if err != nil {
		return nil, errors.Wrap(err, "persister: connect to processor wal")
	}

	plug, err := plugin.New(cfg.Plugin, log)
	if err != nil {
		return nil, errors.Wrap(err, "persister: init plugin")
	}

	return &persister{
		cfg:      cfg,
		log:      log,
		objStore: reader,
		wal:      wal,
		msgCh:    make(chan *message.Message, cfg.MsgBuffer),
		plug:     plug,
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
			objs, err := p.objStore.RetrieveObjNamesByVersion(ctx, msg.Version, msg.Type)
			if err != nil {
				level.Error(p.log).Log("msg", err.Error())
				tryCount++
				continue
			}
			recs := lo.Map(objs, func(item string, index int) *record.Record {
				return record.New(msg.Version, item, msg.Type)
			})
			recs = p.plug.Filter(recs)

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

			level.Debug(p.log).Log("msg", fmt.Sprintf("persister: successfully persisted message: %s", msg.String()))
			processed = true
		}

		callback()
	}
}
