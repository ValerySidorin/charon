package notifier

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/ValerySidorin/charon/pkg/queue"
	"github.com/ValerySidorin/charon/pkg/queue/message"
	walconfig "github.com/ValerySidorin/charon/pkg/wal/config"
	wal "github.com/ValerySidorin/charon/pkg/wal/downloader"
	walrecord "github.com/ValerySidorin/charon/pkg/wal/downloader/record"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/services"
	"github.com/pkg/errors"
)

const (
	channelName = "charon"
)

type Config struct {
	CheckInterval time.Duration `yaml:"check_interval"`
	Queue         queue.Config  `yaml:"queue"`
}

type Notifier struct {
	services.Service

	cfg Config
	log log.Logger

	downloaderID string

	pub queue.Publisher
	wal *wal.WAL
}

func New(ctx context.Context, cfg Config, walcfg walconfig.Config, downloaderID string, log log.Logger) (*Notifier, error) {
	pub, err := queue.NewPublisher(cfg.Queue, log)
	if err != nil {
		return nil, errors.Wrap(err, "notifier connect to queue")
	}

	wal, err := wal.NewWAL(ctx, walcfg, log)
	if err != nil {
		return nil, errors.Wrap(err, "notifier connect to WAL")
	}

	n := &Notifier{
		cfg:          cfg,
		log:          log,
		downloaderID: downloaderID,
		pub:          pub,
		wal:          wal,
	}

	n.Service = services.NewTimerService(n.cfg.CheckInterval, nil, n.run, nil)

	return n, nil
}

func (n *Notifier) run(ctx context.Context) error {
	hasRecs, err := n.wal.HasCompletedRecords(ctx, n.downloaderID)
	if err != nil {
		level.Error(n.log).Log("msg", err.Error())
		return nil
	}

	if !hasRecs {
		return nil
	}

	if err := n.wal.Lock(ctx); err != nil {
		if rbErr := n.wal.Unlock(ctx, false); rbErr != nil {
			return rbErr
		}
	}

	recs, err := n.wal.GetUnsentRecords(ctx)
	sort.Slice(recs[:], func(i, j int) bool {
		return recs[i].Version < recs[j].Version
	})

	if err != nil {
		level.Error(n.log).Log("msg", err.Error())
		if err := n.wal.Unlock(ctx, false); err != nil {
			return err
		}
		return nil
	}

	for _, rec := range recs {
		if rec.DownloaderID != n.downloaderID || rec.Status != walrecord.COMPLETED {
			if err := n.wal.Unlock(ctx, true); err != nil {
				return err
			}

			return nil
		}

		msg := getMsg(rec)
		if err := n.pub.Pub(channelName, msg); err != nil {
			level.Error(n.log).Log("msg", err.Error())
			if err := n.wal.Unlock(ctx, false); err != nil {
				return err
			}

			return nil
		}
		level.Debug(n.log).Log("msg", fmt.Sprintf("sent message '%s' to channel '%s'", msg, channelName))

		rec.Status = walrecord.SENT
		if err := n.wal.UpdateRecord(ctx, rec); err != nil {
			if err := n.wal.Unlock(ctx, false); err != nil {
				return err
			}

			return nil
		}
	}

	if err := n.wal.Unlock(ctx, true); err != nil {
		return err
	}

	return nil
}

func getMsg(rec *walrecord.Record) *message.Message {
	return &message.Message{
		Version: rec.Version,
		Type:    rec.Type,
	}
}
