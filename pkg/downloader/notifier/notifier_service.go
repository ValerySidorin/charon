package notifier

import (
	"context"
	"flag"
	"fmt"
	"sort"
	"time"

	cl_cfg "github.com/ValerySidorin/charon/pkg/cluster/config"
	d_cl "github.com/ValerySidorin/charon/pkg/cluster/downloader"
	cl_rec "github.com/ValerySidorin/charon/pkg/cluster/downloader/record"
	"github.com/ValerySidorin/charon/pkg/queue"
	"github.com/ValerySidorin/charon/pkg/queue/message"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/services"
	"github.com/pkg/errors"
)

const (
	channelName = "charon"
)

type Config struct {
	CheckInterval time.Duration `mapstructure:"check_interval"`
	Queue         queue.Config  `mapstructure:"queue"`
}

func (c *Config) RegisterFlags(flagPrefix string, f *flag.FlagSet) {
	f.DurationVar(&c.CheckInterval, flagPrefix+"check-interval", 5*time.Second, `Notifier check interval, used to monitor completed downloads.`)
	c.Queue.RegisterFlags(flagPrefix+"queue.", f)
}

type Notifier struct {
	services.Service

	cfg Config
	log log.Logger

	downloaderID string

	pub            queue.Publisher
	clusterMonitor *d_cl.Monitor
}

func New(ctx context.Context, cfg Config, clusterCfg cl_cfg.Config, downloaderID string, log log.Logger) (*Notifier, error) {
	pub, err := queue.NewPublisher(cfg.Queue, log)
	if err != nil {
		return nil, errors.Wrap(err, "notifier connect to queue")
	}

	monitor, err := d_cl.NewMonitor(ctx, clusterCfg, log)
	if err != nil {
		return nil, errors.Wrap(err, "notifier connect to WAL")
	}

	n := &Notifier{
		cfg:            cfg,
		log:            log,
		downloaderID:   downloaderID,
		pub:            pub,
		clusterMonitor: monitor,
	}

	n.Service = services.NewTimerService(n.cfg.CheckInterval, nil, n.run, nil)

	return n, nil
}

func (n *Notifier) run(ctx context.Context) error {
	hasRecs, err := n.clusterMonitor.HasCompletedRecords(ctx, n.downloaderID)
	if err != nil {
		_ = level.Error(n.log).Log("msg", err.Error())
		return nil
	}

	if !hasRecs {
		return nil
	}

	if err := n.clusterMonitor.Lock(ctx); err != nil {
		if rbErr := n.clusterMonitor.Unlock(ctx, false); rbErr != nil {
			return rbErr
		}
	}

	recs, err := n.clusterMonitor.GetUnsentRecords(ctx)
	sort.Slice(recs[:], func(i, j int) bool {
		return recs[i].Version < recs[j].Version
	})

	if err != nil {
		_ = level.Error(n.log).Log("msg", err.Error())
		if err := n.clusterMonitor.Unlock(ctx, false); err != nil {
			return err
		}
		return nil
	}

	for _, rec := range recs {
		if rec.DownloaderID != n.downloaderID || rec.Status != cl_rec.COMPLETED {
			if err := n.clusterMonitor.Unlock(ctx, true); err != nil {
				return err
			}

			return nil
		}

		msg := getMsg(rec)
		if err := n.pub.Pub(channelName, msg); err != nil {
			_ = level.Error(n.log).Log("msg", err.Error())
			if err := n.clusterMonitor.Unlock(ctx, false); err != nil {
				return err
			}

			return nil
		}
		_ = level.Debug(n.log).Log("msg", fmt.Sprintf("sent message '%s' to channel '%s'", msg, channelName))

		rec.Status = cl_rec.SENT
		if err := n.clusterMonitor.UpdateRecord(ctx, rec); err != nil {
			if err := n.clusterMonitor.Unlock(ctx, false); err != nil {
				return err
			}

			return nil
		}
	}

	if err := n.clusterMonitor.Unlock(ctx, true); err != nil {
		return err
	}

	return nil
}

func getMsg(rec *cl_rec.Record) *message.Message {
	return &message.Message{
		Version: rec.Version,
		Type:    rec.Type,
	}
}
