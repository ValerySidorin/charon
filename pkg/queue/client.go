package queue

import (
	"flag"

	"github.com/ValerySidorin/charon/pkg/queue/message"
	"github.com/ValerySidorin/charon/pkg/queue/nats"
	"github.com/go-kit/log"
	"github.com/pkg/errors"
)

type Config struct {
	Type string      `mapstructure:"type"`
	Nats nats.Config `mapstructure:"nats"`
}

func (c *Config) RegisterFlags(flagPrefix string, f *flag.FlagSet) {
	f.StringVar(&c.Type, flagPrefix+"type", "nats", `Queue, that will be used for downloader-processor communication.`)
	c.Nats.RegisterFlags(flagPrefix, f)
}

type Publisher interface {
	Pub(channel string, msg *message.Message) error
}

type Subscriber interface {
	Sub(channel string, action func(msg *message.Message)) error
}

func NewPublisher(cfg Config, log log.Logger) (Publisher, error) {
	switch cfg.Type {
	case "nats":
		return nats.NewNatsClient(cfg.Nats, log)
	default:
		return nil, errors.New("invalid queue type")
	}
}

func NewSubscriber(cfg Config, log log.Logger) (Subscriber, error) {
	switch cfg.Type {
	case "nats":
		return nats.NewNatsClient(cfg.Nats, log)
	default:
		return nil, errors.New("invalid queue type")
	}
}
