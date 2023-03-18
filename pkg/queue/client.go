package queue

import (
	"errors"

	"github.com/ValerySidorin/charon/pkg/queue/nats"
	"github.com/go-kit/log"
)

type Config struct {
	Type string      `yaml:"type"`
	Nats nats.Config `yaml:"nats"`
}

type Publisher interface {
	Publish(channel string, msg string) error
}

type Suscriber interface {
	Suscribe(channel string, action func(msg string)) error
}

func NewPublisher(cfg Config, log log.Logger) (Publisher, error) {
	switch cfg.Type {
	case "nats":
		return nats.NewNatsClient(cfg.Nats, log)
	default:
		return nil, errors.New("invalid queue type")
	}
}
