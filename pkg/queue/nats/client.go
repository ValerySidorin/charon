package nats

import (
	"github.com/go-kit/log"
	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
)

type Config struct {
	Url string `yaml:"url"`
}

type NatsClient struct {
	conn *nats.Conn
	log  log.Logger
}

func NewNatsClient(cfg Config, log log.Logger) (*NatsClient, error) {
	conn, err := nats.Connect(cfg.Url)
	if err != nil {
		return nil, errors.Wrap(err, "initialize nats connection")
	}

	return &NatsClient{
		conn: conn,
		log:  log,
	}, nil
}

func (n *NatsClient) Suscribe(channel string, action func(msg string)) error {
	_, err := n.conn.QueueSubscribe(channel, channel, func(msg *nats.Msg) {
		action(string(msg.Data))
	})
	if err != nil {
		return errors.Wrap(err, "nats subscribe")
	}

	return nil
}

func (n *NatsClient) Publish(channel string, msg string) error {
	if err := n.conn.Publish(channel, []byte(msg)); err != nil {
		return errors.Wrap(err, "nats publish")
	}

	return nil
}
