package nats

import (
	"log"

	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
)

type Config struct {
	Url string `yaml:"url"`
}

type NatsClient struct {
	nc *nats.Conn
}

func NewNatsClient() (*NatsClient, error) {
	conn, err := nats.Connect("localhost")
	if err != nil {
		return nil, errors.Wrap(err, "initialize nats connection")
	}

	return &NatsClient{
		nc: conn,
	}, nil
}

func (n *NatsClient) Suscribe(channel string) {
	n.nc.QueueSubscribe(channel, channel, func(msg *nats.Msg) {
		log.Printf("received message: %s", msg.Data)
	})
}

func (n *NatsClient) Publish(channel string, msg string) {
	n.nc.Publish(channel, []byte(msg))
}
