package nats

import (
	"flag"

	"github.com/ValerySidorin/charon/pkg/queue/message"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
)

type Config struct {
	Url string `yaml:"url"`
}

func (c *Config) RegisterFlags(flagPrefix string, f *flag.FlagSet) {
	f.StringVar(&c.Url, flagPrefix+"nats.url", "", `NATS URL`)
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

func (n *NatsClient) Sub(channel string, f func(msg *message.Message)) error {
	_, err := n.conn.Subscribe("charon", func(msg *nats.Msg) {
		mes, err := message.NewMessage(string(msg.Data))
		if err != nil {
			_ = level.Error(n.log).Log("msg", err.Error())
			return
		}

		f(mes)
	})
	if err != nil {
		return errors.Wrap(err, "nats subscribe")
	}

	return nil
}

func (n *NatsClient) Pub(channel string, msg *message.Message) error {
	if err := n.conn.Publish(channel, []byte(msg.String())); err != nil {
		return errors.Wrap(err, "nats publish")
	}

	return nil
}
