package queue

import "github.com/ValerySidorin/charon/pkg/queue/nats"

type Config struct {
	Type string      `yaml:"type"`
	Nats nats.Config `yaml:"nats"`
}

type Publisher interface {
	Post(channel string, msg string)
}

type Suscriber interface {
	Suscribe(channel string)
}
