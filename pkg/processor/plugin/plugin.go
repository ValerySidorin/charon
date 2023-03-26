package plugin

import (
	"context"
	"io"

	"github.com/ValerySidorin/charon/pkg/processor/plugin/impl"
	"github.com/go-kit/log"
)

type Config struct {
	CustomConfig map[string]interface{} `yaml:",inline"`
}

type Plugin interface {
	Exec(ctx context.Context, stream io.Reader) error
}

func New(cfg Config, log log.Logger) (Plugin, error) {
	plugCfg := impl.NewMockPluginConfig(cfg.CustomConfig)
	plug := impl.NewMockPlugin(plugCfg, log)
	return plug, nil
}
