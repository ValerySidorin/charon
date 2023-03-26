package impl

import (
	"context"
	"errors"
	"io"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
)

const (
	fail = false
)

type MockPluginConfig struct {
}

func NewMockPluginConfig(map[string]interface{}) MockPluginConfig {
	return MockPluginConfig{}
}

type MockPlugin struct {
	cfg MockPluginConfig
	log log.Logger
}

func NewMockPlugin(cfg MockPluginConfig, log log.Logger) *MockPlugin {
	return &MockPlugin{
		cfg: cfg,
		log: log,
	}
}

func (m *MockPlugin) Exec(ctx context.Context, stream io.Reader) error {
	level.Debug(m.log).Log("msg", "mock plugin start")
	if fail {
		return errors.New("mock plugin")
	}
	level.Debug(m.log).Log("msg", "mock plugin stop")
	return nil
}
