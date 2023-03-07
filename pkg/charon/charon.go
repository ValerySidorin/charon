package charon

import (
	"github.com/grafana/dskit/modules"
	"github.com/grafana/dskit/services"
)

type Charon struct {
	Cfg Config

	// set during initialization
	ServiceMap    map[string]services.Service
	ModuleManager *modules.Manager
}

type Config struct {
}
