package charon

import (
	"context"

	"github.com/ValerySidorin/charon/pkg/downloader"
	"github.com/ValerySidorin/charon/pkg/processor"
	util_log "github.com/ValerySidorin/charon/pkg/util/log"
	"github.com/grafana/dskit/modules"
	"github.com/grafana/dskit/services"
)

const (
	Downloader = "downloader"
	Processor  = "processor"
	All        = "all"
)

func (c *Charon) initDownloader() (services.Service, error) {
	var err error
	c.Downloader, err = downloader.New(context.Background(), c.Cfg.Downloader, c.Registerer, util_log.Logger)
	if err != nil {
		return nil, err
	}

	return c.Downloader, nil
}

func (c *Charon) initProcessor() (services.Service, error) {
	var err error
	c.Processor, err = processor.New(context.Background(), c.Cfg.Processor, c.Registerer, util_log.Logger)
	if err != nil {
		return nil, err
	}

	return c.Processor, nil
}

func (c *Charon) setupModuleManager() error {
	mm := modules.NewManager(util_log.Logger)

	mm.RegisterModule(Downloader, c.initDownloader, modules.UserInvisibleTargetableModule)
	mm.RegisterModule(Processor, c.initProcessor, modules.UserInvisibleTargetableModule)
	mm.RegisterModule(All, nil)

	deps := map[string][]string{
		All: {Downloader, Processor},
	}
	for mod, targets := range deps {
		if err := mm.AddDependency(mod, targets...); err != nil {
			return err
		}
	}

	c.ModuleManager = mm
	return nil
}
