package charon

import (
	"context"
	"errors"
	"flag"
	"fmt"

	"github.com/ValerySidorin/charon/pkg/downloader"
	"github.com/ValerySidorin/charon/pkg/processor"
	util_log "github.com/ValerySidorin/charon/pkg/util/log"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/modules"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaveworks/common/signals"
	"go.uber.org/atomic"
)

type Config struct {
	Target          flagext.StringSliceCSV `yaml:"target"`
	ApplicationName string                 `yaml:"-"`

	Downloader downloader.Config `yaml:"downloader"`
	Processor  processor.Config  `yaml:"processor"`
	Log        util_log.Config   `yaml:"log"`
}

func newDefaultConfig() *Config {
	defaultConfig := &Config{}
	defaultFS := flag.NewFlagSet("", flag.PanicOnError)
	defaultConfig.RegisterFlags(defaultFS, util_log.Logger)
	return defaultConfig
}

func (c *Config) RegisterFlags(f *flag.FlagSet, log log.Logger) {
	c.ApplicationName = "Charon"
	c.Target = []string{All}

	f.Var(&c.Target, "target", "Comma-separated list of components to include in the instantiated process. "+
		"The default value 'all' includes all components that are required to form a functional Charon instance in single-binary mode. "+
		"Valid targets: [all, downloader, processor].")

	c.Downloader.RegisterFlags(f, log)
	c.Processor.RegisterFlags(f, log)
	c.Log.RegisterFlags(f)
}

type Charon struct {
	Cfg        Config
	Registerer prometheus.Registerer

	ServiceMap    map[string]services.Service
	ModuleManager *modules.Manager

	Downloader *downloader.Downloader
	Processor  *processor.Processor
}

func New(cfg Config, reg prometheus.Registerer) (*Charon, error) {
	charon := &Charon{
		Cfg:        cfg,
		Registerer: reg,
	}

	if err := charon.setupModuleManager(); err != nil {
		return nil, err
	}

	return charon, nil
}

func (c *Charon) Run() error {
	for _, module := range c.Cfg.Target {
		if !c.ModuleManager.IsTargetableModule(module) {
			return fmt.Errorf("selected target (%s) is an internal module, which is not allowed", module)
		}
	}

	var err error
	c.ServiceMap, err = c.ModuleManager.InitModuleServices(c.Cfg.Target...)
	if err != nil {
		return err
	}

	servs := []services.Service(nil)
	for _, s := range c.ServiceMap {
		servs = append(servs, s)
	}

	sm, err := services.NewManager(servs...)
	if err != nil {
		return err
	}

	shutdownRequested := atomic.NewBool(false)

	healthy := func() { level.Info(util_log.Logger).Log("msg", "Application started") }
	stopped := func() { level.Info(util_log.Logger).Log("msg", "Application stopped") }
	serviceFailed := func(service services.Service) {
		sm.StopAsync()

		for m, s := range c.ServiceMap {
			if s == service {
				if errors.Is(service.FailureCase(), modules.ErrStopProcess) {
					level.Info(util_log.Logger).Log("msg", "received stop signal via return error", "module", m, "err", service.FailureCase())
				} else {
					level.Error(util_log.Logger).Log("msg", "module failed", "module", m, "err", service.FailureCase())
				}
				return
			}
		}

		level.Error(util_log.Logger).Log("msg", "module failed", "module", "unknown", "err", service.FailureCase())
	}

	sm.AddListener(services.NewManagerListener(healthy, stopped, serviceFailed))

	handler := signals.NewHandler(c.Cfg.Log.Log)
	go func() {
		handler.Loop()

		shutdownRequested.Store(true)

		sm.StopAsync()
	}()

	err = sm.StartAsync(context.Background())
	if err == nil {
		err = sm.AwaitStopped(context.Background())
	}

	if err == nil {
		if failed := sm.ServicesByState()[services.Failed]; len(failed) > 0 {
			for _, f := range failed {
				if !errors.Is(f.FailureCase(), modules.ErrStopProcess) {
					err = errors.New("failed services")
					break
				}
			}
		}
	}

	return err
}
