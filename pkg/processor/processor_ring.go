package processor

import (
	"flag"

	"github.com/ValerySidorin/charon/pkg/util"
	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/ring"
)

type ProcessorRingConfig struct {
	Common util.CommonRingConfig `yaml:",inline"`
}

func (c *ProcessorRingConfig) RegisterFlags(f *flag.FlagSet, log log.Logger) {
	c.Common.RegisterFlags("processor.ring.", "charon/", "processor", "processors", f, log)
}

func (cfg *ProcessorRingConfig) toBasicLifecyclerConfig(logger log.Logger) (ring.BasicLifecyclerConfig, error) {
	return ring.BasicLifecyclerConfig{
		ID:                              cfg.Common.InstanceID,
		HeartbeatPeriod:                 cfg.Common.HeartbeatPeriod,
		HeartbeatTimeout:                cfg.Common.HeartbeatTimeout,
		TokensObservePeriod:             0,
		NumTokens:                       1,
		KeepInstanceInTheRingOnShutdown: false,
	}, nil
}

func (cfg *ProcessorRingConfig) toRingConfig() ring.Config {
	rc := ring.Config{}
	flagext.DefaultValues(&rc)

	rc.KVStore = cfg.Common.KVStore
	rc.HeartbeatTimeout = cfg.Common.HeartbeatTimeout
	rc.ReplicationFactor = 1

	return rc
}
