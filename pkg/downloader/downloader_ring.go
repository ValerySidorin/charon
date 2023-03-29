package downloader

import (
	"flag"

	"github.com/ValerySidorin/charon/pkg/util"
	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/ring"
)

type DownloaderRingConfig struct {
	Common util.CommonRingConfig `yaml:",inline"`
}

func (c *DownloaderRingConfig) RegisterFlags(f *flag.FlagSet, log log.Logger) {
	c.Common.RegisterFlags("downloader.ring.", "charon/", "downloader", "downloaders", f, log)
}

func (cfg *DownloaderRingConfig) toBasicLifecyclerConfig(logger log.Logger) (ring.BasicLifecyclerConfig, error) {
	return ring.BasicLifecyclerConfig{
		ID:                              cfg.Common.InstanceID,
		HeartbeatPeriod:                 cfg.Common.HeartbeatPeriod,
		HeartbeatTimeout:                cfg.Common.HeartbeatTimeout,
		TokensObservePeriod:             0,
		NumTokens:                       1,
		KeepInstanceInTheRingOnShutdown: false,
	}, nil
}

func (cfg *DownloaderRingConfig) toRingConfig() ring.Config {
	rc := ring.Config{}
	flagext.DefaultValues(&rc)

	rc.KVStore = cfg.Common.KVStore
	rc.HeartbeatTimeout = cfg.Common.HeartbeatTimeout
	rc.ReplicationFactor = 1

	return rc
}
