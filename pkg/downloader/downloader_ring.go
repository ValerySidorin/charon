package downloader

import (
	"github.com/ValerySidorin/charon/pkg/util"
	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/ring"
)

type DownloaderRingConfig struct {
	util.CommonRingConfig `yaml:",inline"`
}

func (cfg *DownloaderRingConfig) toBasicLifecyclerConfig(logger log.Logger) (ring.BasicLifecyclerConfig, error) {
	return ring.BasicLifecyclerConfig{
		ID:                              cfg.InstanceID,
		HeartbeatPeriod:                 cfg.HeartbeatPeriod,
		HeartbeatTimeout:                cfg.HeartbeatTimeout,
		TokensObservePeriod:             0,
		NumTokens:                       1,
		KeepInstanceInTheRingOnShutdown: false,
	}, nil
}

func (cfg *DownloaderRingConfig) toRingConfig() ring.Config {
	rc := ring.Config{}
	flagext.DefaultValues(&rc)

	rc.KVStore = cfg.KVStore
	rc.HeartbeatTimeout = cfg.HeartbeatTimeout
	rc.ReplicationFactor = 1

	return rc
}
