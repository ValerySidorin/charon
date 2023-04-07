package util

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
)

type CommonRingConfig struct {
	Key              string        `mapstructure:"key"`
	HeartbeatPeriod  time.Duration `mapstructure:"heartbeat_period"`
	HeartbeatTimeout time.Duration `mapstructure:"heartbeat_timeout"`

	InstanceID string `mapstructure:"instance_id"`

	KVStore CommonKVConfig `mapstructure:"kvstore"`
}

func (cfg *CommonRingConfig) RegisterFlags(flagPrefix, kvStorePrefix, defaultRingKey, componentPlural string, f *flag.FlagSet, log log.Logger) {
	hostname, err := os.Hostname()
	if err != nil {
		_ = level.Error(log).Log("msg", "failed to get hostname", "err", err)
		os.Exit(1)
	}

	cfg.KVStore.Store = "memberlist"
	cfg.KVStore.RegisterFlagsWithPrefix(flagPrefix, kvStorePrefix, f)
	f.DurationVar(&cfg.HeartbeatPeriod, flagPrefix+"heartbeat-period", 15*time.Second, "Period at which to heartbeat to the ring. 0 = disabled.")
	f.DurationVar(&cfg.HeartbeatTimeout, flagPrefix+"heartbeat-timeout", time.Minute, fmt.Sprintf("The heartbeat timeout after which %s are considered unhealthy within the ring. 0 = never (timeout disabled).", componentPlural))

	f.StringVar(&cfg.InstanceID, flagPrefix+"instance-id", hostname, "Instance ID to register in the ring.")
	f.StringVar(&cfg.Key, flagPrefix+"key", defaultRingKey, `Key, that well be used by ring.`)
}
