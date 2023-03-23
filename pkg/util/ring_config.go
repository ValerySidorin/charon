package util

import (
	"time"

	"github.com/grafana/dskit/kv"
)

type CommonRingConfig struct {
	HeartbeatPeriod  time.Duration `yaml:"heartbeat_period" category:"advanced"`
	HeartbeatTimeout time.Duration `yaml:"heartbeat_timeout" category:"advanced"`

	InstanceID string `yaml:"instance_id" doc:"default=<hostname>" category:"advanced"`

	KVStore kv.Config `yaml:"kvstore"`
}
