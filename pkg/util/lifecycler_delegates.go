package util

import (
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/ring"
	"go.uber.org/atomic"
)

// Healthy instance delegate stuff
type HealthyInstanceDelegate struct {
	count            *atomic.Uint32
	heartbeatTimeout time.Duration
	next             ring.BasicLifecyclerDelegate
}

func NewHealthyInstanceDelegate(count *atomic.Uint32, heartbeatTimeout time.Duration, next ring.BasicLifecyclerDelegate) *HealthyInstanceDelegate {
	return &HealthyInstanceDelegate{count: count, heartbeatTimeout: heartbeatTimeout, next: next}
}

func (d *HealthyInstanceDelegate) OnRingInstanceRegister(lifecycler *ring.BasicLifecycler, ringDesc ring.Desc, instanceExists bool, instanceID string, instanceDesc ring.InstanceDesc) (ring.InstanceState, ring.Tokens) {
	return d.next.OnRingInstanceRegister(lifecycler, ringDesc, instanceExists, instanceID, instanceDesc)
}

func (d *HealthyInstanceDelegate) OnRingInstanceTokens(lifecycler *ring.BasicLifecycler, tokens ring.Tokens) {
	d.next.OnRingInstanceTokens(lifecycler, tokens)
}

func (d *HealthyInstanceDelegate) OnRingInstanceStopping(lifecycler *ring.BasicLifecycler) {
	d.next.OnRingInstanceStopping(lifecycler)
}

func (d *HealthyInstanceDelegate) OnRingInstanceHeartbeat(lifecycler *ring.BasicLifecycler, ringDesc *ring.Desc, instanceDesc *ring.InstanceDesc) {
	activeMembers := uint32(0)
	now := time.Now()

	for _, instance := range ringDesc.Ingesters {
		if ring.ACTIVE == instance.State && instance.IsHeartbeatHealthy(d.heartbeatTimeout, now) {
			activeMembers++
		}
	}

	d.count.Store(activeMembers)
	d.next.OnRingInstanceHeartbeat(lifecycler, ringDesc, instanceDesc)
}

// Auto mark unhealthy delegate stuff
type AutoMarkUnhealthyDelegate struct {
	next         ring.BasicLifecyclerDelegate
	forgetPeriod time.Duration
	log          log.Logger
}

func NewAutoMarkUnhealthyDelegate(forgetPeriod time.Duration, next ring.BasicLifecyclerDelegate, log log.Logger) *AutoMarkUnhealthyDelegate {
	return &AutoMarkUnhealthyDelegate{
		next:         next,
		log:          log,
		forgetPeriod: forgetPeriod,
	}
}

func (d *AutoMarkUnhealthyDelegate) OnRingInstanceRegister(lifecycler *ring.BasicLifecycler, ringDesc ring.Desc, instanceExists bool, instanceID string, instanceDesc ring.InstanceDesc) (ring.InstanceState, ring.Tokens) {
	return d.next.OnRingInstanceRegister(lifecycler, ringDesc, instanceExists, instanceID, instanceDesc)
}

func (d *AutoMarkUnhealthyDelegate) OnRingInstanceTokens(lifecycler *ring.BasicLifecycler, tokens ring.Tokens) {
	d.next.OnRingInstanceTokens(lifecycler, tokens)
}

func (d *AutoMarkUnhealthyDelegate) OnRingInstanceStopping(lifecycler *ring.BasicLifecycler) {
	d.next.OnRingInstanceStopping(lifecycler)
}

func (d *AutoMarkUnhealthyDelegate) OnRingInstanceHeartbeat(lifecycler *ring.BasicLifecycler, ringDesc *ring.Desc, instanceDesc *ring.InstanceDesc) {
	for id, instance := range ringDesc.Ingesters {
		if instance.State != ring.LEFT {
			lastHeartbeat := time.Unix(instance.GetTimestamp(), 0)

			if time.Since(lastHeartbeat) > d.forgetPeriod {
				_ = level.Warn(d.log).Log("msg", "marking instance as unhealthy because it is not heartbeating for a long time", "instance", id, "last_heartbeat", lastHeartbeat.String(), "forget_period", d.forgetPeriod)
				ringDesc.RemoveIngester(id)
				ringDesc.AddIngester(id, instance.Addr, instance.Zone, instance.Tokens, ring.LEFT, instance.GetRegisteredAt())
			}
		}

	}

	d.next.OnRingInstanceHeartbeat(lifecycler, ringDesc, instanceDesc)
}
