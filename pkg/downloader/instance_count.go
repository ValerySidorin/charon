package downloader

import (
	"time"

	"github.com/grafana/dskit/ring"
	"go.uber.org/atomic"
)

type healthyInstanceDelegate struct {
	count            *atomic.Uint32
	heartbeatTimeout time.Duration
	next             ring.BasicLifecyclerDelegate
}

func newHealthyInstanceDelegate(count *atomic.Uint32, heartbeatTimeout time.Duration, next ring.BasicLifecyclerDelegate) *healthyInstanceDelegate {
	return &healthyInstanceDelegate{count: count, heartbeatTimeout: heartbeatTimeout, next: next}
}

func (d *healthyInstanceDelegate) OnRingInstanceRegister(lifecycler *ring.BasicLifecycler, ringDesc ring.Desc, instanceExists bool, instanceID string, instanceDesc ring.InstanceDesc) (ring.InstanceState, ring.Tokens) {
	return d.next.OnRingInstanceRegister(lifecycler, ringDesc, instanceExists, instanceID, instanceDesc)
}

func (d *healthyInstanceDelegate) OnRingInstanceTokens(lifecycler *ring.BasicLifecycler, tokens ring.Tokens) {
	d.next.OnRingInstanceTokens(lifecycler, tokens)
}

func (d *healthyInstanceDelegate) OnRingInstanceStopping(lifecycler *ring.BasicLifecycler) {
	d.next.OnRingInstanceStopping(lifecycler)
}

func (d *healthyInstanceDelegate) OnRingInstanceHeartbeat(lifecycler *ring.BasicLifecycler, ringDesc *ring.Desc, instanceDesc *ring.InstanceDesc) {
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
