package downloader

import (
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/ring"
	"go.uber.org/atomic"
)

// Healthy instance delegate stuff
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

// Instance list delegate stuff
type concurrentInstanceMap struct {
	sync.Mutex
	instances map[string]ring.InstanceDesc
}

func newConcurrentInstanceMap() *concurrentInstanceMap {
	return &concurrentInstanceMap{
		instances: make(map[string]ring.InstanceDesc),
	}
}

func (m *concurrentInstanceMap) Get(instanceID string) (ring.InstanceDesc, bool) {
	v, ok := m.instances[instanceID]
	return v, ok
}

func (m *concurrentInstanceMap) Set(ringInstances map[string]ring.InstanceDesc) {
	m.Lock()
	defer m.Unlock()

	for k := range m.instances {
		delete(m.instances, k)
	}

	for k, v := range ringInstances {
		m.instances[k] = v
	}
}

func (m *concurrentInstanceMap) Keys() []string {
	keys := make([]string, 0)
	m.Lock()
	defer m.Unlock()

	for k := range m.instances {
		keys = append(keys, k)
	}

	return keys
}

type instanceListDelegate struct {
	instances *concurrentInstanceMap
	next      ring.BasicLifecyclerDelegate
}

func newInstanceListDelegate(m *concurrentInstanceMap, next ring.BasicLifecyclerDelegate) *instanceListDelegate {
	return &instanceListDelegate{instances: m, next: next}
}

func (d *instanceListDelegate) OnRingInstanceRegister(lifecycler *ring.BasicLifecycler, ringDesc ring.Desc, instanceExists bool, instanceID string, instanceDesc ring.InstanceDesc) (ring.InstanceState, ring.Tokens) {
	return d.next.OnRingInstanceRegister(lifecycler, ringDesc, instanceExists, instanceID, instanceDesc)
}

func (d *instanceListDelegate) OnRingInstanceTokens(lifecycler *ring.BasicLifecycler, tokens ring.Tokens) {
	d.next.OnRingInstanceTokens(lifecycler, tokens)
}

func (d *instanceListDelegate) OnRingInstanceStopping(lifecycler *ring.BasicLifecycler) {
	d.next.OnRingInstanceStopping(lifecycler)
}

func (d *instanceListDelegate) OnRingInstanceHeartbeat(lifecycler *ring.BasicLifecycler, ringDesc *ring.Desc, instanceDesc *ring.InstanceDesc) {
	d.instances.Set(ringDesc.Ingesters)
	d.next.OnRingInstanceHeartbeat(lifecycler, ringDesc, instanceDesc)
}

// Auto mark unhealthy delegate stuff
type autoMarkUnhealthyDelegate struct {
	next         ring.BasicLifecyclerDelegate
	forgetPeriod time.Duration
	log          log.Logger
}

func newAutoMarkUnhealthyDelegate(forgetPeriod time.Duration, next ring.BasicLifecyclerDelegate, log log.Logger) *autoMarkUnhealthyDelegate {
	return &autoMarkUnhealthyDelegate{
		next:         next,
		log:          log,
		forgetPeriod: forgetPeriod,
	}
}

func (d *autoMarkUnhealthyDelegate) OnRingInstanceRegister(lifecycler *ring.BasicLifecycler, ringDesc ring.Desc, instanceExists bool, instanceID string, instanceDesc ring.InstanceDesc) (ring.InstanceState, ring.Tokens) {
	return d.next.OnRingInstanceRegister(lifecycler, ringDesc, instanceExists, instanceID, instanceDesc)
}

func (d *autoMarkUnhealthyDelegate) OnRingInstanceTokens(lifecycler *ring.BasicLifecycler, tokens ring.Tokens) {
	d.next.OnRingInstanceTokens(lifecycler, tokens)
}

func (d *autoMarkUnhealthyDelegate) OnRingInstanceStopping(lifecycler *ring.BasicLifecycler) {
	d.next.OnRingInstanceStopping(lifecycler)
}

func (d *autoMarkUnhealthyDelegate) OnRingInstanceHeartbeat(lifecycler *ring.BasicLifecycler, ringDesc *ring.Desc, instanceDesc *ring.InstanceDesc) {
	for id, instance := range ringDesc.Ingesters {
		if instance.State != ring.LEFT {
			lastHeartbeat := time.Unix(instance.GetTimestamp(), 0)

			if time.Since(lastHeartbeat) > d.forgetPeriod {
				level.Warn(d.log).Log("msg", "marking instance as unhealthy because it is not heartbeating for a long time", "instance", id, "last_heartbeat", lastHeartbeat.String(), "forget_period", d.forgetPeriod)
				ringDesc.RemoveIngester(id)
				ringDesc.AddIngester(id, instance.Addr, instance.Zone, instance.Tokens, ring.LEFT, instance.GetRegisteredAt())
			}
		}

	}

	d.next.OnRingInstanceHeartbeat(lifecycler, ringDesc, instanceDesc)
}
