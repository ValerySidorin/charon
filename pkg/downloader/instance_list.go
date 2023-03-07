package downloader

import (
	"sync"

	"github.com/grafana/dskit/ring"
)

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
