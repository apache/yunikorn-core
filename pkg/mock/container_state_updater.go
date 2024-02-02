package mock

import (
	"sync"

	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

type ContainerStateUpdater struct {
	ResourceManagerCallback
	sentUpdate *si.UpdateContainerSchedulingStateRequest
	sync.RWMutex
}

func (m *ContainerStateUpdater) UpdateContainerSchedulingState(request *si.UpdateContainerSchedulingStateRequest) {
	m.Lock()
	defer m.Unlock()
	m.sentUpdate = request
}

func (m *ContainerStateUpdater) GetContainerUpdateRequest() *si.UpdateContainerSchedulingStateRequest {
	m.RLock()
	defer m.RUnlock()
	return m.sentUpdate
}

// NewContainerStateUpdater returns a mock that can allows retrieval of the update that was sent.
func NewContainerStateUpdater() *ContainerStateUpdater {
	return &ContainerStateUpdater{}
}
