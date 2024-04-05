/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

package mock

import (
	"github.com/apache/yunikorn-core/pkg/locking"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

type ContainerStateUpdater struct {
	ResourceManagerCallback
	sentUpdate *si.UpdateContainerSchedulingStateRequest
	locking.RWMutex
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
