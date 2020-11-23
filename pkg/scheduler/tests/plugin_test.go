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

package tests

import (
	"sync"
	"testing"
	"time"

	"gotest.tools/assert"

	"github.com/apache/incubator-yunikorn-core/pkg/common"
	"github.com/apache/incubator-yunikorn-core/pkg/plugins"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
)

type fakeContainerStateUpdater struct {
	sentUpdate *si.UpdateContainerSchedulingStateRequest
	sync.RWMutex
}

func (f *fakeContainerStateUpdater) Update(request *si.UpdateContainerSchedulingStateRequest) {
	f.Lock()
	defer f.Unlock()
	f.sentUpdate = request
}

func (f *fakeContainerStateUpdater) getContainerUpdateRequest() *si.UpdateContainerSchedulingStateRequest {
	f.RLock()
	defer f.RUnlock()
	return f.sentUpdate
}

func TestContainerStateUpdater(t *testing.T) {
	configData := `
partitions:
  - name: default
    queues:
      - name: root
        submitacl: "*"
        queues:
          - name: singleleaf
            resources:
              max:
                memory: 100
`
	ms := &mockScheduler{}
	defer ms.Stop()

	err := ms.Init(configData, true)
	assert.NilError(t, err, "RegisterResourceManager failed")

	// register a fake container state updater for testing
	fk := &fakeContainerStateUpdater{}
	plugins.RegisterSchedulerPlugin(fk)

	const leafName = "root.singleleaf"
	const node1 = "node-1"

	// Register a node, and add apps
	err = ms.proxy.Update(&si.UpdateRequest{
		NewSchedulableNodes: []*si.NewNodeInfo{
			{
				NodeID:     node1,
				Attributes: map[string]string{},
				SchedulableResource: &si.Resource{
					Resources: map[string]*si.Quantity{
						"memory": {Value: 10},
					},
				},
			},
		},
		NewApplications: newAddAppRequest(map[string]string{appID1: leafName}),
		RmID:            "rm:123",
	})

	assert.NilError(t, err, "UpdateRequest failed")

	// wait until app and node gets registered
	ms.mockRM.waitForAcceptedApplication(t, appID1, 1000)
	ms.mockRM.waitForAcceptedNode(t, node1, 1000)

	// now submit a request, that uses 8/10 memory from the node
	err = ms.proxy.Update(&si.UpdateRequest{
		Asks: []*si.AllocationAsk{
			{
				AllocationKey: "alloc-1",
				ResourceAsk: &si.Resource{
					Resources: map[string]*si.Quantity{
						"memory": {Value: 8},
					},
				},
				MaxAllocations: 1,
				ApplicationID:  appID1,
			},
		},
		RmID: "rm:123",
	})

	assert.NilError(t, err, "UpdateRequest 2 failed")

	// the request should be able to get 1 allocation
	ms.mockRM.waitForAllocations(t, 1, 1000)

	// now submit another request, ask for 5 memory
	//  - node has 2 left,
	//  - queue has plenty of resources
	// we expect the plugin to be called to trigger an update
	err = ms.proxy.Update(&si.UpdateRequest{
		Asks: []*si.AllocationAsk{
			{
				AllocationKey: "alloc-2",
				ResourceAsk: &si.Resource{
					Resources: map[string]*si.Quantity{
						"memory": {Value: 5},
					},
				},
				MaxAllocations: 1,
				ApplicationID:  appID1,
			},
		},
		RmID: "rm:123",
	})
	assert.NilError(t, err)

	err = common.WaitFor(100*time.Millisecond, 3000*time.Millisecond, func() bool {
		reqSent := fk.getContainerUpdateRequest()
		return reqSent != nil && reqSent.ApplicartionID == appID1 &&
			reqSent.GetState() == si.UpdateContainerSchedulingStateRequest_FAILED
	})
	assert.NilError(t, err)
}
