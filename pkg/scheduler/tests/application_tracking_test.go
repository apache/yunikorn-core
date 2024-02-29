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
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/common"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

const configDataSmokeTestNoLimits = `
partitions:
  - name: default
    queues:
      - name: root
        submitacl: "*"
        queues:
          - name: singleleaf
`

func TestApplicationHistoryTracking(t *testing.T) {
	// Register RM
	ms := &mockScheduler{}
	defer ms.Stop()
	err := ms.Init(configDataSmokeTestNoLimits, true, true)
	assert.NilError(t, err, "RegisterResourceManager failed")

	// Check queue events
	client := RClient{}
	stream, err := client.GetEventsStream(1000)
	assert.NilError(t, err)
	defer stream.Close()
	eventsDao, err := client.GetBatchEvents()
	assert.NilError(t, err)
	assert.Equal(t, 2, len(eventsDao.EventRecords), "number of events generated")
	verifyQueueEvents(t, eventsDao.EventRecords)
	events := getEventsFromStream(t, stream, 2)
	assert.NilError(t, err)
	verifyQueueEvents(t, events)

	// Register a node & check events
	err = ms.proxy.UpdateNode(&si.NodeRequest{
		Nodes: []*si.NodeInfo{
			{
				NodeID:     "node-1:1234",
				Attributes: map[string]string{},
				SchedulableResource: &si.Resource{
					Resources: map[string]*si.Quantity{
						"memory": {Value: 100000000},
						"vcore":  {Value: 20000},
					},
				},
				Action: si.NodeInfo_CREATE,
			},
		},
		RmID: "rm:123",
	})
	assert.NilError(t, err, "NodeRequest failed")
	ms.mockRM.waitForAcceptedNode(t, "node-1:1234", 1000)
	eventsDao, err = client.GetBatchEvents()
	assert.NilError(t, err)
	assert.Equal(t, 5, len(eventsDao.EventRecords), "number of events generated")
	verifyNodeAddedAndQueueMaxSetEvents(t, eventsDao.EventRecords[2:])
	events = getEventsFromStream(t, stream, 3)
	assert.NilError(t, err)
	verifyNodeAddedAndQueueMaxSetEvents(t, events)

	// Add application & check events
	err = ms.proxy.UpdateApplication(&si.ApplicationRequest{
		New:  newAddAppRequest(map[string]string{appID1: "root.singleleaf"}),
		RmID: "rm:123",
	})
	assert.NilError(t, err, "ApplicationRequest failed")
	ms.mockRM.waitForAcceptedApplication(t, appID1, 1000)
	eventsDao, err = client.GetBatchEvents()
	assert.NilError(t, err)
	assert.Equal(t, 7, len(eventsDao.EventRecords), "number of events generated")
	verifyAppAddedEvents(t, eventsDao.EventRecords[5:])
	events = getEventsFromStream(t, stream, 2)
	assert.NilError(t, err)
	verifyAppAddedEvents(t, events)

	// Add allocation ask & check events
	err = ms.proxy.UpdateAllocation(&si.AllocationRequest{
		Asks: []*si.AllocationAsk{
			{
				AllocationKey: "alloc-1",
				ResourceAsk: &si.Resource{
					Resources: map[string]*si.Quantity{
						"memory": {Value: 10000000},
						"vcore":  {Value: 1000},
					},
				},
				MaxAllocations: 1,
				ApplicationID:  appID1,
			},
		},
		RmID: "rm:123",
	})
	assert.NilError(t, err, "AllocationRequest failed")
	ms.mockRM.waitForAllocations(t, 1, 1000)
	eventsDao, err = client.GetBatchEvents()
	assert.NilError(t, err)
	assert.Equal(t, 13, len(eventsDao.EventRecords), "number of events generated")
	verifyAllocationAskAddedEvents(t, eventsDao.EventRecords[7:])
	events = getEventsFromStream(t, stream, 6)
	verifyAllocationAskAddedEvents(t, events)

	allocations := ms.mockRM.getAllocations()
	assert.Equal(t, 1, len(allocations), "number of allocations")
	var allocationID string
	for key := range allocations {
		allocationID = key
	}

	// terminate allocation & check events
	err = ms.proxy.UpdateAllocation(&si.AllocationRequest{
		Releases: &si.AllocationReleasesRequest{
			AllocationsToRelease: []*si.AllocationRelease{
				{
					ApplicationID:   appID1,
					PartitionName:   "default",
					AllocationID:    allocationID,
					TerminationType: si.TerminationType_STOPPED_BY_RM,
				},
			},
		},
		RmID: "rm:123",
	})
	assert.NilError(t, err, "AllocationRequest failed")

	// make sure app transitions to Completing
	app := ms.getApplication(appID1)
	err = common.WaitFor(time.Millisecond*10, time.Second, func() bool {
		return app.IsCompleting()
	})
	assert.NilError(t, err, "timeout waiting for app state Completing")

	eventsDao, err = client.GetBatchEvents()
	assert.NilError(t, err)
	assert.Equal(t, 17, len(eventsDao.EventRecords), "number of events generated")
	verifyAllocationCancelledEvents(t, eventsDao.EventRecords[13:])
	events = getEventsFromStream(t, stream, 4)
	assert.NilError(t, err)
	verifyAllocationCancelledEvents(t, events)
}

func getEventsFromStream(t *testing.T, stream io.ReadCloser, numEvents int) []*si.EventRecord {
	lines, err := readLinesFromStream(stream, numEvents)
	assert.NilError(t, err)

	events, err := siEventFromJson(lines)
	assert.NilError(t, err)

	return events
}

type scanResult struct {
	lines []string
	err   error
}

func readLinesFromStream(stream io.ReadCloser, numLines int) ([]string, error) {
	resp := make(chan scanResult)

	go func() {
		reader := bufio.NewReader(stream)
		lines := make([]string, 0)
		scanner := bufio.NewScanner(reader)
		scanner.Split(bufio.ScanLines)

		for i := 0; i < numLines; i++ {
			if !scanner.Scan() {
				resp <- scanResult{nil, scanner.Err()}
				return
			}
			line := scanner.Text()
			fmt.Println("Event stream - received:", line)
			lines = append(lines, line)
		}
		resp <- scanResult{lines, nil}
	}()

	select {
	case <-time.After(time.Second):
		return nil, errors.New("timeout waiting for events on the event stream")
	case r := <-resp:
		return r.lines, r.err
	}
}

func siEventFromJson(lines []string) ([]*si.EventRecord, error) {
	var events []*si.EventRecord

	for _, line := range lines {
		var evt si.EventRecord
		err := json.Unmarshal([]byte(line), &evt)
		if err != nil {
			return nil, err
		}
		events = append(events, &evt)
	}

	return events, nil
}

func verifyQueueEvents(t *testing.T, events []*si.EventRecord) {
	assert.Equal(t, "root", events[0].ObjectID)
	assert.Equal(t, "", events[0].Message)
	assert.Equal(t, "", events[0].ReferenceID)
	assert.Equal(t, si.EventRecord_ADD, events[0].EventChangeType)
	assert.Equal(t, si.EventRecord_DETAILS_NONE, events[0].EventChangeDetail)

	assert.Equal(t, "root.singleleaf", events[1].ObjectID)
	assert.Equal(t, "", events[1].Message)
	assert.Equal(t, "", events[1].ReferenceID)
	assert.Equal(t, si.EventRecord_ADD, events[1].EventChangeType)
	assert.Equal(t, si.EventRecord_DETAILS_NONE, events[1].EventChangeDetail)
}

func verifyNodeAddedAndQueueMaxSetEvents(t *testing.T, events []*si.EventRecord) {
	assert.Equal(t, "node-1:1234", events[0].ObjectID)
	assert.Equal(t, "schedulable: true", events[0].Message)
	assert.Equal(t, "", events[0].ReferenceID)
	assert.Equal(t, si.EventRecord_NODE, events[0].Type)
	assert.Equal(t, si.EventRecord_SET, events[0].EventChangeType)
	assert.Equal(t, si.EventRecord_NODE_SCHEDULABLE, events[0].EventChangeDetail)

	assert.Equal(t, "root", events[1].ObjectID)
	assert.Equal(t, "", events[1].Message)
	assert.Equal(t, "", events[1].ReferenceID)
	assert.Equal(t, si.EventRecord_SET, events[1].EventChangeType)
	assert.Equal(t, si.EventRecord_QUEUE_MAX, events[1].EventChangeDetail)

	assert.Equal(t, "node-1:1234", events[2].ObjectID)
	assert.Equal(t, "Node added to the scheduler", events[2].Message)
	assert.Equal(t, "", events[2].ReferenceID)
	assert.Equal(t, si.EventRecord_NODE, events[2].Type)
	assert.Equal(t, si.EventRecord_ADD, events[2].EventChangeType)
	assert.Equal(t, si.EventRecord_DETAILS_NONE, events[2].EventChangeDetail)
}

func verifyAppAddedEvents(t *testing.T, events []*si.EventRecord) {
	assert.Equal(t, "app-1", events[0].ObjectID)
	assert.Equal(t, "", events[0].Message)
	assert.Equal(t, "", events[0].ReferenceID)
	assert.Equal(t, si.EventRecord_APP, events[0].Type)
	assert.Equal(t, si.EventRecord_ADD, events[0].EventChangeType)
	assert.Equal(t, si.EventRecord_APP_NEW, events[0].EventChangeDetail)

	assert.Equal(t, "root.singleleaf", events[1].ObjectID)
	assert.Equal(t, "", events[1].Message)
	assert.Equal(t, "app-1", events[1].ReferenceID)
	assert.Equal(t, si.EventRecord_QUEUE, events[1].Type)
	assert.Equal(t, si.EventRecord_ADD, events[1].EventChangeType)
	assert.Equal(t, si.EventRecord_QUEUE_APP, events[1].EventChangeDetail)
}

func verifyAllocationAskAddedEvents(t *testing.T, events []*si.EventRecord) {
	// state transition to Accepted
	assert.Equal(t, "app-1", events[0].ObjectID)
	assert.Equal(t, "", events[0].Message)
	assert.Equal(t, "", events[0].ReferenceID)
	assert.Equal(t, si.EventRecord_APP, events[0].Type)
	assert.Equal(t, si.EventRecord_SET, events[0].EventChangeType)
	assert.Equal(t, si.EventRecord_APP_ACCEPTED, events[0].EventChangeDetail)

	// allocation ask received
	assert.Equal(t, "app-1", events[1].ObjectID)
	assert.Equal(t, "", events[1].Message)
	assert.Equal(t, "alloc-1", events[1].ReferenceID)
	assert.Equal(t, si.EventRecord_APP, events[1].Type)
	assert.Equal(t, si.EventRecord_ADD, events[1].EventChangeType)
	assert.Equal(t, si.EventRecord_APP_REQUEST, events[1].EventChangeDetail)

	// allocation on node
	assert.Equal(t, "node-1:1234", events[2].ObjectID)
	assert.Equal(t, "", events[2].Message)
	assert.Equal(t, "alloc-1", events[2].ReferenceID)
	assert.Equal(t, si.EventRecord_NODE, events[2].Type)
	assert.Equal(t, si.EventRecord_ADD, events[2].EventChangeType)
	assert.Equal(t, si.EventRecord_NODE_ALLOC, events[2].EventChangeDetail)

	// state change to Starting
	assert.Equal(t, "app-1", events[3].ObjectID)
	assert.Equal(t, "", events[3].Message)
	assert.Equal(t, "", events[3].ReferenceID)
	assert.Equal(t, si.EventRecord_APP, events[3].Type)
	assert.Equal(t, si.EventRecord_SET, events[3].EventChangeType)
	assert.Equal(t, si.EventRecord_APP_STARTING, events[3].EventChangeDetail)

	// Track resource usage for the user - increment
	assert.Equal(t, "testuser", events[4].ObjectID)
	assert.Equal(t, "", events[4].Message)
	assert.Equal(t, "root.singleleaf", events[4].ReferenceID)
	assert.Equal(t, si.EventRecord_USERGROUP, events[4].Type)
	assert.Equal(t, si.EventRecord_ADD, events[4].EventChangeType)
	assert.Equal(t, si.EventRecord_UG_USER_RESOURCE, events[4].EventChangeDetail)

	// adding allocation to the App
	assert.Equal(t, "app-1", events[5].ObjectID)
	assert.Equal(t, "", events[5].Message)
	assert.Equal(t, si.EventRecord_APP, events[5].Type)
	assert.Equal(t, si.EventRecord_ADD, events[5].EventChangeType)
	assert.Equal(t, si.EventRecord_APP_ALLOC, events[5].EventChangeDetail)
}

func verifyAllocationCancelledEvents(t *testing.T, events []*si.EventRecord) {
	// Track resource usage for the user - decrement
	assert.Equal(t, "testuser", events[0].ObjectID)
	assert.Equal(t, "", events[0].Message)
	assert.Equal(t, "root.singleleaf", events[0].ReferenceID)
	assert.Equal(t, si.EventRecord_USERGROUP, events[0].Type)
	assert.Equal(t, si.EventRecord_REMOVE, events[0].EventChangeType)
	assert.Equal(t, si.EventRecord_UG_USER_RESOURCE, events[0].EventChangeDetail)

	// state transition to Completing
	assert.Equal(t, "app-1", events[1].ObjectID)
	assert.Equal(t, "", events[1].Message)
	assert.Equal(t, "", events[1].ReferenceID)
	assert.Equal(t, si.EventRecord_APP, events[1].Type)
	assert.Equal(t, si.EventRecord_SET, events[1].EventChangeType)
	assert.Equal(t, si.EventRecord_APP_COMPLETING, events[1].EventChangeDetail)

	// cancel allocation
	assert.Equal(t, "app-1", events[2].ObjectID)
	assert.Equal(t, "", events[2].Message)
	assert.Equal(t, si.EventRecord_APP, events[2].Type)
	assert.Equal(t, si.EventRecord_REMOVE, events[2].EventChangeType)
	assert.Equal(t, si.EventRecord_ALLOC_CANCEL, events[2].EventChangeDetail)

	// remove allocation from the node
	assert.Equal(t, "node-1:1234", events[3].ObjectID)
	assert.Equal(t, "", events[3].Message)
	assert.Equal(t, "alloc-1", events[3].ReferenceID)
	assert.Equal(t, si.EventRecord_NODE, events[3].Type)
	assert.Equal(t, si.EventRecord_REMOVE, events[3].EventChangeType)
	assert.Equal(t, si.EventRecord_NODE_ALLOC, events[3].EventChangeDetail)
}
