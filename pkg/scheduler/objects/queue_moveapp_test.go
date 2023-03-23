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

package objects

import (
	"fmt"
	"testing"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/common/resources"
)

// to aid assert.DeepEquals() for parents
var zeroMemoryAndVcore = &resources.Resource{
	Resources: map[string]resources.Quantity{
		"memory": 0,
		"vcore":  0,
	},
}

type expectedQueueState struct {
	pending                *resources.Resource
	allocated              *resources.Resource
	runningApps            uint64
	allocatingAcceptedApps int
	applications           []string
	reservedApps           map[string]int
}

func TestPreserveApplications(t *testing.T) {
	var root *Queue
	var parent *Queue
	var subParent *Queue
	var leaf *Queue
	var err error
	root, err = createRootQueue(nil)
	assert.NilError(t, err, "unable to create root queue")
	parent, err = createManagedQueue(root, "parent", true, nil)
	assert.NilError(t, err, "unable to create parent")
	subParent, err = createManagedQueue(parent, "subparent", true, nil)
	assert.NilError(t, err, "unable to create subparent")
	leaf, err = createManagedQueue(subParent, "leaf", false, nil)
	leaf.root = root
	assert.NilError(t, err, "unable to create leaf")
	app1 := newApplication(appID0, "default", leaf.QueuePath)
	app2 := newApplication(appID1, "default", leaf.QueuePath)
	app3 := newApplication(appID2, "default", leaf.QueuePath)
	leaf.AddApplication(app1)
	leaf.AddApplication(app2)
	leaf.AddApplication(app3)
	var pending *resources.Resource
	pending, err = resources.NewResourceFromConf(map[string]string{"memory": "10000", "vcore": "10m"})
	assert.NilError(t, err, "unable to create resource")
	leaf.incPendingResource(pending)
	var allocated *resources.Resource
	allocated, err = resources.NewResourceFromConf(map[string]string{"memory": "5000", "vcore": "5m"})
	assert.NilError(t, err, "unable to create resource")
	err = leaf.IncAllocatedResource(allocated, true)
	assert.NilError(t, err)
	leaf.incRunningApps(appID0)
	leaf.setAllocatingAccepted(appID0)
	leaf.incRunningApps(appID2)
	leaf.setAllocatingAccepted(appID2)
	leaf.setReservations(appID1, 1)
	leaf.setReservations(appID2, 2)
	app1.incUserResourceUsage(allocated)

	parentAndLeafExpectedBefore := expectedQueueState{
		pending:                pending,
		allocated:              allocated,
		runningApps:            2,
		allocatingAcceptedApps: 2,
	}
	leafExpectedBefore := expectedQueueState{
		pending:                pending,
		allocated:              allocated,
		runningApps:            2,
		allocatingAcceptedApps: 2,
		applications:           []string{appID0, appID1, appID2},
		reservedApps: map[string]int{
			appID1: 1,
			appID2: 2,
		},
	}
	rootExpected := parentAndLeafExpectedBefore
	checkQueue(t, parentAndLeafExpectedBefore, root)
	checkQueue(t, parentAndLeafExpectedBefore, parent)
	checkQueue(t, parentAndLeafExpectedBefore, subParent)
	checkQueue(t, leafExpectedBefore, leaf)
	var stat ApplicationMoveStat
	stat, err = leaf.preserveRunningApps()
	assert.NilError(t, err, "error while moving running apps")
	assert.Equal(t, true, stat.Moved)
	assert.Equal(t, leaf.QueuePath, stat.FromQueue)
	assert.Equal(t, 3, stat.NumApps)
	assert.Equal(t, 3, stat.Reservations)
	assert.Equal(t, 2, stat.AllocatingAcceptedApps)
	assert.Equal(t, uint64(2), stat.RunningApps)
	assert.Equal(t, 2, stat.ReservedApps)
	assert.DeepEqual(t, pending, stat.PendingResource)
	assert.DeepEqual(t, allocated, stat.AllocatedResource)

	parentExpectedAfter := expectedQueueState{
		pending:   zeroMemoryAndVcore,
		allocated: zeroMemoryAndVcore,
	}
	leafExpectedAfter := expectedQueueState{
		pending:   resources.Zero,
		allocated: resources.Zero,
	}
	checkQueue(t, rootExpected, root)
	checkQueue(t, parentExpectedAfter, parent)
	checkQueue(t, parentExpectedAfter, subParent)
	checkQueue(t, leafExpectedAfter, leaf)
	checkQueue(t, leafExpectedBefore, root.preserved)
}

func TestPreserveRunningAppsInHierarchy(t *testing.T) { //nolint:funlen
	var root *Queue
	var parent *Queue
	var subParent *Queue
	var leaf1 *Queue
	var leaf2 *Queue
	var err error
	root, err = createRootQueue(nil)
	assert.NilError(t, err, "unable to create root queue")
	parent, err = createManagedQueue(root, "parent", true, nil)
	assert.NilError(t, err, "unable to create parent")
	subParent, err = createManagedQueue(parent, "subparent", true, nil)
	assert.NilError(t, err, "unable to create subparent")
	leaf1, err = createManagedQueue(subParent, "leaf1", false, nil)
	leaf1.root = root
	assert.NilError(t, err, "unable to create leaf1")
	leaf2, err = createManagedQueue(subParent, "leaf2", false, nil)
	leaf2.root = root
	assert.NilError(t, err, "unable to create leaf2")

	app1 := newApplication(appID0, "default", leaf1.QueuePath)
	app2 := newApplication(appID1, "default", leaf1.QueuePath)
	app3 := newApplication(appID2, "default", leaf2.QueuePath)
	leaf1.AddApplication(app1)
	leaf1.AddApplication(app2)
	leaf2.AddApplication(app3)
	var allocated1 *resources.Resource
	allocated1, err = resources.NewResourceFromConf(map[string]string{"memory": "5000", "vcore": "5m"})
	assert.NilError(t, err, "unable to create resource")
	err = leaf1.IncAllocatedResource(allocated1, true)
	assert.NilError(t, err)
	var allocated2 *resources.Resource
	allocated2, err = resources.NewResourceFromConf(map[string]string{"memory": "2000", "vcore": "2m"})
	assert.NilError(t, err, "unable to create resource")
	err = leaf2.IncAllocatedResource(allocated2, true)
	assert.NilError(t, err)
	var pending1 *resources.Resource
	pending1, err = resources.NewResourceFromConf(map[string]string{"memory": "7000", "vcore": "7m"})
	assert.NilError(t, err, "unable to create resource")
	leaf1.incPendingResource(pending1)
	var pending2 *resources.Resource
	pending2, err = resources.NewResourceFromConf(map[string]string{"memory": "1000", "vcore": "1m"})
	assert.NilError(t, err, "unable to create resource")
	leaf2.incPendingResource(pending2)
	assert.NilError(t, err)
	leaf1.incRunningApps(appID0)
	leaf1.setAllocatingAccepted(appID0)
	leaf2.incRunningApps(appID2)
	leaf2.setAllocatingAccepted(appID2)
	leaf1.setReservations(appID1, 1)
	leaf2.setReservations(appID2, 2)
	app1.incUserResourceUsage(allocated1)
	app3.incUserResourceUsage(allocated2)
	pendingSum := resources.NewResource()
	pendingSum.AddTo(pending1)
	pendingSum.AddTo(pending2)
	allocatedSum := resources.NewResource()
	allocatedSum.AddTo(allocated1)
	allocatedSum.AddTo(allocated2)

	parentAndLeafExpectedBefore := expectedQueueState{
		pending:                pendingSum,
		allocated:              allocatedSum,
		runningApps:            2,
		allocatingAcceptedApps: 2,
	}
	leaf1ExpectedBefore := expectedQueueState{
		pending:                pending1,
		allocated:              allocated1,
		runningApps:            1,
		allocatingAcceptedApps: 1,
		applications:           []string{appID0, appID1},
		reservedApps: map[string]int{
			appID1: 1,
		},
	}
	leaf2ExpectedBefore := expectedQueueState{
		pending:                pending2,
		allocated:              allocated2,
		runningApps:            1,
		allocatingAcceptedApps: 1,
		applications:           []string{appID2},
		reservedApps: map[string]int{
			appID2: 2,
		},
	}
	rootExpected := parentAndLeafExpectedBefore
	checkQueue(t, rootExpected, root)
	checkQueue(t, parentAndLeafExpectedBefore, parent)
	checkQueue(t, parentAndLeafExpectedBefore, subParent)
	checkQueue(t, leaf1ExpectedBefore, leaf1)
	checkQueue(t, leaf2ExpectedBefore, leaf2)
	var moveStats []ApplicationMoveStat
	moveStats, err = root.PreserveRunningAppsInHierarchy()
	assert.NilError(t, err, "error while moving running apps")
	assert.Equal(t, 2, len(moveStats))
	for _, stat := range moveStats {
		if stat.FromQueue == leaf1.QueuePath {
			assert.Equal(t, 2, stat.NumApps)
			assert.Equal(t, 1, stat.Reservations)
			assert.Equal(t, 1, stat.AllocatingAcceptedApps)
			assert.Equal(t, uint64(1), stat.RunningApps)
			assert.Equal(t, 1, stat.ReservedApps)
			assert.DeepEqual(t, pending1, stat.PendingResource)
			assert.DeepEqual(t, allocated1, stat.AllocatedResource)
			continue
		}
		if stat.FromQueue == leaf2.QueuePath {
			assert.Equal(t, 1, stat.NumApps)
			assert.Equal(t, 2, stat.Reservations)
			assert.Equal(t, 1, stat.AllocatingAcceptedApps)
			assert.Equal(t, uint64(1), stat.RunningApps)
			assert.Equal(t, 1, stat.ReservedApps)
			assert.DeepEqual(t, pending2, stat.PendingResource)
			assert.DeepEqual(t, allocated2, stat.AllocatedResource)
			continue
		}
		t.Errorf("unknown queue %s", stat.FromQueue)
	}
	parentExpectedAfter := expectedQueueState{
		pending:   zeroMemoryAndVcore,
		allocated: zeroMemoryAndVcore,
	}
	leafExpectedAfter := expectedQueueState{
		pending:   resources.Zero,
		allocated: resources.Zero,
	}
	preservedExpected := expectedQueueState{
		pending:                pendingSum,
		allocated:              allocatedSum,
		runningApps:            2,
		allocatingAcceptedApps: 2,
		applications:           []string{appID0, appID1, appID2},
		reservedApps: map[string]int{
			appID1: 1,
			appID2: 2,
		},
	}
	checkQueue(t, rootExpected, root)
	checkQueue(t, parentExpectedAfter, parent)
	checkQueue(t, parentExpectedAfter, subParent)
	checkQueue(t, leafExpectedAfter, leaf1)
	checkQueue(t, leafExpectedAfter, leaf2)
	checkQueue(t, preservedExpected, root.preserved)
}

func checkQueue(t *testing.T, expected expectedQueueState, queue *Queue) {
	queueName := queue.QueuePath
	assert.DeepEqual(t, expected.pending, queue.pending)
	assert.DeepEqual(t, expected.allocated, queue.allocatedResource)
	assert.Equal(t, expected.runningApps, queue.runningApps, "running apps check failed for queue "+queueName)
	assert.Equal(t, expected.allocatingAcceptedApps, len(queue.allocatingAcceptedApps), "allocating accepted apps check failed for queue"+queueName)
	assert.Equal(t, len(expected.applications), len(queue.applications), "no of apps check failed for queue "+queueName)
	if len(expected.applications) != 0 {
		for _, app := range expected.applications {
			assert.Assert(t, queue.applications[app] != nil, fmt.Sprintf("app %s not found in queue %s", app, queueName))
		}
	}
	assert.Equal(t, len(expected.reservedApps), len(queue.reservedApps))
	if len(expected.reservedApps) != 0 {
		for appID, num := range expected.reservedApps {
			assert.Equal(t, num, queue.reservedApps[appID],
				fmt.Sprintf("invalid reservation count for app %s in queue %s", appID, queueName))
		}
	}
	assert.Equal(t, Active.String(), queue.stateMachine.Current())
}
