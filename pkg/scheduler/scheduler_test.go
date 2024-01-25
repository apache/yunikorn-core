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
package scheduler

import (
	"testing"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

func TestInspectOutstandingRequests(t *testing.T) {
	scheduler := NewScheduler()
	partition, err := newBasePartition()
	assert.NilError(t, err, "unable to create partition: %v", err)
	scheduler.clusterContext.partitions["test"] = partition

	// two applications with no asks
	app1 := newApplication(appID1, "test", "root.default")
	app2 := newApplication(appID2, "test", "root.default")
	err = partition.AddApplication(app1)
	assert.NilError(t, err)
	err = partition.AddApplication(app2)
	assert.NilError(t, err)

	// add asks
	askResource := resources.NewResourceFromMap(map[string]resources.Quantity{
		"vcores": 1,
		"memory": 1,
	})
	siAsk1 := &si.AllocationAsk{
		AllocationKey:  "ask-uuid-1",
		ApplicationID:  appID1,
		ResourceAsk:    askResource.ToProto(),
		MaxAllocations: 1,
	}
	siAsk2 := &si.AllocationAsk{
		AllocationKey:  "ask-uuid-2",
		ApplicationID:  appID1,
		ResourceAsk:    askResource.ToProto(),
		MaxAllocations: 1,
	}
	siAsk3 := &si.AllocationAsk{
		AllocationKey:  "ask-uuid-3",
		ApplicationID:  appID2,
		ResourceAsk:    askResource.ToProto(),
		MaxAllocations: 1,
	}
	err = partition.addAllocationAsk(siAsk1)
	assert.NilError(t, err)
	err = partition.addAllocationAsk(siAsk2)
	assert.NilError(t, err)
	err = partition.addAllocationAsk(siAsk3)
	assert.NilError(t, err)

	// mark asks as attempted
	expectedTotal := resources.NewResourceFromMap(map[string]resources.Quantity{
		"memory": 3,
		"vcores": 3,
	})
	app1.GetAllocationAsk("ask-uuid-1").SetSchedulingAttempted(true)
	app1.GetAllocationAsk("ask-uuid-2").SetSchedulingAttempted(true)
	app2.GetAllocationAsk("ask-uuid-3").SetSchedulingAttempted(true)

	// Check #1: collected 3 requests
	noRequests, totalResources := scheduler.inspectOutstandingRequests()
	assert.Equal(t, 3, noRequests)
	assert.Assert(t, resources.Equals(totalResources, expectedTotal),
		"total resource expected: %v, got: %v", expectedTotal, totalResources)

	// Check #2: try again, pending asks are not collected
	noRequests, totalResources = scheduler.inspectOutstandingRequests()
	assert.Equal(t, 0, noRequests)
	assert.Assert(t, resources.IsZero(totalResources), "total resource is not zero: %v", totalResources)
}
