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
	"time"

	"gotest.tools/assert"

	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
)

func newAllocationAsk(allocKey, appID string, res *resources.Resource) *schedulingAllocationAsk {
	return newAllocationAskRepeat(allocKey, appID, res, 1)
}

func newAllocationAskRepeat(allocKey, appID string, res *resources.Resource, repeat int) *schedulingAllocationAsk {
	ask := &si.AllocationAsk{
		AllocationKey:  allocKey,
		ApplicationID:  appID,
		PartitionName:  "default",
		ResourceAsk:    res.ToProto(),
		MaxAllocations: int32(repeat),
	}
	return newSchedulingAllocationAsk(ask)
}

func TestPendingAskRepeat(t *testing.T) {
	res := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10})
	ask := newAllocationAsk("alloc-1", "app-1", res)
	assert.Equal(t, ask.getPendingAskRepeat(), int32(1), "pending ask repeat should be 1")
	if !ask.updatePendingAskRepeat(1) {
		t.Errorf("increase of pending ask with 1 failed, expected repeat 2, current repeat: %d", ask.getPendingAskRepeat())
	}
	if !ask.updatePendingAskRepeat(-1) {
		t.Errorf("decrease of pending ask with 1 failed, expected repeat 1, current repeat: %d", ask.getPendingAskRepeat())
	}
	if ask.updatePendingAskRepeat(-2) {
		t.Errorf("decrease of pending ask with 2 did not fail, expected repeat 1, current repeat: %d", ask.getPendingAskRepeat())
	}
	if !ask.updatePendingAskRepeat(-1) {
		t.Errorf("decrease of pending ask with 1 failed, expected repeat 0, current repeat: %d", ask.getPendingAskRepeat())
	}
}

// the create time should not be manipulated but we need it for reservation testing
func TestGetCreateTime(t *testing.T) {
	res := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10})
	ask := newAllocationAskRepeat("alloc-1", "app-1", res, 2)
	created := ask.getCreateTime()
	// move time 10 seconds back
	ask.createTime = created.Add(time.Second * -10)
	createdNow := ask.getCreateTime()
	if createdNow.Equal(created) {
		t.Fatal("create time stamp should have been modified")
	}
}

// Check priority defaults and normalisation
func TestPriority(t *testing.T) {
	ask := &si.AllocationAsk{}
	allocAsk := newSchedulingAllocationAsk(ask)
	if allocAsk == nil {
		t.Fatalf("empty ask proto should not fail creation")
	}
	priority := int32(0)
	assert.Equal(t, allocAsk.priority, priority, "priority should default to 0 value")

	ask = &si.AllocationAsk{
		Priority: &si.Priority{Priority: &si.Priority_PriorityValue{PriorityValue: int32(-1)}},
	}
	allocAsk = newSchedulingAllocationAsk(ask)
	assert.Equal(t, allocAsk.priority, priority, "priority should have been normalised (min value 0)")
	priority = int32(100)
	ask = &si.AllocationAsk{
		Priority: &si.Priority{Priority: &si.Priority_PriorityValue{PriorityValue: priority}},
	}
	allocAsk = newSchedulingAllocationAsk(ask)
	assert.Equal(t, allocAsk.priority, priority, "priority should have been set to 100")
}
