/*
Copyright 2020 Cloudera, Inc.  All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

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

	"gotest.tools/assert"

	"github.com/cloudera/yunikorn-core/pkg/common/resources"
	"github.com/cloudera/yunikorn-scheduler-interface/lib/go/si"
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
	if !ask.addPendingAskRepeat(1) {
		t.Errorf("increase of pending ask with 1 failed, expected repeat 2, current repeat: %d", ask.getPendingAskRepeat())
	}
	if !ask.addPendingAskRepeat(-1) {
		t.Errorf("decrease of pending ask with 1 failed, expected repeat 1, current repeat: %d", ask.getPendingAskRepeat())
	}
	if ask.addPendingAskRepeat(-2) {
		t.Errorf("decrease of pending ask with 2 did not fail, expected repeat 1, current repeat: %d", ask.getPendingAskRepeat())
	}
	if !ask.addPendingAskRepeat(-1) {
		t.Errorf("decrease of pending ask with 1 failed, expected repeat 0, current repeat: %d", ask.getPendingAskRepeat())
	}
}
