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
	"sort"
	"testing"
	"time"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/events/mock"
	schedEvt "github.com/apache/yunikorn-core/pkg/scheduler/objects/events"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/common"
	siCommon "github.com/apache/yunikorn-scheduler-interface/lib/go/common"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

func TestAskToString(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatal("String panic on nil ask")
		}
	}()
	var ask *AllocationAsk
	// ignore nil check from IDE we really want to do this
	askString := ask.String()
	assert.Equal(t, askString, "ask is nil", "Unexpected string returned for nil ask")
}

func TestNewAsk(t *testing.T) {
	res := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10})
	siAsk := &si.AllocationAsk{
		AllocationKey: "ask-1",
		ApplicationID: "app-1",
		ResourceAsk:   res.ToProto(),
	}
	ask := NewAllocationAskFromSI(siAsk)
	if ask == nil {
		t.Fatal("NewAllocationAskFromSI create failed while it should not")
	}
	askStr := ask.String()
	expected := "allocationKey ask-1, applicationID app-1, Resource map[first:10], Allocated false"
	assert.Equal(t, askStr, expected, "Strings should have been equal")
	assert.Equal(t, "app-1|ask-1", ask.resKeyWithoutNode) //nolint:staticcheck
}

func TestAskAllocateDeallocate(t *testing.T) {
	res := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10})
	ask := newAllocationAsk("alloc-1", "app-1", res)
	assert.Assert(t, !ask.IsAllocated(), "pending ask should return false for IsAllocated()")
	assert.Assert(t, !ask.deallocate(), "attempt to deallocate pending ask should fail")
	assert.Assert(t, ask.allocate(), "attempt to allocate pending ask should not fail")
	assert.Assert(t, ask.IsAllocated(), "allocated ask should return true for IsAllocated()")
	assert.Assert(t, !ask.allocate(), "attempt to allocate previously allocated ask should fail")
	assert.Assert(t, ask.deallocate(), "deallocating previously allocated ask should succeed")
	assert.Assert(t, !ask.IsAllocated(), "deallocated ask should return false for IsAllocated()")
}

// the create time should not be manipulated but we need it for reservation testing
func TestGetCreateTime(t *testing.T) {
	res := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10})
	ask := newAllocationAsk("alloc-1", "app-1", res)
	created := ask.GetCreateTime()
	// move time 10 seconds back
	ask.createTime = created.Add(time.Second * -10)
	createdNow := ask.GetCreateTime()
	if createdNow.Equal(created) {
		t.Fatal("create time stamp should have been modified")
	}
}

func TestPreemptionPolicy(t *testing.T) {
	ask1 := NewAllocationAskFromSI(&si.AllocationAsk{
		AllocationKey:    "allow-self-deny-other",
		ApplicationID:    "allow-self-deny-other",
		PreemptionPolicy: &si.PreemptionPolicy{AllowPreemptSelf: true, AllowPreemptOther: false}})
	assert.Check(t, ask1.IsAllowPreemptSelf(), "preempt self not allowed")
	assert.Check(t, !ask1.IsAllowPreemptOther(), "preempt other allowed")

	ask2 := NewAllocationAskFromSI(&si.AllocationAsk{
		AllocationKey:    "deny-self-allow-other",
		ApplicationID:    "deny-self-allow-other",
		PreemptionPolicy: &si.PreemptionPolicy{AllowPreemptSelf: false, AllowPreemptOther: true}})
	assert.Check(t, !ask2.IsAllowPreemptSelf(), "preempt self allowed")
	assert.Check(t, ask2.IsAllowPreemptOther(), "preempt other not allowed")
}

func TestPreemptCheckTime(t *testing.T) {
	siAsk := &si.AllocationAsk{
		AllocationKey: "ask1",
		ApplicationID: "app1",
		PartitionName: "default",
	}
	ask := NewAllocationAskFromSI(siAsk)
	assert.Equal(t, ask.GetPreemptCheckTime(), time.Time{}, "preemptCheckTime was not default")

	now := time.Now().Add(-1 * time.Second)
	ask.UpdatePreemptCheckTime()
	assert.Check(t, now.Before(ask.GetPreemptCheckTime()), "preemptCheckTime was not current")
}

func TestPlaceHolder(t *testing.T) {
	siAsk := &si.AllocationAsk{
		AllocationKey: "ask1",
		ApplicationID: "app1",
		PartitionName: "default",
	}
	ask := NewAllocationAskFromSI(siAsk)
	assert.Assert(t, !ask.IsPlaceholder(), "standard ask should not be a placeholder")
	assert.Equal(t, ask.GetTaskGroup(), "", "standard ask should not have a TaskGroupName")

	siAsk = &si.AllocationAsk{
		AllocationKey: "ask1",
		ApplicationID: "app1",
		PartitionName: "default",
		TaskGroupName: "",
		Placeholder:   true,
	}
	ask = NewAllocationAskFromSI(siAsk)
	var nilAsk *AllocationAsk
	assert.Equal(t, ask, nilAsk, "placeholder ask created without a TaskGroupName")

	siAsk.TaskGroupName = "TestPlaceHolder"
	ask = NewAllocationAskFromSI(siAsk)
	assert.Assert(t, ask != nilAsk, "placeholder ask creation failed unexpectedly")
	assert.Assert(t, ask.IsPlaceholder(), "ask should have been a placeholder")
	assert.Equal(t, ask.GetTaskGroup(), siAsk.TaskGroupName, "TaskGroupName not set as expected")
}

func TestGetRequiredNode(t *testing.T) {
	tag := make(map[string]string)
	// unset case
	siAsk := &si.AllocationAsk{
		AllocationKey: "ask1",
		ApplicationID: "app1",
		PartitionName: "default",
		Tags:          tag,
	}
	ask := NewAllocationAskFromSI(siAsk)
	assert.Equal(t, ask.GetRequiredNode(), "", "required node is empty as expected")
	// set case
	tag[common.DomainYuniKorn+common.KeyRequiredNode] = "NodeName"
	siAsk = &si.AllocationAsk{
		AllocationKey: "ask1",
		ApplicationID: "app1",
		PartitionName: "default",
		Tags:          tag,
	}
	ask = NewAllocationAskFromSI(siAsk)
	assert.Equal(t, ask.GetRequiredNode(), "NodeName", "required node should be NodeName")
}

func TestAllocationLog(t *testing.T) {
	res := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10})
	siAsk := &si.AllocationAsk{
		AllocationKey: "ask-1",
		ApplicationID: "app-1",
		ResourceAsk:   res.ToProto(),
	}
	ask := NewAllocationAskFromSI(siAsk)

	// log a reservation event
	ask.LogAllocationFailure("reserve1", false)
	log := sortedLog(ask)
	assert.Equal(t, 0, len(log), "non-allocation events was logged")

	// log an allocation event
	ask.LogAllocationFailure("alloc1", true)
	log = sortedLog(ask)
	assert.Equal(t, 1, len(log), "allocation event should be logged")
	assert.Equal(t, "alloc1", log[0].Message, "wrong message for event 1")
	assert.Equal(t, 1, int(log[0].Count), "wrong count for event 1")

	// add a second allocation event
	ask.LogAllocationFailure("alloc2", true)
	log = sortedLog(ask)
	assert.Equal(t, 2, len(log), "allocation event 2 should be logged")
	assert.Equal(t, "alloc2", log[0].Message, "wrong message for event 1")
	assert.Equal(t, "alloc1", log[1].Message, "wrong message for event 2")
	assert.Equal(t, 1, int(log[0].Count), "wrong count for event 1")
	assert.Equal(t, 1, int(log[1].Count), "wrong count for event 2")

	// duplicate the first one
	ask.LogAllocationFailure("alloc1", true)
	log = sortedLog(ask)
	assert.Equal(t, 2, len(log), "allocation event alloc1 (#2) should not create a new event")
	assert.Equal(t, "alloc1", log[0].Message, "wrong message for event 1")
	assert.Equal(t, "alloc2", log[1].Message, "wrong message for event 2")
	assert.Equal(t, 2, int(log[0].Count), "wrong count for event 1")
	assert.Equal(t, 1, int(log[1].Count), "wrong count for event 2")
}

func TestSendPredicateFailed(t *testing.T) {
	res := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10})
	siAsk := &si.AllocationAsk{
		AllocationKey: "ask-1",
		ApplicationID: "app-1",
		ResourceAsk:   res.ToProto(),
	}
	ask := NewAllocationAskFromSI(siAsk)
	eventSystem := mock.NewEventSystemDisabled()
	ask.askEvents = schedEvt.NewAskEvents(eventSystem)
	ask.SendPredicatesFailedEvent(map[string]int{})
	assert.Equal(t, 0, len(eventSystem.Events))

	eventSystem = mock.NewEventSystem()
	ask.askEvents = schedEvt.NewAskEvents(eventSystem)
	ask.SendPredicatesFailedEvent(map[string]int{
		"failure": 1,
	})
	assert.Equal(t, 1, len(eventSystem.Events))
	event := eventSystem.Events[0]
	assert.Equal(t, "Unschedulable request 'ask-1': failure (1x); ", event.Message)
}

func TestCreateTime(t *testing.T) {
	siAsk := &si.AllocationAsk{
		AllocationKey: "ask1",
		ApplicationID: "app1",
		PartitionName: "default",
	}
	siAsk.Tags = map[string]string{siCommon.CreationTime: "1622530800"}
	ask := NewAllocationAskFromSI(siAsk)
	assert.Equal(t, ask.GetTag(siCommon.CreationTime), "1622530800")
	assert.Equal(t, ask.GetTag("unknown"), "")
	assert.Equal(t, ask.GetCreateTime(), time.Unix(1622530800, 0))
}

func sortedLog(ask *AllocationAsk) []*AllocationLogEntry {
	log := ask.GetAllocationLog()
	sort.SliceStable(log, func(i int, j int) bool {
		return log[i].LastOccurrence.After(log[j].LastOccurrence)
	})
	return log
}
