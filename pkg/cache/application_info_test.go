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

package cache

import (
	"testing"
	"time"

	"gotest.tools/assert"

	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
	"github.com/apache/incubator-yunikorn-core/pkg/common/security"
	"github.com/apache/incubator-yunikorn-core/pkg/common/testutils"
)

func newApplicationInfo(appID, partition, queueName string) *ApplicationInfo {
	user := security.UserGroup{
		User:   "testuser",
		Groups: []string{},
	}
	tags := make(map[string]string)
	return NewApplicationInfo(appID, partition, queueName, user, tags)
}

func newApplicationInfoWithTags(appID, partition, queueName string, tags map[string]string) *ApplicationInfo {
	user := security.UserGroup{
		User:   "testuser",
		Groups: []string{},
	}
	return NewApplicationInfo(appID, partition, queueName, user, tags)
}

func TestNewApplicationInfo(t *testing.T) {
	appInfo := newApplicationInfo("app-00001", "default", "root.a")
	assert.Equal(t, appInfo.ApplicationID, "app-00001")
	assert.Equal(t, appInfo.Partition, "default")
	assert.Equal(t, appInfo.QueueName, "root.a")
	assert.Equal(t, appInfo.GetApplicationState(), New.String())
}

func TestAllocations(t *testing.T) {
	appInfo := newApplicationInfo("app-00001", "default", "root.a")

	// nothing allocated
	if !resources.IsZero(appInfo.GetAllocatedResource()) {
		t.Error("new application has allocated resources")
	}
	// create an allocation and check the assignment
	resMap := map[string]string{"memory": "100", "vcores": "10"}
	res, err := resources.NewResourceFromConf(resMap)
	assert.NilError(t, err, "failed to create resource with error")
	alloc := CreateMockAllocationInfo("app-00001", res, "uuid-1", "root.a", "node-1")
	appInfo.addAllocation(alloc)
	if !resources.Equals(appInfo.allocatedResource, res) {
		t.Errorf("allocated resources is not updated correctly: %v", appInfo.allocatedResource)
	}
	allocs := appInfo.GetAllAllocations()
	assert.Equal(t, len(allocs), 1)

	// add more allocations to test the removals
	alloc = CreateMockAllocationInfo("app-00001", res, "uuid-2", "root.a", "node-1")
	appInfo.addAllocation(alloc)
	alloc = CreateMockAllocationInfo("app-00001", res, "uuid-3", "root.a", "node-1")
	appInfo.addAllocation(alloc)
	allocs = appInfo.GetAllAllocations()
	assert.Equal(t, len(allocs), 3)
	// remove one of the 3
	if alloc = appInfo.removeAllocation("uuid-2"); alloc == nil {
		t.Error("returned allocations was nil allocation was not removed")
	}
	// try to remove a non existing alloc
	if alloc = appInfo.removeAllocation("does-not-exist"); alloc != nil {
		t.Errorf("returned allocations was not allocation was incorrectly removed: %v", alloc)
	}
	// remove all left over allocations
	if allocs = appInfo.removeAllAllocations(); allocs == nil || len(allocs) != 2 {
		t.Errorf("returned number of allocations was incorrect: %v", allocs)
	}
	allocs = appInfo.GetAllAllocations()
	assert.Equal(t, len(allocs), 0)
}

func TestQueueUpdate(t *testing.T) {
	appInfo := newApplicationInfo("app-00001", "default", "root.a")

	queue, err := NewUnmanagedQueue("test", true, nil)
	assert.NilError(t, err, "failed to create queue")
	appInfo.SetQueue(queue)
	assert.Equal(t, appInfo.QueueName, "test")
}

func TestStateTimeOut(t *testing.T) {
	startingTimeout = time.Microsecond * 100
	defer func() { startingTimeout = time.Minute * 5 }()
	appInfo := newApplicationInfo("app-00001", "default", "root.a")
	err := appInfo.HandleApplicationEvent(RunApplication)
	assert.NilError(t, err, "no error expected new to accepted (timeout test)")
	err = appInfo.HandleApplicationEvent(RunApplication)
	assert.NilError(t, err, "no error expected accepted to starting (timeout test)")
	// give it some time to run and progress
	time.Sleep(time.Millisecond * 100)
	if appInfo.IsStarting() {
		t.Fatal("Starting state should have timed out")
	}
	if appInfo.stateTimer != nil {
		t.Fatalf("Startup timer has not be cleared on time out as expected, %v", appInfo.stateTimer)
	}

	startingTimeout = time.Millisecond * 100
	appInfo = newApplicationInfo("app-00001", "default", "root.a")
	err = appInfo.HandleApplicationEvent(RunApplication)
	assert.NilError(t, err, "no error expected new to accepted (timeout test2)")
	err = appInfo.HandleApplicationEvent(RunApplication)
	assert.NilError(t, err, "no error expected accepted to starting (timeout test2)")
	// give it some time to run and progress
	time.Sleep(time.Microsecond * 100)
	if !appInfo.IsStarting() || appInfo.stateTimer == nil {
		t.Fatalf("Starting state and timer should not have timed out yet, state: %s", appInfo.stateMachine.Current())
	}
	err = appInfo.HandleApplicationEvent(RunApplication)
	assert.NilError(t, err, "no error expected starting to run (timeout test2)")
	// give it some time to run and progress
	time.Sleep(time.Microsecond * 100)
	if !appInfo.stateMachine.Is(Running.String()) || appInfo.stateTimer != nil {
		t.Fatalf("State is not running or timer was not cleared, state: %s, timer %v", appInfo.stateMachine.Current(), appInfo.stateTimer)
	}
}

func TestGetTag(t *testing.T) {
	appInfo := newApplicationInfoWithTags("app-1", "default", "root.a", nil)
	tag := appInfo.GetTag("")
	assert.Equal(t, tag, "", "expected empty tag value if tags nil")
	tags := make(map[string]string)
	appInfo = newApplicationInfoWithTags("app-1", "default", "root.a", tags)
	tag = appInfo.GetTag("")
	assert.Equal(t, tag, "", "expected empty tag value if no tags defined")
	tags["test"] = "test value"
	appInfo = newApplicationInfoWithTags("app-1", "default", "root.a", tags)
	tag = appInfo.GetTag("notfound")
	assert.Equal(t, tag, "", "expected empty tag value if tag not found")
	tag = appInfo.GetTag("test")
	assert.Equal(t, tag, "test value", "expected tag value")
}

func TestOnStatusChangeCalled(t *testing.T) {
	appInfo := newApplicationInfo("app-00001", "default", "root.a")
	assert.Equal(t, appInfo.GetApplicationState(), New.String())
	mockHandler := &testutils.MockEventHandler{EventHandled: false}
	appInfo.eventHandlers.RMProxyEventHandler = mockHandler

	err := appInfo.HandleApplicationEvent(RunApplication)
	assert.NilError(t, err, "no error expected")
	assert.Assert(t, mockHandler.EventHandled)

	mockHandler.EventHandled = false
	// accepted to rejected: error expected
	err = appInfo.HandleApplicationEvent(RejectApplication)
	assert.Assert(t, err != nil, "error expected ")
	assert.Equal(t, appInfo.GetApplicationState(), Accepted.String())
	assert.Assert(t, !mockHandler.EventHandled)
}
