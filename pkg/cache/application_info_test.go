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

package cache

import (
	"testing"

	"gotest.tools/assert"

	"github.com/cloudera/yunikorn-core/pkg/common/resources"
	"github.com/cloudera/yunikorn-core/pkg/common/security"
)

func newApplicationInfo(appID, partition, queueName string) *ApplicationInfo {
	user := security.UserGroup{
		User:   "testuser",
		Groups: []string{},
	}
	tags := make(map[string]string)
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
	if err != nil {
		t.Fatalf("failed to create resource with error: %v", err)
	}
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
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}
	appInfo.SetQueue(queue)
	assert.Equal(t, appInfo.QueueName, "test")
}

func TestAcceptStateTransition(t *testing.T) {
	// Accept only from new
	appInfo := newApplicationInfo("app-00001", "default", "root.a")
	assert.Equal(t, appInfo.GetApplicationState(), New.String())

	// new to accepted
	err := appInfo.HandleApplicationEvent(AcceptApplication)
	assert.Assert(t, err == nil)
	assert.Equal(t, appInfo.GetApplicationState(), Accepted.String())

	// app already accepted: error expected
	err = appInfo.HandleApplicationEvent(AcceptApplication)
	assert.Assert(t, err != nil)
	assert.Equal(t, appInfo.GetApplicationState(), Accepted.String())

	// accepted to killed
	err = appInfo.HandleApplicationEvent(KillApplication)
	assert.Assert(t, err == nil)
	assert.Equal(t, appInfo.GetApplicationState(), Killed.String())
}

func TestRejectTransition(t *testing.T) {
	// Reject only from new
	appInfo := newApplicationInfo("app-00001", "default", "root.a")
	assert.Equal(t, appInfo.GetApplicationState(), New.String())

	// new to rejected
	err := appInfo.HandleApplicationEvent(RejectApplication)
	assert.Assert(t, err == nil)
	assert.Equal(t, appInfo.GetApplicationState(), Rejected.String())

	// app already rejected: error expected
	err = appInfo.HandleApplicationEvent(RejectApplication)
	assert.Assert(t, err != nil)
	assert.Equal(t, appInfo.GetApplicationState(), Rejected.String())

	// rejected to killed: error expected
	err = appInfo.HandleApplicationEvent(KillApplication)
	assert.Assert(t, err != nil)
	assert.Equal(t, appInfo.GetApplicationState(), Rejected.String())

	// Reject fails from all but new
	appInfo = newApplicationInfo("app-00002", "default", "root.a")
	assert.Equal(t, appInfo.GetApplicationState(), New.String())
	err = appInfo.HandleApplicationEvent(AcceptApplication)
	assert.Assert(t, err == nil)
	assert.Equal(t, appInfo.GetApplicationState(), Accepted.String())
	err = appInfo.HandleApplicationEvent(RejectApplication)
	assert.Assert(t, err != nil)
	assert.Equal(t, appInfo.GetApplicationState(), Accepted.String())
}

func TestRunTransition(t *testing.T) {
	// run from accepted
	appInfo := newApplicationInfo("app-00001", "default", "root.a")
	assert.Equal(t, appInfo.GetApplicationState(), New.String())
	err := appInfo.HandleApplicationEvent(AcceptApplication)
	assert.Assert(t, err == nil)
	assert.Equal(t, appInfo.GetApplicationState(), Accepted.String())

	// run app
	err = appInfo.HandleApplicationEvent(RunApplication)
	assert.Assert(t, err == nil)
	assert.Equal(t, appInfo.GetApplicationState(), Running.String())

	// run app: same state is allowed for running
	err = appInfo.HandleApplicationEvent(RunApplication)
	assert.Assert(t, err == nil)
	assert.Equal(t, appInfo.GetApplicationState(), Running.String())

	// run to killed
	err = appInfo.HandleApplicationEvent(KillApplication)
	assert.Assert(t, err == nil)
	assert.Equal(t, appInfo.GetApplicationState(), Killed.String())

	// run fails from all but running or accepted
	err = appInfo.HandleApplicationEvent(RunApplication)
	assert.Assert(t, err != nil)
	assert.Equal(t, appInfo.GetApplicationState(), Killed.String())
}

func TestCompletedTransition(t *testing.T) {
	// complete only from run
	appInfo := newApplicationInfo("app-00001", "default", "root.a")
	assert.Equal(t, appInfo.GetApplicationState(), New.String())
	err := appInfo.HandleApplicationEvent(AcceptApplication)
	assert.Assert(t, err == nil)
	assert.Equal(t, appInfo.GetApplicationState(), Accepted.String())
	err = appInfo.HandleApplicationEvent(RunApplication)
	assert.Assert(t, err == nil)
	assert.Equal(t, appInfo.GetApplicationState(), Running.String())

	// completed only from run
	err = appInfo.HandleApplicationEvent(CompleteApplication)
	assert.Assert(t, err == nil)
	assert.Equal(t, appInfo.GetApplicationState(), Completed.String())

	// completed to killed: error expected
	err = appInfo.HandleApplicationEvent(KillApplication)
	assert.Assert(t, err != nil)
	assert.Equal(t, appInfo.GetApplicationState(), Completed.String())

	// completed fails from all but running
	appInfo2 := newApplicationInfo("app-00002", "default", "root.a")
	assert.Equal(t, appInfo2.GetApplicationState(), New.String())
	err = appInfo.HandleApplicationEvent(CompleteApplication)
	assert.Assert(t, err != nil)
	assert.Equal(t, appInfo2.GetApplicationState(), New.String())
}

func TestKilledTransition(t *testing.T) {
	// complete only from run
	appInfo := newApplicationInfo("app-00001", "default", "root.a")
	assert.Equal(t, appInfo.GetApplicationState(), New.String())

	// new to killed
	err := appInfo.HandleApplicationEvent(KillApplication)
	assert.Assert(t, err == nil)
	assert.Equal(t, appInfo.GetApplicationState(), Killed.String())

	// killed to killed
	err = appInfo.HandleApplicationEvent(KillApplication)
	assert.Assert(t, err == nil)
	assert.Equal(t, appInfo.GetApplicationState(), Killed.String())
}
