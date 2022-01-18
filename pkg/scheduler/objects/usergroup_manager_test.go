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
	"sync"
	"sync/atomic"
	"testing"

	"gotest.tools/assert"
)

func TestUserLimitsConcurrently(t *testing.T) {
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "failed to create root queue with limit")
	var queue1 *Queue
	queue1, err = createManagedQueue(root, "queue1", false, map[string]string{"cpu": "10"})
	assert.NilError(t, err, "failed to create queue1 queue")

	count := 1000
	user := "testuser"
	userGroupManager := NewUserGroupManager(queue1)
	userA := NewUser(user)
	userA.SetMaxApplications(int32(count + 1))
	userGroupManager.AddUserIfAbsent(userA)

	// writers
	var wg sync.WaitGroup
	wg.Add(count)
	for i := 1; i <= count; i++ {
		go func() {
			userGroupManager.GetUser(user).IncRunningApplications()
			wg.Done()
		}()
	}

	// readers
	var wgRead sync.WaitGroup
	readCount := count * 2
	wgRead.Add(readCount)
	for i := 1; i <= readCount; i++ {
		go func() {
			assert.DeepEqual(t, userGroupManager.GetUser(user).IsRunningAppsUnderLimit(), true)
			if !userGroupManager.GetUser(user).IsRunningAppsUnderLimit() {
				t.Error("Not yet reached user limit. Should allow user to run apps.")
			}
			wgRead.Done()
		}()
	}

	// ensures both readers and writers happens randomly and concurrently at some point
	wg.Wait()
	wgRead.Wait()
}

func TestUserLimitsExceedingMaxConcurrently(t *testing.T) {
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "failed to create root queue with limit")
	var queue1 *Queue
	queue1, err = createManagedQueue(root, "queue1", false, map[string]string{"cpu": "10"})
	assert.NilError(t, err, "failed to create queue1 queue")

	count := 1000
	user := "testuser"
	userGroupManager := NewUserGroupManager(queue1)
	userA := NewUser(user)
	userA.SetMaxApplications(int32(count))
	userGroupManager.AddUserIfAbsent(userA)

	// writers
	var wg sync.WaitGroup
	newc := 1010
	wg.Add(newc)

	for i := 1; i <= newc; i++ {
		go func() {
			userGroupManager.GetUser(user).IncRunningApplications()
			wg.Done()
		}()
	}

	// readers
	var wgRead sync.WaitGroup
	readCount := newc * 2
	wgRead.Add(readCount)
	var exceeded int32
	atomic.StoreInt32(&exceeded, 0)
	for i := 1; i <= readCount; i++ {
		go func() {
			if !userGroupManager.GetUser(user).IsRunningAppsUnderLimit() {
				atomic.StoreInt32(&exceeded, 1)
			}
			// once exceeded, cannot allow user to run apps.
			if atomic.LoadInt32(&exceeded) == 1 {
				if userGroupManager.GetUser(user).IsRunningAppsUnderLimit() {
					t.Error("Reached user limit. Cannot allow user to run application anymore from this moment")
				}
			}
			wgRead.Done()
		}()
	}

	// ensures both readers and writers happens randomly and concurrently at some point
	wg.Wait()
	wgRead.Wait()
}

func TestGroupLimitsConcurrently(t *testing.T) {
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "failed to create root queue with limit")
	var queue1 *Queue
	queue1, err = createManagedQueue(root, "queue1", false, map[string]string{"cpu": "10"})
	assert.NilError(t, err, "failed to create queue1 queue")

	count := 1000
	group := "testgroup"
	userGroupManager := NewUserGroupManager(queue1)
	groupA := NewGroup(group)
	groupA.SetMaxApplications(int32(count + 1))
	userGroupManager.AddGroupIfAbsent(groupA)

	// writers
	var wg sync.WaitGroup
	wg.Add(count)
	for i := 1; i <= count; i++ {
		go func() {
			userGroupManager.GetGroup(group).IncRunningApplications()
			wg.Done()
		}()
	}

	// readers
	var wgRead sync.WaitGroup
	readCount := count * 2
	wgRead.Add(readCount)
	for i := 1; i <= readCount; i++ {
		go func() {
			assert.DeepEqual(t, userGroupManager.GetGroup(group).IsRunningAppsUnderLimit(), true)
			if !userGroupManager.GetGroup(group).IsRunningAppsUnderLimit() {
				t.Error("Not yet reached group limit. Should allow users of the group to run apps.")
			}
			wgRead.Done()
		}()
	}

	// ensures both readers and writers happens randomly and concurrently at some point
	wg.Wait()
	wgRead.Wait()
}

func TestGroupLimitsExceedingMaxConcurrently(t *testing.T) {
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "failed to create root queue with limit")
	var queue1 *Queue
	queue1, err = createManagedQueue(root, "queue1", false, map[string]string{"cpu": "10"})
	assert.NilError(t, err, "failed to create queue1 queue")

	count := 1000
	group := "testgroup"
	userGroupManager := NewUserGroupManager(queue1)
	groupA := NewGroup(group)
	groupA.SetMaxApplications(int32(count))
	userGroupManager.AddGroupIfAbsent(groupA)

	// writers
	var wg sync.WaitGroup
	newc := 1010
	wg.Add(newc)

	for i := 1; i <= newc; i++ {
		go func() {
			userGroupManager.GetGroup(group).IncRunningApplications()
			wg.Done()
		}()
	}

	// readers
	var wgRead sync.WaitGroup
	readCount := newc * 2
	wgRead.Add(readCount)
	var exceeded int32
	atomic.StoreInt32(&exceeded, 0)
	for i := 1; i <= readCount; i++ {
		go func() {
			if !userGroupManager.GetGroup(group).IsRunningAppsUnderLimit() {
				atomic.StoreInt32(&exceeded, 1)
			}
			if atomic.LoadInt32(&exceeded) == 1 {
				// once exceeded, cannot allow users of group to run apps.
				if userGroupManager.GetGroup(group).IsRunningAppsUnderLimit() {
					t.Error("Reached group limit. Cannot allow users of group to run application anymore from this moment")
				}
			}
			wgRead.Done()
		}()
	}

	// ensures both readers and writers happens randomly and concurrently at some point
	wg.Wait()
	wgRead.Wait()
}
