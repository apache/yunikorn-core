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

package security

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/common"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

func (c *UserGroupCache) getUGsize() int {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return len(c.ugs)
}

func (c *UserGroupCache) getUGGroupSize(user string) int {
	c.lock.RLock()
	defer c.lock.RUnlock()
	ug := c.ugs[user]
	return len(ug.Groups)
}

func (c *UserGroupCache) getUGmap() map[string]*UserGroup {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.ugs
}

func TestGetUserGroupCache(t *testing.T) {
	// get the cache with the test resolver set
	testCache := GetUserGroupCache("test")
	assert.Assert(t, testCache != nil, "Cache create failed")
	assert.Equal(t, 0, testCache.getUGsize(), "Cache is not empty: %v", testCache.getUGmap())

	testCache.Stop()
	assert.Assert(t, instance == nil, "instance should be nil")
	assert.Assert(t, stopped.Load())

	// get the cache with the os resolver set
	testCache = GetUserGroupCache("os")
	assert.Assert(t, testCache != nil, "Cache create failed")
	assert.Equal(t, 0, testCache.getUGsize(), "Cache is not empty: %v", testCache.getUGmap())

	testCache.Stop()
	assert.Assert(t, instance == nil, "instance should be nil")
	assert.Assert(t, stopped.Load())

	// get the cache with the default resolver set
	testCache = GetUserGroupCache("unknown")
	assert.Assert(t, testCache != nil, "Cache create failed")
	assert.Equal(t, 0, testCache.getUGsize(), "Cache is not empty: %v", testCache.getUGmap())

	testCache.Stop()
	assert.Assert(t, instance == nil, "instance should be nil")
	assert.Assert(t, stopped.Load())

	// test for re stop again
	testCache.Stop()
	assert.Assert(t, instance == nil, "instance should be nil")
	assert.Assert(t, stopped.Load())
}

func TestGetUserGroup(t *testing.T) {
	testCache := GetUserGroupCache("test")
	testCache.resetCache()
	// test cache should be empty now
	assert.Equal(t, 0, testCache.getUGsize(), "Cache is not empty: %v", testCache.getUGmap())

	ug, err := testCache.GetUserGroup("testuser1")
	assert.NilError(t, err, "Lookup should not have failed: testuser1")
	if ug.failed {
		t.Errorf("lookup failed which should not have: %t", ug.failed)
	}
	if len(testCache.ugs) != 1 {
		t.Errorf("Cache not updated should have 1 entry %d", len(testCache.ugs))
	}
	// check returned info: primary and secondary groups etc
	if ug.User != "testuser1" || len(ug.Groups) != 2 || ug.resolved == 0 || ug.failed {
		t.Errorf("User 'testuser1' not resolved correctly: %v", ug)
	}
	testCache.lock.Lock()
	cachedUG := testCache.ugs["testuser1"]
	if ug.resolved != cachedUG.resolved {
		t.Errorf("User 'testuser1' not cached correctly resolution time differs: %d got %d", ug.resolved, cachedUG.resolved)
	}
	// click over the clock: if we do not get the cached version the new time will differ from the cache update
	cachedUG.resolved -= 5
	testCache.lock.Unlock()

	ug, err = testCache.GetUserGroup("testuser1")
	if err != nil || ug.resolved != cachedUG.resolved {
		t.Errorf("User 'testuser1' not returned from Cache, resolution time differs: %d got %d (err = %v)", ug.resolved, cachedUG.resolved, err)
	}
}

func TestBrokenUserGroup(t *testing.T) {
	testCache := GetUserGroupCache("test")
	testCache.resetCache()
	// test cache should be empty now
	assert.Equal(t, 0, testCache.getUGsize(), "Cache is not empty: %v", testCache.getUGmap())

	ug, err := testCache.GetUserGroup("testuser2")
	if err != nil {
		t.Error("Lookup should not have failed: testuser2")
	}

	assert.Equal(t, 1, testCache.getUGsize(), "Cache not updated should have 1 entry %d", testCache.getUGmap())
	// check returned info: 3 groups etc
	if ug.User != "testuser2" || len(ug.Groups) != 3 || ug.resolved == 0 || ug.failed {
		t.Errorf("User 'testuser2' not resolved correctly: %v", ug)
	}
	// first group should have failed resolution: just the ID expected
	if ug.Groups[0] != "100" {
		t.Errorf("User 'testuser2' primary group resolved while it should not: %v", ug)
	}

	ug, err = testCache.GetUserGroup("testuser3")
	if err != nil {
		t.Error("Lookup should not have failed: testuser3")
	}

	assert.Equal(t, 2, testCache.getUGsize(), "Cache not updated should have 2 entries %d", len(testCache.ugs))
	assert.Equal(t, 4, testCache.getUGGroupSize("testuser3"), "User 'testuser3' not resolved correctly: duplicate primary group not filtered %v", ug)

	ug, err = testCache.GetUserGroup("unknown")
	assert.ErrorContains(t, err, "lookup failed for user: unknown")

	ug, err = testCache.GetUserGroup("testuser4")
	assert.NilError(t, err)

	ug, err = testCache.GetUserGroup("testuser5")
	assert.ErrorContains(t, err, "lookup failed for user: testuser5")

	ug, err = testCache.GetUserGroup("invalid-gid-user")
	assert.ErrorContains(t, err, "lookup failed for user: invalid-gid-user")
	exceptedGroup := []string{"1_001"}
	assert.Assert(t, reflect.DeepEqual(ug.Groups, exceptedGroup), fmt.Errorf("group should be: %v, but got: %v", exceptedGroup, ug.Groups))
}

func TestGetUserGroupFail(t *testing.T) {
	testCache := GetUserGroupCache("test")
	testCache.resetCache()
	// test cache should be empty now
	assert.Equal(t, 0, testCache.getUGsize(), "Cache is not empty: %v", testCache.getUGmap())

	// resolve an empty user
	ug, err := testCache.GetUserGroup("")
	if err == nil {
		t.Error("Lookup should have failed: empty user")
	}
	// ug is empty everything should be nil..
	if ug.User != "" || len(ug.Groups) != 0 || ug.resolved != 0 || ug.failed {
		t.Errorf("UserGroup is not empty: %v", ug)
	}

	// resolve a non existing user
	ug, err = testCache.GetUserGroup("unknown")
	if err == nil {
		t.Error("Lookup should have failed: unknown user")
	}
	// ug is partially filled and failed flag is set
	if ug.User != "unknown" || len(ug.Groups) != 0 || !ug.failed {
		t.Errorf("UserGroup is not empty: %v", ug)
	}

	ug, err = testCache.GetUserGroup("unknown")
	if err == nil {
		t.Error("Lookup should have failed: unknown user")
	}
	// ug is partially filled and failed flag is set: error message should show that the cache was returned
	if err != nil && !strings.Contains(err.Error(), "cached data returned") {
		t.Errorf("UserGroup not returned from Cache: %v, error: %v", ug, err)
	}
}

func TestCacheCleanUp(t *testing.T) {
	testCache := GetUserGroupCache("test")
	testCache.resetCache()
	// test cache should be empty now
	assert.Equal(t, 0, testCache.getUGsize(), "Cache is not empty: %v", testCache.getUGmap())

	// resolve an existing user
	_, err := testCache.GetUserGroup("testuser1")
	if err != nil {
		t.Error("Lookup should not have failed: testuser1 user")
	}
	_, err = testCache.GetUserGroup("testuser2")
	if err != nil {
		t.Error("Lookup should not have failed: testuser2 user")
	}

	testCache.lock.Lock()
	ug := testCache.ugs["testuser1"]
	if ug.failed {
		t.Error("User 'testuser1' not resolved as a success")
	}
	// expire the successful lookup
	ug.resolved -= 2 * poscache
	testCache.lock.Unlock()

	// resolve a non existing user
	_, err = testCache.GetUserGroup("unknown")
	if err == nil {
		t.Error("Lookup should have failed: unknown user")
	}
	testCache.lock.Lock()
	ug = testCache.ugs["unknown"]
	if !ug.failed {
		t.Error("User 'unknown' not resolved as a failure")
	}
	// expire the failed lookup
	ug.resolved -= 2 * negcache
	testCache.lock.Unlock()

	testCache.cleanUpCache()
	assert.Equal(t, 1, testCache.getUGsize(), "Cache is not empty: %v", testCache.getUGmap())
}

func TestIntervalCacheCleanUp(t *testing.T) {
	testCache := GetUserGroupCache("test")
	testCache.resetCache()
	// test cache should be empty now
	assert.Equal(t, 0, testCache.getUGsize(), "Cache is not empty: %v", testCache.getUGmap())

	// resolve an existing user
	user1ug, err := testCache.GetUserGroup("testuser1")
	assert.NilError(t, err, "Lookup should not have failed: testuser1 user")
	assert.Assert(t, !user1ug.failed, "User 'testuser1' not resolved as a success")

	_, err = testCache.GetUserGroup("testuser2")
	assert.NilError(t, err, "Lookup should not have failed: testuser1 user")

	// expire the successful lookup
	testCache.lock.Lock()
	ug := testCache.ugs["testuser1"]
	ug.resolved -= 2 * poscache

	testCache.lock.Unlock()
	// resolve a non existing user
	_, err = testCache.GetUserGroup("unknown")
	assert.Assert(t, err != nil, "Lookup should have failed: unknown user")
	testCache.lock.Lock()
	ug = testCache.ugs["unknown"]
	assert.Assert(t, ug.failed, "User 'unknown' not resolved as a failure")

	// expire the failed lookup
	ug.resolved -= 2 * negcache
	testCache.lock.Unlock()

	// sleep to wait for interval, it will trigger cleanUpCache
	time.Sleep(testCache.interval + time.Second)
	assert.Equal(t, 1, testCache.getUGsize(), "Cache not cleaned up : %v", testCache.getUGmap())
}

func TestConvertUGI(t *testing.T) {
	testCache := GetUserGroupCache("test")
	testCache.resetCache()
	// test cache should be empty now
	assert.Equal(t, 0, testCache.getUGsize(), "Cache is not empty: %v", testCache.getUGmap())

	ugi := &si.UserGroupInformation{
		User:   "",
		Groups: nil,
	}
	ug, err := testCache.ConvertUGI(ugi, false)
	if err == nil {
		t.Errorf("empty user convert should have failed and did not: %v", ug)
	}
	// try known user without groups
	ugi.User = "testuser1"
	ug, err = testCache.ConvertUGI(ugi, false)
	if err != nil {
		t.Errorf("known user, no groups, convert should not have failed: %v", err)
	}
	if ug.User != "testuser1" || len(ug.Groups) != 2 || ug.resolved == 0 || ug.failed {
		t.Errorf("User 'testuser1' not resolved correctly: %v", ug)
	}
	// try unknown user without groups
	ugi.User = "unknown"
	ug, err = testCache.ConvertUGI(ugi, false)
	if err == nil {
		t.Errorf("unknown user, no groups, convert should have failed: %v", ug)
	}
	// try empty user when forced
	ugi.User = ""
	ug, err = testCache.ConvertUGI(ugi, true)
	if err != nil {
		t.Errorf("empty user but forced, convert should not have failed: %v", err)
	}
	// try unknown user with groups
	ugi.User = "unknown2"
	group := "passedin"
	ugi.Groups = []string{group}
	ug, err = testCache.ConvertUGI(ugi, false)
	if err != nil {
		t.Errorf("unknown user with groups, convert should not have failed: %v", err)
	}
	if ug.User != "unknown2" || len(ug.Groups) != 1 || ug.resolved == 0 || ug.failed {
		t.Fatalf("User 'unknown2' not resolved correctly: %v", ug)
	}
	if ug.Groups[0] != group {
		t.Errorf("groups not initialised correctly on convert: expected '%s' got '%s'", group, ug.Groups[0])
	}
	// try valid username with groups
	ugi.User = "validuserABCD1234@://#"
	ugi.Groups = []string{group}
	ug, err = testCache.ConvertUGI(ugi, false)
	if err != nil {
		t.Errorf("valid username with groups, convert should not have failed: %v", err)
	}
	// try invalid username with groups
	ugi.User = "invaliduser><+"
	ugi.Groups = []string{group}
	ug, err = testCache.ConvertUGI(ugi, false)
	if err == nil {
		t.Errorf("invalid username, convert should have failed: %v", err)
	}

	// try unknown user with empty group when forced
	ugi.User = "unknown"
	ugi.Groups = []string{}
	ug, err = testCache.ConvertUGI(ugi, true)
	exceptedGroup := []string{common.AnonymousGroup}
	assert.Assert(t, reflect.DeepEqual(ug.Groups, exceptedGroup), "group should be: %v, but got: %v", exceptedGroup, ug.Groups)
	assert.NilError(t, err, "unknown user, no groups, convert should not have failed")
}
