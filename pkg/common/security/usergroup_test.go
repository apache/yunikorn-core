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
	"strings"
	"testing"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

func TestGetUserGroupCache(t *testing.T) {
	// get the cache with the test resolver set
	testCache := GetUserGroupCache("test")
	if testCache == nil {
		t.Fatal("Cache create failed")
	}
	if len(testCache.ugs) != 0 {
		t.Errorf("Cache not empty: %v", testCache.ugs)
	}
}

func TestGetUserGroup(t *testing.T) {
	testCache := GetUserGroupCache("test")
	testCache.resetCache()
	// test cache should be empty now
	if len(testCache.ugs) != 0 {
		t.Fatalf("Cache not empty: %v", testCache.ugs)
	}

	ug, err := testCache.GetUserGroup("testuser1")
	assert.NilError(t, err, "Lookup should not have failed: testuser1")
	if ug.failed {
		t.Errorf("lookup failed which should not have: %t", ug.failed)
	}
	if len(testCache.ugs) != 1 {
		t.Errorf("Cache not updated should have 1 entry %d", len(testCache.ugs))
	}
	// check returned info: primary and secondary groups etc
	const Testuser1 = "testuser1"
	if ug.User != Testuser1 || len(ug.Groups) != 2 || ug.resolved == 0 || ug.failed {
		t.Errorf("User 'testuser1' not resolved correctly: %v", ug)
	}
	cachedUG := testCache.ugs["testuser1"]
	if ug.resolved != cachedUG.resolved {
		t.Errorf("User 'testuser1' not cached correctly resolution time differs: %d got %d", ug.resolved, cachedUG.resolved)
	}
	// click over the clock: if we do not get the cached version the new time will differ from the cache update
	cachedUG.resolved -= 5
	ug, err = testCache.GetUserGroup("testuser1")
	if err != nil || ug.resolved != cachedUG.resolved {
		t.Errorf("User 'testuser1' not returned from Cache, resolution time differs: %d got %d (err = %v)", ug.resolved, cachedUG.resolved, err)
	}
}

func TestBrokenUserGroup(t *testing.T) {
	testCache := GetUserGroupCache("test")
	testCache.resetCache()
	// test cache should be empty now
	if len(testCache.ugs) != 0 {
		t.Fatalf("Cache not empty: %v", testCache.ugs)
	}

	ug, err := testCache.GetUserGroup("testuser2")
	if err != nil {
		t.Error("Lookup should not have failed: testuser2")
	}
	if len(testCache.ugs) != 1 {
		t.Errorf("Cache not updated should have 1 entry %d", len(testCache.ugs))
	}
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
	if len(testCache.ugs) != 2 {
		t.Errorf("Cache not updated should have 2 entries %d", len(testCache.ugs))
	}
	// check returned info: 4 groups, primary group is duplicate
	if len(ug.Groups) != 4 {
		t.Errorf("User 'testuser3' not resolved correctly: duplicate primary group not filtered %v", ug)
	}
}

func TestGetUserGroupFail(t *testing.T) {
	testCache := GetUserGroupCache("test")
	testCache.resetCache()
	// test cache should be empty now
	if len(testCache.ugs) != 0 {
		t.Fatalf("Cache not empty: %v", testCache.ugs)
	}

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
	if len(testCache.ugs) != 0 {
		t.Fatalf("Cache not empty: %v", testCache.ugs)
	}

	// resolve an existing user
	_, err := testCache.GetUserGroup("testuser1")
	if err != nil {
		t.Error("Lookup should not have failed: testuser1 user")
	}
	_, err = testCache.GetUserGroup("testuser2")
	if err != nil {
		t.Error("Lookup should not have failed: testuser2 user")
	}

	ug := testCache.ugs["testuser1"]
	if ug.failed {
		t.Error("User 'testuser1' not resolved as a success")
	}
	// expire the successful lookup
	ug.resolved -= 2 * poscache

	// resolve a non existing user
	_, err = testCache.GetUserGroup("unknown")
	if err == nil {
		t.Error("Lookup should have failed: unknown user")
	}
	ug = testCache.ugs["unknown"]
	if !ug.failed {
		t.Error("User 'unknown' not resolved as a failure")
	}
	// expire the failed lookup
	ug.resolved -= 2 * negcache

	testCache.cleanUpCache()
	if len(testCache.ugs) != 1 {
		t.Errorf("Cache not cleaned up : %v", testCache.ugs)
	}
}

func TestConvertUGI(t *testing.T) {
	testCache := GetUserGroupCache("test")
	testCache.resetCache()
	// test cache should be empty now
	if len(testCache.ugs) != 0 {
		t.Fatalf("Cache not empty: %v", testCache.ugs)
	}
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
}
