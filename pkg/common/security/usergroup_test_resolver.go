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
	"os/user"
	"strconv"
	"time"
)

// Get the cache with a test resolver
// cleaner runs every second
func GetUserGroupCacheTest() *UserGroupCache {
	return &UserGroupCache{
		ugs:           map[string]*UserGroup{},
		interval:      time.Second,
		lookup:        lookup,
		lookupGroupID: lookupGroupID,
		groupIds:      groupIds,
	}
}

// test function only
func lookup(userName string) (*user.User, error) {
	// 1st test user: all OK
	if userName == "testuser1" {
		return &user.User{
			Uid:      "1000",
			Gid:      "1000",
			Username: "testuser1",
		}, nil
	}
	// 2nd test user: primary group does not resolve
	if userName == "testuser2" {
		return &user.User{
			Uid:      "100",
			Gid:      "100",
			Username: "testuser2",
		}, nil
	}
	if userName == "testuser3" {
		return &user.User{
			Uid:      "1001",
			Gid:      "1001",
			Username: "testuser3",
		}, nil
	}
	// all other users fail
	return nil, fmt.Errorf("lookup failed for user: %s", userName)
}

// test function only
func lookupGroupID(gid string) (*user.Group, error) {
	gID, err := strconv.Atoi(gid)
	if err != nil {
		return nil, err
	}
	// fail all groups under 1000
	if gID < 1000 {
		return nil, fmt.Errorf("lookup failed for group: %s", gid)
	}
	// fixed return: group + id as the name
	group := user.Group{Gid: gid}
	group.Name = "group" + gid
	return &group, nil
}

// test function only
func groupIds(osUser *user.User) ([]string, error) {
	if osUser.Username == "testuser1" {
		return []string{"1001"}, nil
	}
	if osUser.Username == "testuser2" {
		return []string{"1001", "1002"}, nil
	}
	// group list might return primary group ID also
	if osUser.Username == "testuser3" {
		return []string{"1002", "1001", "1003", "1004"}, nil
	}
	return nil, fmt.Errorf("lookup failed for user: %s", osUser.Username)
}
