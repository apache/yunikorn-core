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
	"os/user"
	"time"
)

// Get the cache without a resolver.
// In k8shim we currently have internal users to K8s which might not resolve against anything.
// Just echo the object in the correct format based on the user passed in.
func GetUserGroupNoResolve() *UserGroupCache {
	return &UserGroupCache{
		ugs:           map[string]*UserGroup{},
		interval:      cleanerInterval * time.Second,
		lookup:        noLookupUser,
		lookupGroupID: noLookupGroupID,
		groupIds:      noLookupGroupIds,
		stop:          make(chan struct{}),
	}
}

// Default linux behaviour: a user is member of the primary group with the same name
func noLookupUser(userName string) (*user.User, error) {
	return &user.User{
		Uid:      "-1",
		Gid:      userName,
		Username: userName,
	}, nil
}

// Echo the group as it comes in
func noLookupGroupID(gid string) (*user.Group, error) {
	group := user.Group{Gid: gid}
	group.Name = gid
	return &group, nil
}

// No further groups returned just the primary group
func noLookupGroupIds(osUser *user.User) ([]string, error) {
	return []string{}, nil
}
