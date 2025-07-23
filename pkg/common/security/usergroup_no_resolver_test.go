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
	"testing"

	"gotest.tools/v3/assert"
)

func TestNoLookupUser(t *testing.T) {
	// Test with various usernames
	testCases := []struct {
		name     string
		username string
	}{
		{"Empty username", ""},
		{"Standard username", "testuser"},
		{"Username with special chars", "test-user_123"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			u, err := noLookupUser(tc.username)

			// Should never return error
			assert.NilError(t, err)

			// Verify user properties
			assert.Equal(t, tc.username, u.Username)
			assert.Equal(t, "-1", u.Uid)
			assert.Equal(t, tc.username, u.Gid)
		})
	}
}

func TestNoLookupGroupID(t *testing.T) {
	// Test with various group IDs
	testCases := []struct {
		name string
		gid  string
	}{
		{"Empty GID", ""},
		{"Numeric GID", "1000"},
		{"String GID", "users"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			g, err := noLookupGroupID(tc.gid)

			// Should never return error
			assert.NilError(t, err)

			// Verify group properties
			assert.Equal(t, tc.gid, g.Gid)
			assert.Equal(t, tc.gid, g.Name)
		})
	}
}

func TestNoLookupGroupIds(t *testing.T) {
	// Test with various users
	testCases := []struct {
		name string
		user *user.User
	}{
		{"Standard user", &user.User{Username: "testuser", Uid: "1000", Gid: "1000"}},
		{"Empty user", &user.User{}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			groups, err := noLookupGroupIds(tc.user)

			// Should never return error
			assert.NilError(t, err)

			// Should always return empty slice
			assert.Equal(t, 0, len(groups))
		})
	}
}
