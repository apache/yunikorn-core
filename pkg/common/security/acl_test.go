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
	"errors"
	"fmt"
	"testing"
)

func IsSameACL(got, expected ACL) error {
	if got.allAllowed != expected.allAllowed {
		return errors.New("allAllowed is not same")
	}
	if expected.users != nil && got.users != nil {
		for username, expectedAllow := range expected.users {
			if gotAllow, ok := got.users[username]; !ok {
				return fmt.Errorf("username %s does not exist", username)
			} else if expectedAllow != gotAllow {
				return fmt.Errorf("username %s is not same", username)
			}
		}
	} else if (expected.users != nil && got.users == nil) || (expected.users == nil && got.users != nil) {
		return errors.New("users of an acl is not nil, expect one and got one should be same")
	}

	if expected.groups != nil && got.groups != nil {
		for groupname, expectedAllow := range expected.groups {
			if gotAllow, ok := got.groups[groupname]; !ok {
				return fmt.Errorf("groupname %s does not exist", groupname)
			} else if expectedAllow != gotAllow {
				return fmt.Errorf("groupname %s is not same", groupname)
			}
		}
	} else if (expected.groups != nil && got.groups == nil) || (expected.groups == nil && got.groups != nil) {
		return errors.New("groups of an acl is not nil, expect one and got one should be same")
	}
	return nil
}

func TestACLCreate(t *testing.T) {
	tests := []struct {
		input    string
		expected ACL
	}{
		{
			"",
			ACL{allAllowed: false}},
		{
			" ",
			ACL{users: make(map[string]bool), groups: make(map[string]bool), allAllowed: false}},
		{
			"user1",
			ACL{users: map[string]bool{"user1": true}, allAllowed: false}},
		{
			"user1,user2",
			ACL{users: map[string]bool{"user1": true, "user2": true}, allAllowed: false}},
		{
			"user1,user2 ",
			ACL{users: map[string]bool{"user1": true, "user2": true}, groups: make(map[string]bool), allAllowed: false}},
		{
			"user1,user2 group1",
			ACL{users: map[string]bool{"user1": true, "user2": true}, groups: map[string]bool{"group1": true}, allAllowed: false},
		},
		{
			"user1,user2 group1,group2",
			ACL{users: map[string]bool{"user1": true, "user2": true}, groups: map[string]bool{"group1": true, "group2": true}, allAllowed: false},
		},
		{
			"user2 group1,group2",
			ACL{users: map[string]bool{"user2": true}, groups: map[string]bool{"group1": true, "group2": true}, allAllowed: false},
		},
		{
			" group1,group2",
			ACL{users: make(map[string]bool), groups: map[string]bool{"group1": true, "group2": true}, allAllowed: false},
		},
		{
			"* group1,group2",
			ACL{users: make(map[string]bool), groups: make(map[string]bool), allAllowed: true},
		},
		{
			"user1,user2 *",
			ACL{users: make(map[string]bool), groups: make(map[string]bool), allAllowed: true},
		},
		{
			"*",
			ACL{users: make(map[string]bool), allAllowed: true},
		},
		{
			"* ",
			ACL{users: make(map[string]bool), groups: make(map[string]bool), allAllowed: true},
		},
		{
			" *",
			ACL{users: make(map[string]bool), groups: make(map[string]bool), allAllowed: true},
		},
		{
			"dotted.user",
			ACL{users: map[string]bool{"dotted.user": true}, allAllowed: false},
		},
		{
			"user,user",
			ACL{users: map[string]bool{"user": true}, allAllowed: false},
		},
		{
			" dotted.group",
			ACL{users: make(map[string]bool), groups: make(map[string]bool), allAllowed: false},
		},
		{
			" group,group",
			ACL{users: make(map[string]bool), groups: map[string]bool{"group": true}, allAllowed: false},
		},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := NewACL(tt.input)
			if err != nil {
				t.Errorf("parsing failed for string: %s", tt.input)
			}

			if err = IsSameACL(got, tt.expected); err != nil {
				t.Error(err.Error())
			}
		})
	}
}

func TestNewACLErrorCase(t *testing.T) {
	tests := []struct {
		caseName string
		acl      string
	}{
		{"spaces", "  "},
		{"number of spaces is higher than 1 is not allowed", " a b "},
	}
	for _, tt := range tests {
		t.Run(tt.caseName, func(t *testing.T) {
			if _, err := NewACL(tt.acl); err == nil {
				t.Errorf("parsing %s string should be failed", tt.acl)
			}
		})
	}
}

func TestACLAccess(t *testing.T) {
	tests := []struct {
		acl      string
		visitor  UserGroup
		expected bool
	}{
		{
			"user1,user2 group1,group2",
			UserGroup{User: "", Groups: nil},
			false,
		},
		{
			"user1,user2 group1,group2",
			UserGroup{User: "user1", Groups: nil},
			true,
		},
		{
			"user1,user2 group1,group2",
			UserGroup{User: "user3", Groups: []string{"group1"}},
			true,
		},
		{
			"user1,user2 group1,group2",
			UserGroup{User: "user3", Groups: []string{"group3", "group1"}},
			true,
		},
		{
			"user1,user2 group1,group2",
			UserGroup{User: "user3", Groups: []string{"group3"}},
			false,
		},
		{
			"*",
			UserGroup{User: "", Groups: nil},
			true,
		},
		{
			"*",
			UserGroup{User: "user1", Groups: []string{"group1"}},
			true,
		},
		{
			acl:      "",
			visitor:  UserGroup{User: "", Groups: nil},
			expected: false,
		},
		{
			"",
			UserGroup{User: "user1", Groups: []string{"group1"}},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("vistor %v, acl %s", tt.visitor, tt.acl), func(t *testing.T) {
			acl, err := NewACL(tt.acl)
			if err != nil {
				t.Error(err.Error())
			}
			if pass := acl.CheckAccess(tt.visitor); pass != tt.expected {
				t.Errorf("allow expect:%v, got %v", tt.expected, pass)
			}
		})
	}
}
