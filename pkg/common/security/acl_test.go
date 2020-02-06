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
	"testing"

	"gotest.tools/assert"
)

func TestACLCreate(t *testing.T) {
	_, err := NewACL("")
	if err != nil {
		t.Errorf("parsing failed for string: ''")
	}
	_, err = NewACL(" ")
	if err != nil {
		t.Errorf("parsing failed for string: ' '")
	}
	_, err = NewACL("user1")
	if err != nil {
		t.Errorf("parsing failed for string: 'user1'")
	}
	_, err = NewACL("user1,user2")
	if err != nil {
		t.Errorf("parsing failed for string: 'user1,user2'")
	}
	_, err = NewACL("user1,user2 ")
	if err != nil {
		t.Errorf("parsing failed for string: 'user1,user2 '")
	}
	_, err = NewACL("user1,user2 group1")
	if err != nil {
		t.Errorf("parsing failed for string: 'user1,user2 group1'")
	}
	_, err = NewACL("user1,user2 group1,group2")
	if err != nil {
		t.Errorf("parsing failed for string: 'user1,user2 group1,group2'")
	}
	_, err = NewACL("user2 group1,group2")
	if err != nil {
		t.Errorf("parsing failed for string: 'user2 group1,group2'")
	}
	_, err = NewACL(" group1,group2")
	if err != nil {
		t.Errorf("parsing failed for string: ' group1,group2'")
	}
	_, err = NewACL("* group1,group2")
	if err != nil {
		t.Errorf("parsing failed for string: '* group1,group2'")
	}
	_, err = NewACL("user1,user2 *")
	if err != nil {
		t.Errorf("parsing failed for string: 'user1,user2 *'")
	}
	_, err = NewACL("*")
	if err != nil {
		t.Errorf("parsing failed for string: '*'")
	}
	_, err = NewACL("* ")
	if err != nil {
		t.Errorf("parsing failed for string: '* '")
	}
	_, err = NewACL(" *")
	if err != nil {
		t.Errorf("parsing failed for string: ' *'")
	}
}

func TestACLSpecialCase(t *testing.T) {
	acl, err := NewACL("  ")
	if err == nil {
		t.Errorf("parsing passed for string: '  '")
	}

	acl, err = NewACL("dotted.user")
	if err != nil || len(acl.users) != 1 {
		t.Errorf("parsing failed for string: 'dotted.user' acl has incorrect user list: %v", acl)
	}
	acl, err = NewACL("user,user")
	if err != nil || len(acl.users) != 1 {
		t.Errorf("parsing failed for string: 'user,user' acl has incorrect user list: %v", acl)
	}
	acl, err = NewACL(" dotted.group")
	if err != nil || len(acl.groups) > 0 {
		t.Errorf("parsing failed for string: ' dotted.group' acl has incorrect group list: %v", acl)
	}
	acl, err = NewACL(" group,group")
	if err != nil || len(acl.groups) != 1 {
		t.Errorf("parsing failed for string: 'group,group' acl has incorrect group list: %v", acl)
	}
}

func TestACLAccess(t *testing.T) {
	acl, err := NewACL("user1,user2 group1,group2")
	if err != nil {
		t.Fatalf("parsing failed for string: 'user1,user2 group1,group2'")
	}
	user := UserGroup{User: "", Groups: nil}
	assert.Assert(t, !acl.CheckAccess(user), "no user, should have been denied")
	user = UserGroup{User: "user1", Groups: nil}
	assert.Assert(t, acl.CheckAccess(user), "user1 (no groups) should have been allowed")
	user = UserGroup{User: "user3", Groups: []string{"group1"}}
	assert.Assert(t, acl.CheckAccess(user), "group1 should have been allowed")
	user = UserGroup{User: "user3", Groups: []string{"group3", "group1"}}
	assert.Assert(t, acl.CheckAccess(user), "group1 (2nd group) should have been allowed")
	user = UserGroup{User: "user3", Groups: []string{"group3"}}
	assert.Assert(t, !acl.CheckAccess(user), "user3/group3 should have been denied")

	acl, err = NewACL("*")
	if err != nil {
		t.Fatalf("parsing failed for string: '*'")
	}
	user = UserGroup{User: "", Groups: nil}
	assert.Assert(t, acl.CheckAccess(user), "no user, wildcard should have been allowed")
	user = UserGroup{User: "user1", Groups: []string{"group1"}}
	assert.Assert(t, acl.CheckAccess(user), "user1/group1, wildcard should have been allowed")

	acl, err = NewACL("")
	if err != nil {
		t.Fatalf("parsing failed for string: ''")
	}
	user = UserGroup{User: "", Groups: nil}
	assert.Assert(t, !acl.CheckAccess(user), "no user, empty ACL always deny")
	user = UserGroup{User: "user1", Groups: []string{"group1"}}
	assert.Assert(t, !acl.CheckAccess(user), "user1/group1, empty ACL always deny")
}
