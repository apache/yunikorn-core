/*
Copyright 2019 Cloudera, Inc.  All rights reserved.

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

package security

import (
    "testing"
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
    if err != nil || len(acl.users) > 0 {
        t.Errorf("parsing failed for string: 'dotted.user' acl has user list: %v", acl)
    }
    acl, err = NewACL("user,user")
    if err != nil || len(acl.users) != 1 {
        t.Errorf("parsing failed for string: 'user,user' acl has incorrect user list: %v", acl)
    }
    acl, err = NewACL(" group,group")
    if err != nil || len(acl.groups) != 1 {
        t.Errorf("parsing failed for string: 'group,group' acl has incorrect group list: %v", acl)
    }
}
