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
	"regexp"
	"strings"

	"go.uber.org/zap"

	"github.com/apache/incubator-yunikorn-core/pkg/log"
)

const (
	WildCard  = "*"
	Separator = ","
	Space     = " "
)

// User and group regexp, must allow at least what we allow in the config checks
// See configs.UserNameRegExp and configs.GroupRegExp in the config validator.
var userNameRegExp = regexp.MustCompile("^[_a-zA-Z][a-zA-Z0-9_.@-]*[$]?$")
var groupRegExp = regexp.MustCompile("^[_a-zA-Z][a-zA-Z0-9_-]*$")

type ACL struct {
	users      map[string]bool
	groups     map[string]bool
	allAllowed bool
}

// the ACL allows all access, set the flag
func (a *ACL) setAllAllowed(part string) {
	part = strings.TrimSpace(part)
	a.allAllowed = part == WildCard
}

// set the user list in the ACL, invalid user names are ignored
func (a *ACL) setUsers(userList []string) {
	a.users = make(map[string]bool)
	// list could be empty
	if len(userList) == 0 {
		return
	}
	// special case if the user list is just the wildcard
	if len(userList) == 1 && userList[0] == WildCard {
		log.Logger().Info("user list is wildcard, allowing all access")
		a.allAllowed = true
		return
	}
	// add all users to the map
	for _, user := range userList {
		// skip an empty user (happens if ACL is just groups)
		if user == "" {
			continue
		}
		// check the users validity
		if userNameRegExp.MatchString(user) {
			a.users[user] = true
		} else {
			log.Logger().Info("ignoring user in ACL definition",
				zap.String("user", user))
		}
	}
}

// set the group list in the ACL, invalid group names are ignored
func (a *ACL) setGroups(groupList []string) {
	a.groups = make(map[string]bool)
	// list could be empty
	if len(groupList) == 0 {
		return
	}
	// special case if the wildcard was already set
	if a.allAllowed {
		log.Logger().Info("ignoring group list in ACL: wildcard set")
		return
	}
	if len(groupList) == 1 && groupList[0] == WildCard {
		log.Logger().Info("group list is wildcard, allowing all access")
		a.users = make(map[string]bool)
		a.allAllowed = true
		return
	}
	// add all groups to the map
	for _, group := range groupList {
		// skip an empty group (happens if ACL is just users and ends in space)
		if group == "" {
			continue
		}
		// check the group validity
		if groupRegExp.MatchString(group) {
			a.groups[group] = true
		} else {
			log.Logger().Info("ignoring group in ACL",
				zap.String("group", group))
		}
	}
}

// create a new ACL from scratch
func NewACL(aclStr string) (ACL, error) {
	acl := ACL{}
	if aclStr == "" {
		return acl, nil
	}
	// before trimming check
	// should have no more than two groups defined
	fields := strings.Split(aclStr, Space)
	if len(fields) > 2 {
		return acl, fmt.Errorf("multiple spaces found in ACL: '%s'", aclStr)
	}
	// trim and check for wildcard
	acl.setAllAllowed(aclStr)
	// an empty ACL is a deny
	if len(fields) == 0 {
		return acl, nil
	}
	// parse users and groups
	acl.setUsers(strings.Split(fields[0], Separator))
	if len(fields) == 2 {
		acl.setGroups(strings.Split(fields[1], Separator))
	}
	return acl, nil
}

// Check if the user has access
func (a ACL) CheckAccess(userObj UserGroup) bool {
	// shortcut allow all
	if a.allAllowed {
		return true
	}
	// if the ACL is not the wildcard we have non nil lists
	// check user access
	if a.users[userObj.User] {
		return true
	}
	// get groups for the user and check them
	for _, group := range userObj.Groups {
		if a.groups[group] {
			return true
		}
	}
	return false
}
