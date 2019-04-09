/*
Copyright 2019 The Unity Scheduler Authors

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
    "fmt"
    "github.com/golang/glog"
    "regexp"
    "strings"
)

const (
    WildCard = "*"
    Separator = ","
)

var nameRegExp = regexp.MustCompile("^[a-z_]([a-z0-9_-]{0,31}|[a-z0-9_-]{0,30}\\$)$")

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
        glog.V(0).Infof("User list is wildcard, allowing all access")
        a.allAllowed = true
        return
    }
    // add all users to the map
    for _, user := range userList {
        if nameRegExp.MatchString(user) {
            a.users[user] = true
        } else {
            glog.V(0).Infof("Ignoring user %s in ACL definition ", user)
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
        glog.V(0).Infof("Ignoring group list in ACL: wildcard set")
        return
    }
    if len(groupList) == 1 && groupList[0] == WildCard {
        glog.V(0).Infof("Group list is wildcard, allowing all access")
        a.users = make(map[string]bool)
        a.allAllowed = true
        return
    }
    // add all groups to the map
    for _, group := range groupList {
        if nameRegExp.MatchString(group) {
            a.groups[group] = true
        } else {
            glog.V(0).Infof("Ignoring group %s in ACL ", group)
        }
    }
}

// create a new ACL from scratch
func NewACL(aclStr string) (*ACL, error) {
    acl := &ACL{}
    // before trimming check
    // trim and check for wildcard
    aclStr = strings.TrimSpace(aclStr)
    acl.setAllAllowed(aclStr)
    // should have no more than two groups defined
    fields := strings.Fields(aclStr)
    if len(fields) > 2 {
        return nil, fmt.Errorf("multiple spaces found in ACL: '%s'", aclStr)
    }
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
func (a *ACL) CheckAccess(user string) (bool, error) {
    // shortcut allow all
    if a.allAllowed {
        return true, nil
    }
    // if the ACL is not the wildcard we have non nil lists
    // check user access
    if a.users[user] {
        return true, nil
    }
    // get groups for the user and check them
    // TODO: resolve groups and check
    return false, nil
}