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

package placement

import (
	"reflect"
	"regexp"
	"testing"

	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-core/pkg/common/security"
	"github.com/apache/yunikorn-core/pkg/webservice/dao"
)

func TestNewFilterLists(t *testing.T) {
	// test simple no user or group: allow
	conf := configs.Filter{}
	conf.Type = filterAllow

	filter := newFilter(conf)
	if !filter.allow {
		t.Error("filter create did not set allow flag correctly from 'allow'")
	}
	if filter.userExp != nil || len(filter.userList) != 0 {
		t.Error("filter create did not set user filter correctly")
	}
	if filter.groupExp != nil || len(filter.groupList) != 0 {
		t.Error("filter create did not set group filter correctly")
	}
	if !filter.empty {
		t.Error("filter create did not create empty filter")
	}

	// test simple no user or group: deny
	conf = configs.Filter{}
	conf.Type = filterDeny

	filter = newFilter(conf)
	if filter.allow {
		t.Error("filter create did not set allow flag correctly from 'allow'")
	}
	if filter.userExp != nil || len(filter.userList) != 0 {
		t.Error("filter create did not set user filter correctly")
	}
	if filter.groupExp != nil || len(filter.groupList) != 0 {
		t.Error("filter create did not set group filter correctly")
	}
	if !filter.empty {
		t.Error("filter create did not create empty filter")
	}

	// test simple empty lists
	conf = configs.Filter{}
	conf.Type = filterAllow
	conf.Users = []string{}
	conf.Groups = []string{}

	filter = newFilter(conf)
	if !filter.allow {
		t.Error("filter create did not set allow flag correctly from 'allow'")
	}
	if filter.userExp != nil || len(filter.userList) != 0 {
		t.Error("filter create did not set user filter correctly")
	}
	if filter.groupExp != nil || len(filter.groupList) != 0 {
		t.Error("filter create did not set group filter correctly")
	}
	if !filter.empty {
		t.Error("filter create did not create empty filter")
	}

	// test simple single user or group
	conf = configs.Filter{}
	conf.Type = ""
	conf.Users = []string{"user1_a_#_b_c-d-/-e:f_@_gmail.com"}
	conf.Groups = []string{"group1_a_b_c-d-e:f_gmail.com"}

	filter = newFilter(conf)
	if !filter.allow {
		t.Error("filter create did not set allow flag correctly from empty string")
	}
	if filter.userExp != nil || len(filter.userList) != 1 {
		t.Error("filter create did not set user filter correctly single entry not regexp")
	}
	if filter.groupExp != nil || len(filter.groupList) != 1 {
		t.Error("filter create did not set group filter correctly single entry not regexp")
	}
	if filter.empty {
		t.Error("filter create did not set empty flag correctly")
	}

	// test simple multiple user or group
	conf = configs.Filter{}
	conf.Type = ""
	conf.Users = []string{"user1", "user2"}
	conf.Groups = []string{"group1", "group2"}

	filter = newFilter(conf)
	if !filter.allow {
		t.Error("filter create did not set allow flag correctly from empty string")
	}
	if filter.userExp != nil || len(filter.userList) != 2 {
		t.Error("filter create did not set user filter correctly single entry not regexp")
	}
	if filter.groupExp != nil || len(filter.groupList) != 2 {
		t.Error("filter create did not set group filter correctly single entry not regexp")
	}
}

// New filter creation failure tests
func TestNewFilterExpressions(t *testing.T) {
	// test expression
	conf := configs.Filter{}
	conf.Type = filterAllow
	conf.Users = []string{"user*"}
	conf.Groups = []string{"group[1-9]"}

	filter := newFilter(conf)
	if !filter.allow {
		t.Error("filter create did not set allow flag correctly from 'allow'")
	}
	if filter.userExp == nil || len(filter.userList) != 0 {
		t.Error("filter create did not set user filter correctly")
	}
	if filter.groupExp == nil || len(filter.groupList) != 0 {
		t.Error("filter create did not set group filter correctly")
	}
	if filter.empty {
		t.Error("filter create did not set empty flag correctly")
	}

	// test invalid regexp format
	conf.Users = []string{"user[a-z"}
	conf.Groups = []string{"group[a-z"}
	filter = newFilter(conf)
	if filter.userExp != nil {
		t.Error("The userExp should be nil for an invalid regexp format.")
	}
	if filter.groupExp != nil {
		t.Error("The groupExp should be nil for an invalid regexp format.")
	}
}

// New filter creation failure tests
func TestNewFilterExceptions(t *testing.T) {
	// test duplicate user or group
	conf := configs.Filter{}
	conf.Type = ""
	conf.Users = []string{"user1", "user1"}
	conf.Groups = []string{"group1", "group1"}

	filter := newFilter(conf)
	if !filter.allow {
		t.Error("filter create did not set allow flag correctly from empty string")
	}
	if filter.userExp != nil || len(filter.userList) != 1 {
		t.Error("filter create did not set user filter correctly duplicate entry")
	}
	if filter.groupExp != nil || len(filter.groupList) != 1 {
		t.Error("filter create did not set group filter correctly duplicate entry")
	}

	// test expression as part of list
	conf = configs.Filter{}
	conf.Type = ""
	conf.Users = []string{"user1", "user*"}
	conf.Groups = []string{"group1", "group[1-9]"}

	filter = newFilter(conf)
	if !filter.allow {
		t.Error("filter create did not set allow flag correctly from empty string")
	}
	if filter.userExp != nil || len(filter.userList) != 1 {
		t.Error("filter create did not set user filter correctly regexp not in first entry")
	}
	if filter.groupExp != nil || len(filter.groupList) != 1 {
		t.Error("filter create did not set group filter correctly regexp not in first entry")
	}

	// test single invalid user or group
	conf = configs.Filter{}
	conf.Type = ""
	conf.Users = []string{"user!1"}
	conf.Groups = []string{"grou#p1"}

	filter = newFilter(conf)
	if !filter.allow {
		t.Error("filter create did not set allow flag correctly from empty string")
	}
	if filter.userExp != nil || len(filter.userList) != 0 {
		t.Error("filter create cannot set user filter correctly single invalid entry not regexp")
	}
	if filter.groupExp != nil || len(filter.groupList) != 0 {
		t.Error("filter create cannot not set group filter correctly single invalid entry not regexp")
	}
	if filter.empty {
		t.Error("filter create did not set empty flag correctly")
	}

	// test multiple invalid user or group
	conf = configs.Filter{}
	conf.Type = ""
	conf.Users = []string{"use!r1", "user2"}
	conf.Groups = []string{"gro!up1", "gro#up2"}

	filter = newFilter(conf)
	if !filter.allow {
		t.Error("filter create did not set allow flag correctly from empty string")
	}
	if filter.userExp != nil || len(filter.userList) != 1 {
		t.Error("filter create cannot set user filter correctly invalid multiple entry not regexp")
	}
	if filter.groupExp != nil || len(filter.groupList) != 0 {
		t.Error("filter create cannot set group filter correctly invalid multiple entry not regexp")
	}
}

// Test user matching
func TestFilterUser(t *testing.T) {
	// simple single user (case sensitive)
	conf := configs.Filter{}
	conf.Users = []string{"user1"}

	filter := newFilter(conf)
	if !filter.filterUser("user1") {
		t.Error("filter did not match user 'user1' while in list")
	}
	if filter.filterUser("USER1") {
		t.Error("filter did match user 'USER1' while not in list")
	}
	if filter.filterUser("user2") {
		t.Error("filter did match user 'user2' while not in list")
	}

	// multiple user list (case sensitive)
	conf = configs.Filter{}
	conf.Users = []string{"user1", "USER2"}

	filter = newFilter(conf)
	if !filter.filterUser("USER2") {
		t.Error("filter did not match user 'USER2' while in list")
	}
	if filter.filterUser("user2") {
		t.Error("filter did match user 'user2' while not in list")
	}

	// expression user list
	conf = configs.Filter{}
	conf.Users = []string{"user?"}

	filter = newFilter(conf)
	if !filter.filterUser("user1") {
		t.Error("filter did not match user 'user1' while in expression")
	}
	if !filter.filterUser("user2") {
		t.Error("filter did match user 'user2' while not in expression")
	}
}

// test complex expression
func TestComplexExpression(t *testing.T) {
	// expression user list (case insensitive)
	// expression group list (two capture groups)
	conf := configs.Filter{}
	conf.Users = []string{"(?i)user1"}
	conf.Groups = []string{"^(group1.|other)$"}

	filter := newFilter(conf)
	if !filter.filterUser("USER1") {
		t.Error("filter did not match user 'USER1' while in expression")
	}
	if filter.filterUser("user2") {
		t.Error("filter did match user 'user2' while not in expression")
	}
	if !filter.filterGroup("group12") {
		t.Error("filter did not match group 'group12' while in expression")
	}
	if !filter.filterGroup("other") {
		t.Error("filter did not match group 'other'  while in expression")
	}
	if filter.filterGroup("group101") {
		t.Error("filter did match group 'group101' while not in expression")
	}
}

// test group matching
func TestFilterGroup(t *testing.T) {
	conf := configs.Filter{}
	conf.Groups = []string{"group1"}

	filter := newFilter(conf)
	if !filter.filterGroup("group1") {
		t.Error("filter did not match group 'group1' while in list")
	}
	if filter.filterGroup("group2") {
		t.Error("filter did match group 'group2' while not in list")
	}
}

// test allowing user access with user list
func TestAllowUser(t *testing.T) {
	// user object to test with
	userObj := security.UserGroup{
		User:   "",
		Groups: nil,
	}
	// test deny user list
	const (
		testUser1 = "user1"
	)
	conf := configs.Filter{}
	conf.Type = filterDeny
	conf.Users = []string{testUser1}

	filter := newFilter(conf)
	userObj.User = testUser1
	if filter.allowUser(userObj) {
		t.Error("deny filter did not deny user 'user1' while in list")
	}
	userObj.User = "user2"
	if !filter.allowUser(userObj) {
		t.Error("deny filter did deny user 'user2' while not in list")
	}

	// test allow user list
	conf = configs.Filter{}
	conf.Type = filterAllow
	conf.Users = []string{testUser1}

	filter = newFilter(conf)
	userObj.User = testUser1
	if !filter.allowUser(userObj) {
		t.Error("allow filter did not allow user 'user1' while in list")
	}
	userObj.User = "user2"
	if filter.allowUser(userObj) {
		t.Error("allow filter did allow user 'user1' while not in list")
	}

	// test deny user exp
	conf = configs.Filter{}
	conf.Type = filterDeny
	conf.Users = []string{"user[0-9]"}

	filter = newFilter(conf)
	userObj.User = "user1"
	if filter.allowUser(userObj) {
		t.Error("deny filter did not deny user 'user1' while in expression")
	}
	userObj.User = "nomatch"
	if !filter.allowUser(userObj) {
		t.Error("deny filter did deny user 'nomatch' while not in expression")
	}

	// test allow user exp
	conf = configs.Filter{}
	conf.Type = filterAllow
	conf.Users = []string{"user[0-9]"}

	filter = newFilter(conf)
	userObj.User = "user1"
	if !filter.allowUser(userObj) {
		t.Error("allow filter did not allow user 'user1' while in expression")
	}
	userObj.User = "nomatch"
	if filter.allowUser(userObj) {
		t.Error("allow filter did allow user 'nomatch' while not in expression")
	}
}

// test allowing user access with group list
func TestAllowGroup(t *testing.T) {
	// user object to test with
	userObj := security.UserGroup{
		User:   "",
		Groups: nil,
	}

	// test deny group list
	conf := configs.Filter{}
	conf.Type = filterDeny
	conf.Groups = []string{"group1"}

	filter := newFilter(conf)
	userObj.Groups = []string{"group1"}
	if filter.allowUser(userObj) {
		t.Error("deny filter did not deny group 'group1' while in list")
	}
	userObj.Groups = []string{"group2"}
	if !filter.allowUser(userObj) {
		t.Error("deny filter did deny group 'group2' while not in list")
	}

	// test allow group list
	conf = configs.Filter{}
	conf.Type = filterAllow
	conf.Groups = []string{"group1"}

	filter = newFilter(conf)
	userObj.Groups = []string{"group1"}
	if !filter.allowUser(userObj) {
		t.Error("allow filter did not allow group 'group1' while in list")
	}
	userObj.Groups = []string{"group2"}
	if filter.allowUser(userObj) {
		t.Error("allow filter did allow group 'group2' while not in list")
	}

	// test deny group exp
	conf = configs.Filter{}
	conf.Type = filterDeny
	conf.Groups = []string{"group[0-9]"}

	filter = newFilter(conf)
	userObj.Groups = []string{"group1"}
	if filter.allowUser(userObj) {
		t.Error("deny filter did not deny group 'group1' while in expression")
	}
	userObj.Groups = []string{"nomatch"}
	if !filter.allowUser(userObj) {
		t.Error("deny filter did deny group 'nomatch' while not in expression")
	}

	// test allow group exp
	conf = configs.Filter{}
	conf.Type = filterAllow
	conf.Groups = []string{"group[0-9]"}

	filter = newFilter(conf)
	userObj.Groups = []string{"group1"}
	if !filter.allowUser(userObj) {
		t.Error("allow filter did not allow group 'group1' while in expression")
	}
	userObj.Groups = []string{"nomatch"}
	if filter.allowUser(userObj) {
		t.Error("allow filter did allow group 'nomatch' while not in expression")
	}
}

// test allowing user access with secondary group list
func TestAllowSecondaryGroup(t *testing.T) {
	// user object to test with
	userObj := security.UserGroup{
		User:   "",
		Groups: nil,
	}

	// test deny group list
	conf := configs.Filter{}
	conf.Type = filterDeny
	conf.Groups = []string{"group2"}

	filter := newFilter(conf)
	userObj.Groups = []string{"nomatch", "group2"}
	if filter.allowUser(userObj) {
		t.Error("deny filter did not deny second group 'group2' while in list")
	}

	// test allow group list
	conf = configs.Filter{}
	conf.Type = filterAllow
	conf.Groups = []string{"group1", "group2"}

	filter = newFilter(conf)
	userObj.Groups = []string{"nomatch", "group2"}
	if !filter.allowUser(userObj) {
		t.Error("allow filter did not allow second group 'group2' while in list")
	}

	// test deny group exp
	conf = configs.Filter{}
	conf.Type = filterDeny
	conf.Groups = []string{"group[0-9]"}

	filter = newFilter(conf)
	userObj.Groups = []string{"nomatch", "group2"}
	if filter.allowUser(userObj) {
		t.Error("deny filter did not deny second group 'group2' while in expression")
	}

	// test allow group exp
	conf = configs.Filter{}
	conf.Type = filterAllow
	conf.Groups = []string{"group[0-9]"}

	filter = newFilter(conf)
	userObj.Groups = []string{"nomatch", "group2"}
	if !filter.allowUser(userObj) {
		t.Error("allow filter did not allow group 'group2' while in expression")
	}
}

// test allowing user access with no list
func TestAllowNoLists(t *testing.T) {
	// user object to test with
	userObj := security.UserGroup{
		User:   "user1",
		Groups: []string{"group1"},
	}

	// test default behaviour (no filter)
	conf := configs.Filter{}

	filter := newFilter(conf)
	if !filter.allowUser(userObj) {
		t.Error("allow filter no config did not allow user")
	}
	// test default allow behaviour (no filter)
	conf = configs.Filter{}
	conf.Type = filterAllow

	filter = newFilter(conf)
	if !filter.allowUser(userObj) {
		t.Error("allow filter type only did not allow user")
	}
	// test default deny behaviour (no filter)
	conf = configs.Filter{}
	conf.Type = filterDeny

	filter = newFilter(conf)
	if filter.allowUser(userObj) {
		t.Error("deny filter type only did not deny user")
	}
}

func TestFilter_filterDAO(t *testing.T) {
	// filters are tested also from each rule in different combinations
	// this does the outliers and cases that should not happen
	reg := regexp.MustCompile("^.*$")
	tests := []struct {
		name   string
		filter Filter
		want   *dao.FilterDAO
	}{
		{"empty", Filter{empty: true}, nil},
		{"empty", Filter{}, &dao.FilterDAO{Type: filterDeny}},
		{
			"everything",
			Filter{allow: true, userList: map[string]bool{"user": true}, groupList: map[string]bool{"group": true}, userExp: reg, groupExp: reg},
			&dao.FilterDAO{Type: filterAllow, UserList: []string{"user"}, GroupList: []string{"group"}, UserExp: "^.*$", GroupExp: "^.*$"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.filter.filterDAO(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("filterDAO() = %v, want %v", got, tt.want)
			}
		})
	}
}
