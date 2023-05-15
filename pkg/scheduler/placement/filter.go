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
	"regexp"

	"go.uber.org/zap"

	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-core/pkg/common/security"
	"github.com/apache/yunikorn-core/pkg/log"
)

type Filter struct {
	allow     bool
	empty     bool
	userList  map[string]bool
	groupList map[string]bool
	userExp   *regexp.Regexp
	groupExp  *regexp.Regexp
}

// Check if the user is allowed by the filter
// First check the user list and then the group list
// List matching is case sensitive, expression matching might not be (depends on regexp)
func (filter Filter) allowUser(userObj security.UserGroup) bool {
	user := userObj.User
	groups := userObj.Groups
	// if nothing is filtered just return the type
	if filter.empty {
		return filter.allow
	}
	filteredUser := filter.filterUser(user)
	// if we have found the user in the list stop looking and return
	if filteredUser {
		log.Logger().Debug("Filter matched user getName", zap.String("user", user))
		return filteredUser && filter.allow
	}
	// not in the user list, check the groups in the list
	// walk over the list first match is taken
	for _, group := range groups {
		filteredUser = filter.filterGroup(group)
		if filteredUser {
			log.Logger().Debug("Filter matched user group", zap.String("group", group))
			return filteredUser && filter.allow
		}
	}
	return !filter.allow
}

// Filter the user based on username: return true if the user is in the list or regexp and false if not
func (filter Filter) filterUser(user string) bool {
	// if regexp is set run regexp
	if filter.userExp != nil {
		return filter.userExp.MatchString(user)
	}
	// check the members
	return filter.userList[user]
}

// Filter the user based on group getName: return true if the group is in the list or regexp and false if not
func (filter Filter) filterGroup(group string) bool {
	// if regexp is set run regexp
	if filter.groupExp != nil {
		return filter.groupExp.MatchString(group)
	}
	// check the members
	return filter.groupList[group]
}

// Create a new filter based on the checked config
// There should be no errors as the config is syntax checked before we get to this point.
func newFilter(conf configs.Filter) Filter {
	filter := Filter{
		userList:  map[string]bool{},
		groupList: map[string]bool{},
		empty:     true,
	}
	// type can only be '' , allow or deny.
	filter.allow = conf.Type != "deny"

	var err error
	// create the user list or regexp
	if len(conf.Users) == 1 {
		user := conf.Users[0]
		// check for regexp characters that cannot be in a user
		if configs.SpecialRegExp.MatchString(user) {
			filter.userExp, err = regexp.Compile(user)
			if err != nil {
				log.Logger().Debug("Filter user expression does not compile", zap.Any("userFilter", conf.Users))
			}
		} else if configs.UserRegExp.MatchString(user) {
			// regexp not found consider this a user, sanity check the entry
			// single entry just a user
			filter.userList[user] = true
		}
		filter.empty = false
	}
	// if there are 2 or more users create a list
	if len(conf.Users) >= 2 {
		for _, user := range conf.Users {
			// sanity check the entry, do not add if it does not comply
			if configs.UserRegExp.MatchString(user) {
				filter.userList[user] = true
			}
		}
		if len(filter.userList) != len(conf.Users) {
			log.Logger().Info("Filter creation duplicate or invalid users found", zap.Any("userFilter", conf.Users))
		}
		filter.empty = false
	}

	// check what we have created
	if len(conf.Users) > 0 && filter.userExp == nil && len(filter.userList) == 0 {
		log.Logger().Info("Filter creation partially failed (user)", zap.Any("userFilter", conf.Users))
	}

	// create the group list or regexp
	if len(conf.Groups) == 1 {
		group := conf.Groups[0]
		// check for regexp characters that cannot be in a group
		if configs.SpecialRegExp.MatchString(group) {
			filter.groupExp, err = regexp.Compile(group)
			if err != nil {
				log.Logger().Debug("Filter group expression does not compile", zap.Any("groupFilter", conf.Groups))
			}
		} else if configs.GroupRegExp.MatchString(group) {
			// regexp not found consider this a group, sanity check the entry
			// single entry just a group
			filter.groupList[group] = true
		}
		filter.empty = false
	}
	// if there are 2 or more groups create a list
	if len(conf.Groups) >= 2 {
		for _, group := range conf.Groups {
			// sanity check the entry, do not add if it does not comply
			if configs.GroupRegExp.MatchString(group) {
				filter.groupList[group] = true
			}
		}
		if len(filter.groupList) != len(conf.Groups) {
			log.Logger().Info("Filter creation duplicate or invalid groups found", zap.Any("groupFilter", conf.Groups))
		}
		filter.empty = false
	}

	// check what we have created
	if len(conf.Groups) > 0 && filter.groupExp == nil && len(filter.groupList) == 0 {
		log.Logger().Info("Filter creation partially failed (groups)", zap.Any("groupFilter", conf.Groups))
	}

	// log the filter with all details (only at debug)
	if log.IsDebugEnabled() {
		var userfilter, groupfilter string
		if filter.userExp == nil {
			userfilter = "nil"
		} else {
			userfilter = filter.userExp.String()
		}
		if filter.groupExp == nil {
			groupfilter = "nil"
		} else {
			groupfilter = filter.groupExp.String()
		}
		log.Logger().Debug("Filter creation passed",
			zap.Bool("allow", filter.allow),
			zap.Bool("empty", filter.empty),
			zap.Any("userList", filter.userList),
			zap.Any("groupList", filter.groupList),
			zap.String("userFilter", userfilter),
			zap.String("groupFilter", groupfilter))
	}
	return filter
}
