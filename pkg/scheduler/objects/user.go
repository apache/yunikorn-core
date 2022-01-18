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

package objects

import (
	"sync/atomic"
)

const AllUser = "*"
const UserMaxApplicationLimitNotSet = -1

type User struct {
	name                string
	maxApplications     int32
	runningApplications int32
	usedGroup           string
}

func NewUser(user string) *User {
	return &User{
		name:                user,
		runningApplications: 0,
		maxApplications:     UserMaxApplicationLimitNotSet,
	}
}

func (u *User) GetName() string {
	return u.name
}

func (u *User) SetMaxApplications(maxApplications int32) {
	u.maxApplications = maxApplications
}

func (u *User) GetMaxApplications() int32 {
	return u.maxApplications
}

func (u *User) IncRunningApplications() {
	atomic.AddInt32(&u.runningApplications, 1)
}

func (u *User) DecRunningApplications() {
	atomic.AddInt32(&u.runningApplications, -1)
}

func (u *User) IsRunningAppsUnderLimit() bool {
	return atomic.LoadInt32(&u.runningApplications) < u.maxApplications
}

func (u *User) IsMaxAppsLimitSet() bool {
	return u.GetMaxApplications() != UserMaxApplicationLimitNotSet
}

// SetUsedGroup A user may belong to more than one group. In case of any group changes for
// any user while running applications, without this usedGroup info might lead
// to confusion while doing any metrics calculation as and when user is done with
// their activities. Also, this info avoids parsing all mapped groups while doing calculations
func (u *User) SetUsedGroup(usedGroup string) {
	u.usedGroup = usedGroup
}

func (u *User) GetUsedGroup() string {
	return u.usedGroup
}
