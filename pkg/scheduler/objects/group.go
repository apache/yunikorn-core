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

const AllGroup = "*"
const GroupMaxApplicationLimitNotSet = -1

type Group struct {
	name                string
	maxApplications     int32
	runningApplications int32
}

func NewGroup(group string) *Group {
	return &Group{
		name:                group,
		runningApplications: 0,
		maxApplications:     GroupMaxApplicationLimitNotSet,
	}
}

func (g *Group) GetName() string {
	return g.name
}

func (g *Group) SetMaxApplications(maxApplications int32) {
	g.maxApplications = maxApplications
}

func (g *Group) GetMaxApplications() int32 {
	return g.maxApplications
}

func (g *Group) IncRunningApplications() {
	atomic.AddInt32(&g.runningApplications, 1)
}

func (g *Group) DecRunningApplications() {
	atomic.AddInt32(&g.runningApplications, -1)
}

func (g *Group) IsRunningAppsUnderLimit() bool {
	return atomic.LoadInt32(&g.runningApplications) < g.maxApplications
}

func (g *Group) IsMaxAppsLimitSet() bool {
	return g.GetMaxApplications() != GroupMaxApplicationLimitNotSet
}
