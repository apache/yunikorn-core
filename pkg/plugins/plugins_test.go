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

package plugins

import (
	"testing"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

type NoPluginImplemented struct{}

type RMPluginImplemented struct{}

type StateDumpImplemented struct{}

type AllPluginsImplemented struct {
	RMPluginImplemented
	StateDumpImplemented
}

func (f *RMPluginImplemented) UpdateApplication(_ *si.ApplicationResponse) error {
	return nil
}

func (f *RMPluginImplemented) UpdateAllocation(_ *si.AllocationResponse) error {
	return nil
}

func (f *RMPluginImplemented) UpdateNode(_ *si.NodeResponse) error {
	return nil
}

func (f *RMPluginImplemented) Predicates(_ *si.PredicatesArgs) error {
	return nil
}

func (f *RMPluginImplemented) PreemptionPredicates(_ *si.PreemptionPredicatesArgs) *si.PreemptionPredicatesResponse {
	return nil
}

func (f *RMPluginImplemented) SendEvent(_ []*si.EventRecord) {
	// do nothing
}

func (f *RMPluginImplemented) UpdateContainerSchedulingState(_ *si.UpdateContainerSchedulingStateRequest) {
}

func (f *StateDumpImplemented) GetStateDump() (string, error) {
	return "", nil
}

func TestRegisterPlugins(t *testing.T) {
	plugins = SchedulerPlugins{}
	RegisterSchedulerPlugin(&NoPluginImplemented{})
	assert.Assert(t, GetResourceManagerCallbackPlugin() == nil, "ResourceManagerCallback plugin should not have been registered")
	assert.Assert(t, GetStateDumpPlugin() == nil, "StateDumpCallback plugin should not have been registered")

	RegisterSchedulerPlugin(&RMPluginImplemented{})
	assert.Assert(t, GetResourceManagerCallbackPlugin() != nil, "ResourceManagerCallback plugin should have been registered")
	assert.Assert(t, GetStateDumpPlugin() == nil, "StateDumpCallback plugin should not have been registered")
	UnregisterSchedulerPlugins()

	RegisterSchedulerPlugin(&StateDumpImplemented{})
	assert.Assert(t, GetResourceManagerCallbackPlugin() == nil, "ResourceManagerCallback plugin should not have been registered")
	assert.Assert(t, GetStateDumpPlugin() != nil, "StateDumpCallback plugin should have been registered")
	UnregisterSchedulerPlugins()

	RegisterSchedulerPlugin(&AllPluginsImplemented{})
	assert.Assert(t, GetResourceManagerCallbackPlugin() != nil, "ResourceManagerCallback plugin should have been registered")
	assert.Assert(t, GetStateDumpPlugin() != nil, "StateDumpCallback plugin should have been registered")
	UnregisterSchedulerPlugins()

	// registration are additive
	RegisterSchedulerPlugin(&RMPluginImplemented{})
	RegisterSchedulerPlugin(&StateDumpImplemented{})
	assert.Assert(t, GetResourceManagerCallbackPlugin() != nil, "ResourceManagerCallback plugin should have been registered")
	assert.Assert(t, GetStateDumpPlugin() != nil, "StateDumpCallback plugin should have been registered")
	UnregisterSchedulerPlugins()
}
