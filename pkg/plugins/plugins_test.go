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

	"gotest.tools/assert"

	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
)

type FakeResourceManagerCallback struct{}

func (f *FakeResourceManagerCallback) UpdateAllocation(response *si.AllocationResponse) error {
	// do nothing
	return nil
}

func (f *FakeResourceManagerCallback) UpdateApplication(response *si.ApplicationResponse) error {
	// do nothing
	return nil
}

func (f *FakeResourceManagerCallback) UpdateNode(response *si.NodeResponse) error {
	// do nothing
	return nil
}

func (f *FakeResourceManagerCallback) Predicates(args *si.PredicatesArgs) error {
	// do nothing
	return nil
}

func (f *FakeResourceManagerCallback) ReSyncSchedulerCache(args *si.ReSyncSchedulerCacheArgs) error {
	// do nothing
	return nil
}

func (f *FakeResourceManagerCallback) SendEvent(events []*si.EventRecord) {
	// do nothing
}

func (f *FakeResourceManagerCallback) UpdateContainerSchedulingState(request *si.UpdateContainerSchedulingStateRequest) {
	// do nothing
}

type FakeConfigPlugin struct {
}

func (f FakeConfigPlugin) UpdateConfiguration(args *si.UpdateConfigurationRequest) *si.UpdateConfigurationResponse {
	// do nothing
	return nil
}

func TestRegisterPlugins(t *testing.T) {
	plugins = SchedulerPlugins{}
	RegisterSchedulerPlugin(&FakeResourceManagerCallback{})
	assert.Assert(t, GetResourceManagerCallbackPlugin() != nil, "ResourceManagerCallbackPlugin plugin should have been registered")
	assert.Assert(t, GetConfigPlugin() == nil, "config plugin should not have been registered")
}

func TestRegisterConfigPlugin(t *testing.T) {
	plugins = SchedulerPlugins{}
	RegisterSchedulerPlugin(&FakeConfigPlugin{})
	assert.Assert(t, GetResourceManagerCallbackPlugin() == nil, "ResourceManagerCallback plugin should not have been registered")
	assert.Assert(t, GetConfigPlugin() != nil, "config plugin should have been registered")
}
