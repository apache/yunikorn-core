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

	"github.com/apache/incubator-yunikorn-core/pkg/plugins/defaults"

	"github.com/apache/incubator-yunikorn-core/pkg/interfaces"

	"gotest.tools/assert"

	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
)

type fakePredicatePluginImpl struct{}

func (f *fakePredicatePluginImpl) Predicates(args *si.PredicatesArgs) error {
	// do nothing
	return nil
}

func (f *fakePredicatePluginImpl) ReSyncSchedulerCache(args *si.ReSyncSchedulerCacheArgs) error {
	// do nothing
	return nil
}

type FakeConfigPlugin struct {
}

func (f FakeConfigPlugin) UpdateConfiguration(args *si.UpdateConfigurationRequest) *si.UpdateConfigurationResponse {
	// do nothing
	return nil
}

type fakeApplicationsPlugin struct {
}

func (f fakeApplicationsPlugin) NewApplications(queue interfaces.Queue) interfaces.Applications {
	// do nothing
	return nil
}

type fakeRequestsPlugin struct {
}

func (f fakeRequestsPlugin) NewRequests() interfaces.Requests {
	// do nothing
	return nil
}

func TestRegisterPlugins(t *testing.T) {
	plugins = SchedulerPlugins{}
	RegisterSchedulerPlugin(&fakePredicatePluginImpl{})
	assert.Assert(t, GetPredicatesPlugin() != nil, "predicates plugin should have been registered")
	assert.Assert(t, GetReconcilePlugin() != nil, "reconcile plugin should have been registered")
	assert.Assert(t, GetContainerSchedulingStateUpdaterPlugin() == nil, "volume plugin should not have been registered")
	assert.Assert(t, GetConfigPlugin() == nil, "config plugin should not have been registered")
}

func TestRegisterConfigPlugin(t *testing.T) {
	plugins = SchedulerPlugins{}
	RegisterSchedulerPlugin(&FakeConfigPlugin{})
	assert.Assert(t, GetPredicatesPlugin() == nil, "predicates plugin should not have been registered")
	assert.Assert(t, GetReconcilePlugin() == nil, "reconcile plugin should have been registered")
	assert.Assert(t, GetContainerSchedulingStateUpdaterPlugin() == nil, "volume plugin should not have been registered")
	assert.Assert(t, GetConfigPlugin() != nil, "config plugin should have been registered")
}

func TestRegisterApplicationsPlugin(t *testing.T) {
	defaultApplicationsPlugin := defaults.DefaultApplicationsPluginInstance
	assert.Assert(t, GetApplicationsPlugin() == defaultApplicationsPlugin, "config plugin should have been registered")
	plugins = SchedulerPlugins{}
	RegisterSchedulerPlugin(&fakeApplicationsPlugin{})
	assert.Assert(t, GetApplicationsPlugin() != defaultApplicationsPlugin, "config plugin should have been registered")
}

func TestRegisterRequestsPlugin(t *testing.T) {
	defaultRequestsPlugin := defaults.DefaultRequestsPluginInstance
	assert.Assert(t, GetRequestsPlugin() == defaultRequestsPlugin, "config plugin should have been registered")
	plugins = SchedulerPlugins{}
	RegisterSchedulerPlugin(&fakeRequestsPlugin{})
	assert.Assert(t, GetRequestsPlugin() != defaultRequestsPlugin, "config plugin should have been registered")
}
