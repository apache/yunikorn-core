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

	"github.com/apache/incubator-yunikorn-core/pkg/common/configs"

	"github.com/apache/incubator-yunikorn-core/pkg/scheduler/plugins/defaults"

	"github.com/apache/incubator-yunikorn-core/pkg/interfaces"

	"gotest.tools/assert"
)

type fakeApplicationsPlugin struct {
}

func NewFakeApplicationsPlugin(_ interface{}) (interfaces.Plugin, error) {
	return &fakeApplicationsPlugin{}, nil
}

func (f *fakeApplicationsPlugin) Name() string {
	return "fakeApplicationsPlugin"
}

func (f *fakeApplicationsPlugin) NewApplications(queue interfaces.Queue) interfaces.Applications {
	// do nothing
	return nil
}

type fakeRequestsPlugin struct {
}

func (f *fakeRequestsPlugin) Name() string {
	return "fakeRequestsPlugin"
}

func (f *fakeRequestsPlugin) NewRequests() interfaces.Requests {
	// do nothing
	return nil
}

func NewFakeRequestsPlugin(_ interface{}) (interfaces.Plugin, error) {
	return &fakeRequestsPlugin{}, nil
}


type fakeAllInOnePlugin struct {
}

func (f *fakeAllInOnePlugin) Name() string {
	return "fakeAllInOnePlugin"
}

func (f *fakeAllInOnePlugin) NewRequests() interfaces.Requests {
	// do nothing
	return nil
}

func (f *fakeAllInOnePlugin) NewApplications(queue interfaces.Queue) interfaces.Applications {
	// do nothing
	return nil
}

func NewFakeAllInOnePlugin(_ interface{}) (interfaces.Plugin, error) {
	return &fakeAllInOnePlugin{}, nil
}

func TestDefaultPlugins(t *testing.T) {
	// check default applications plugin
	applicationsPlugin := GetApplicationsPlugin()
	checkPlugin(t, applicationsPlugin, defaults.DefaultApplicationsPluginName, "default applications")
	// check default requests plugin
	requestsPlugin := GetRequestsPlugin()
	checkPlugin(t, requestsPlugin, defaults.DefaultRequestsPluginName, "default requests")
}

func TestRegisterPlugins(t *testing.T) {
	registry := NewRegistry()
	// error check: unknown plugin
	pluginsConfig := &configs.PluginsConfig{
		Plugins: []*configs.PluginConfig{
			{
				Name: "unknown",
			},
		},
	}
	err := Init(registry, pluginsConfig)
	assert.Error(t, err, "unregistered plugin: unknown")
	// register new plugins
	fakeAppsPlugin := &fakeApplicationsPlugin{}
	fakeRequestsPlugin := &fakeRequestsPlugin{}
	registry.Register(fakeAppsPlugin.Name(), NewFakeApplicationsPlugin)
	registry.Register(fakeRequestsPlugin.Name(), NewFakeRequestsPlugin)
	pluginsConfig = &configs.PluginsConfig{
		Plugins: []*configs.PluginConfig{
			{
				Name: fakeAppsPlugin.Name(),
			},
			{
				Name: fakeRequestsPlugin.Name(),
			},
		},
	}
	err = Init(registry, pluginsConfig)
	assert.NilError(t, err, "init plugins with error")
	applicationsPlugin := GetApplicationsPlugin()
	checkPlugin(t, applicationsPlugin, fakeAppsPlugin.Name(), "fake applications")
	requestsPlugin := GetRequestsPlugin()
	checkPlugin(t, requestsPlugin, fakeRequestsPlugin.Name(), "fake requests")
	// register all-in-one plugin
	fakeAllInOnePlugin := &fakeAllInOnePlugin{}
	registry.Register(fakeAllInOnePlugin.Name(), NewFakeAllInOnePlugin)
	pluginsConfig = &configs.PluginsConfig{
		Plugins: []*configs.PluginConfig{
			{
				Name: fakeAllInOnePlugin.Name(),
			},
		},
	}
	err = Init(registry, pluginsConfig)
	assert.NilError(t, err, "init plugins with error")
	checkPlugin(t, GetApplicationsPlugin(), fakeAllInOnePlugin.Name(), "fake all-in-one")
	checkPlugin(t, GetRequestsPlugin(), fakeAllInOnePlugin.Name(), "fake all-in-one")
}

func checkPlugin(t *testing.T, plugin interfaces.Plugin, expectedName string, flag string) {
	assert.Assert(t, plugin != nil, "failed to find %s plugin", flag)
	assert.Equal(t, plugin.Name(), expectedName, "unexpected %s plugin", flag)
}
