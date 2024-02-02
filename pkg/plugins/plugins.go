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
	"github.com/apache/yunikorn-core/pkg/log"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/api"
)

var plugins SchedulerPlugins

func init() {
	plugins = SchedulerPlugins{}
}

// RegisterSchedulerPlugin registers the plugin based on the interfaces(s) it implements.
// The known interfaces are defined in yunikorn-scheduler-interface/lib/go/api
func RegisterSchedulerPlugin(plugin interface{}) {
	plugins.Lock()
	defer plugins.Unlock()
	if rmc, ok := plugin.(api.ResourceManagerCallback); ok {
		log.Log(log.RMProxy).Info("register scheduler plugin: ResourceManagerCallback")
		plugins.ResourceManagerCallbackPlugin = rmc
	}
	if sdp, ok := plugin.(api.StateDumpPlugin); ok {
		log.Log(log.RMProxy).Info("register scheduler plugin: StateDumpPlugin")
		plugins.StateDumpPlugin = sdp
	}
}

// UnregisterSchedulerPlugins removes all earlier set plugins
// visible for testing only
func UnregisterSchedulerPlugins() {
	plugins.Lock()
	defer plugins.Unlock()
	plugins.ResourceManagerCallbackPlugin = nil
	plugins.StateDumpPlugin = nil
}

// GetResourceManagerCallbackPlugin returns the registered callback plugin or nil if none was registered.
func GetResourceManagerCallbackPlugin() api.ResourceManagerCallback {
	plugins.RLock()
	defer plugins.RUnlock()
	return plugins.ResourceManagerCallbackPlugin
}

// GetStateDumpPlugin returns the registered state dump plugin or nil if none was registered.
func GetStateDumpPlugin() api.StateDumpPlugin {
	plugins.RLock()
	defer plugins.RUnlock()
	return plugins.StateDumpPlugin
}
