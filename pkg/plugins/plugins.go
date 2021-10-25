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
	"github.com/apache/incubator-yunikorn-core/pkg/log"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/api"
)

var plugins SchedulerPlugins

func init() {
	plugins = SchedulerPlugins{}
}

func RegisterSchedulerPlugin(plugin interface{}) {
	plugins.Lock()
	defer plugins.Unlock()
	if t, ok := plugin.(api.ResourceManagerCallback); ok {
		log.Logger().Info("register scheduler plugin: ResourceManagerCallback")
		plugins.ResourceManagerCallbackPlugin = t
	}
	if t, ok := plugin.(ConfigurationPlugin); ok {
		log.Logger().Info("register scheduler plugin: ConfigMapPlugin")
		plugins.configPlugin = t
	}
}

func GetResourceManagerCallbackPlugin() api.ResourceManagerCallback {
	plugins.RLock()
	defer plugins.RUnlock()
	return plugins.ResourceManagerCallbackPlugin
}

func GetConfigPlugin() ConfigurationPlugin {
	plugins.RLock()
	defer plugins.RUnlock()

	return plugins.configPlugin
}
