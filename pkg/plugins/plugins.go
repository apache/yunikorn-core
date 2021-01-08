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
	"reflect"

	"github.com/apache/incubator-yunikorn-core/pkg/plugins/default_plugins_impl"

	"github.com/apache/incubator-yunikorn-core/pkg/log"
	"go.uber.org/zap"
)

var plugins SchedulerPlugins

func init() {
	plugins = SchedulerPlugins{}
}

func RegisterSchedulerPlugin(plugin interface{}) {
	plugins.Lock()
	defer plugins.Unlock()

	if t, ok := plugin.(PredicatesPlugin); ok {
		log.Logger().Info("register scheduler plugin: PredicatesPlugin")
		plugins.predicatesPlugin = t
	}
	if t, ok := plugin.(ReconcilePlugin); ok {
		log.Logger().Info("register scheduler plugin: ReconcilePlugin")
		plugins.reconcilePlugin = t
	}
	if t, ok := plugin.(EventPlugin); ok {
		log.Logger().Info("register scheduler plugin: EventPlugin")
		plugins.eventPlugin = t
	}
	if t, ok := plugin.(ContainerSchedulingStateUpdater); ok {
		log.Logger().Info("register scheduler plugin: ContainerSchedulingStateUpdater")
		plugins.schedulingStateUpdater = t
	}
	if t, ok := plugin.(ConfigurationPlugin); ok {
		log.Logger().Info("register scheduler plugin: ConfigMapPlugin")
		plugins.configPlugin = t
	}
	if t, ok := plugin.(ApplicationsPlugin); ok {
		log.Logger().Info("register scheduler plugin: ApplicationsPlugin",
			zap.String("type", reflect.TypeOf(plugin).String()))
		plugins.applicationsPlugin = t
	}
	if t, ok := plugin.(RequestsPlugin); ok {
		log.Logger().Info("register scheduler plugin: RequestsPlugin",
			zap.String("type", reflect.TypeOf(plugin).String()))
		plugins.requestsPlugin = t
	}
}

func GetPredicatesPlugin() PredicatesPlugin {
	plugins.RLock()
	defer plugins.RUnlock()

	return plugins.predicatesPlugin
}

func GetReconcilePlugin() ReconcilePlugin {
	plugins.RLock()
	defer plugins.RUnlock()

	return plugins.reconcilePlugin
}

func GetEventPlugin() EventPlugin {
	plugins.RLock()
	defer plugins.RUnlock()

	return plugins.eventPlugin
}

func GetContainerSchedulingStateUpdaterPlugin() ContainerSchedulingStateUpdater {
	plugins.RLock()
	defer plugins.RUnlock()

	return plugins.schedulingStateUpdater
}

func GetConfigPlugin() ConfigurationPlugin {
	plugins.RLock()
	defer plugins.RUnlock()

	return plugins.configPlugin
}

func GetRequestsPlugin() RequestsPlugin {
	plugins.RLock()
	defer plugins.RUnlock()

	if plugins.requestsPlugin == nil {
		return default_plugins_impl.DefaultRequestsPluginInstance
	}
	return plugins.requestsPlugin
}

func GetApplicationsPlugin() ApplicationsPlugin {
	plugins.RLock()
	defer plugins.RUnlock()

	if plugins.applicationsPlugin == nil {
		return default_plugins_impl.DefaultApplicationsPluginInstance
	}
	return plugins.applicationsPlugin
}
