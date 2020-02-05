/*
Copyright 2020 Cloudera, Inc.  All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package plugins

import (
	"go.uber.org/zap"

	"github.com/apache/incubator-yunikorn-core/pkg/log"
)

var plugins SchedulerPlugins

func init() {
	plugins = SchedulerPlugins{}
}

func RegisterSchedulerPlugin(plugin interface{}) {
	registered := false
	if t, ok := plugin.(PredicatesPlugin); ok {
		log.Logger().Debug("register scheduler plugin",
			zap.String("type", "PredicatesPlugin"))
		plugins.predicatesPlugin = t
		registered = true
	}
	if t, ok := plugin.(VolumesPlugin); ok {
		log.Logger().Debug("register scheduler plugin",
			zap.String("type", "VolumesPlugin"))
		plugins.volumesPlugin = t
		registered = true
	}
	if t, ok := plugin.(ReconcilePlugin); ok {
		log.Logger().Debug("register scheduler plugin",
			zap.String("type", "ReconcilePlugin"))
		plugins.reconcilePlugin = t
		registered = true
	}
	if !registered {
		log.Logger().Debug("no scheduler plugin implemented, none registered")
	}
}

func GetPredicatesPlugin() PredicatesPlugin {
	return plugins.predicatesPlugin
}

func GetVolumesPlugin() VolumesPlugin {
	return plugins.volumesPlugin
}

func GetReconcilePlugin() ReconcilePlugin {
	return plugins.reconcilePlugin
}
