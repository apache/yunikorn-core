/*
Copyright 2019 The Unity Scheduler Authors

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
	"github.com/golang/glog"
)

var plugins SchedulerPlugins

func init() {
	plugins = SchedulerPlugins{}
}

func RegisterSchedulerPlugin(plugin interface{}) {
	registered := false
	if t, ok := plugin.(PredicatesPlugin); ok {
		glog.V(4).Info("register scheduler plugin, type: PredicatesPlugin")
		plugins.predicatesPlugin = t
		registered = true
	}
	if t, ok := plugin.(VolumesPlugin); ok {
		glog.V(4).Info("register scheduler plugin, type: VolumesPlugin")
		plugins.volumesPlugin = t
		registered = true
	}
	if !registered {
		glog.V(4).Info("no scheduler plugin implemented, none registered")
	}
}

func GetPredicatesPlugin() PredicatesPlugin {
	return plugins.predicatesPlugin
}

func GetVolumesPlugin() VolumesPlugin {
	return plugins.volumesPlugin
}
