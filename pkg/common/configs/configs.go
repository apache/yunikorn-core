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

package configs

import "sync"

const (
	SchedulerConfigPath        = "scheduler-config-path"
	DefaultSchedulerConfigPath = "/etc/yunikorn"
)

var ConfigMap map[string]string
var ConfigContext *SchedulerConfigContext

func init() {
	ConfigMap = make(map[string]string)
	ConfigContext = &SchedulerConfigContext{
		configs: make(map[string]*SchedulerConfig),
		lock:    &sync.RWMutex{},
	}
}

// scheduler config context provides thread-safe access for scheduler configurations
type SchedulerConfigContext struct {
	configs map[string]*SchedulerConfig
	lock    *sync.RWMutex
}

func (ctx *SchedulerConfigContext) Set(policyGroup string, config *SchedulerConfig) {
	ctx.lock.Lock()
	defer ctx.lock.Unlock()
	ctx.configs[policyGroup] = config
}

func (ctx *SchedulerConfigContext) Get(policyGroup string) *SchedulerConfig {
	ctx.lock.RLock()
	defer ctx.lock.RUnlock()
	return ctx.configs[policyGroup]
}
