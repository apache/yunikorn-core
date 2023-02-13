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

package configs

import (
	"sync"
	"time"
)

const (
	HealthCheckInterval      = "health.checkInterval"
	InstanceTypeNodeLabelKey = "service.instanceTypeNodeLabelKey"
)

var DefaultHealthCheckInterval = 30 * time.Second
var DefaultInstanceTypeNodeLabelKey = "node.kubernetes.io/instance-type"

var ConfigContext *SchedulerConfigContext

var configMap map[string]string
var configMapCallbacks map[string]func()
var configMapLock sync.RWMutex

func init() {
	configMap = make(map[string]string)
	configMapCallbacks = make(map[string]func())
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

// AddConfigMapCallback registers a callback to detect configuration updates
func AddConfigMapCallback(id string, callback func()) {
	configMapLock.Lock()
	defer configMapLock.Unlock()
	configMapCallbacks[id] = callback
}

// RemoveConfigMapCallback removes a previously registered configuration update callback
func RemoveConfigMapCallback(id string) {
	configMapLock.Lock()
	defer configMapLock.Unlock()
	delete(configMapCallbacks, id)
}

// Gets the ConfigMap
func GetConfigMap() map[string]string {
	configMapLock.RLock()
	defer configMapLock.RUnlock()
	return configMap
}

// Sets the ConfigMap based on configuration refresh
func SetConfigMap(newConfigMap map[string]string) {
	defer processConfigMapCallbacks()

	configMapLock.Lock()
	defer configMapLock.Unlock()

	if newConfigMap == nil {
		newConfigMap = make(map[string]string)
	}
	configMap = newConfigMap
}

func processConfigMapCallbacks() {
	for _, callback := range getConfigMapCallbacks() {
		callback()
	}
}

func getConfigMapCallbacks() []func() {
	configMapLock.RLock()
	defer configMapLock.RUnlock()
	result := make([]func(), 0)
	for _, callback := range configMapCallbacks {
		result = append(result, callback)
	}
	return result
}
