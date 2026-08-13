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
	"time"

	"github.com/apache/yunikorn-core/pkg/locking"
	"github.com/apache/yunikorn-core/pkg/log"
)

const (
	// prefixes
	PrefixEvent  = "event."
	PrefixHealth = "health."

	HealthCheckInterval = PrefixHealth + "checkInterval"

	// events
	CMEventTrackingEnabled    = PrefixEvent + "trackingEnabled"    // Application Tracking
	CMEventRequestCapacity    = PrefixEvent + "requestCapacity"    // Request Capacity
	CMEventRingBufferCapacity = PrefixEvent + "ringBufferCapacity" // Ring Buffer Capacity
	CMMaxEventStreams         = PrefixEvent + "maxStreams"
	CMMaxEventStreamsPerHost  = PrefixEvent + "maxStreamsPerHost"
	CMRESTResponseSize        = PrefixEvent + "RESTResponseSize"

	// defaults
	DefaultHealthCheckInterval     = 30 * time.Second
	DefaultEventTrackingEnabled    = true
	DefaultEventRequestCapacity    = 1000
	DefaultEventRingBufferCapacity = 100000
	DefaultEventChannelSize        = 100000
	DefaultMaxStreams              = uint64(100)
	DefaultMaxStreamsPerHost       = uint64(15)
	DefaultRESTResponseSize        = uint64(10000)
)

var ConfigContext *SchedulerConfigContext

// The declarations below are grouped so that the annotations are attached to the variable they
// belong to.
//
// The guard is unexported, so these annotations are enforced for the code in this package only:
// other packages cannot resolve the guard and the analysis skips them there.
var (
	// +checklocks:configMapLock
	configMap map[string]string
	// +checklocks:configMapLock
	configMapCallbacks map[string]func()

	configMapLock locking.RWMutex
)

func init() {
	// package initialisation, nothing can reach the maps yet so no lock is taken
	configMap = make(map[string]string)          // +checklocksignore
	configMapCallbacks = make(map[string]func()) // +checklocksignore
	ConfigContext = &SchedulerConfigContext{
		configs: make(map[string]*SchedulerConfig),
		lock:    &locking.RWMutex{},
	}

	// add a callback to reconfigure logging
	AddConfigMapCallback("logging", func() {
		log.UpdateLoggingConfig(GetConfigMap())
	})
}

// scheduler config context provides thread-safe access for scheduler configurations
type SchedulerConfigContext struct {
	// +checklocks:lock
	configs map[string]*SchedulerConfig
	lock    *locking.RWMutex
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
// +checklocksexclude:configMapLock
func AddConfigMapCallback(id string, callback func()) {
	configMapLock.Lock()
	defer configMapLock.Unlock()
	configMapCallbacks[id] = callback
}

// RemoveConfigMapCallback removes a previously registered configuration update callback
// +checklocksexclude:configMapLock
func RemoveConfigMapCallback(id string) {
	configMapLock.Lock()
	defer configMapLock.Unlock()
	delete(configMapCallbacks, id)
}

// Gets the ConfigMap
// +checklocksexcludewrite:configMapLock
func GetConfigMap() map[string]string {
	configMapLock.RLock()
	defer configMapLock.RUnlock()
	return configMap
}

// Sets the ConfigMap based on configuration refresh
// +checklocksexclude:configMapLock
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

// +checklocksexcludewrite:configMapLock
func getConfigMapCallbacks() []func() {
	configMapLock.RLock()
	defer configMapLock.RUnlock()
	result := make([]func(), 0)
	for _, callback := range configMapCallbacks {
		result = append(result, callback)
	}
	return result
}
