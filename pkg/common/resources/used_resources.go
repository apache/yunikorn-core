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

package resources

import (
	"sync"
	"time"
)

// UsedResource is a utility struct to keep track of application resource usage.
type UsedResource struct {
	// UsedResourceMap is a two-level map for aggregated resource usage.
	// The top-level key is the instance type, and the value is a map:
	//   resource type (CPU, memory, etc.) -> aggregated used time (in seconds) of the resource type.
	UsedResourceMap map[string]map[string]int64

	sync.RWMutex
}

// NewUsedResource creates a new instance of UsedResource.
func NewUsedResource() *UsedResource {
	return &UsedResource{UsedResourceMap: make(map[string]map[string]int64)}
}

// NewUsedResourceFromMap creates NewUsedResource from the given map.
// Using for Testing purpose only.
func NewUsedResourceFromMap(m map[string]map[string]int64) *UsedResource {
	if m == nil {
		return NewUsedResource()
	}
	return &UsedResource{UsedResourceMap: m}
}

// Clone creates a deep copy of UsedResource.
func (ur *UsedResource) Clone() *UsedResource {
	if ur == nil {
		return nil
	}
	ret := NewUsedResource()
	ur.RLock()
	defer ur.RUnlock()
	for k, v := range ur.UsedResourceMap {
		dest := make(map[string]int64)
		for key, element := range v {
			dest[key] = element
		}
		ret.UsedResourceMap[k] = dest
	}
	return ret
}

// AggregateUsedResource aggregates resource usage to UsedResourceMap[instType].
// The time the given resource used is the delta between the resource createTime and currentTime.
func (ur *UsedResource) AggregateUsedResource(instType string,
	resource *Resource, bindTime time.Time) {
	if resource == nil {
		return
	}
	ur.Lock()
	defer ur.Unlock()

	releaseTime := time.Now()
	timeDiff := int64(releaseTime.Sub(bindTime).Seconds())
	aggregatedResourceTime, ok := ur.UsedResourceMap[instType]
	if !ok {
		aggregatedResourceTime = map[string]int64{}
	}
	for key, element := range resource.Resources {
		curUsage, ok := aggregatedResourceTime[key]
		if !ok {
			curUsage = 0
		}
		curUsage += int64(element) * timeDiff // resource size times timeDiff
		aggregatedResourceTime[key] = curUsage
	}
	ur.UsedResourceMap[instType] = aggregatedResourceTime
}
