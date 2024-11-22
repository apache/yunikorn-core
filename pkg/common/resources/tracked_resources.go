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
	"fmt"
	"strings"
	"time"

	"golang.org/x/exp/maps"

	"github.com/apache/yunikorn-core/pkg/locking"
)

// TrackedResource is a utility struct to keep track of application resource usage.
type TrackedResource struct {
	// TrackedResourceMap is a two-level map for aggregated resource usage.
	// The top-level key is the instance type, and the value is a map:
	//   resource type (CPU, memory, etc.) -> aggregated used time (in seconds) of the resource type.
	TrackedResourceMap map[string]*Resource

	locking.RWMutex
}

// NewTrackedResource creates a new instance of TrackedResource.
func NewTrackedResource() *TrackedResource {
	return &TrackedResource{TrackedResourceMap: make(map[string]*Resource)}
}

// NewTrackedResourceFromMap creates NewTrackedResource from the given map.
// Using for Testing purpose only.
func NewTrackedResourceFromMap(m map[string]map[string]Quantity) *TrackedResource {
	trackedMap := make(map[string]*Resource)
	for inst, inner := range m {
		trackedMap[inst] = NewResourceFromMap(inner)
	}
	return &TrackedResource{TrackedResourceMap: trackedMap}
}

func (tr *TrackedResource) String() string {
	if tr == nil {
		return "TrackedResource{}"
	}
	tr.RLock()
	defer tr.RUnlock()

	var resourceUsage []string
	for instanceType, resourceTypeMap := range tr.TrackedResourceMap {
		for resourceType, usageTime := range resourceTypeMap.Resources {
			resourceUsage = append(resourceUsage, fmt.Sprintf("%s:%s=%d", instanceType, resourceType, usageTime))
		}
	}

	return fmt.Sprintf("TrackedResource{%s}", strings.Join(resourceUsage, ","))
}

// Clone creates a deep copy of TrackedResource.
func (tr *TrackedResource) Clone() *TrackedResource {
	if tr == nil {
		return nil
	}
	ret := NewTrackedResource()
	tr.RLock()
	defer tr.RUnlock()
	for k, v := range tr.TrackedResourceMap {
		ret.TrackedResourceMap[k] = v.Clone()
	}
	return ret
}

// AggregateTrackedResource aggregates resource usage to TrackedResourceMap[instType].
// The time the given resource used is the delta between the resource createTime and currentTime.
func (tr *TrackedResource) AggregateTrackedResource(instType string, resource *Resource, bindTime time.Time) {
	if resource == nil {
		return
	}
	tr.Lock()
	defer tr.Unlock()

	releaseTime := time.Now()
	timeDiff := int64(releaseTime.Sub(bindTime).Seconds())
	aggregatedResourceTime, ok := tr.TrackedResourceMap[instType]
	if !ok {
		aggregatedResourceTime = NewResource()
	}
	for key, element := range resource.Resources {
		aggregatedResourceTime.Resources[key] += element * Quantity(timeDiff)
	}
	tr.TrackedResourceMap[instType] = aggregatedResourceTime
}

// EqualsDAO compares the TrackedResource against the DAO map that was created of the resource.
// Test use only
func (tr *TrackedResource) EqualsDAO(right map[string]map[string]int64) bool {
	if tr == nil {
		return len(right) == 0
	}
	tr.RLock()
	defer tr.RUnlock()
	if len(tr.TrackedResourceMap) != len(right) {
		return false
	}

	for k, v := range tr.TrackedResourceMap {
		if inner, ok := right[k]; !ok {
			return false
		} else if !maps.Equal(v.DAOMap(), inner) {
			return false
		}
	}
	return true
}

// DAOMap converts the TrackedResource into a map structure for use in the REST API.
func (tr *TrackedResource) DAOMap() map[string]map[string]int64 {
	daoMAP := make(map[string]map[string]int64)
	if tr != nil {
		tr.RLock()
		defer tr.RUnlock()
		for k, res := range tr.TrackedResourceMap {
			daoMAP[k] = res.DAOMap()
		}
	}
	return daoMAP
}
