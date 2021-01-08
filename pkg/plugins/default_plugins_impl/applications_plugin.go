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

package default_plugins_impl

import (
	"sort"
	"time"

	"github.com/apache/incubator-yunikorn-core/pkg/interfaces"
	"github.com/apache/incubator-yunikorn-core/pkg/metrics"
	"github.com/apache/incubator-yunikorn-core/pkg/scheduler/policies"
)

var DefaultApplicationsPluginInstance = &DefaultApplicationsPlugin{}

// This is the default implementation of ApplicationsPlugin
type DefaultApplicationsPlugin struct {
}

func (drp *DefaultApplicationsPlugin) NewApplications(queue interfaces.Queue) interfaces.Applications {
	return NewDefaultApplications(queue)
}

// This is an implementation of Requests which keeps all requests in a map.
// It's not thread-safe, must be called while holding the lock of application.
type DefaultApplications struct {
	mapper *CommonMapper
	queue  interfaces.Queue
}

func NewDefaultApplications(queue interfaces.Queue) interfaces.Applications {
	return &DefaultApplications{
		mapper: NewCommonMapper(),
		queue:  queue.(interfaces.Queue),
	}
}

func (da *DefaultApplications) AddApplication(app interfaces.Application) interfaces.Application {
	if app == nil {
		return nil
	}
	if existingApp := da.mapper.Add(app.GetApplicationID(), app); existingApp != nil {
		return existingApp.(interfaces.Application)
	}
	return nil
}

func (da *DefaultApplications) RemoveApplication(appID string) interfaces.Application {
	if removedApp := da.mapper.Remove(appID); removedApp != nil {
		return removedApp.(interfaces.Application)
	}
	return nil
}

func (da *DefaultApplications) GetApplication(appID string) interfaces.Application {
	if app := da.mapper.Get(appID); app != nil {
		return app.(interfaces.Application)
	}
	return nil
}

func (da *DefaultApplications) GetApplications(
	filter func(request interfaces.Application) bool) []interfaces.Application {
	apps := make([]interfaces.Application, 0)
	for _, app := range da.mapper.items {
		if filter == nil || filter(app.(interfaces.Application)) {
			apps = append(apps, app.(interfaces.Application))
		}
	}
	return apps
}

func (da *DefaultApplications) Size() int {
	return da.mapper.Size()
}

func (da *DefaultApplications) Reset() {
	da.mapper.Reset()
}

func (da *DefaultApplications) SortForAllocation() interfaces.AppIterator {
	// find pending apps
	var apps []interfaces.Application
	var comparators []func(l, r interfaces.Application, queue interfaces.Queue) (ok bool, less bool)
	switch da.queue.GetSortType() {
	case policies.FifoSortPolicy:
		apps = FilterOnPendingResources(da.queue.GetCopyOfApps())
		comparators = append(comparators, CompareSubmissionTime)
	case policies.FairSortPolicy:
		apps = FilterOnPendingResources(da.queue.GetCopyOfApps())
		comparators = append(comparators, CompareFairness)
	case policies.StateAwarePolicy:
		apps = StateAwareFilter(da.queue.GetCopyOfApps())
		comparators = append(comparators, CompareSubmissionTime)
	}
	// Sort applications based on the sort policy of queue
	SortApps(da.queue, apps, false, comparators)
	// Return iterator of apps
	return NewDefaultAppIterator(apps)
}

func (da *DefaultApplications) SortForPreemption() interfaces.AppIterator {
	//TODO this should be implemented when refactoring the preemption process
	return nil
}

func SortApps(queue interfaces.Queue, apps []interfaces.Application, reverse bool,
	comparators []func(l, r interfaces.Application, queue interfaces.Queue) (ok bool, less bool)) {
	if len(apps) > 1 {
		sortingStart := time.Now()
		sort.SliceStable(apps, func(i, j int) bool {
			for _, comparator := range comparators {
				if ok, compV := comparator(apps[i], apps[j], queue); ok {
					if reverse {
						return !compV
					}
					return compV
				}
			}
			return true
		})
		metrics.GetSchedulerMetrics().ObserveAppSortingLatency(sortingStart)
	}
}

// This struct helps to manage items using map structure, can be used by DefaultRequests and DefaultApplications etc.
type CommonMapper struct {
	items map[string]interface{}
}

func NewCommonMapper() *CommonMapper {
	return &CommonMapper{
		items: make(map[string]interface{}),
	}
}

func (cm *CommonMapper) Add(key string, item interface{}) interface{} {
	existingItem := cm.items[key]
	cm.items[key] = item
	return existingItem
}

func (cm *CommonMapper) Remove(key string) interface{} {
	existingItem := cm.items[key]
	if existingItem != nil {
		delete(cm.items, key)
	}
	return existingItem
}

func (cm *CommonMapper) Get(key string) interface{} {
	return cm.items[key]
}

func (cm *CommonMapper) Size() int {
	return len(cm.items)
}

func (cm *CommonMapper) Reset() {
	cm.items = make(map[string]interface{})
}

func (cm *CommonMapper) GetItems() map[string]interface{} {
	return cm.items
}

type DefaultAppIterator struct {
	apps  []interfaces.Application
	index int
}

func NewDefaultAppIterator(apps []interfaces.Application) interfaces.AppIterator {
	return &DefaultAppIterator{
		apps:  apps,
		index: 0,
	}
}

func (dai *DefaultAppIterator) HasNext() (ok bool) {
	return dai.index < len(dai.apps)
}

func (dai *DefaultAppIterator) Next() interfaces.Application {
	if dai.index >= len(dai.apps) {
		return nil
	}
	app := dai.apps[dai.index]
	dai.index++
	return app
}

func (dai *DefaultAppIterator) Size() int {
	return len(dai.apps)
}
