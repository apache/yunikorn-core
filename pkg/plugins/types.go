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
	"sync"

	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
)

type SchedulerPlugins struct {
	predicatesPlugin       PredicatesPlugin
	reconcilePlugin        ReconcilePlugin
	eventPlugin            EventPlugin
	schedulingStateUpdater ContainerSchedulingStateUpdater
	configPlugin		   ConfigMapPlugin

	sync.RWMutex
}

// RM side implements this API when it can provide plugin for predicates.
type PredicatesPlugin interface {
	// Run a certain set of predicate functions to determine if a proposed allocation
	// can be allocated onto a node.
	Predicates(args *si.PredicatesArgs) error
}

type ReconcilePlugin interface {
	// RM side implements this API when it can provide plugin for reconciling
	// Re-sync scheduler cache can sync some in-cache (yunikorn-core side) state changes
	// to scheduler cache (shim-side), such as assumed allocations.
	ReSyncSchedulerCache(args *si.ReSyncSchedulerCacheArgs) error
}

type EventPlugin interface {
	// This plugin is responsible for transmitting events to the shim side.
	// Events can be further exposed from the shim.
	SendEvent(events []*si.EventRecord)
}

// Scheduler core can update container scheduling state to the RM,
// the shim side can determine what to do incorporate with the scheduling state
type ContainerSchedulingStateUpdater interface {
	// update container scheduling state to the shim side
	// this might be called even the container scheduling state is unchanged
	// the shim side cannot assume to only receive updates on state changes
	// the shim side implementation must be thread safe
	Update(request *si.UpdateContainerSchedulingStateRequest)
}

type ConfigMapPlugin interface {
	UpdateConfigMap(args *si.ConfigMapArgs) (string, error)
}
