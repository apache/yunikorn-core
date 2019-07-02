/*
Copyright 2019 Cloudera, Inc.  All rights reserved.

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

import "github.com/cloudera/scheduler-interface/lib/go/si"

type SchedulerPlugins struct {
	predicatesPlugin PredicatesPlugin
	volumesPlugin    VolumesPlugin
	reconcilePlugin  ReconcilePlugin
}

// RM side implements this API when it can provide plugin for predicates.
type PredicatesPlugin interface {
	// Run a certain set of predicate functions to determine if a proposed allocation
	// can be allocated onto a node.
	Predicates(args *si.PredicatesArgs) error
}

// RM side implements this API when it can provide plugin for volumes.
type VolumesPlugin interface {
	// Bind the volumes after allocation has been confirmed
	//VolumesBind(allocationId string, node string) error
}

type ReconcilePlugin interface {
	// RM side implements this API when it can provide plugin for reconciling
	// Re-sync scheduler cache can sync some in-cache (yunikorn-core side) state changes
	// to scheduler cache (shim-side), such as assumed allocations.
	ReSyncSchedulerCache(args *si.ReSyncSchedulerCacheArgs) error
}
