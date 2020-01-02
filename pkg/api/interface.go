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

package api

import "github.com/cloudera/yunikorn-scheduler-interface/lib/go/si"

type SchedulerAPI interface {
	// Register a new RM, if it is a reconnect from previous RM, cleanup
	// all in-memory data and resync with RM.
	RegisterResourceManager(request *si.RegisterResourceManagerRequest, callback ResourceManagerCallback) (*si.RegisterResourceManagerResponse, error)

	// Update Scheduler status (including node status update, allocation request
	// updates, etc.
	Update(request *si.UpdateRequest) error

	// Notify scheduler to reload configuration and hot-refresh in-memory state based on configuration changes
	ReloadConfiguration(clusterID string) error
}

// RM side needs to implement this API
type ResourceManagerCallback interface {
	RecvUpdateResponse(response *si.UpdateResponse) error
}
