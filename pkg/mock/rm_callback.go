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

package mock

import (
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

type ResourceManagerCallback struct{}

func (f *ResourceManagerCallback) UpdateApplication(_ *si.ApplicationResponse) error {
	return nil
}

func (f *ResourceManagerCallback) UpdateAllocation(_ *si.AllocationResponse) error {
	return nil
}

func (f *ResourceManagerCallback) UpdateNode(_ *si.NodeResponse) error {
	return nil
}

func (f *ResourceManagerCallback) Predicates(_ *si.PredicatesArgs) error {
	// do nothing
	return nil
}

func (f *ResourceManagerCallback) PreemptionPredicates(args *si.PreemptionPredicatesArgs) *si.PreemptionPredicatesResponse {
	// simulate "ideal" preemption check
	return &si.PreemptionPredicatesResponse{
		Success: true,
		Index:   args.StartIndex,
	}
}

func (f *ResourceManagerCallback) SendEvent(_ []*si.EventRecord) {
	// do nothing
}

func (f *ResourceManagerCallback) GetStateDump() (string, error) {
	return "{}", nil
}

func (f *ResourceManagerCallback) UpdateContainerSchedulingState(_ *si.UpdateContainerSchedulingStateRequest) {
	// do nothing
}
