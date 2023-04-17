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

package tests

import (
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

type MockResourceManagerCallback struct{}

func (f *MockResourceManagerCallback) UpdateApplication(response *si.ApplicationResponse) error {
	return nil
}

func (f *MockResourceManagerCallback) UpdateAllocation(response *si.AllocationResponse) error {
	return nil
}

func (f *MockResourceManagerCallback) UpdateNode(response *si.NodeResponse) error {
	return nil
}

func (f *MockResourceManagerCallback) Predicates(args *si.PredicatesArgs) error {
	// do nothing
	return nil
}

func (f *MockResourceManagerCallback) PreemptionPredicates(args *si.PreemptionPredicatesArgs) *si.PreemptionPredicatesResponse {
	// simulate "ideal" preemption check
	return &si.PreemptionPredicatesResponse{
		Success: true,
		Index:   args.StartIndex,
	}
}

func (f *MockResourceManagerCallback) SendEvent(events []*si.EventRecord) {
	// do nothing
}

func (f *MockResourceManagerCallback) GetStateDump() (string, error) {
	return "{}", nil
}

func (f *MockResourceManagerCallback) UpdateContainerSchedulingState(request *si.UpdateContainerSchedulingStateRequest) {
}
