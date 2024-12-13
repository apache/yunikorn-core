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

package common

import "errors"

var (
	// InvalidQueueName returned when queue name is invalid
	InvalidQueueName = errors.New("invalid queue name, max 64 characters consisting of alphanumeric characters and '-', '_', '#', '@', '/', ':' allowed")
	// ErrorReservingAlloc returned when an ask that is allocated tries to reserve a node.
	ErrorReservingAlloc = errors.New("ask already allocated, no reservation allowed")
	// ErrorDuplicateReserve returned when the same reservation already exists on the application
	ErrorDuplicateReserve = errors.New("reservation already exists")
	// ErrorNodeAlreadyReserved returned when the node is already reserved, failing the reservation
	ErrorNodeAlreadyReserved = errors.New("node is already reserved")
	// ErrorNodeNotFitReserve returned when the allocation does not fit on an empty node, failing the reservation
	ErrorNodeNotFitReserve = errors.New("reservation does not fit on node")
)

// Constant messages for AllocationLog entries
const (
	PreemptionPreconditionsFailed = "Preemption preconditions failed"
	PreemptionDoesNotGuarantee    = "Preemption queue guarantees check failed"
	PreemptionShortfall           = "Preemption helped but short of resources"
	PreemptionDoesNotHelp         = "Preemption does not help"
	NoVictimForRequiredNode       = "No fit on required node, preemption does not help"
)
