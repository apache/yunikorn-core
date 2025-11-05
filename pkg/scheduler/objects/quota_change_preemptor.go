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

package objects

type QuotaChangePreemptionContext struct {
	queue *Queue
}

func NewQuotaChangePreemptor(queue *Queue) *QuotaChangePreemptionContext {
	preemptor := &QuotaChangePreemptionContext{
		queue: queue,
	}
	return preemptor
}

func (qcp *QuotaChangePreemptionContext) CheckPreconditions() bool {
	if !qcp.queue.IsLeafQueue() || !qcp.queue.IsManaged() || qcp.queue.HasTriggerredQuotaChangePreemption() || qcp.queue.IsQuotaChangePreemptionRunning() {
		return false
	}
	if qcp.queue.GetMaxResource().StrictlyGreaterThanOnlyExisting(qcp.queue.GetAllocatedResource()) {
		return false
	}
	return true
}

func (qcp *QuotaChangePreemptionContext) tryPreemption() {
	// quota change preemption has started, so mark the flag
	qcp.queue.MarkQuotaChangePreemptionRunning()

	// Preemption logic goes here

	// quota change preemption has really evicted victims, so mark the flag
	qcp.queue.MarkTriggerredQuotaChangePreemption()
}
