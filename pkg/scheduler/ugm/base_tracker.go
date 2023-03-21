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

package ugm

import (
	"sync"

	"github.com/apache/yunikorn-core/pkg/common/resources"
)

type BaseTracker struct {
	sync.RWMutex
	queueTracker *QueueTracker // Holds the actual resource usage of queue path where application run
}

func (bt *BaseTracker) increaseTrackedResource(queuePath, applicationID string, usage *resources.Resource, f func()) error {
	bt.Lock()
	defer bt.Unlock()
	f() // specific extra piece of code based on User/Group tracker
	return bt.queueTracker.increaseTrackedResource(queuePath, applicationID, usage)
}

func (bt *BaseTracker) decreaseTrackedResource(queuePath, applicationID string, usage *resources.Resource, removeApp bool, f func()) error {
	bt.Lock()
	defer bt.Unlock()
	f() // specific extra piece of code based on User/Group tracker
	return bt.queueTracker.decreaseTrackedResource(queuePath, applicationID, usage, removeApp)
}
