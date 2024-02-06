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
	"github.com/apache/yunikorn-core/pkg/common"
	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/events"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

type ugmEvents struct {
	eventSystem events.EventSystem
}

func (evt *ugmEvents) sendIncResourceUsageForUser(user, queuePath string, allocated *resources.Resource) {
	if !evt.eventSystem.IsEventTrackingEnabled() {
		return
	}
	event := events.CreateUserGroupEventRecord(user, common.Empty, queuePath, si.EventRecord_ADD, si.EventRecord_UG_USER_RESOURCE, allocated)
	evt.eventSystem.AddEvent(event)
}

func (evt *ugmEvents) sendIncResourceUsageForGroup(group, queuePath string, allocated *resources.Resource) {
	if !evt.eventSystem.IsEventTrackingEnabled() {
		return
	}
	event := events.CreateUserGroupEventRecord(group, common.Empty, queuePath, si.EventRecord_ADD, si.EventRecord_UG_GROUP_RESOURCE, allocated)
	evt.eventSystem.AddEvent(event)
}

func (evt *ugmEvents) sendDecResourceUsageForUser(user, queuePath string, allocated *resources.Resource) {
	if !evt.eventSystem.IsEventTrackingEnabled() {
		return
	}
	event := events.CreateUserGroupEventRecord(user, common.Empty, queuePath, si.EventRecord_REMOVE, si.EventRecord_UG_USER_RESOURCE, allocated)
	evt.eventSystem.AddEvent(event)
}

func (evt *ugmEvents) sendDecResourceUsageForGroup(group, queuePath string, allocated *resources.Resource) {
	if !evt.eventSystem.IsEventTrackingEnabled() {
		return
	}
	event := events.CreateUserGroupEventRecord(group, common.Empty, queuePath, si.EventRecord_REMOVE, si.EventRecord_UG_GROUP_RESOURCE, allocated)
	evt.eventSystem.AddEvent(event)
}

func (evt *ugmEvents) sendAppGroupLinked(group, applicationID string) {
	if !evt.eventSystem.IsEventTrackingEnabled() {
		return
	}
	event := events.CreateUserGroupEventRecord(group, common.Empty, applicationID, si.EventRecord_SET, si.EventRecord_UG_APP_LINK, nil)
	evt.eventSystem.AddEvent(event)
}

func (evt *ugmEvents) sendAppGroupUnlinked(group, applicationID string) {
	if !evt.eventSystem.IsEventTrackingEnabled() {
		return
	}
	event := events.CreateUserGroupEventRecord(group, common.Empty, applicationID, si.EventRecord_REMOVE, si.EventRecord_UG_APP_LINK, nil)
	evt.eventSystem.AddEvent(event)
}

func (evt *ugmEvents) sendLimitSetForUser(user, queuePath string) {
	if !evt.eventSystem.IsEventTrackingEnabled() {
		return
	}
	event := events.CreateUserGroupEventRecord(user, common.Empty, queuePath, si.EventRecord_SET, si.EventRecord_UG_USER_LIMIT, nil)
	evt.eventSystem.AddEvent(event)
}

func (evt *ugmEvents) sendLimitSetForGroup(group, queuePath string) {
	if !evt.eventSystem.IsEventTrackingEnabled() {
		return
	}
	event := events.CreateUserGroupEventRecord(group, common.Empty, queuePath, si.EventRecord_SET, si.EventRecord_UG_GROUP_LIMIT, nil)
	evt.eventSystem.AddEvent(event)
}

func (evt *ugmEvents) sendLimitRemoveForUser(user, queuePath string) {
	if !evt.eventSystem.IsEventTrackingEnabled() {
		return
	}
	event := events.CreateUserGroupEventRecord(user, common.Empty, queuePath, si.EventRecord_REMOVE, si.EventRecord_UG_USER_LIMIT, nil)
	evt.eventSystem.AddEvent(event)
}

func (evt *ugmEvents) sendLimitRemoveForGroup(group, queuePath string) {
	if !evt.eventSystem.IsEventTrackingEnabled() {
		return
	}
	event := events.CreateUserGroupEventRecord(group, common.Empty, queuePath, si.EventRecord_REMOVE, si.EventRecord_UG_GROUP_LIMIT, nil)
	evt.eventSystem.AddEvent(event)
}

func newUGMEvents(evt events.EventSystem) *ugmEvents {
	return &ugmEvents{
		eventSystem: evt,
	}
}
