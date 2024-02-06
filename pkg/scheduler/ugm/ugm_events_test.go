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
	"testing"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/events/mock"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

var usage = resources.NewResourceFromMap(map[string]resources.Quantity{"cpu": 123})

func TestIncUsageUser(t *testing.T) {
	eventSystem := mock.NewEventSystemDisabled()
	events := newUGMEvents(eventSystem)
	events.sendIncResourceUsageForUser("user1", path1, usage)
	assert.Equal(t, 0, len(eventSystem.Events))

	eventSystem = mock.NewEventSystem()
	events = newUGMEvents(eventSystem)
	events.sendIncResourceUsageForUser("user1", path1, usage)
	assert.Equal(t, 1, len(eventSystem.Events))
	event := eventSystem.Events[0]
	assert.Equal(t, "user1", event.ObjectID)
	assert.Equal(t, path1, event.ReferenceID)
	assert.Equal(t, si.EventRecord_USERGROUP, event.Type)
	assert.Equal(t, si.EventRecord_UG_USER_RESOURCE, event.EventChangeDetail)
	assert.Equal(t, si.EventRecord_ADD, event.EventChangeType)
	assert.Equal(t, "", event.Message)
	assert.Assert(t, event.Resource != nil)
	assert.Equal(t, 1, len(event.Resource.Resources))
	assert.Equal(t, int64(123), event.Resource.Resources["cpu"].Value)
}

func TestIncUsageGroup(t *testing.T) {
	eventSystem := mock.NewEventSystemDisabled()
	events := newUGMEvents(eventSystem)
	events.sendIncResourceUsageForGroup("group1", path1, usage)
	assert.Equal(t, 0, len(eventSystem.Events))

	eventSystem = mock.NewEventSystem()
	events = newUGMEvents(eventSystem)
	events.sendIncResourceUsageForGroup("group1", path1, usage)
	assert.Equal(t, 1, len(eventSystem.Events))
	event := eventSystem.Events[0]
	assert.Equal(t, "group1", event.ObjectID)
	assert.Equal(t, path1, event.ReferenceID)
	assert.Equal(t, si.EventRecord_USERGROUP, event.Type)
	assert.Equal(t, si.EventRecord_UG_GROUP_RESOURCE, event.EventChangeDetail)
	assert.Equal(t, si.EventRecord_ADD, event.EventChangeType)
	assert.Equal(t, "", event.Message)
	assert.Assert(t, event.Resource != nil)
	assert.Equal(t, 1, len(event.Resource.Resources))
	assert.Equal(t, int64(123), event.Resource.Resources["cpu"].Value)
}

func TestDecUsageUser(t *testing.T) {
	eventSystem := mock.NewEventSystemDisabled()
	events := newUGMEvents(eventSystem)
	events.sendDecResourceUsageForUser("user1", path1, usage)
	assert.Equal(t, 0, len(eventSystem.Events))

	eventSystem = mock.NewEventSystem()
	events = newUGMEvents(eventSystem)
	events.sendDecResourceUsageForUser("user1", path1, usage)
	assert.Equal(t, 1, len(eventSystem.Events))
	event := eventSystem.Events[0]
	assert.Equal(t, "user1", event.ObjectID)
	assert.Equal(t, path1, event.ReferenceID)
	assert.Equal(t, si.EventRecord_USERGROUP, event.Type)
	assert.Equal(t, si.EventRecord_UG_USER_RESOURCE, event.EventChangeDetail)
	assert.Equal(t, si.EventRecord_REMOVE, event.EventChangeType)
	assert.Equal(t, "", event.Message)
	assert.Assert(t, event.Resource != nil)
	assert.Equal(t, 1, len(event.Resource.Resources))
	assert.Equal(t, int64(123), event.Resource.Resources["cpu"].Value)
}

func TestDecUsageGroup(t *testing.T) {
	eventSystem := mock.NewEventSystemDisabled()
	events := newUGMEvents(eventSystem)
	events.sendDecResourceUsageForGroup("group1", path1, usage)
	assert.Equal(t, 0, len(eventSystem.Events))

	eventSystem = mock.NewEventSystem()
	events = newUGMEvents(eventSystem)
	events.sendDecResourceUsageForGroup("group1", path1, usage)
	assert.Equal(t, 1, len(eventSystem.Events))
	event := eventSystem.Events[0]
	assert.Equal(t, "group1", event.ObjectID)
	assert.Equal(t, path1, event.ReferenceID)
	assert.Equal(t, si.EventRecord_USERGROUP, event.Type)
	assert.Equal(t, si.EventRecord_UG_GROUP_RESOURCE, event.EventChangeDetail)
	assert.Equal(t, si.EventRecord_REMOVE, event.EventChangeType)
	assert.Equal(t, "", event.Message)
	assert.Assert(t, event.Resource != nil)
	assert.Equal(t, 1, len(event.Resource.Resources))
	assert.Equal(t, int64(123), event.Resource.Resources["cpu"].Value)
}

func TestAppGroupLinked(t *testing.T) {
	eventSystem := mock.NewEventSystemDisabled()
	events := newUGMEvents(eventSystem)
	events.sendAppGroupLinked("group1", "app1")
	assert.Equal(t, 0, len(eventSystem.Events))

	eventSystem = mock.NewEventSystem()
	events = newUGMEvents(eventSystem)
	events.sendAppGroupLinked("group1", "app1")
	assert.Equal(t, 1, len(eventSystem.Events))
	event := eventSystem.Events[0]
	assert.Equal(t, "group1", event.ObjectID)
	assert.Equal(t, "app1", event.ReferenceID)
	assert.Equal(t, si.EventRecord_USERGROUP, event.Type)
	assert.Equal(t, si.EventRecord_UG_APP_LINK, event.EventChangeDetail)
	assert.Equal(t, si.EventRecord_SET, event.EventChangeType)
	assert.Equal(t, "", event.Message)
	assert.Equal(t, 0, len(event.Resource.Resources))
}

func TestAppGroupUnlinked(t *testing.T) {
	eventSystem := mock.NewEventSystemDisabled()
	events := newUGMEvents(eventSystem)
	events.sendAppGroupUnlinked("group1", "app1")
	assert.Equal(t, 0, len(eventSystem.Events))

	eventSystem = mock.NewEventSystem()
	events = newUGMEvents(eventSystem)
	events.sendAppGroupUnlinked("group1", "app1")
	assert.Equal(t, 1, len(eventSystem.Events))
	event := eventSystem.Events[0]
	assert.Equal(t, "group1", event.ObjectID)
	assert.Equal(t, "app1", event.ReferenceID)
	assert.Equal(t, si.EventRecord_USERGROUP, event.Type)
	assert.Equal(t, si.EventRecord_UG_APP_LINK, event.EventChangeDetail)
	assert.Equal(t, si.EventRecord_REMOVE, event.EventChangeType)
	assert.Equal(t, "", event.Message)
	assert.Equal(t, 0, len(event.Resource.Resources))
}

func TestUserLimitSet(t *testing.T) {
	eventSystem := mock.NewEventSystemDisabled()
	events := newUGMEvents(eventSystem)
	events.sendLimitSetForUser("user1", path1)
	assert.Equal(t, 0, len(eventSystem.Events))

	eventSystem = mock.NewEventSystem()
	events = newUGMEvents(eventSystem)
	events.sendLimitSetForUser("user1", path1)
	assert.Equal(t, 1, len(eventSystem.Events))
	event := eventSystem.Events[0]
	assert.Equal(t, "user1", event.ObjectID)
	assert.Equal(t, path1, event.ReferenceID)
	assert.Equal(t, si.EventRecord_USERGROUP, event.Type)
	assert.Equal(t, si.EventRecord_UG_USER_LIMIT, event.EventChangeDetail)
	assert.Equal(t, si.EventRecord_SET, event.EventChangeType)
	assert.Equal(t, "", event.Message)
	assert.Equal(t, 0, len(event.Resource.Resources))
}

func TestUserLimitCleared(t *testing.T) {
	eventSystem := mock.NewEventSystemDisabled()
	events := newUGMEvents(eventSystem)
	events.sendLimitRemoveForUser("user1", path1)
	assert.Equal(t, 0, len(eventSystem.Events))

	eventSystem = mock.NewEventSystem()
	events = newUGMEvents(eventSystem)
	events.sendLimitRemoveForUser("user1", path1)
	assert.Equal(t, 1, len(eventSystem.Events))
	event := eventSystem.Events[0]
	assert.Equal(t, "user1", event.ObjectID)
	assert.Equal(t, path1, event.ReferenceID)
	assert.Equal(t, si.EventRecord_USERGROUP, event.Type)
	assert.Equal(t, si.EventRecord_UG_USER_LIMIT, event.EventChangeDetail)
	assert.Equal(t, si.EventRecord_REMOVE, event.EventChangeType)
	assert.Equal(t, "", event.Message)
	assert.Equal(t, 0, len(event.Resource.Resources))
}

func TestGroupLimitSet(t *testing.T) {
	eventSystem := mock.NewEventSystemDisabled()
	events := newUGMEvents(eventSystem)
	events.sendLimitSetForGroup("group1", path1)
	assert.Equal(t, 0, len(eventSystem.Events))

	eventSystem = mock.NewEventSystem()
	events = newUGMEvents(eventSystem)
	events.sendLimitSetForGroup("group1", path1)
	assert.Equal(t, 1, len(eventSystem.Events))
	event := eventSystem.Events[0]
	assert.Equal(t, "group1", event.ObjectID)
	assert.Equal(t, path1, event.ReferenceID)
	assert.Equal(t, si.EventRecord_USERGROUP, event.Type)
	assert.Equal(t, si.EventRecord_UG_GROUP_LIMIT, event.EventChangeDetail)
	assert.Equal(t, si.EventRecord_SET, event.EventChangeType)
	assert.Equal(t, "", event.Message)
	assert.Equal(t, 0, len(event.Resource.Resources))
}

func TestGroupLimitCleared(t *testing.T) {
	eventSystem := mock.NewEventSystemDisabled()
	events := newUGMEvents(eventSystem)
	events.sendLimitRemoveForGroup("group1", path1)
	assert.Equal(t, 0, len(eventSystem.Events))

	eventSystem = mock.NewEventSystem()
	events = newUGMEvents(eventSystem)
	events.sendLimitRemoveForGroup("group1", path1)
	assert.Equal(t, 1, len(eventSystem.Events))
	event := eventSystem.Events[0]
	assert.Equal(t, "group1", event.ObjectID)
	assert.Equal(t, path1, event.ReferenceID)
	assert.Equal(t, si.EventRecord_USERGROUP, event.Type)
	assert.Equal(t, si.EventRecord_UG_GROUP_LIMIT, event.EventChangeDetail)
	assert.Equal(t, si.EventRecord_REMOVE, event.EventChangeType)
	assert.Equal(t, "", event.Message)
	assert.Equal(t, 0, len(event.Resource.Resources))
}
