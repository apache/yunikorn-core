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

package events

import (
	"fmt"

	"github.com/apache/incubator-yunikorn-core/pkg/log"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
	"go.uber.org/zap"
)

type Event interface {
	GetSource() interface{}
	GetReason() string
	GetMessage() string
}

func ToEventMessage(e Event) (*si.EventMessage, error) {
	eventType, id, err := convertSourceToTypeAndID(e.GetSource())
	if err != nil {
		return nil, err
	}
	return &si.EventMessage{
		Type:    eventType,
		ID:      id,
		Reason:  e.GetReason(),
		Message: e.GetMessage(),
	}, nil
}

func convertSourceToTypeAndID(obj interface{}) (si.EventMessage_Type, string, error) {
	// TODO other type checks
	if ask, ok := obj.(*si.AllocationAsk); ok {
		return si.EventMessage_APP, ask.ApplicationID, nil
	}
	log.Logger().Warn("Could not convert source object to EventMessageType", zap.Any("object", obj))

	// TODO should add UNKNOWN request?
	return si.EventMessage_REQUEST, "", fmt.Errorf("Could not ")
}

type baseEvent struct {
	source  interface{}
	reason  string
	message string
}

func (be *baseEvent) GetSource() interface{} {
	return be.source
}

func (be *baseEvent) GetReason() string {
	return be.reason
}

func (be *baseEvent) GetMessage() string {
	return be.message
}


func CreateInsufficientQueueResourcesEvent(ask *si.AllocationAsk, message string) Event {
	return &baseEvent{
		source:  ask,
		reason:  "InsufficientQueueResources",
		message: message,
	}
}