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

package scheduler

type SubjectType int

const (
	NodeSubject = iota
)

// SubjectManager is intended for tracking all events on specific subjects like node subject
// and notifying the events to registered observers such as an implementation of NodeSortingAlgorithm.
type SubjectManager struct {
	observers map[SubjectType][]Observer
}

func NewSubjectManager() *SubjectManager {
	return &SubjectManager{
		observers: make(map[SubjectType][]Observer),
	}
}

func (sm *SubjectManager) RegisterObserver(subjectType SubjectType, observer Observer) {
	observers, ok := sm.observers[subjectType]
	if !ok {
		observers = make([]Observer, 0)
	}
	observers = append(observers, observer)
	sm.observers[subjectType] = observers
}

func (sm *SubjectManager) NotifyEvent(subjectType SubjectType, event interface{}) {
	observers, ok := sm.observers[subjectType]
	if ok {
		for _, observer := range observers {
			observer.HandleSubjectEvent(subjectType, event)
		}
	}
}

type Observer interface {
	HandleSubjectEvent(subjectType SubjectType, event interface{})
}

type NodeSubjectAddEvent struct {
	node *SchedulingNode
}

type NodeSubjectRemoveEvent struct {
	nodeID string
}

type NodeSubjectResourceUpdateEvent struct {
	node *SchedulingNode
}
