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

package metrics

import "sync"

var once sync.Once
var m *Metrics

type Metrics struct {
	scheduler CoreSchedulerMetrics
	queues QueuesMetrics
}

func init() {
	once.Do(func() {
		m = &Metrics{
			scheduler: initSchedulerMetrics(),
			queues: initQueuesMetrics(),
		}
	})
}

func GetSchedulerMetrics() CoreSchedulerMetrics {
	return m.scheduler
}

func GetQueueMetrics(name string) CoreQueueMetrics {
	return m.queues.forQueue(name)
}