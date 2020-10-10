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

package trace

import (
	"io"
	"sync/atomic"

	"github.com/opentracing/opentracing-go"
)

type SchedulerTracer interface {
	// trace collecting function
	NewTraceContext() SchedulerTraceContext

	// switch function for on-demand tracing requests
	StartOnDemand()
	StopOnDemand()

	Close()
}

var _ SchedulerTracer = &SchedulerTracerImpl{}

type SchedulerTracerImpl struct {
	Tracer         opentracing.Tracer
	Closer         io.Closer
	OnDemandAtomic int32
}

func (s *SchedulerTracerImpl) NewTraceContext() SchedulerTraceContext {
	return &SchedulerTraceContextImpl{
		Tracer:       s.Tracer,
		Spans:        []opentracing.Span{},
		OnDemandFlag: atomic.LoadInt32(&s.OnDemandAtomic) != 0,
	}
}

func (s *SchedulerTracerImpl) StartOnDemand() {
	atomic.StoreInt32(&s.OnDemandAtomic, 1)
}

func (s *SchedulerTracerImpl) StopOnDemand() {
	atomic.StoreInt32(&s.OnDemandAtomic, 0)
}

func (s *SchedulerTracerImpl) Close() {
	s.Closer.Close()
}

func NewSchedulerTracer() (SchedulerTracer, error) {
	tracer, closer, err := NewTracerFromEnv("yunikorn-core-scheduler")
	if err != nil {
		return nil, err
	} else {
		return &SchedulerTracerImpl{
			Tracer:         tracer,
			Closer:         closer,
			OnDemandAtomic: 0,
		}, nil
	}

}
