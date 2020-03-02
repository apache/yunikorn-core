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
package webservice

import (
	"net/http"
	"net/http/pprof"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Route struct {
	Name        string
	Method      string
	Pattern     string
	HandlerFunc http.HandlerFunc
}

type Routes []Route

var routes = Routes{
	// endpoints to retrieve general scheduler info
	Route{
		"Scheduler",
		"GET",
		"/ws/v1/queues",
		GetQueueInfo,
	},
	Route{
		"Cluster",
		"GET",
		"/ws/v1/clusters",
		GetClusterInfo,
	},
	Route{
		"Scheduler",
		"GET",
		"/ws/v1/apps",
		GetApplicationsInfo,
	},
	Route{
		"Scheduler",
		"GET",
		"/ws/v1/nodes",
		GetNodesInfo,
	},

	// endpoint to retrieve goroutines info
	Route{
		"Scheduler",
		"GET",
		"/ws/v1/stack",
		GetStackInfo,
	},

	// endpoint to retrieve server metrics
	Route{
		"Scheduler",
		"GET",
		"/ws/v1/metrics",
		promhttp.Handler().ServeHTTP,
	},

	// endpoint to retrieve CPU, Memory profiling data,
	// this works with pprof tool. By default, pprof endpoints
	// are only registered to http.DefaultServeMux. Here, we
	// need to explicitly register all handlers.
	Route{
		Name:        "System",
		Method:      "GET",
		Pattern:     "/debug/pprof/",
		HandlerFunc: pprof.Index,
	},
	Route{
		Name:        "System",
		Method:      "GET",
		Pattern:     "/debug/pprof/heap",
		HandlerFunc: pprof.Index,
	},
	Route{
		Name:        "System",
		Method:      "GET",
		Pattern:     "/debug/pprof/threadcreate",
		HandlerFunc: pprof.Index,
	},
	Route{
		Name:        "System",
		Method:      "GET",
		Pattern:     "/debug/pprof/goroutine",
		HandlerFunc: pprof.Index,
	},
	Route{
		Name:        "System",
		Method:      "GET",
		Pattern:     "/debug/pprof/allocs",
		HandlerFunc: pprof.Index,
	},
	Route{
		Name:        "System",
		Method:      "GET",
		Pattern:     "/debug/pprof/block",
		HandlerFunc: pprof.Index,
	},
	Route{
		Name:        "System",
		Method:      "GET",
		Pattern:     "/debug/pprof/mutex",
		HandlerFunc: pprof.Index,
	},
	Route{
		Name:        "System",
		Method:      "GET",
		Pattern:     "/debug/pprof/cmdline",
		HandlerFunc: pprof.Cmdline,
	},
	Route{
		Name:        "System",
		Method:      "GET",
		Pattern:     "/debug/pprof/profile",
		HandlerFunc: pprof.Profile,
	},
	Route{
		Name:        "System",
		Method:      "GET",
		Pattern:     "/debug/pprof/symbol",
		HandlerFunc: pprof.Symbol,
	},
	Route{
		Name:        "System",
		Method:      "GET",
		Pattern:     "/debug/pprof/trace",
		HandlerFunc: pprof.Trace,
	},
}
