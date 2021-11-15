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

type route struct {
	Name        string
	Method      string
	Pattern     string
	HandlerFunc http.HandlerFunc
}

type routes []route

var webRoutes = routes{
	// endpoints to retrieve general scheduler info
	route{
		"Scheduler",
		"GET",
		"/ws/v1/queues",
		getQueueInfo,
	},
	route{
		"Cluster",
		"GET",
		"/ws/v1/clusters",
		getClusterInfo,
	},
	route{
		"Cluster",
		"GET",
		"/ws/v1/clusters/utilization",
		getClusterUtilization,
	},
	route{
		"Scheduler",
		"GET",
		"/ws/v1/apps",
		getApplicationsInfo,
	},
	route{
		"Scheduler",
		"GET",
		"/ws/v1/nodes",
		getNodesInfo,
	},
	route{
		"Scheduler",
		"GET",
		"/ws/v1/nodes/utilization",
		getNodesUtilization,
	},

	// endpoint to retrieve goroutines info
	route{
		"Scheduler",
		"GET",
		"/ws/v1/stack",
		getStackInfo,
	},

	// endpoint to retrieve server metrics
	route{
		"Scheduler",
		"GET",
		"/ws/v1/metrics",
		promhttp.Handler().ServeHTTP,
	},

	// endpoint to retrieve the current conf
	route{
		"Scheduler",
		"GET",
		"/ws/v1/config",
		getClusterConfig,
	},

	// endpoint to update the current conf
	route{
		"Scheduler",
		"PUT",
		"/ws/v1/config",
		updateClusterConfig,
	},

	// endpoint to create the conf, but currently limited for conf validation purpose alone
	route{
		"Scheduler",
		"POST",
		"/ws/v1/config",
		createClusterConfig,
	},

	// endpoint to validate conf
	route{
		"Scheduler",
		"POST",
		"/ws/v1/validate-conf",
		validateConf,
	},

	// endpoint to retrieve historical data
	route{
		"Scheduler",
		"GET",
		"/ws/v1/history/apps",
		getApplicationHistory,
	},
	route{
		"Scheduler",
		"GET",
		"/ws/v1/history/containers",
		getContainerHistory,
	},
	route{
		"Partitions",
		"GET",
		"/ws/v1/partitions",
		getPartitions,
	},
	route{
		"Scheduler",
		"GET",
		"/ws/v1/partition/{partition}/queues",
		getPartitionQueues,
	},
	route{
		"Scheduler",
		"PUT",
		"/ws/v1/loglevel/{level}",
		setLogLevel,
	},
	route{
		"Scheduler",
		"GET",
		"/ws/v1/loglevel",
		getLogLevel,
	},
	route{
		"Scheduler",
		"GET",
		"/ws/v1/partition/{partition}/nodes",
		getPartitionNodes,
	},
	route{
		"Scheduler",
		"GET",
		"/ws/v1/partition/{partition}/queue/{queue}/applications",
		getQueueApplications,
	},
	route{
		"Scheduler",
		"GET",
		"/ws/v1/fullstatedump",
		getFullStateDump,
	},
	route{
		"Scheduler",
		"PUT",
		"/ws/v1/periodicstatedump/{switch}/{period}",
		handlePeriodicStateDump,
	},
	// endpoint to retrieve CPU, Memory profiling data,
	// this works with pprof tool. By default, pprof endpoints
	// are only registered to http.DefaultServeMux. Here, we
	// need to explicitly register all handlers.
	route{
		Name:        "System",
		Method:      "GET",
		Pattern:     "/debug/pprof/",
		HandlerFunc: pprof.Index,
	},
	route{
		Name:        "System",
		Method:      "GET",
		Pattern:     "/debug/pprof/heap",
		HandlerFunc: pprof.Index,
	},
	route{
		Name:        "System",
		Method:      "GET",
		Pattern:     "/debug/pprof/threadcreate",
		HandlerFunc: pprof.Index,
	},
	route{
		Name:        "System",
		Method:      "GET",
		Pattern:     "/debug/pprof/goroutine",
		HandlerFunc: pprof.Index,
	},
	route{
		Name:        "System",
		Method:      "GET",
		Pattern:     "/debug/pprof/allocs",
		HandlerFunc: pprof.Index,
	},
	route{
		Name:        "System",
		Method:      "GET",
		Pattern:     "/debug/pprof/block",
		HandlerFunc: pprof.Index,
	},
	route{
		Name:        "System",
		Method:      "GET",
		Pattern:     "/debug/pprof/mutex",
		HandlerFunc: pprof.Index,
	},
	route{
		Name:        "System",
		Method:      "GET",
		Pattern:     "/debug/pprof/cmdline",
		HandlerFunc: pprof.Cmdline,
	},
	route{
		Name:        "System",
		Method:      "GET",
		Pattern:     "/debug/pprof/profile",
		HandlerFunc: pprof.Profile,
	},
	route{
		Name:        "System",
		Method:      "GET",
		Pattern:     "/debug/pprof/symbol",
		HandlerFunc: pprof.Symbol,
	},
	route{
		Name:        "System",
		Method:      "GET",
		Pattern:     "/debug/pprof/trace",
		HandlerFunc: pprof.Trace,
	},
	// endpoint to check health status
	route{
		"Scheduler",
		"GET",
		"/ws/v1/scheduler/healthcheck",
		checkHealthStatus,
	},
}
