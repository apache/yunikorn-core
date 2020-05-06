<!--
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 -->

# YuniKorn Roadmap

## What's next

**yunikorn-core**

* [YUNIKORN-1](https://issues.apache.org/jira/browse/YUNIKORN-1): Support app/task priority aware scheduling.
* [YUNIKORN-2](https://issues.apache.org/jira/browse/YUNIKORN-2): Gang Scheduling.
* [YUNIKORN-21](https://issues.apache.org/jira/browse/YUNIKORN-21): Optimize node sorting algorithms.
* [YUNIKORN-42](https://issues.apache.org/jira/browse/YUNIKORN-42): High efficient scheduling events framework phase 1.
* [YUNIKORN-33](https://issues.apache.org/jira/browse/YUNIKORN-33): Performance benchmark with Kubemark.
* [YUNIKORN-131](https://issues.apache.org/jira/browse/YUNIKORN-131): Prometheus integration - phase 2.

**yunikorn-k8shim**

* [YUNIKORN-133](https://issues.apache.org/jira/browse/YUNIKORN-133): Performance improvement: optimize predicate function performance.
* [YUNIKORN-42](https://issues.apache.org/jira/browse/YUNIKORN-42): Publish comprehensive scheduler events to K8s event system. 

**yunikorn-web**

* [YUNIKORN-83](https://issues.apache.org/jira/browse/YUNIKORN-83): Implements the nodes info page.

## v0.8.0 (May 4, 2020)

This release ships a fully functional resource scheduler for Kubernetes with a number of useful features that empower
to run Big Data workloads on K8s. See more at [Release Notes](http://yunikorn.apache.org/release/v0.8.0.html).

**yunikorn-scheduler-interface**

* Communication protocols between RM and scheduler-shim.
* gRPC interfaces.
* Scheduler plugin interfaces.

**yunikorn-core**

* Hierarchy queues with min/max resource quotas.
* Resource fairness between queues, users and apps.
* Cross-queue preemption based on fairness.
* Fair/Bin-packing scheduling policies.
* Placement rules (auto queue creation/mapping).
* Customized resource types (like GPU) scheduling support.
* Rich placement constraints support.
* Automatically map incoming container requests to queues by policies. 
* Node partition: partition cluster to sub-clusters with dedicated quota/ACL management.
* Configuration hot-refresh.
* Stateful recovery.
* Metrics framework.

**yunikorn-k8shim**

* Support K8s predicates. Such as pod affinity/anti-affinity, node selectors.
* Support Persistent Volumes, Persistent Volume Claims, etc.
* Load scheduler configuration from configmap dynamically (hot-refresh).
* 3rd Operator/controller integration, pluggable app discovery.
* Helm chart support.

**yunikorn-web**

* Cluster overview page with brief info about the cluster.
* Read-only application view, including app info and task breakdown info.
* Read-only queue view, displaying queue structure, queue resource, usage info dynamically.
