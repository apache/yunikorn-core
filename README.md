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
# Apache YuniKorn (Incubating) - A Universal Scheduler

[![Build Status](https://travis-ci.org/apache/incubator-yunikorn-core.svg?branch=master)](https://travis-ci.org/apache/incubator-yunikorn-core)
[![codecov](https://codecov.io/gh/apache/incubator-yunikorn-core/branch/master/graph/badge.svg)](https://codecov.io/gh/apache/incubator-yunikorn-core)
[![Go Report Card](https://goreportcard.com/badge/github.com/apache/incubator-yunikorn-core)](https://goreportcard.com/report/github.com/apache/incubator-yunikorn-core)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Repo Size](https://img.shields.io/github/repo-size/apache/incubator-yunikorn-core)](https://img.shields.io/github/repo-size/apache/incubator-yunikorn-core)

<img src="https://raw.githubusercontent.com/apache/incubator-yunikorn-core/master/images/logo/yunikorn-logo-blue.png" width="200">

----

Apache YuniKorn (Incubating) is a light-weight, universal resource scheduler for container orchestrator systems.
It is created to achieve fine-grained resource sharing for various workloads efficiently on a large scale, multi-tenant,
and cloud-native environment. YuniKorn brings a unified, cross-platform, scheduling experience for mixed workloads that consist
of stateless batch workloads and stateful services. 

YuniKorn now supports K8s and can be deployed as a custom K8s scheduler. YuniKorn's architecture design also allows adding different
shim layer and adopt to different ResourceManager implementation including Apache Hadoop YARN, or any other systems.

## Get Started

See how to get started with running YuniKorn on Kubernetes, please read the documentation on [yunikorn.apache.org](http://yunikorn.apache.org/docs/).

Want to know more about the value of the YuniKorn project, and what YuniKorn can do? Here is some
[session recordings and demos](http://yunikorn.apache.org/community/sessions).

## Get Involved

Please read [get involved](http://yunikorn.apache.org/community/get_involved) document if you want to discuss issues,
contribute your ideas, explore use cases, or participate the development.

## Code Structure

Apache YuniKorn (Incubating) project has the following git repositories:

- [yunikorn-core](https://github.com/apache/incubator-yunikorn-core/) : the scheduler brain :round_pushpin: 
- [yunikorn-k8shim](https://github.com/apache/incubator-yunikorn-k8shim) : the adaptor to Kubernetes
- [yunikorn-scheduler-interface](https://github.com/apache/incubator-yunikorn-scheduler-interface) : the common scheduling interface
- [yunikorn-web](https://github.com/apache/incubator-yunikorn-web) : the web UI
- [yunikorn-release](https://github.com/apache/incubator-yunikorn-release/): the repo manages yunikorn releases, including the helm charts
- [yunikorn-site](https://github.com/apache/incubator-yunikorn-site/): the source code for [yunikorn website](http://yunikorn.apache.org/)

The `yunikorn-core` is the brain of the scheduler, which makes placement decisions (allocate container X on node Y) according
to the builtin rich scheduling policies. Scheduler core implementation is agnostic to the underneath resource manager system.

## Core component build

The scheduler core, this repository build, by itself does not provide a functional scheduler. 
It just builds the core scheduler functionality without any resource managers or shims.
A functional scheduler must have at least one resource manager that registers.

### Build steps

The core component contains two command line tools: the `simplescheduler` and the `schedulerclient`.
The two command line tools have been provided as examples only and are not supposed to implement all functionality.

Building the example command line tools:
```
make commands
```  

Run all unit tests for the core component: 
```
make test
```
Any changes made to the core code should not cause any existing tests to fail.

Running the lint tool over the current code:
```
make lint
```
See the [coding guidelines documentation](http://yunikorn.apache.org/community/coding_guidelines) for more details. 

As a utility target you can check that all files that must have a license have the correct license by running: 
```
make common-check-license
```

## Design documents

All design documents are located in the YuniKorn website. The core component design documents 
also contains the design documents for cross component designs.
[List of design documents](http://yunikorn.apache.org/docs/next/design/architecture)
