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

# Developer Guide

YuniKorn always works with a container orchestrator system. Currently, a Kubernetes shim [yunikorn-k8shim](https://github.com/apache/incubator-yunikorn-k8shim)
is provided in our repositories, you can leverage it to develop YuniKorn scheduling features and integrate with Kubernetes.
This document describes resources how to setup dev environment and how to do the development.

## Development Environment setup

Read the [environment setup guide](setup/env-setup.md) first to setup Docker and Kubernetes development environment.

## Build YuniKorn for Kubernetes

Prerequisite:
- Go 1.11+

You can build the scheduler for Kubernetes from [yunikorn-k8shim](https://github.com/apache/incubator-yunikorn-k8shim) project.
The build procedure will built all components into a single executable that can be deployed and running on Kubernetes.

Start the integrated build process by pulling the `yunikorn-k8shim` repository:
```bash
mkdir $HOME/yunikorn/
cd $HOME/yunikorn/
git clone https://github.com/apache/incubator-yunikorn-k8shim.git
```
At this point you have an environment that will allow you to build an integrated image for the YuniKorn scheduler.

### Build Docker image

Building a docker image can be triggered by following command.

```
make image
```

The image with the build in configuration can be deployed directly on kubernetes.
Some sample deployments that can be used are found under [deployments](https://github.com/apache/incubator-yunikorn-k8shim/tree/master/deployments/scheduler) directory.
For the deployment that uses a config map you need to set up the ConfigMap in kubernetes.
How to deploy the scheduler with a ConfigMap is explained in the [scheduler configuration deployment](setup/configure-scheduler.md) document.

The image build command will first build the integrated executable and then create the docker image.
Currently, there are some published docker images under [this docker hub repo](https://hub.docker.com/u/yunikorn), you are free to fetch and use.
But keep in mind, YuniKorn has no official release yet, the latest version image can only be used for testing or evaluating, do not use it in production.
The default image tags are not be suitable for deployments to an accessible repository as it uses a hardcoded user and would push to Docker Hub with proper credentials.
You *must* update the `TAG` variable in the `Makefile` to push to an accessible repository.
When you update the image tag be aware that the deployment examples given will also need to be updated to reflect the same change.

### Inspect the docker image

The docker image built from previous step has embedded some important build info in image's metadata. You can retrieve
these info with docker `inspect` command.

```
docker inspect apache/yunikorn:scheduler-latest
```

these info includes git revisions (last commit SHA) for each component, to help you understand which version of the source code
was shipped by this image. They are listed as docker image `labels`, such as

```
"Labels": {
    "BuildTimeStamp": "2019-07-16T23:08:06+0800",
    "Version": "0.1",
    "yunikorn-core-revision": "dca66c7e5a9e",
    "yunikorn-k8shim-revision": "bed60f720b28",
    "yunikorn-scheduler-interface-revision": "3df392eded1f"
}
```

### Dependencies

The dependencies in the projects are managed using [go modules](https://blog.golang.org/using-go-modules).
Go Modules require at least Go version 1.11 to be installed on the development system.

If you want to modify one of the projects locally and build with your local dependencies you will need to change the module file. 
Changing dependencies uses mod `replace` directives as explained in the [local build document](setup/build-local.md).

## Build the web UI

Example deployments reference the [YuniKorn web UI](https://github.com/apache/incubator-yunikorn-web). 
The YuniKorn web UI has its own specific requirements for the build. The project has specific requirements for the build follow the steps in the README to prepare a development environment and build how to build the projects.
The scheduler is fully functional without the web UI. 

## Locally run the integrated scheduler

When you have a local development environment setup you can run the scheduler in your local kubernetes environment.
This has been tested in a Docker desktop with docker for desktop and Minikube. See the [environment setup guide](setup/env-setup.md) for further details.

```
make run
```
It will connect with the kubernetes cluster using the users configured configuration located in `$HOME/.kube/config`.

You can also use the same approach to run the scheduler locally but connecting to a remote kubernetes cluster,
as long as the `$HOME/.kube/config` file is pointing to that remote cluster.

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
See the [coding guidelines documentation](./coding-guidelines.md) for more details. 

As a utility target you can check that all files that must have a license have the correct license by running: 
```
make common-check-license
```

## Design documents

All design documents are located in a central location per component. The core component design documents also contains the design documents for cross component designs.
[List of design documents](design/README.md)
