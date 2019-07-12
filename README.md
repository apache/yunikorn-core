# YuniKorn - A Universal Scheduler

YuniKorn is a light-weighted, universal resource scheduler for container orchestrator systems.
It was created to achieve fine-grained resource sharing for various workloads efficiently on a large scale, multi-tenant,
and cloud-native environment. YuniKorn brings a unified, cross-platform scheduling experience for mixed workloads consists
of stateless batch workloads and stateful services. Support for but not limited to, YARN and Kubernetes.

## Architecture

![Architecture](docs/images/architecture.jpg)

YuniKorn is a platform-neutral scheduler. Core scheduling logic is encapsulated in [yunikorn-core](./), that is decoupled with
underneath platform by leveraging a common interface [yunikorn-scheduler-interface](https://github.com/cloudera/yunikorn-scheduler-interface).
To integrate with a platform, a scheduler shim needs to be deployed on the host system, such as [yunikorn-k8shim](https://github.com/cloudera/yunikorn-k8shim) for Kubernetes.
The underneath platform still manages all resources, but delegates all scheduling decisions to YuniKorn.

## Key features

Here are some key features of YuniKorn.

- Features to support both batch jobs and long-running/stateful services
- Hierarchy queues with min/max resource quotas.
- Resource fairness between queues, users and apps.
- Cross-queue preemption based on fairness.
- Customized resource types (like GPU) scheduling support.
- Rich placement constraints support.
- Automatically map incoming container requests to queues by policies. 
- Node partition: partition cluster to sub-clusters with dedicated quota/ACL management. 

The current road map for the whole project is [here](docs/roadmap.md), where you can find more information about what
are already supported and future plans.

## Components

YuniKorn consists of the following components spread over multiple code repositories.

- Scheduler core:
  + Purpose: Define the brain of the scheduler, which makes placement decisions (Allocate container X on node Y) according to pre configured policies.
  + Link: [this repository](./)
- Scheduler interface:
  + Purpose: Define the common scheduler interface used by shims and the core scheduler.
  Contains the API layer (with GRPC/programming language bindings) which is agnostic to resource management platform like YARN/K8s.
  + Link: [https://github.com/cloudera/yunikorn-scheduler-interface](https://github.com/cloudera/yunikorn-scheduler-interface)
- Resource Manager shims: 
  + Purpose: Built-in support to allow YARN/K8s talks to scheduler interface. Which can be configured on existing clusters without code change.
    + k8s-shim: [https://github.com/cloudera/yunikorn-k8shim](https://github.com/cloudera/yunikorn-k8shim)
    + Purpose: Define the Kubernetes scheduler shim 
- Scheduler User Interface
  + Purpose: Define the YuniKorn web interface
  + Link: [https://github.com/cloudera/yunikorn-web](https://github.com/cloudera/yunikorn-web)

## Building and using Yunikorn

The build of Yunikorn differs per component. Each component has its own build scripts.
Building an integrated image and the build for just the core component is in [this guide](docs/build.md).

An detailed overview on how to build each component, separately, is part of the specific component readme.

## Design documents
All design documents are located in a central location per component. The core component design documents also contains the design documents for cross component designs.
[List of design documents](docs/design/design-index.md)

## How do I contribute code?

See how to contribute code from [this guide](docs/how-to-contribute.md).