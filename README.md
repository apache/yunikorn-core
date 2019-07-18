# YuniKorn - A Universal Scheduler

YuniKorn is a light-weighted, universal resource scheduler for container orchestrator systems.
It was created to achieve fine-grained resource sharing for various workloads efficiently on a large scale, multi-tenant,
and cloud-native environment. YuniKorn brings a unified, cross-platform scheduling experience for mixed workloads consists
of stateless batch workloads and stateful services. Support for but not limited to, YARN and Kubernetes.

## Architecture

Following chart illustrates the high-level architecture of YuniKorn.

![Architecture](docs/images/architecture.jpg)

YuniKorn consists of the following components spread over multiple code repositories.

- _Scheduler core_: Define the brain of the scheduler, which makes placement decisions (Allocate container X on node Y)
  according to pre configured policies. See more in current repo [yunikorn-core](https://github.com/cloudera/yunikorn-core).
- _Scheduler interface_: Define the common scheduler interface used by shims and the core scheduler.
  Contains the API layer (with GRPC/programming language bindings) which is agnostic to container orchestrator systems like YARN/K8s.
  See more in [yunikorn-scheduler-interface](https://github.com/cloudera/yunikorn-scheduler-interface).
- _Resource Manager shims_: Built-in support to allow container orchestrator systems talks to scheduler interface.
   Which can be configured on existing clusters without code change.
   Currently, [yunikorn-k8shim](https://github.com/cloudera/yunikorn-k8shim) is available for Kubernetes integration. 
- _Scheduler User Interface_: Define the YuniKorn web interface for app/queue management.
   See more in [yunikorn-web](https://github.com/cloudera/yunikorn-web).
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

## How to use

The simplest way to run YuniKorn is to build a docker image and then deployed to Kubernetes with a yaml file,
running as a customized scheduler. Then you can run workloads with this scheduler.
See more instructions from [here](./docs/user-guide.md).

## How can I get involved?

We welcome any form of contributions, code, documentation or even discussions. To get involved, please read following resources.
- Before you contributing code or documentation to YuniKorn, please read our [Developer Guide](docs/developer-guide.md).
- Please read [How to Contribute](docs/how-to-contribute.md) to understand the procedure and guidelines of making contributions.

## Other Resources

YuniKorn demo videos are published to a [Youtube channel](https://www.youtube.com/channel/UCDSJ2z-lEZcjdK27tTj_hGw),
watch these demo videos may help you to get started with YuniKorn easier.

- [Running YuniKorn on Kubernetes - a 12 minutes Hello-world demo](https://www.youtube.com/watch?v=cCHVFkbHIzo)

Blog posts

- [YuniKorn: a universal resource scheduler](https://blog.cloudera.com/blog/2019/07/yunikorn-a-universal-resource-scheduler/)