# User Guide

Before reading this guide, we assume you either have a Kubernetes cluster, or a local Kubernetes dev environment, e.g MiniKube.
It is also assumed that `kubectl` is on your path and properly configured.
Not get there? Here is a [guide](https://github.infra.cloudera.com/yunikorn/k8s-shim/blob/master/docs/env-setup.md) about how to setup a local Kubernetes cluster using docker-desktop.

## Deploy YuniKorn Scheduler onto Kubernetes Cluster

Download [scheduler.yaml](https://github.infra.cloudera.com/yunikorn/k8s-shim/blob/master/deployments/scheduler/scheduler.yaml) to local, run command:

```
kubectl create -f scheduler.yaml
```

the deployment will run 2 containers from our pre-built docker images in 1 pod,

* yunikorn-scheduler-core (scheduler core plus k8s-shim)
* yunikorn-scheduler-web (web UI)

the pod is deployed as a customized scheduler, it will take the responsibility to schedule pods which explicitly specifies `schedulerName: yunikorn` in pod's spec.
Here is a [sample yaml file](https://raw.githubusercontent.com/kubernetes/website/master/content/en/examples/admin/sched/pod3.yaml) that specifies scheduler name for a pod.

## Run workloads with YuniKorn Scheduler

Unlike default Kubernetes scheduler, YuniKorn has `job` notion in order to support batch workloads better.
There are a few ways to run workloads with YuniKorn scheduler

- Add labels `jobId` and `queue` in pod's spec. Pods that has same jobId will be considered as tasks from 1 job.   

  Here are some examples:
  - [sleep](https://github.infra.cloudera.com/yunikorn/k8s-shim/blob/master/deployments/sleep/sleeppods.yaml): 3 pods simply runs sleep command as a 3 tasks job.
  - [nginx](https://github.infra.cloudera.com/yunikorn/k8s-shim/blob/master/deployments/nigix/nginxjob.yaml): a single nginx application as a single task job.
   
  Simply deploy them with following command,
   
  ```
  kubectl create -f sleep.yaml
  ```

- Run Spark on Kubernetes with pod templates, `spark-app-id` is considered as Spark jobId.   

  Unfortunately pod-template support is only available in Spark upstream. The full guide of running Spark with YuniKorn can be found [here](https://github.infra.cloudera.com/yunikorn/k8s-shim/blob/master/docs/spark.md).