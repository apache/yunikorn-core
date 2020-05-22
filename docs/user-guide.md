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

# User Guide

Before reading this guide, we assume you either have a Kubernetes cluster, or a local Kubernetes dev environment, e.g MiniKube.
It is also assumed that `kubectl` is on your path and properly configured.
Follow this [guide](setup/env-setup.md) on how to setup a local Kubernetes cluster using docker-desktop.

All files mentioned in this user guide are part of the [yunikorn-k8shim](https://github.com/apache/incubator-yunikorn-k8shim) repository.
They are located in the [deployments](https://github.com/apache/incubator-yunikorn-k8shim/tree/master/deployments/scheduler) sub directory. The command given assume that you are located in that directory.

## Quick Start

The easiest way to get started is to our [helm charts](https://github.com/apache/incubator-yunikorn-k8shim/tree/master/helm-charts) to deploy YuniKorn on an existing Kubernetes cluster. Recommand to use Helm 3 or later versions.

```shell script
helm install yunikorn ./yunikorn
```

it will firstly create a `configmap` where stores YuniKorn configuration, and then deploy YuniKorn scheduler
and web UI containers in a pod as a `deployment` in the `default` namespace. If you want to deploy YuniKorn to another namespace, you can do following:

```shell script
kubectl create namespace yunikorn-ns
helm install yunikorn ./yunikorn --namespace yunikorn-ns
```

Uninstall yunikorn:
```shell script
helm uninstall yunikorn --namespace yunikorn-ns
```

If you don't want to use helm charts, you can find our step-by-step
tutorial [here](setup/deployment.md).

## Run workloads with YuniKorn Scheduler

Unlike default Kubernetes scheduler, YuniKorn has `application` notion in order to support batch workloads better.
There are a few ways to run batch workloads with YuniKorn scheduler

- Add labels `applicationId` and `queue` in pod's spec.
- Pods that have the same applicationId will be considered as tasks from 1 application.

Here is an example of the entry to add:
```yaml
  labels:
    applicationId: "MyOwnApplicationId"
    queue: "root.sandbox"
```   
All examples provided in the next section have the labels already set. The `queue` name must be a known queue name from the configuration.
Unknown queue names will cause the pod to be rejected by the YuniKorn scheduler.  

### Running simple sample applications

All sample deployments can be found under `examples` directory.
The list of all examples is in the [README](https://github.com/apache/incubator-yunikorn-k8shim/blob/master/deployments/examples).
Not all examples are given here. Further details can be found in that README.

A single pod based on a standard nginx image: 
```
kubectl create -f examples/nginx/nginx.yaml
```
A simple sleep job example:
```
kubectl create -f examples/sleep/sleeppods.yaml
```
The files for these examples can be found in the [README nginx](https://github.com/apache/incubator-yunikorn-k8shim/tree/master/deployments/examples#nginx) and the [README sleep](https://github.com/apache/incubator-yunikorn-k8shim/tree/master/deployments/examples#sleep) sections.

### Running a spark application
Kubernetes support for Apache Spark is not part of all releases. You must have a current release of Apache Spark with Kubernetes support built in. 

The `examples/spark` directory contains pod template files for the Apache Spark driver and executor, they can be used if you want to run Spark on K8s using this scheduler.

* Get latest spark from github (only latest code supports to specify pod template), URL: https://github.com/apache/spark
* Build spark with Kubernetes support:
```
mvn -Pyarn -Phadoop-2.7 -Dhadoop.version=2.7.0 -Phive -Pkubernetes -Phive-thriftserver -DskipTests package
```
* Run spark submit
```
spark-submit --master k8s://http://localhost:8001 --deploy-mode cluster --name spark-pi \
  --class org.apache.spark.examples.SparkPi \
  --conf spark.executor.instances=1 \
  --conf spark.kubernetes.container.image=yunikorn/spark:latest \
  --conf spark.kubernetes.driver.podTemplateFile=examples/spark/driver.yaml \
  --conf spark.kubernetes.executor.podTemplateFile=examples/spark/executor.yaml \
  local:///opt/spark/examples/jars/spark-examples_2.12-3.0.0-SNAPSHOT.jar
```

Spark uses its own version of the application ID tag called *spark-app-id*. This tags is required for the pods to be recognised as specific spark pods.  
* examples/spark/driver.yaml
* examples/spark/executor.yaml
When you run Spark on Kubernetes with pod templates, *spark-app-id* is considered the applicationId.
A script to run the spark application and the yaml files are in the [README spark](https://github.com/apache/incubator-yunikorn-k8shim/tree/master/deployments/examples#spark) section.

### Running a simple Tensorflow job 
There is an example for Tensorflow job. You must install tf-operator first. 
You can install tf-operator by applying all yaml from two website down below:
* CRD: https://github.com/kubeflow/manifests/tree/master/tf-training/tf-job-crds/base
* Deployment: https://github.com/kubeflow/manifests/tree/master/tf-training/tf-job-operator/base
Also you can install kubeflow which can auto install tf-operator for you, URL: https://www.kubeflow.org/docs/started/getting-started/

A simple Tensorflow job example:

You need to [build the image](https://github.com/kubeflow/tf-operator/tree/master/examples/v1/dist-mnist) which used in example yaml.
```
kubectl create -f examples/tfjob/tf-job-mnist.yaml
```

The file for this example can be found in the [README Tensorflow job](https://github.com/apache/incubator-yunikorn-k8shim/tree/master/deployments/examples#Tensorflow-job) section.

### Affinity scheduling
The scheduler supports affinity and ati affinity scheduling on kubernetes using predicates:
```
kubectl create -f examples/predicates/pod-anti-affinity-example.yaml
```
This deployment ensures 2 pods cannot be co-located together on same node.
If this yaml is deployed on 1 node cluster, expect 1 pod to be started and the other pod should stay in a pending state.
More examples on affinity and anti affinity scheduling in the predicates section of the [README predicates](https://github.com/apache/incubator-yunikorn-k8shim/tree/master/deployments/examples#predicates)

### Volume examples
There are three examples with volumes available. The NFS example does not work on docker desktop and requires [minikube](https://kubernetes.io/docs/tasks/tools/install-minikube/). 
The EBS volume requires a kubernetes cluster running on AWS (EKS).
Further instructions for the volume examples in the section of the [README Volumes](https://github.com/apache/incubator-yunikorn-k8shim/tree/master/deployments/examples#volumes).

CAUTION: All examples will generate an unending stream of data in a file called `dates.txt` on the mounted volume. This could cause a disk to fill up and execution time should be limited. 

#### Local volume
* create the local volume and volume claim
```
kubectl create -f examples/volume/local-pv.yaml
```
* create the pod that uses the volume
```
kubectl create -f examples/volume/pod-local.yaml
```

#### NFS volume
* create the NFS server
```
kubectl create -f nfs-server.yaml
```
* get the IP address for the NFS server and update the pod yaml by replacing the existing example IP with the one returned:
```
kubectl get services | grep nfs-server | awk '{print $3}'
```
* create the pod that uses the volume
```
kubectl create -f pod-nfs.yaml
```

#### EBS volume
The Volume for the first two examples must be created before you can run the examples. The `VolumeId` must be updated in the yaml files to get this to work.
To create a volume you can use the command line or web UI:
```
aws ec2 create-volume --volume-type gp2 --size 10 --availability-zone us-west-1
```
The `VolumeId` is part of the returned information of the create command.

* create the pod that uses a direct volume reference:
```
kubectl create -f pod-ebs-direct.yaml
```
* create the persistent volume (pv) and a pod that uses a persistent volume claim (pvc) to claim the existing volume:
```
kubectl create -f ebs-pv.yaml
kubectl create -f pod-ebs-exist.yaml
```
* create a storage class to allow dynamic provisioning and create the pod that uses this mechanism:
```
kubectl create -f storage-class.yaml
kubectl create -f pod-ebs-dynamic.yaml
```
Dynamic provisioning has a number of pre-requisites for it to work, see [Dynamic Volume Provisioning](https://kubernetes.io/docs/concepts/storage/dynamic-provisioning/) in the kubernetes docs. 
The dynamically created volume will be automatically destroyed as soon as the pod is stopped.
