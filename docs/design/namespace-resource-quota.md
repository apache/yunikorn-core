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

# Use K8s Namespace in YuniKorn

In K8s, user can setup namespace with [resource quotas](https://kubernetes.io/docs/concepts/policy/resource-quotas/) to limit aggregated resource consumption in this namespace. The validation of namespace resource quotas is handled in api-server directly, therefore YuniKorn simply honors the quotas like the default scheduler.

## Best practice

It is not mandatory to setup YuniKorn queues with respect of namespaces.
However, in practice, it makes more sense to do so.
Namespace is often used to set a cap for resource consumptions per user-group/team,
YuniKorn queue is also meant to divide cluster resource into multiple groups.
Let's go through an example.

### 1. Setup namespace

Namespace: `advertisement`:
```
apiVersion: v1
kind: ResourceQuota
metadata:
  name: advertisement
spec:
  hard:
    requests.cpu: "200m"
    requests.memory: 2000Mi
    limits.cpu: "200m"
    limits.memory: 4000Mi
```
Create the namespace
```
kubectl create namespace advertisement
kubectl create -f ./advertisement.yaml --namespace=advertisement
kubectl get quota --namespace=advertisement
kubectl describe quota advertisement --namespace=advertisement

// output
Name:            advertisement
Namespace:       advertisement
Resource         Used  Hard
--------         ----  ----
limits.cpu       0     200m
limits.memory    0     4000Mi
requests.cpu     0     200m
requests.memory  0     2000Mi
```

### 2. Setup YuniKorn queues

Queue: `advertisement`:
```
name: advertisement
resources:
  guaranteed:
    vcore: 100
    memory: 1000
  max:
    vcore: 200
    memory: 2000
```

ensure `QueueMaxResource <= NamespaceResourceQuotaRequests`

### 3. Mapping applications to queues & namespace

In a pod spec

```
apiVersion: v1
kind: Pod
metadata:
  namespace: advertisement
  labels:
    app: sleep
    applicationId: "application_2019_01_22_00001"
    queue: "root.advertisement"
  name: task0
spec:
  schedulerName: yunikorn
  containers:
    - name: sleep-5s
      image: "alpine:latest"
      command: ["/bin/ash", "-ec", "while :; do echo '.'; sleep 5 ; done"]
      resources:
        requests:
          cpu: "50m"
          memory: "800M"
        limits:
          cpu: "100m"
          memory: "1000M"
```

Check Quota

```
kubectl describe quota advertisement --namespace=advertisement

Name:            advertisement
Namespace:       advertisement
Resource         Used  Hard
--------         ----  ----
limits.cpu       100m  200m
limits.memory    1G    4000Mi
requests.cpu     50m   200m
requests.memory  800M  2000Mi
```

Now submit another application,

```
apiVersion: v1
kind: Pod
metadata:
  namespace: advertisement
  labels:
    app: sleep
    applicationId: "application_2019_01_22_00002"
    queue: "root.advertisement"
  name: task1
spec:
  schedulerName: yunikorn
  containers:
    - name: sleep-5s
      image: "alpine:latest"
      command: ["/bin/ash", "-ec", "while :; do echo '.'; sleep 5 ; done"]
      resources:
        requests:
          cpu: "200m"
          memory: "800M"
        limits:
          cpu: "200m"
          memory: "1000M"
```

pod will not be able to submitted to api-server, because the requested cpu `200m` + used cpu `100m` = `300m` which exceeds the resource quota.

```
kubectl create -f pod_ns_adv_task1.yaml
Error from server (Forbidden): error when creating "pod_ns_adv_task1.yaml": pods "task1" is forbidden: exceeded quota: advertisement, requested: limits.cpu=200m,requests.cpu=200m, used: limits.cpu=100m,requests.cpu=50m, limited: limits.cpu=200m,requests.cpu=200m
```

## Future Work

For compatibility, we should respect namespaces and resource quotas.
Resource quota is overlapped with queue configuration in many ways,
for example the `requests` quota is just like queue's max resource. However,
there are still a few features resource quota can do but queue cannot, such as

1. Resource `limits`. The aggregated resource from all pods in a namespace cannot exceed this limit.
2. Storage Resource Quota, e.g storage size, PVC number, etc.
3. Object Count Quotas, e.g count of PVCs, services, configmaps, etc.
4. Resource Quota can map to priority class.

Probably we can build something similar to cover (3) in this list.
But it would be hard to completely support all these cases.

But currently, setting applications mapping to a queue as well as a corresponding namespace is over complex.
Some future improvements might be:

1. Automatically detects namespaces in k8s-shim and map them to queues. Behind the scenes, we automatically generates queue configuration based on namespace definition. Generated queues are attached under root queue.
2. When new namespace added/updated/removed, similarly to (1), we automatically update queues.
3. User can add more configuration to queues, e.g add queue ACL, add child queues on the generated queues.
4. Applications submitted to namespaces are transparently submitted to corresponding queues.