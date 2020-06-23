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

# YuniKorn Sorting Policies
The scheduler uses policies allow changing the scheduling behaviour without code changes.
Policies can be set for:
* [Applications](#application-sorting)
* [Nodes](#node-sorting)
* [Requests](#request-sorting)

## Application sorting
The application sorting policy is set for each queue via the config.
A sorting policy setting is only effective on a `leaf` queue.
Each `leaf` queue can use a different policy.

A sorting policy only specifies the order in which the applications are sorted within a queue.
That order is crucial in specifying which application is considered first when assigning resources.
Sorting policies do _not_ affect the number of applications that are scheduled or active in the queue at the same time.
All applications that have pending resource requests can and will be scheduled in a queue unless specifically filtered out.
Even when applications are sorted using a first in first out policy multiple applications will run in a queue in parallel. 

A `parent` queue will always use the fair policy to sort the child queues.

The following configuration entry sets the application sorting policy to `fifo` for the queue `root.sandbox`: 
```yaml
partitions:
  - name: default
    queues:
    - name: root
      queues:
      - name: sandbox
        properties:
          application.sort.policy: fifo
```

The only applications that are considered during scheduling must have outstanding requests.
A filter is applied _while_ sorting the applications to remove all that do not have outstanding requests.

### FifoSortPolicy
Short description: first in first out, based on application create time  
Config value: fifo (default)  
Behaviour:  
Before sorting the applications are filtered and must have pending resource requests.

After filtering the applications left are sorted based on the application create time stamp only, no other filtering is applied. 
Since applications can only be added while the system is locked there can never be two applications with the exact same time stamp. 

The result is that the oldest application that requests resources gets resources.
Younger applications will be given resources when all the current requests of older applications have been fulfilled. 

### FairSortPolicy
Short description: fair based on usage  
Config value: fair  
Behaviour:  
Before sorting the applications are filtered and must have pending resource requests.

After filtering the applications left are sorted based on the application usage.
The usage of the application is defined as all confirmed and unconfirmed allocations for the applications. 
All resources defined on the application will be taken into account when calculating the usage.

The result is that the resources available are spread equally over all applications that request resources.

### StateAwarePolicy
Short description: limit of one (1) application in Starting or Accepted state  
Config value: stateaware  
Behaviour:  
This sorting policy requires an understanding of the application states.
Applications states are described in the [application states](object_states.md#application-state) documentation.

Before sorting applications the following filters are applied to all applications in the queue:
The first filter is based on the application state.
The following applications pass through the filter and generate the first intermediate list:
* all applications in the state _running_
* _one_ (1) application in the _starting_ state
* if there are _no_ applications in the _starting_ state _one_ (1) application in the _accepted_ state is added

The second filter takes the result of the first filter as an input.
The preliminary list is filtered again: all applications _without_ a pending request are removed.

After filtering based on status and pending requests the applications that remain are sorted.
The final list is thus filtered twice with the remaining applications sorted on create time.

To recap the _staring_ and _accepted_ state interactions: 
The application in the _accepted_ state is only added if there is no application in the _starting_ state.
The application in the _starting_ state does not have to have pending requests.
Any application in the _starting_ state will prevent _accepted_ applications from being added to the filtered list.

For further details see the [Example run](design/state-aware-scheduling.md#example-run) in the design document.

The result is that already running applications that request resources will get resources first.
A drip feed of one new applications is added to the list of running applications to be allocated after all running applications.  

## Node sorting
The node sorting policy is set for a partition via the config.
Each partition can use a different policy.

The following configuration entry sets the node sorting policy to `fair` for the partition `default`: 
```yaml
partitions:
  - name: default
    nodesortpolicy:
        type: fair
```

### FairnessPolicy
Short description: available resource, descending order  
Config value: fair (default)  
Behaviour:  
Sort the list of nodes by the amount of available resources so that the node with the _highest_ amount of available resource is the first in the list.
All resources defined on a node will be taken into account when calculating the usage.
Resources of the same type are compared for the nodes. 

This results in a node with the lowest utilisation to be considered first for assigning new allocation.
Resulting in a spread of allocations over all available nodes.
Leading to an overall lower utilisation of the individual available nodes, unless the whole environment is highly utilised.
Keeping the load on all nodes at a similar level does help 
In an environment that auto scales by adding new nodes this could trigger unexpected auto scale requests.   

### BinPackingPolicy
Short description: available resource, ascending order  
Config value: binpacking  
Behaviour:  
Sort the list of nodes by the amount of available resources so that the node with the _lowest_ amount of available resource is the first in the list.
All resources defined on a node will be taken into account when calculating the usage. 
Resources of the same type are compared for the nodes. 

This results in a node with the highest utilisation to be considered first for assigning new allocation.
Resulting in a high(er) utilisation of a small(er) number of nodes, better suited for cloud deployments.   

## Request sorting
There are currently two policies for sorting requests within an application.
A request is an ask from an application which maps to one or more pods or containers.
The policy is not set directly on the application but inherited from the queue. 
A sorting policy setting is only effective on a `leaf` queue.
Each `leaf` queue can use a different policy.

Requests are sorted but this is not a guarantee that a newer request will not be allocated before an older request.
There might be cases that a request cannot be allocated immediately.
In that case is a newer request might be allocated before an older request (i.e. a reservation for an old request).  

The following configuration entry sets the request sorting policy to `priority` for the queue `root.sandbox`: 
```yaml
partitions:
  - name: default
    queues:
    - name: root
      queues:
      - name: sandbox
        properties:
          request.sort.policy: priority
```

### FifoSortPolicy
Short description: first in first out, based on application create time  
Config value: fifo (default)  
Behaviour:  
Before sorting the requests are filtered and must have pending asks.

Requests are sorted based on the time the request was created only.
The oldest request will be considered first.

### PrioritySortPolicy
Short description: sorts requests from highest to the lowest priority   
Config value: priority  
Behaviour:  
Before sorting the requests are filtered and must have pending asks.

Priority is a non-negative integer number.
Zero (0) is considered the lowest priority in the range. 

Requests with the same priority for one application are sorted based on the time the request was created.
Within a priority requests are thus always considered in a first in first out order.
