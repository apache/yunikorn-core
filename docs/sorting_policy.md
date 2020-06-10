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
The applications are sorted based on the application create time stamp.
Since applications can only be added while the system is locked there can never be two applications with the exact same time stamp. 

Before sorting the applications are filtered and must have outstanding requests.

### FairSortPolicy
Short description: fair based on usage  
Config value: fair  
Behaviour:  
The applications are sorted based on the application usage.
The usage of the application is defined as all confirmed and unconfirmed allocations for the applications. 
All resources defined on the application will be taken into account when calculating the usage.

Before sorting the applications are filtered and must have outstanding requests.

### StateAwarePolicy
Short description: limit of one (1) application in Starting or Accepted state  
Config value: stateaware  
Behaviour:  
The applications are sorted based on the application create time stamp, after filtering based on status.
Applications states are described in the [application states](object_states.md#application-state) documentation.

Before sorting applications the following filter is applied to the applications:
* all applications in the state _running_
* only one of the following two:  
    * one application in the _starting_ state
    * one application in the _accepted_ state
* applications must have outstanding requests

The application in the _accepted_ state is only added if there is no application in the _starting_ state.
The application in the _starting_ state does not have to have outstanding requests.
Any application in the _starting_ state will prevent _accepted_ applications from being added to the filtered list.

After the list is filtered all applications are sorted on create time.

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
There is currently one policy for sorting requests within an application.
This policy is not configurable.
Sorting requests is only possible based on the priority of the request.
If there are multiple requests within an application that have the same priority the order of the requests is undetermined.
This means that the order of requests with the same priority can, and most likely will, change between runs.