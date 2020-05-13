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

# YuniKorn Partition and Queue Configuration

The basis for the queue configuration is given in the [scheduler design document](./design/scheduler-configuration.md#queue-configuration).

This document provides the generic queue configuration.
It references both the [Access control lists](./acls.md) and [placement rule](./placement_rules.md) documentation.

This document explains how to create the partition and queue configuration for the scheduler with examples.

The scheduler relies on the shim to reliably provide user information as part of the application submission.
In the current shim there is no reliable way to identify the user and the groups the user belongs to.
The user and group information provided by the shim is incomplete in the best case.
This shim limitation impacts the behaviour of user based limits and access control in the scheduler.
The current implementation only provides the framework in the scheduler and will not be fully functional until the shim changes are added. 

## Configuration
The configuration file for the scheduler that is described here only provides the configuration for the partitions and queues.

By default we use the file called `queues.yaml` in our deployments.
The filename can be changed via the command line flag `policyGroup` of the scheduler.
Changing the filename must be followed by corresponding changes in the deployment details, either the `configmap` or the file included in the docker container.

The example file for the configuration is located in [config/queues.yaml](../config/queues.yaml).  

## Partitions
Partitions are the top level of the scheduler configuration.
There can be more than one partition defined in the configuration.

Basic structure for the partition definition in the configuration:
```yaml
partitions:
  - name: <name of the 1st partition>
  - name: <name of the 2nd partition>
```
The default name for the partition is `default`.
The partition definition contains the full configuration for the scheduler for a particular shim.
Each shim uses its own unique partition.

The partition must have at least the following keys defined:
* name
* [queues](#queues)

The queues configuration is explained below.

Optionally the following keys can be defined for a partition:
* [placementrules](#placement-rules)
* [limits](#limits)
* preemption

Placement rules and limits are explained in their own chapters
The preemption key can currently have only one sub key: _enabled_.
This boolean value defines the preemption behaviour for the whole partition.

The default value for _enabled_ is _false_.
Allowed values: _true_ or _false_, any other value will cause a parse error.

Example `partition` yaml entry with _preemption_ flag:
```yaml
partitions:
  - name: <name of the partition>
    preemption:
      enabled: true
```
NOTE:
Currently the Kubernetes unique shim does not support any other partition than the `default` partition..
This has been logged as an [jira](https://issues.apache.org/jira/browse/YUNIKORN-22) for the shim.

### Queues
The _queues_ entry is the main configuration element. 
It defines a hierarchical structure for the queues.

It can have a `root` queue defined but it is not a required element.
If the `root` queue is not defined the configuration parsing will insert the root queue for consistency.
The insertion of the root queue is triggered by:
* If the configuration has more than one queue defined at the top level a root queue is inserted.
* If there is only one queue defined at the top level and it is not called `root` a root queue is inserted.  

The defined queue or queues will become a child queue of the inserted `root` queue.

Basic `queues` yaml entry with sub queue:
```yaml
queues:
- name: <name of the queue>
  queues:
  - name: <name of the queue>
```

Supported parameters for the queues:
* name
* parent
* queues
* properties
* adminacl
* submitacl
* [resources](#resources)
* [limits](#limits)

Each queue must have a _name_.
The name of a queue must be unique at the level that the queue is defined.
Since the queue structure is fully hierarchical queues at different points in the hierarchy may have the same name.
As an example: the queue structure `root.testqueue` and `root.parent.testqueue` is a valid structure.
A queue cannot contain a dot "." character as that character is used to separate the queues in the hierarchy.
If the name is not unique for the queue in the configuration or contains a dot a parsing error is generated and the configuration is rejected. 

Queues in the structure will automatically get a type assigned.
The type of the queue is based on the fact that the queue has children or sub queues.
The two types of queues are:
* parent
* leaf

Applications can only be assigned to a _leaf_ queue.
A queue that has a child or sub queue in the configuration is automatically a _parent_ queue.
If a queue does not have a sub the queue in the configuration it is a _leaf_ queue, unless the `parent` parameter is set to _true_.
Trying to override a _parent_ queue type in the configuration will cause a parsing error of the configuration.   

Sub queues for a parent queue are defined under the `queues` entry.
The `queues` entry is a recursive entry for a queue level and uses the exact same set of parameters.  

The `properties` parameter is a simple key value pair list. 
The list provides a simple set of properties for the queue.
There are no limitations on the key or value values, anything is allowed.
Currently the property list is not used in the scheduler and is only provided for future expansion like the option to turn on or off preemption on a queue or define a sorting order specific for a queue.  

Access to a queue is set via the `adminacl` for administrative actions and for submitting an application via the `submitacl` entry.
ACLs are documented in the [Access control lists](./acls.md) document.

Queue resource limits are set via the `resources` parameter.
User and group limits are set via the `limits` parameter.
As both entries are complex configuration entries they are explained in [resources](#resources) and [limits](#limits) below.

An example configuration of a queue `root.namespaces` as a _parent_ queue with limits:
```yaml
partitions:
  - name: default
    queues:
      - name: namespaces
        parent: true
        resources:
          guaranteed:
            {memory: 1000, vcore: 10}
          max:
            {memory: 10000, vcore: 100}
```

### Placement rules
The placement rules are defined and documented in the [placement rule](./placement_rules.md) document.

Each partition can have only one set of placement rules defined. 
If no rules are defined the placement manager is not started and each application *must* have a queue set on submit.  

### Limits
Limits define a set of limit objects for a partition or queue.
It can be set on either the partition or on a queue at any level.
```yaml
limits:
  - limit: <description>
  - limit: <description>
```

A limit object is a complex configuration object.
It defines one limit for a set of users and or groups.
Multiple independent limits can be set as part of one `limits` entry on a queue or partition.
Users and or groups that are not part of the limit setting will not be limited.

A sample limits entry:
```yaml
limits:
  - limit: <description>
    users:
    - <user name or "*"">
    - <user name>
    groups:
    - <group name or "*"">
    - <group name>
    maxapplications: <1..maxint>
    maxresources:
      <resource name 1>: <0..maxint>
      <resource name 2>: <0..maxint>
```

Limits are applied recursively in the case of a queue limit.
This means that a limit on the `root` queue is an overall limit in the cluster for the user or group.
A `root` queue limit is thus also equivalent with the `partition` limit.

The limit object parameters:
* limit
* users
* groups
* maxapplications
* maxresources

The _limit_ parameter is an optional description of the limit entry.
It is not used for anything but making the configuration understandable and readable. 

The _users_ and _groups_ that can be configured can be one of two types:
* a star "*" 
* a list of users or groups.

If the entry for users or groups contains more than one (1) entry it is always considered a list of either users or groups.
The star "*" is the wildcard character and matches all users or groups.
Duplicate entries in the lists are ignored and do not cause a parsing error.
Specifying a star beside other list elements is not allowed.

_maxapplications_ is an unsigned integer value, larger than 1, which allows you to limit the number of running applications for the configured user or group.
Specifying a zero maximum applications limit is not allowed as it would implicitly deny access.
Denying access must be handled via the ACL entries.  

The _maxresources_ parameter can be used to specify a limit for one or more resources.
The _maxresources_ uses the same syntax as the [resources](#resources) parameter for the queue. 
Resources that are not specified in the list are not limited.
A resource limit can be set to 0.
This prevents the user or group from requesting the specified resource even though the queue or partition has that specific resource available.  
Specifying an overall resource limit of zero is not allowed.
This means that at least one of the resources specified in the limit must be greater than zero.

If a resource is not available on a queue the maximum resources on a queue definition should be used.
Specifying a limit that is effectively zero, _maxapplications_ is zero and all resource limits are zero, is not allowed and will cause a parsing error.
 
A limit is per user or group. 
It is *not* a combined limit for all the users or groups together.

As an example: 
```yaml
limit: "example entry"
maxapplications: 10
users:
- sue
- bob
```
In this case both the users `sue` and `bob` are allowed to run 10 applications.

### Resources
The resources entry for the queue can set the _guaranteed_ and or _maximum_ resources for a queue.
Resource limits are checked recursively.
The usage of a leaf queue is the sum of all assigned resources for that queue.
The usage of a parent queue is the sum of the usage of all queues, leafs and parents, below the parent queue. 

The root queue, when defined, cannot have any resource limit set.
If the root queue has any limit set a parsing error will occur.
The maximum resource limit for the root queue is automatically equivalent to the cluster size.
There is no guaranteed resource setting for the root queue.

Maximum resources when configured place a hard limit on the size of all allocations that can be assigned to a queue at any point in time.
A maximum resource can be set to 0 which makes the resource not available to the queue.
Guaranteed resources are used in calculating the share of the queue and during allocation. 
It is used as one of the inputs for deciding which queue to give the allocation to.
Preemption uses the _guaranteed_ resource of a queue as a base which a queue cannot go below.

Basic `resources` yaml entry:
```yaml
resources:
  guaranteed:
    <resource name 1>: <0..maxint>
    <resource name 2>: <0..maxint>
  max:
    <resource name 1>: <0..maxint>
    <resource name 2>: <0..maxint>
```
Resources that are not specified in the list are not limited, for max resources, or guaranteed in the case of guaranteed resources. 
