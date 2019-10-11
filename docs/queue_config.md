# YuniKorn Partition and Queue Configuration

The basis for the queue configuration is given in the [scheduler design document](./design/scheduler-configuration.md#queue-configuration).

This document provides the generic queue configuration. It references both the [Access control lists](./acls.md) and [placement rule](./placement_rules.md) documentation.

This document explains how to create the partition and queue configuration for the scheduler with examples.

## Configuration
The configuration file for the scheduler that is described here only provides the configuration for the partitions and queues.

By default we use the file called `queues.yaml` in our deployments. The filename can be changed via the command line flag `policyGroup` of the scheduler.
Changing the filename must be followed by corresponding changes in the deployment details, either the `configmap` or the file included in the docker container.

The example file for the configuration is located in [config/queues.yaml](../config/queues.yaml).  

## Partitions
Partitions are the top level of the scheduler configuration. There can be more than one partition defined in the configuration.

```yaml
partitions:
  - name: partition1
  - name: partition2
```
The default name for the partition is `default`.
Currently there is no 

### Placement rules
The placement rules are 

### Queues


### Users
