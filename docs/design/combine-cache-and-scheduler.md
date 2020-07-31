# YuniKorn-core: Proposal to combine Cache and Scheduler's implementation

## Background 

Between Cache and Scheduler, they communicate with each other using async events, which causes lots of inconsistent issues. (Such as [~~YUNIKORN-169~~](https://issues.apache.org/jira/browse/YUNIKORN-169)). Also, it complex logic a lot. (See the below table for details).

## Existing event flows between Cache/Scheduler/RMProxy

| Event                                     | Flow                  | Proposal |
| ----------------------------------------- | --------------------- | -------- |
| AllocationProposalBundleEvent             | Scheduler -> Cache    | Remove   |
| RejectedNewApplicationEvent               | Scheduler -> Cache    | Remove   |
| ReleaseAllocationsEvent                   | Scheduler -> Cache    | Remove   |
| RemoveRMPartitionsEvent                   | Scheduler -> Cache    | Remove   |
| RemovedApplicationEvent                   | Scheduler -> Cache    | Remove   |
| RMUpdateRequestEvent                      | RM -> Cache           |          |
| RegisterRMEvent                           | RM -> Cache           |          |
| ConfigUpdateRMEvent                       | RM -> Cache           |          |
| SchedulerNodeEvent                        | Cache -> Scheduler    | Remove   |
| SchedulerAllocationUpdatesEvent           | Cache -> Scheduler    | Remove   |
| SchedulerApplicationsUpdateEvent          | Cache -> Scheduler    | Remove   |
| RemoveRMPartitionsEvent                   | RM -> Scheduler       |          |
| SchedulerUpdatePartitionsConfigEvent      | Cache -> Scheduler    | Remove   |
| SchedulerDeletePartitionsConfigEvent      | Cache -> Scheduler    | Remove   |
| RMApplicationUpdateEvent (add/remove app) | Cache/Scheduler -> RM |          |
| RMNewAllocationsEvent                     | Cache -> RM           |          |
| RMReleaseAllocationEvent                  | Cache -> RM           |          |
| RMRejectedAllocationAskEvent              | Cache/Scheduler -> RM |          |
| RMNodeUpdateEvent                         | Cache -> RM           |          |
|                                           |                       |          |
|                                           |                       |          |
|                                           |                       |          |
|                                           |                       |          |
|                                           |                       |          |

Please note that, there're 10 out of 19 events are only between cache and scheduler. We should have change to remove these events and related logics.

## Detailed analysis (Existing behavior)

### Object exists in both cache and scheduler

There's a contract for scheduler and cache: If there's an object exists in both scheduler and cache (such as `app_info` and  `scheduler_app`, when add an object, it will add to cache first, then scheduler; Removing the object is reverse: it will remove object from scheduler first, then remove from cache.

#### Operations to add/remove app

```
1. RMProxy send cacheevent.RMUpdateRequestEvent, which include UpdateRequest
2. cluster_info.processApplicationUpdateFromRMUpdate
   2.1: Add new apps to partition.
   2.2: Send removed apps to scheduler (but not remove anything from cache)
3. scheduler.processApplicationUpdateEvent
   3.1: Add new apps to scheduler 
        (when fails, send RejectedNewApplicationEvent to cache)
        No matter if failed or not, send RMApplicationUpdateEvent to RM.
   3.2: Remove app from scheduler
        Send RemovedApplicationEvent to cache
```

#### Operations to add/remove allocations

**New allocation initiated by scheduler**

```
1. Scheduler send AllocationProposals
2. cluster_info.processAllocationProposalEvent
   2.1 partitionInfo.addNewAllocation:
   Send SchedulerAllocationUpdatesEvent to scheduler for *any* result:
       When fails: Send SchedulerAllocationUpdatesEvent back to scheduler, 
         ... and set RejectedAllocation.
       When succeeded: set AcceptedAllocations
3. Scheduler handles new add, if fails: 
   Send ReleaseAllocationsEvent to Cache *again* to remove from Cache.
   [YUNIKORN-169]
```

**Release allocation initiated by scheduler (preemption)**

```
1. Scheduler send AllocationProposals
2. cluster_info.processAllocationProposalEvent
   2.1 partitionInfo.processAllocationReleases:
       When failed: nothing happened. 
       When succeeded: 
           - Send to scheduler, notify allocation released.
           - Send to RM for the same.
```

**Release allocation initiated by RM**

````
1. cluster_info handles si.UpdateRequest
2. cluster_info.processNewAndReleaseAllocationRequests
3. (by-pass): Send to scheduler via SchedulerAllocationUpdatesEvent
   set ToRelease
4. scheduler.processAllocationUpdateEvent 
   Update ReconcilePlugin
   Send back to Cache
````

**New ask** 

```
1. cluster_info handles si.UpdateRequest
   - Do sanity check (such as existence of partition/app)
   - If yes, send new ask to scheduler. (via SchedulerAllocationUpdatesEvent)
2. scheduler receive NewAsk
   - (If succeeded) Update scheduler. 
   - (If failed) Send back to *RMProxy* (via RMRejectedAllocationAskEvent) 
     Why directly send to RM? Because Cache doesn't store asks. 
```

**Remove ask (initiated by RM)**

```
1. cluster_info handles si.UpdateRequest
   - Directly send to scheduler (via SchedulerAllocationUpdatesEvent)
2. scheduler.processAllocationReleaseByAllocationKey
   - find partition, app. 
   - Update local asks.
```

**Recover Allocations**

```
1. RM send NewSchedulableNode 
2. Cache handles, and send to Scheduler (via SchedulerAllocationUpdatesEvent)
3. Scheduler: 
   - Create a new ask based on the (to recover) Allocation 
   - Create Allocation Proposal
   ... After that, same as create a new allocation. 
```

**Add/Update Partition** (Scheduler config update)

```
Add/Update Partition: 
- RM register/Config update (Initiated by RMProxy)
- RMProxy send RMRegistrationEvent/ConfigUpdateRMEvent to cache.
- Cache update internal partitions/queues accordingly.
- Cache send to scheduler SchedulerUpdatePartitionsConfigEvent.
- Scheduler update partition/queue info accordingly.
```

**Remove partition**

```
- RM -> Scheduler
- Scheduler remove partition reference.
  - Scheduler set partitionManager.stop = true.
  - PartitionManager removes queues, applications, nodes async. (Why async?)
```

**Remove node** 

```
- RM notify Action of node 
- When a node to be "decomissioned", cache remove the node first (!!), and then notify scheduler  
- Scheduler delete node, and release reserved resources.
```

## How to merge Cache and scheduler objects 

1. `application_info` & `scheduling_application`: **merge** to `scheduler.application`. 
2. `allocation_info` & `scheduling_allocation`: **merge** to `scheduler.allocation`.
3. `node_info` & `scheduling_node`: **merge** to `scheduler.node`
4. `queue_info` & `scheduling_queue`: **merge** to `scheduler.queue`
5. `partition_info` & `scheduling_partition`: **merge** to `scheduler.partition`
6. `cluster_info` &  `scheduling_context`: **merge** to `scheduler.ClusterContext`

## New locking orders (inside scheduler)

### Highlight of lock rules (inside scheduler)

**Cluster Lock:** 

```
Partition is sub object of Cluster: 
- Add/Remove ANY partition needs wlock(Cluster)
- Get/Loop partition needs rlock(Cluster)
```

**Partition Lock:**

```
{Queue, App, Node} belongs to Partition:
- Add/Remove {Queue, App, Node} needs wlock(Partition) 
- Any (potential) write-operation involves > 1 object (∈ {Queue, App, Node}) 
  needs wlock(Partition)
  
  Here's a list of operation needs wlock(Partition)
  - Do new allocation / remove allocation, will change application, queue and node
    so lock partition is required.
    - So commit a scheduling/preemption proposal need acquire wlock(Partition).
  - Same above, new/delete allocation reservation. 
    (Preempt container is a special case of reservation)
  - Change pending request of ask, will change application (pending resource), 
    queue (pending resource). 
    # wangda: this is arguable, if we lock app first, update pending resource, 
    # then lock queue, it maybe fine to not make this operation atomic.
  - Update of Node Resource 
    # It not only affect node's available resource, 
    # but affect Partition's total allocatable Resource)
    
  Example of operations doesn't need wlock(Partition): 
  - Update pre-sorted queues, apps. 
    # This doesn't change any of properties of queues/apps.
  
- Get/Loop {Queue, App, Node} needs rlock(Partition)
  - Scheduler logic to get proposal only needs rlock.
  - Preemption logic to get proposal of preemption only need rlock. 
```

**Queue lock:**

```
{App (for leaf-queue), (direct-children) Queue, resource-usages (allocating, allocated, reserved, etc.)} belongs to Queue: 
- Change of any above field needs wlock(Queue), including:
	- Add/remove App/Queue belong to this queue.
	- Change resource-usage.
	- Change of configuration property.
	
- Read any of the above mentioned fields require rlock(Queue).
```

**Application lock:** 

```
{ParentQueue, ResourceUsage, (reserved/allocated)Allocations, PendingRequests} belong to application: 
- Change of any of above fields need wlock(Application)
- Read of any of above fields need rlock(Application) 
```

**Node lock** 

```
{ResourceUsage, reserved allocation, allocated resource} belong to node: 
- Change of any of above fields need wlock(Node)
- Read of any of above fields need rlock(Node) 
```

### Direction of lock 

It is possible to acquire another lock while holding a lock, but we need to make sure not allow both of: 

- Holding A.lock and acquire B's lock. 
- Holding B.lock and acquire B's lock. 

Here's allowed lock direction: (Anything not mentioned here is NOT allowed).

```
Holding lock of |              Aquire  
--------------------------------------------------------------------------
Queue             Direct-children-queue (For ParentQueue)
Queue             Application (For LeafQueue)
Partition         Any of {Queue, Application, Node} belong to the partition
Application       Node (We need this for allocation / reservation)
```

### Pseudo code for important logics <Need add more>

**RM Registration (add new partition), and RM Config Update (like update partition, remove partition)**

These operations shares a lot of common properties:

```
- RM send RegisterRMEvent/ConfigUpdateRMEvent to scheduler
- Scheduler load config file:
- Scheduler wlock(cluster): 
  - For new partitions: scheduler add new partition #with queues
  - For updated partitions: 
    - scheduler wlock(partition)
      - scheduler wlock(queue) #along the hierarchy
        - update queue configs. 
  - For delete partitions: 
    - scheduler wlock(partition)
      - scheduler.delete_all_apps(partition) # See common method below
      - scheduler.delete_queue(root) # See common method below
      - scheduler.delete_nodes(partition) # See common method below
```

**Function: scheduler.delete_queue(queue)**

```
# Always under wlock(partition)
scheduler.delete_queue(queue)   
   - wlock(queue) 
   - if parent: 
     for child : queue.children()
        delete_queue(child)
   - if leaf: 
     for app: queue.apps() 
        delete_app(app)
        update queue's resource_usage for app.
```

**Function: scheduler.delete_app(queue)**

```
# Always under wlock(partition)
scheduler.delete_app(app)   
   - wlock(app)
     - for alloc : allocated
       - app.remove_allocation(alloc)
     - for reserved: reserved 
       - app.unreserve_allocation(alloc)
```

**Function: app.remove_allocation(alloc)** 

```
# Always under wlock(application)
app.remove_alloc(alloc)
   - if alloc.exist
     - node := alloc.node
     - node.remove_alloc(alloc): 
       - wlock(node)
       - remove alloc from node (also update node.resource_usage)
     - update app.resource_usage
     - return alloc.resource
     >> Emit alloc_completed_event to RMProxy 
     
```



## Next Steps 

1. Get rid of Cache object (there should have one object for one purpose)
2. Write locking design.