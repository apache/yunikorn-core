| Event                                     | Flow                  | Notes  |
| ----------------------------------------- | --------------------- | ------ |
| AllocationProposalBundleEvent             | Scheduler -> Cache    | Remove |
| RejectedNewApplicationEvent               | Scheduler -> Cache    | Remove |
| ReleaseAllocationsEvent                   | Scheduler -> Cache    | Remove |
| RemoveRMPartitionsEvent                   | Scheduler -> Cache    | Remove |
| RemovedApplicationEvent                   | Scheduler -> Cache    | Remove |
| RMUpdateRequestEvent                      | RM -> Cache           |        |
| RegisterRMEvent                           | RM -> Cache           |        |
| ConfigUpdateRMEvent                       | RM -> Cache           |        |
| SchedulerNodeEvent                        | Cache -> Scheduler    | Remove |
| SchedulerAllocationUpdatesEvent           | Cache -> Scheduler    | Remove |
| SchedulerApplicationsUpdateEvent          | Cache -> Scheduler    | Remove |
| RemoveRMPartitionsEvent                   | RM -> Scheduler       |        |
| SchedulerUpdatePartitionsConfigEvent      | Cache -> Scheduler    | Remove |
| SchedulerDeletePartitionsConfigEvent      | Cache -> Scheduler    | Remove |
| RMApplicationUpdateEvent (add/remove app) | Cache/Scheduler -> RM |        |
| RMNewAllocationsEvent                     | Cache -> RM           |        |
| RMReleaseAllocationEvent                  | Cache -> RM           |        |
| RMRejectedAllocationAskEvent              | Cache/Scheduler -> RM |        |
| RMNodeUpdateEvent                         | Cache -> RM           |        |
|                                           |                       |        |
|                                           |                       |        |
|                                           |                       |        |
|                                           |                       |        |
|                                           |                       |        |

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

**[TODO]: Add/remove partition, add/remove node, add/remove queue, scheduler config update.** 

## Next Steps 

1. Get rid of Cache object (there should have one object for one purpose)
2. Write locking design.