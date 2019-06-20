# Resilience Design

This is not a HA (High-availability) design, HA implies that a service can
survive from a fatal software/hardware failure. That requires one or more
standby instances providing same services to take over active instance on failures.
Resilience here means for yunikorn, we can restart it without losing its state.

## The problem

YuniKorn is designed as a stateless service, it doesn't persist its state, e.g
applications/queues/allocations etc, to any persistent storage. All states are
in memory only. This design ensures yunikorn to be able to response requests with
low latency, and deployment mode is simple. However, a restart (or recovery) will
have the problem to lose state data. We need a decent way to reconstruct all
previous states on a restart.

## Design

yunikorn-core state

```
New -----------------------------> Running
 |    Recover                Success  |
  ------------> Recovering -----------
                    |   Fail
                     ---------> Failed
```

shim scheduler state

```
      Register                            Run
New ------------> Registered ---------------------------> Running
                      |    Recover                   Success  |
                       --------------> Recovering ------------
                                           |   Fail
                                            ---------> Failed
```

Restart (with recovery) process
- start yunikorn-core and shim with option "recover"
- yunikorn-core and shim both enter "recovering" state
- under "recovering" state, yunikorn-core doesn't handle new allocation requests, shim doesn't send new allocation requests
- shim register itself with yunikorn-core
- shim starts to recovery state
  - shim detects nodes added from node informer and added them to cache
  - shim detects pods added from pod informer, filter out the pods that already assigned (scheduled to a node), and added that to cache (allocation in that node)
- once a node is reached to "recovered" state, register it to yunikorn-core
  - recovered criteria: node (capacity - available = sum of pod requested), then this node is ready to register to yunikorn-core
- when yunikorn-core received recovered nodes being registered, it starts to recover state from this node, all steps should look like a reply of allocation process, including
  - adding node
  - adding applications
  - adding allocations
  - modifying queue resources
  - update partition info
- when all nodes are sent to yunikorn-core and are fully recovered, yunikorn-core transits the state to "running"
- when yunikorn-core is "running", shim can also transit to "running", then start to work on allocations.

A few things
- shim's state should be incorporate with yunikorn-core, the recover path is triggered by a command flag during start.
- if recovery failed in yunikorn-core, that means the scheduler might be running in inconsistent state, what should we do, panic? Probably not. We need to investigate to run scheduler in such state, and how to let it self healing.
- how do we know recovery is done is a problem. Currently, we assume nodes can be informed in a short time. Is there a way for us to know how many nodes are known in this cluster? That way we can ensure we wait enough time for all nodes coming up.