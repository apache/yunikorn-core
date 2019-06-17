# Design of Yunikornscheduler

## Overall

## Architecture

### Components:

**Scheduler API Server**:

Responsible for communication between RM and Scheduler, which implements scheduler-interface GRPC protocol, or just APIs. (For intra-process communication w/o Serde).

**Scheduler Cache**:

Caches all data related to scheduler state, such as used resources of each queues, nodes, allocations. Relationship between allocations and nodes, etc.
Should not include temporary data helps with scheduler. For example to-be-preempted allocation candidates. Fair share resource of queues, etc.

**Scheduler Cache Event Handler**:

Handles all events which needs to update scheduler internal state. So all the write operations will be carefully handled.

**Admin Service**

Handles request from Admin, which can also load configurations from storage and update scheduler policies.

**Scheduler and Preemptor**

Handles Scheduler's internal state. (Which is not belong to scheduelr cache), such as internal reservations, etc. Scheduler and preemptor will work together, make scheduling or preemption decisions.

All allocate/preempt request will be handled by event handler.


```
                      GRPC Protocol
+---------------------------------------------------------------------------+
                     +--------------------+
                     |Scheduler API Server|
 +-------------+     +---------+----------+
 |AdminService |               |
 +-------------+               |Write Ops                    +----------------+
 +-------------+               V                            ++Scheduler       |
 |Configurator |      +-------------------+  Allocate       ||   And          |
 +-------------+      |Cache Event Handler+<-----------------|                |
         +----------> +-------------------+  Preempt        ++Preemptor       |
          Update Cfg   Handled by policies                   +----------------+
                               +  (Stateless)
                        +------v--------+
                        |Scheduler Cache|
                        +---------------+
                +---------------------------------------------+
                |--------+ +------+ +----------+ +----------+ |
                ||Node   | |Queue | |Allocation| |Requests  | |
                |--------+ +------+ +----------+ +----------+ |
                +---------------------------------------------+

```

## Implementation

**Scheduler needs to do following responsibilities**

- According to resource usages between queues, sort queues, applications, and figure out order of application allocation. (This will be used by preemption as well).
- It is possible that we cannot satisfy some of the allocation request, we need to skip them and find next request.
- It is possible that some allocation request cannot be satisfied because of resource fragmentation. We need to reserve room for such requests.
- Different nodes may belong to different disjoint partitions, we can make independent scheduler runs
- Locality is still important for many scenarios, especially for on-prem cases.
- Be able to config and change ordering policies for apps, queues.
- Application can choose their own way to manage sort of nodes.

**Preemption:**

- It is important to know "who wanna the resource", so we can do preemption based on allocation orders.
- When do preemption, it is also efficient to trigger allocation op. Think about how to do it.
- Preemption needs to take care about queue resource balancing.

## Configurations & Semantics

Example of configuration:

- Partition is name space.
- Same queues can under different partitions, but enforced to have same hierarchy.

    Good:

    ```
     partition=x    partition=y
         a           a
       /   \        / \
      b     c      b   c
    ```

    Good (c in partition y acl=""):

    ```
     partition=x    partition=y
         a           a
       /   \        /
      b     c      b
    ```

    Bad (c in different hierarchy)

    ```
     partition=x    partition=y
         a           a
       /   \        /  \
      b     c      b    d
                  /
                 c
    ```

    Bad (Duplicated c)

    ```
     partition=x
         a
       /   \
      b     c
     /
    c

    ```

- Different hierarchies can be added

    ```scheduler-conf.yaml
    partitions:
      - name:  default
        queues:
            root:
              configs:
                acls:
              childrens:
                - a
                - b
                - c
                - ...
            a:
              configs:
                acls:
                capacity: (capacity is not allowed to set for root)
                max-capacity: ...
          mapping-policies:
            ...
      - name: partition_a:
        queues:
            root:...
    ```
    
## Flow of events 

```
Job Add:
--------
RM -> Cache -> Scheduler (Implemented)

Job Remove:
-----------
RM -> Scheduler -> Cache (Implemented)
Released allocations: (Same as normal release) (Implemented)
Note: Make sure remove from scheduler first to avoid new allocated created. 

Scheduling Request Add:
-----------------------
RM -> Cache -> Scheduler (Implemented)
Note: Will check if requested job exists, queue exists, etc.
When any request invalid:
   Cache -> RM (Implemented)
   Scheduler -> RM (Implemented)

Scheduling Request remove:
------------------------- 
RM -> Scheduler -> Cache (Implemented)
Note: Make sure removed from scheduler first to avoid new container allocated

Allocation remove (Preemption) 
-----------------
Scheduler -> Cache -> RM (TODO)
              (confirmation)

Allocation remove (RM voluntarilly ask)
---------------------------------------
RM -> Scheduler -> Cache -> RM. (Implemented)
                      (confirmation)

Node Add: 
---------
RM -> Cache -> Scheduler (Implemented)
Note: Inside Cache, update allocated resources.
Error handling: Reject Node to RM (Implemented)

Node Remove: 
------------
Implemented in cache side
RM -> Scheduler -> Cache (TODO)

Allocation Proposal:
--------------------
Scheduler -> Cache -> RM
When rejected/accepted:
    Cache -> Scheduler
    
Initial: (TODO)
--------
1. Admin configured partitions
2. Cache initializes
3. Scheduler copies configurations

Relations between Entities 
-------------------------
1. RM includes one or multiple:
   - Partitions 
   - Jobs
   - Nodes 
   - Queues
   
2. One queue: 
   - Under one partition
   - Under one RM.
   
3. One job: 
   - Under one queue (Job with same name can under different partitions)
   - Under one partition

RM registration: (TODO)
----------------
1. RM send registration
2. If RM already registered, remove old one, including everything belong to RM.

RM termination (TODO) 
--------------
Just remove the old one.

Update of queues (TODO) 
------------------------
Admin Service -> Cache

About partition (TODO) 
-----------------------
Internal partition need to be normalized, for example, RM specify node with partition = xyz. 
Scheduler internally need to normalize it to <rm-id>_xyz
This need to be done by RMProxy

```