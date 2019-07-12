# YuniKorn Roadmap

## Next

**yunikorn-core**

* Gang Scheduling.
* Bin-packing.
* Preemption based on fairness at the user/app level.
* Application priority.
* Rich scheduler metrics.
* Workload simulator.
* Prometheus integration (phase 2).
* Grafana integration.

**yunikorn-k8shim**

* Placement rules.
* Security support.
* Helm chart support.


## v0.1 (Done)

**yunikorn-scheduler-interface**

* Communication protocols between RM and scheduler-shim.
* gRPC interfaces.
* Scheduler plugin interfaces.

**yunikorn-core**

* Hierarchy queues with min/max resource quotas.
* Resource fairness between queues, users and apps.
* Cross-queue preemption based on fairness.
* Customized resource types (like GPU) scheduling support.
* Rich placement constraints support.
* Automatically map incoming container requests to queues by policies. 
* Node partition: partition cluster to sub-clusters with dedicated quota/ACL management.
* Configuration hot-refresh.
* Stateful recovery.
* Metrics framework.

**yunikorn-k8shim**

* Support K8s predicates. Such as pod affinity/anti-affinity, node selectors.
* Support Persistent Volumes, Persistent Volume Claims, etc.
* Load scheduler configuration from configmap dynamically (hot-refresh).

**yunikorn-web**

* Cluster overview page with brief info about the cluster.
* Read-only application view, including app info and task breakdown info.
* Read-only queue view, displaying queue structure, queue resource, usage info dynamically.
