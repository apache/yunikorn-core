# Yunikorn Scheduler Configuration
The Yunikorn core scheduler configuration has two separate areas that need to be configured. The scheduler service itself, things like web service ports etc, and the queue configuration. The split between the two types of configuration is proposed with two points in mind:
* Separation of duty
* Dynamic vs Static

The scheduler configuration is mainly static. There is no need to change a web service port or a scheduling policy while the service is running. The queue configuration is far more dynamic and can change while the service is running.

From a separation of duty we can allow an operator that manages the cluster to make changes to the scheduler queues. You would not want to allow that administrator to change the scheduler configuration itself.

Separated from the core scheduler configuration we have one or more shim configurations. We currently can not anticipate the deployment model of the scheduler and its shims. A shim, like the k8s-shim, might run in the same container or node but there is no guarantee it will. We also do not know the number of shims that will be used with one core scheduler. There is also still the possibility to have multiple instances of the same shim with one core scheduler.

Shim configuration must be independent of the core scheduler configuration.
## Scheduler Configuration
Scheduler configuration covers all the configuration needed to start the scheduler and the dependent services. The configuration consists of a simple key value pair. All configuration to start the service must be part of this configuration.
The scheduler configuration must exclude the queue related configuration. 

Scheduler configuration as currently identified
* Bind host
* Service port
* Web bind host
* Web service port
* SSL config
* Shims Configured 
* SchedulerACL

Configuration to consider:
* Assign multiple containers in one go: use case is bin packing, don’t spread an application over large number of nodes. Needs to become configurable.
* Pre-emption related configuration:
    * threshold: do not pre-empt from a queue if the cluster load is below a certain threshold.
    * Interval: pause between pre-emption checks
## Queue Configuration
### Queue Definition
On startup the scheduler will load the configuration for the queues from the provided configuration file after initialising the service. If there is no queue configuration provided the scheduler should start up with a simple default configuration which performs a well documented default behaviour.
Based on the kubernetes definition this configuration could be a configMap  <sup id="s1">[1](#footnote1)</sup> but not a CRD.

The queue configuration is dynamic. Changing the queue configuration must not require a scheduler restart. 
Changes should be allowed by either calling the GO based API, the REST based API or by updating the configuration file. Changes made through the API must be persisted in the configuration file. Making changes through an API is not a high priority requirement and could be postponed to a later release.

The queue configuration defines queues in a hierarchy: a tree. The base of the tree is the _root_ queue. The queue configuration must define a single _root_ queue. All queues that are defined in queue configuration are considered _managed_ queues.

The root queue reflect the whole cluster. Resource settings on the root queue are not allowed. The resources available to the root queue are calculated based on the registered node resources in the cluster. If resources would be specified on the root limit the cluster would either be artificially limited to a specific size or expect resources to be available that are not there.

Queues in the hierarchy in the tree are separated by the “.” dot character (ASCII 0x2E). This indirectly means that a queue name itself cannot contain a dot as it interferes with the hierarchy separator. Any queue name in the configuration that contains a dot will cause the configuration to be considered invalid. However we must allow placement rules to create a queue with a dot based input. This 

Not all queues can be used to submit an application to. Applications can only be submitted to a queue which does not have a queue below it. These queues are defined as the _leaf_ queues of the tree. Queues that are not a _leaf_ and thus can contain other queues or child queues are considered _parent_ queues.

Each queue must have exactly one _parent_ queue, besides the root queue. The root queue cannot have a _parent_ and will be automatically defined as a _parent_ queue type.
A fully qualified queue name, case insensitive, must be unique in the hierarchy. A queue in the hierarchy can thus be only uniquely identified by its fully qualified path. This means that a queue with the same name is allowed at a different point in the hierarchy.
Example:
```
root.companyA.development
root.companyB.development
root.production.companyA
```
In the example the queues _companyA_ and _companyB_ are _parent_ queues. Both _development_ queues are _leaf_ queues.
The second instance of the _companyA_ queue is a _leaf_ queue which is not related to the first instance as it is defined at a different level in the hierarchy.

The queue as defined in the configuration will be assigned a queue type. This can either be implicit based on how the queue is defined in the hierarchy or explicit by setting the optional _parent_ property as part of the queue definition. By default all queues will be assigned their type based on the configuration. There is only one case in which this should automatic process would need to be overridden and that is to mark a _leaf_ in the configuration as a _parent_. The use case is part of the [placement rules](#placement-rules-definition). In that case the configuration could be used to define a _parent_ queue for only _unmanaged_ queues.

Access control lists provide a split between submission permission and administration permissions. Submission access to a queue allows an application to be submitted to the queue by the users or groups specified. The administration permissions allows submission to the queue plus the administrative actions. Administrative actions are currently limited to killing an application and moving an application to a different queue.

Access control lists are checked recursively up to the root of the tree starting at the lowest point in the tree. In other words when the access control list of a queue does not allow access the parent queue is checked. The checks are repeated all the way up to the root of the queues.  

On each queue, except the root queue, the following properties can be set:
* QueueType:
    * Parent (boolean)
* Resource settings:
    * Guaranteed (resource)
    * Maximum (resource)
* Running Application limit:
    * Maximum (integer)
* Queue Permissions:
    * SubmitACL (ACL)
    * AdminACL (ACL)
* Pre emption setting:
    * PreEmptionAllowed (boolean)
* Application sort algorithm:
    * ApplicationSortPolicy (enumeration: fair, fifo)

On the root queue only the following properties can be set:
* Running Application limit:
    * Maximum (integer)
* Queue Permissions:
    * SubmitACL (ACL)
    * AdminACL (ACL)
* Application sort algorithm:
    * ApplicationSortPolicy (enumeration: fair, fifo)

### User definition
Applications are run by a user could run in one or more queues. The queues can have limits set on the resources that can be used. This does not limit the amount of resources that can be used by the user in the cluster.

From an administrative perspective setting a limit of the resources that can be used by a specific user can be important.  In this case a user is broadly defined as the identity that submits the application. This can be a service or a person, from a scheduling perspective there is no difference.
User limits can prevent a take over of a queue or the cluster by a misbehaving user or application. From a multi tenancy perspective user limits also allows for sharing or subdivision of resources within the tenancy however that is defined.

Adding user based limits will allow the cluster administrators to control the cluster wide resource usage of a user:
* Running Application limit:
    * Maximum (integer)
* Resource setting:
    * Maximum (resource)

### Placement Rules definition
Dynamically placing an application in a queue has been a feature of schedulers for a long time. This means that an application when submitted does not have to include a queue to run in. 

A placement rule will use the application details to place the application in the queue. The outcome of running a placement rule will be a fully qualified queue or a `fail`, which means execute the next rule in the list. Rules will be executed in the order that they are defined.

During the evaluation of the rule the result could be a queue name that contains a dot. This is especially true for user and group names which are POSIX compliant. When a rule generates a partial queue name that contains a dot it must be replaced as it is the separator in the hierarchy. The replacement text will be `_dot_`

The first rule that matches, i.e. returns a fully qualified queue name, will halt the execution of the rules. If the application is not placed at the end of the list of rules the application will be rejected. Rules can return queues that are not defined in the configuration only if the rule allows creation of queues. These queues created by the placement rules are considered _unmanaged_ queues as they are not managed by the administrator in the configuration.

Rules provide a fully qualified queue name as the result. To allow for deeper nesting of queues the parent of the queue can be set as part of the rule evaluation. The rule definition should allow a fixed configured fully qualified parent to be specified or it can call a second rule to generate the parent queue.  By default a queue is generated as a child of the root queue.

Example:
Placing an application submitted by the user _user1_ whom is a member of the groups _user1_ and _companyA_ in a queue based on UserName:
```
Rule name: UserName
    Parent: root.fixedparent
Result: root.fixedparent.user1

Rule name: UserName
    Parent: SecondaryGroup
	Filter:
        Type: allow
	    Groups: company.*
Result: root.companyA.user1

Rule name: UserName
Filter: 
    Users: user2,user3
Result: denied placement
```
The default behaviour for placing an application in a queue, which would do the same as using the queue that is provided during submit, would be a rule that takes the provided queue with the create flag set to false.

Access permissions will be enforced as part of the rule evaluation. For _managed_ queues this means that the ACL for the queue itself is checked. For an _unmanaged_ queue the parent queue ACL is the one that is checked. For the definition of the access control list and checks see the [Access Control Lists](#access-control-lists) chapter.

Defining placement rules in the configuration requires the following information per rule:
* Name:
    * Name (string)
* Parent
    * Parent (string)
* Create Flag:
    * Create (boolean)
* Filter:
    * A regular expression or list of users/groups to apply the rule to.
    
The filter can be used to allow the rule to be used (default behaviour) or deny the rule to be used. User or groups matching the filter will be either allowed or denied.
The filter is defined as follow:
* Type:
    * Type (string) which can have no value (empty) or "allow" or "deny", case insensitive.
* Users:
    * A list of zero or more user names. If the list is exactly one long it will be interpreted as a regular expression.
* Groups:
    * A list of zero or more group names. If the list is exactly one long it will be interpreted as a regular expression.

Proposed rules for placing applications would be:
* Provided: returns the queue provided during the submission
* UserName: returns the user name
* PrimaryGroupName: returns the primary group of the user
* SecondaryGroupName: returns the first secondary group of the user that matches
* Fixed: returns the queue name configured in the rule
* ApplicationType: returns the application type (if available)

For _unmanaged_ queues in the current revision of the configuration you cannot provide any queue specific properties. However in the future we should consider propagating specific resource related settings from a _managed_ parent to the _unmanaged_ child, specifically:
* Dynamic Resource settings:
    * Guaranteed (resource)
    * Maximum (resource)
* Dynamic Running Application limit:
    * Maximum (integer)

### Configuration updates
Updating the queue definition will allow updating the existing queue properties as well as adding and removing queues. A new queue definition will only become active if the configuration can be parsed. The change of the definition is an atomic change which applies all modification in one action.

Updating the queue properties will not automatically trigger further action. This means that if the maximum number of resources of a queue or its parent is changed we leave the applications in the queue running as they are. The scheduler will adhere to the new property values which should see the convergence over time.

A _managed_ queue will only be removed if it is removed from the configuration. Before we can remove a queue it must not be running applications. This means that when a _managed_ queue is removed from the configuration it must be empty or the system needs to allow the queue to drain. Forcing a _managed_ queue to be empty before we can remove it is not possible which means that _managed_ queues are removed in multiple steps:
1. The queue is removed from the configuration
1. The queue is marked as `draining`
1. All managed queues that are `draining` and empty are removed

Long running applications should be handled gracefully when removing a _managed_ queue. The scheduler should at least track and expose that a queue has been in a _draining_ state for an extended period of time. In the optimal case the application should be notified of the queue change to allow it to release resources. In all cases the queue administrators should be notified to allow them to take action. This action would currently be a manual move of the application to a different queue by the administrators.

_Unmanaged_ queues that are not defined in the queue definition are created by the scheduler automatically based on the placement rules. _Unmanaged_ queues have a lifespan independent of the configuration. Whenever an _unmanaged_ queue is empty it will get removed. The queue will automatically be created again when a new application is requesting it via triggering the placement rule.

Removing an empty _managed_ or _unmanaged_ queue is handled by the same removal code which must run independent of the configuration updates and scheduling actions.

Configurations can change over time. The impact of a fail over or restart must still be investigated.
Base point to make: a changed configuration should not impact the currently running applications. Queues that no longer exist should be handled somehow.

### Access Control Lists
The scheduler ACL is independent of the queue ACLs. A scheduler administrator is not by default allowed to submit an application or administer the queues in the system.

All ACL types should use the same definition pattern. We should allow at least POSIX user and group names which uses the portable filename character set <sup id="s2">[2](#footnote2)</sup>. However we should take into account that we could have domain specifiers based on the environment that the system runs in (@ sign as per HADOOP-12751).

By default access control is enabled and access is denied. The only special case is for the core scheduler which automatically adds the system user, the scheduler process owner, to the scheduler ACL. The scheduler process owner is allowed to make sure that the process owner can use the API to call any administrative actions.

Access control lists give access to the users and groups that have been specified in the list. They do not provide the possibility to explicitly remove or deny access to the users and groups specified in the list. 

The access control list is defined as:
```
ACL ::= “*” |  userlist [ “ “ grouplist ]
userlist ::= “” | user { “,” user }
grouplist ::= “” | group { “,” group }
```

This definition specifies a wildcard of * which results in access for everyone. If the user list is empty and the group list is empty nobody will have access. This deny all ACL has two possible representations:
* an empty access control list.
* a single space.

If there is no access control list is configured access is denied by default.
## Shim Configuration
The shim configuration is highly dependent on the shim implementation. The k8s shim differs from the YARN shim. Currently the k8s shim is configured via command line options but we should not depend on that.

### K8s shim
The full configuration of the K8s shim is still under development.

### YARN shim
The full configuration of the YARN shim is still under development.

# 
<a id="footnote1">1</a>: https://kubernetes.io/docs/concepts/extend-kubernetes/api-extension/custom-resources/#should-i-use-a-configmap-or-a-custom-resource  [↩](#s1)<br>
<a id="footnote2">2</a>: The set of characters from which portable filenames are constructed.  [↩](#s2)<br>
```A B C D E F G H I J K L M N O P Q R S T U V W X Y Z a b c d e f g h i j k l m n o p q r s t u v w x y z 0 1 2 3 4 5 6 7 8 9 . _ -```