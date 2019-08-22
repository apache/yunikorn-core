# YuniKorn Scheduler ACLs

## Usage
Access Control Lists are generic for YuniKorn.
They can be used in multiple places in YuniKorn.
The current use case is limited to queue ACLs.

Access control lists give access to the users and groups that have been specified in the list.
They do not provide the possibility to explicitly remove or deny access to the users and groups specified in the list. 

If there is no access control list is configured access is *denied* by default.

## Syntax
The access control list is defined as:
```
ACL ::= “*” |  userlist [ “ “ grouplist ]
userlist ::= “” | user { “,” user }
grouplist ::= “” | group { “,” group }
```

This definition specifies a wildcard of * which results in access for everyone.

If the user list is empty and the group list is empty nobody will have access.
This deny all ACL has two possible representations:
* an empty access control list. (implicit)
* a single space. (explicit)

## Example config
ACLs are currently implemented in the queues which use a yaml.
This places some limitations on the how to escape the values.
Incorrectly quoted values will cause a yaml parse error or could lead to the incorrect interpretation of the value.

The following points need to be taken into account:
1. The wildcard entry must be quoted in the yaml config.
1. A simple list of users and groups does not need quoting
1. An ACL without users and just a group must be quoted to find the starting space:

Correctly quoted ACL example
```yaml
partitions:
  - name: default
    queues:
      - name: test
        submitacl: "*"
        adminacl: user group1,group2
      - name: 
        submitacl: " group1"
```

## Access check
The access check follows the pattern:
* check if the ACL is the wildcard
* check if the user is in the user list
* check if any of the groups the user is a member of is part of the group list

If a check matches the ACL allows access and returns.
If none of the checks match the ACL denies access.
 
## User and Group information
ACLs require the user's name and group membership.
User information must be provided by the shims to the core scheduler.
The current expectation is that the shims only provide the user information and leave the group information empty.

User information is passed around in the scheduler as a combined user and groups object.
These objects are cached to allow fast lookup and minimise resolution of the groups.

Based on the fact that the shims do not have to provide group information the core has the possibility to resolve the group memberships.
Group membership resolution is pluggable, see [resoluion](#resolution) below.
If the resolution of the groups of a user fails the result is still cached with a shortened lifetime.
Users resolution is cached, negatively and positively, per partition.
Users resolution like many other configs can differ between partition. 

### Resolution
Groups do not have to be part of provided user and group object.
When the object is added to the cache the groups are automatically resolved based on the resolution that is configured. 
The resolver which is linked to the cache can be set per partition.
 
The default group resolver is "no resolver".
This resolver just echos the user name and a primary group with the same name as the user.

Other resolvers are:
* OS resolver
* test resolver
