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

### Simple examples
An ACL that allows access to just the user `sue`
```yaml
  adminacl: sue
```
Nobody else will get access, this is just for `sue`.
`john` and `bob` will be denied access.

An ACL that allows access to the user `sue` and the members of the group `dev`.
```yaml
  adminacl: sue dev
```
The user `sue` gets access based on her explicit mention in the user part of the ACL.
Even though she is not a member of the group `dev`. Her group membership is irrelevant.

The user named `john` whom is a member of the group `dev` will be allowed access based on his group membership.
A third user called `bob` whom is not mentioned explicitly and is not a member of the `dev` group will be denied access.

An ACL that allows access to the members of the groups `dev` and `test`.
```yaml
  adminacl: " dev,test"
```
The ACL must start with a space to indicate that there is no user list.
If the ACL is not correctly quoted the space is dropped by the yaml parser.
Since the user list is empty none of the users will get access unless they are a member of either the `dev` or `test` group.

Looking at the same three users as before:
The user `sue` is not a member of either group and is denied access.
The user named `john` whom is a member of the group `dev` will be allowed access based on his group membership.
`bob` is not a member of the `dev` group but is a member of `test` and will be allowed access.

### Escaping and quotation marks
ACLs are currently implemented in the queue configuration which uses a yaml file.
This places some limitations on the how to escape the values.
Incorrectly quoted values will cause a yaml parse error or could lead to the incorrect interpretation of the value.

The following points need to be taken into account:
1. The wildcard entry must be quoted in the yaml config.
1. A simple list of users with or without it being followed by a list of groups does not need quoting but may be quoted.
1. An ACL without a user list and just one or more groups must be quoted to find the starting space:

Correctly quoted ACL example
```yaml
partitions:
  - name: default
    queues:
      - name: test
        submitacl: "*"
        adminacl: sue dev,test
      - name: product
        submitacl: " product"
```

## Access check
The access check follows the pattern:
* check if the ACL is the wildcard
* check if the user is in the user list
* check if any of the groups the user is a member of is part of the group list

If a check matches the ACL allows access and checking is stopped.
If none of the checks match the ACL denies access.

## User and Group information
ACLs require the user's name and group membership.
User information must be provided by the shims to the core scheduler.
The current expectation is that the shims only provide the user information and leave the group information empty.

User information is passed around in the scheduler as a combined user and groups object.
These objects are cached to allow fast lookup and minimise resolution of the groups.

Based on the fact that the shims do not have to provide group information the core has the possibility to resolve the group memberships.
Group membership resolution is pluggable, see [resolution](#resolution) below.
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
