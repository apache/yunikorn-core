/*
Copyright 2019 Cloudera, Inc.  All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package api

// Constants for node attribtues
const (
	ARCH                  = "si.io/arch"
	HOSTNAME              = "si.io/hostname"
	RACKNAME              = "si.io/rackname"
	OS                    = "si.io/os"
	INSTANCE_TYPE         = "si.io/instance-type"
	FAILURE_DOMAIN_ZONE   = "si.io/zone"
	FAILURE_DOMAIN_REGION = "si.io/region"
	LOCAL_IMAGES          = "si.io/local-images"
	NODE_PARTITION        = "si.io/node-partition"
)

// Constants for allocation attribtues
const (
	APPLICATION_ID  = "si.io/application-id"
	CONTAINER_IMAGE = "si.io/container-image"
	CONTAINER_PORTS = "si.io/container-ports"
)
