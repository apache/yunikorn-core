/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

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
	ARCH                = "si.io/arch"
	HostName            = "si.io/hostname"
	RackName            = "si.io/rackname"
	OS                  = "si.io/os"
	InstanceType        = "si.io/instance-type"
	FailureDomainZone   = "si.io/zone"
	FailureDomainRegion = "si.io/region"
	LocalImages         = "si.io/local-images"
	NodePartition       = "si.io/node-partition"
)

// Constants for allocation attribtues
const (
	ApplicationID  = "si.io/application-id"
	ContainerImage = "si.io/container-image"
	ContainerPorts = "si.io/container-ports"
)
