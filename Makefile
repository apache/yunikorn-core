#
# Copyright 2019 Cloudera, Inc.  All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Check if this is at least GO 1.11 for Go Modules
GO_VERSION := $(shell go version | awk '$$3 ~ /go1.(10|0-9])/ {print $$3}')
ifdef GO_VERSION
$(error Build requires go 1.11 or later)
endif

# Make sure we are in the same directory as the Makefile
BASE_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Force Go modules even when checked out inside GOPATH
GO111MODULE := on
export GO111MODULE

REPO=github.com/cloudera/yunikorn-core/pkg
# when using the -race option CGO_ENABLED is set to 1 (automatically)
# it breaks cross compilation.
RACE=-race
# build commands on local os by default, uncomment for cross-compilation
#GOOS=darwin
#GOARCH=amd64

all:
	$(MAKE) -C $(dir $(BASE_DIR)) build

.PHONY: common-check-license
common-check-license:
	@echo "checking license header"
	@licRes=$$(grep -Lr --include="*.go" "Copyright 20[1-2][0-9] Cloudera" .) ; \
	if [ -n "$${licRes}" ]; then \
		echo "following files have incorrect license header:\n$${licRes}" ; \
		exit 1; \
	fi

# Build the example binaries for dev and test
.PHONY: commands
commands:
	@echo "building examples"
	go build $(RACE) -a -ldflags '-extldflags "-static"' -o _output/simplescheduler ./cmd/simplescheduler
	go build $(RACE) -a -ldflags '-extldflags "-static"' -o _output/schedulerclient ./cmd/schedulerclient

# Build binaries for dev and test
.PHONY: build
build: commands

# Run the tests after building
.PHONY: test
test:
	@echo "running unit tests"
	go test ./... -cover $(RACE) -tags deadlock
	go vet $(REPO)...

# Simple clean of generated files only (no local cleanup).
.PHONY: clean
clean:
	go clean -r -x ./...
	-rm -rf _output