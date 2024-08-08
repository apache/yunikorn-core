#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Check if this GO tools version used is at least the version of go specified in
# the go.mod file. The version in go.mod should be in sync with other repos.

# Go compiler selection
ifeq ($(GO),)
GO := go
endif

GO_VERSION := $(shell "$(GO)" version | awk '{print substr($$3, 3, 4)}')
MOD_VERSION := $(shell cat .go_version) 

GM := $(word 1,$(subst ., ,$(GO_VERSION)))
MM := $(word 1,$(subst ., ,$(MOD_VERSION)))
FAIL := $(shell if [ $(GM) -lt $(MM) ]; then echo MAJOR; fi)
ifdef FAIL
$(error Build should be run with at least go $(MOD_VERSION) or later, found $(GO_VERSION))
endif
GM := $(word 2,$(subst ., ,$(GO_VERSION)))
MM := $(word 2,$(subst ., ,$(MOD_VERSION)))
FAIL := $(shell if [ $(GM) -lt $(MM) ]; then echo MINOR; fi)
ifdef FAIL
$(error Build should be run with at least go $(MOD_VERSION) or later, found $(GO_VERSION))
endif

# Make sure we are in the same directory as the Makefile
BASE_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))
TOOLS_DIR=tools

# Force Go modules even when checked out inside GOPATH
GO111MODULE := on
export GO111MODULE

REPO=github.com/apache/yunikorn-core/pkg
# when using the -race option CGO_ENABLED is set to 1 (automatically)
# it breaks cross compilation.
RACE=-race
# build commands on local os by default, uncomment for cross-compilation
#GOOS=darwin
#GOARCH=amd64

ifeq ($(HOST_ARCH),)
HOST_ARCH := $(shell uname -m)
endif

# Kernel (OS) Name
OS := $(shell uname -s | tr '[:upper:]' '[:lower:]')

# Allow architecture to be overwritten
ifeq ($(HOST_ARCH),)
HOST_ARCH := $(shell uname -m)
endif

# Build architecture settings:
# EXEC_ARCH defines the architecture of the executables that gets compiled
ifeq (x86_64, $(HOST_ARCH))
EXEC_ARCH := amd64
else ifeq (i386, $(HOST_ARCH))
EXEC_ARCH := 386
else ifneq (,$(filter $(HOST_ARCH), arm64 aarch64))
EXEC_ARCH := arm64
else ifeq (armv7l, $(HOST_ARCH))
EXEC_ARCH := arm
else
$(info Unknown architecture "${HOST_ARCH}" defaulting to: amd64)
EXEC_ARCH := amd64
endif

# shellcheck
SHELLCHECK_VERSION=v0.9.0
SHELLCHECK_PATH=${TOOLS_DIR}/shellcheck-$(SHELLCHECK_VERSION)
SHELLCHECK_BIN=${SHELLCHECK_PATH}/shellcheck
SHELLCHECK_ARCHIVE := shellcheck-$(SHELLCHECK_VERSION).$(OS).$(HOST_ARCH).tar.xz
ifeq (darwin, $(OS))
ifeq (arm64, $(HOST_ARCH))
SHELLCHECK_ARCHIVE := shellcheck-$(SHELLCHECK_VERSION).$(OS).x86_64.tar.xz
endif
else ifeq (linux, $(OS))
ifeq (armv7l, $(HOST_ARCH))
SHELLCHECK_ARCHIVE := shellcheck-$(SHELLCHECK_VERSION).$(OS).armv6hf.tar.xz
endif
endif

# golangci-lint
GOLANGCI_LINT_VERSION=1.57.2
GOLANGCI_LINT_PATH=$(TOOLS_DIR)/golangci-lint-v$(GOLANGCI_LINT_VERSION)
GOLANGCI_LINT_BIN=$(GOLANGCI_LINT_PATH)/golangci-lint
GOLANGCI_LINT_ARCHIVE=golangci-lint-$(GOLANGCI_LINT_VERSION)-$(OS)-$(EXEC_ARCH).tar.gz
GOLANGCI_LINT_ARCHIVEBASE=golangci-lint-$(GOLANGCI_LINT_VERSION)-$(OS)-$(EXEC_ARCH)

all:
	$(MAKE) -C $(dir $(BASE_DIR)) build

# Install tools
.PHONY: tools
tools: $(SHELLCHECK_BIN) $(GOLANGCI_LINT_BIN)

# Install shellcheck
$(SHELLCHECK_BIN):
	@echo "installing shellcheck $(SHELLCHECK_VERSION)"
	@mkdir -p "$(SHELLCHECK_PATH)"
	@curl -sSfL "https://github.com/koalaman/shellcheck/releases/download/$(SHELLCHECK_VERSION)/$(SHELLCHECK_ARCHIVE)" \
		| tar -x -J --strip-components=1 -C "$(SHELLCHECK_PATH)" "shellcheck-$(SHELLCHECK_VERSION)/shellcheck"

# Install golangci-lint
$(GOLANGCI_LINT_BIN):
	@echo "installing golangci-lint v$(GOLANGCI_LINT_VERSION)"
	@mkdir -p "$(GOLANGCI_LINT_PATH)"
	@curl -sSfL "https://github.com/golangci/golangci-lint/releases/download/v$(GOLANGCI_LINT_VERSION)/$(GOLANGCI_LINT_ARCHIVE)" \
		| tar -x -z --strip-components=1 -C "$(GOLANGCI_LINT_PATH)" "$(GOLANGCI_LINT_ARCHIVEBASE)/golangci-lint"

.PHONY: lint
# Run lint against the previous commit for PR and branch build
# In dev setup look at all changes on top of master
lint: $(GOLANGCI_LINT_BIN)
	@echo "running golangci-lint"
	@"${GOLANGCI_LINT_BIN}" run

# Check scripts
.PHONY: check_scripts
ALLSCRIPTS := $(shell find . -not \( -path ./tools -prune \) -not \( -path ./build -prune \) -name '*.sh')
check_scripts: $(SHELLCHECK_BIN)
	@echo "running shellcheck"
	@"$(SHELLCHECK_BIN)" ${ALLSCRIPTS}

.PHONY: license-check
# This is a bit convoluted but using a recursive grep on linux fails to write anything when run
# from the Makefile. That caused the pull-request license check run from the github action to
# always pass. The syntax for find is slightly different too but that at least works in a similar
# way on both Mac and Linux. Excluding all .git* files from the checks.
license-check:
	@echo "checking license headers:"
ifeq (darwin,$(OS))
	$(shell mkdir -p build && find -E . -not \( -path './.git*' -prune \) -not \( -path ./build -prune \) -not \( -path ./tools -prune \) -regex ".*\.(go|sh|md|yaml|yml|mod)" -exec grep -L "Licensed to the Apache Software Foundation" {} \; > build/license-check.txt)
else
	$(shell mkdir -p build && find . -not \( -path './.git*' -prune \) -not \( -path ./build -prune \) -not \( -path ./tools -prune \) -regex ".*\.\(go\|sh\|md\|yaml\|yml\|mod\)" -exec grep -L "Licensed to the Apache Software Foundation" {} \; > build/license-check.txt)
endif
	@if [ -s "build/license-check.txt" ]; then \
		echo "following files are missing license header:" ; \
		cat build/license-check.txt ; \
		exit 1; \
	fi
	@echo "  all OK"

# Check that we use pseudo versions in master
.PHONY: pseudo
BRANCH := $(shell git branch --show-current)
SI_REF := $(shell "$(GO)" list -m -f '{{ .Version }}' github.com/apache/yunikorn-scheduler-interface)
SI_MATCH := $(shell expr "${SI_REF}" : "v0.0.0-")
pseudo:
	@echo "pseudo version check"
	@if [ "${BRANCH}" = "master" ]; then \
		if [ ${SI_MATCH} -ne 7 ]; then \
			echo "YuniKorn references MUST all be pseudo versions:" ; \
			echo " SI ref: ${SI_REF}" ; \
			exit 1; \
		fi \
	fi
	@echo "  all OK"

# Build the example binaries for dev and test
.PHONY: commands
commands: build/simplescheduler build/schedulerclient build/queueconfigchecker

build/simplescheduler: go.mod go.sum $(shell find cmd pkg)
	@echo "building example scheduler"
	@mkdir -p build
	"$(GO)" build $(RACE) -a -ldflags '-extldflags "-static"' -o build/simplescheduler ./cmd/simplescheduler

build/schedulerclient: go.mod go.sum $(shell find cmd pkg)
	@echo "building example client"
	@mkdir -p build
	"$(GO)" build $(RACE) -a -ldflags '-extldflags "-static"' -o build/schedulerclient ./cmd/schedulerclient

build/queueconfigchecker: go.mod go.sum $(shell find cmd pkg)
	@echo "building queueconfigchecker"
	@mkdir -p build
	"$(GO)" build $(RACE) -a -ldflags '-extldflags "-static"' -o build/queueconfigchecker ./cmd/queueconfigchecker

# Build binaries for dev and test
.PHONY: build
build: commands

# Run the tests after building
.PHONY: test
test: export DEADLOCK_DETECTION_ENABLED = true
test: export DEADLOCK_TIMEOUT_SECONDS = 10
test: export DEADLOCK_EXIT = true
test:
	@echo "running unit tests"
	@mkdir -p build
	"$(GO)" clean -testcache
	"$(GO)" test ./... $(RACE) -tags deadlock -coverprofile=build/coverage.txt -covermode=atomic
	"$(GO)" vet $(REPO)...

# Run benchmarks
.PHONY: bench
bench:
	@echo "running benchmarks"
	"$(GO)" clean -testcache
	"$(GO)" test -v -run '^Benchmark' -bench . ./pkg/...

# Generate FSM graphs (dot/png)
.PHONY: fsm_graph
fsm_graph:
	@echo "generating FSM graphs"
	"$(GO)" clean -testcache
	"$(GO)" test -tags graphviz -run 'Test.*FsmGraph' ./pkg/scheduler/objects
	scripts/generate-fsm-graph-images.sh

# Remove generated build artifacts
.PHONY: clean
clean:
	@echo "cleaning up caches and output"
	"$(GO)" clean -cache -testcache -r
	@echo "removing generated files"
	@rm -rf build

# Remove all generated content
.PHONY: distclean
distclean: clean
	@echo "removing tools"
	@rm -rf "${TOOLS_DIR}"

