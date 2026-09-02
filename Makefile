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

.PHONY: lint vetlock vetlock-toolchain check_scripts license-check pseudo test test_all bench fsm_graph
.PHONY: build tools clean distclean

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
TOOLS_DIR := tools
BUILD_DIR := build

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
GOLANGCI_LINT_VERSION=2.10.1
GOLANGCI_LINT_PATH=${TOOLS_DIR}/golangci-lint-v$(GOLANGCI_LINT_VERSION)
GOLANGCI_LINT_BIN=$(GOLANGCI_LINT_PATH)/golangci-lint
GOLANGCI_LINT_ARCHIVEBASE=golangci-lint-$(GOLANGCI_LINT_VERSION)-$(OS)-$(EXEC_ARCH)
GOLANGCI_LINT_ARCHIVE=$(GOLANGCI_LINT_ARCHIVEBASE).tar.gz

# vetlock
# The version is pinned in the go.mod of the tools module: bump it with "go get" in that directory
# followed by "go mod tidy".
VETLOCK_MOD_DIR=scripts/vetlock
VETLOCK_VERSION=$(shell "$(GO)" list -C "$(VETLOCK_MOD_DIR)" -m -f '{{ .Version }}' github.com/tigerquoll/vet-lock)
# The path is keyed on the go version as well as the tool version: the export data of a vet tool
# must match the toolchain that runs "go vet", so a go upgrade must rebuild the tool.
VETLOCK_PATH=${TOOLS_DIR}/vetlock-$(VETLOCK_VERSION)-$(shell "$(GO)" env GOVERSION)
VETLOCK_BIN=$(VETLOCK_PATH)/vet-lock
# The go version the tools module needs against the version that is running, both without the
# "go" prefix. Read from the go directive of the tools module, which is the newer of what that
# module asks for and what vet-lock itself asks for, so a tool update cannot make these drift.
VETLOCK_GO_REQUIRED=$(shell "$(GO)" list -C "$(VETLOCK_MOD_DIR)" -m -f '{{ .GoVersion }}')
VETLOCK_GO_CURRENT=$(patsubst go%,%,$(shell "$(GO)" env GOVERSION))
# Fixture holding a known lock violation, see the vetlock target.
VETLOCK_CANARY=pkg/locking/checklocks_canary.go
# The messages the canary must produce, one per class of violation it holds.
# The last two are the derived facts: an exclusion the canary does not state, and a field guard
# the canary states on the type rather than on the field. Both are required by name because the
# other messages here would keep the canary green if either derivation were lost, and hundreds of
# annotations were deleted from this repository on the strength of them.
VETLOCK_CANARY_MESSAGES := "invalid field access" "must not hold" "already locked" "to call callbackSelfLocking" "guarded read races" "a wait under a lock" "to call derivedSelfLocking" "when accessing structGuardedValue"

all:
	$(MAKE) -C $(dir $(BASE_DIR)) build

test_all: check_scripts license-check lint vetlock test

# Install tools
tools: $(SHELLCHECK_BIN) $(GOLANGCI_LINT_BIN) $(VETLOCK_BIN)

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

# Install vetlock
# Built from the tools module in $(VETLOCK_MOD_DIR): the analyser is a tool only dependency and
# must not end up in the go.mod of the scheduler. Building from a module instead of using
# "go install pkg@version" pins the whole dependency tree via its go.sum.
$(VETLOCK_BIN): | vetlock-toolchain
	@echo "installing vet-lock $(VETLOCK_VERSION)"
	@mkdir -p "$(VETLOCK_PATH)"
	@"$(GO)" build -C "$(VETLOCK_MOD_DIR)" -o "$(BASE_DIR)$(VETLOCK_BIN)" github.com/tigerquoll/vet-lock/cmd/vet-lock

# Refuse to build or run the analyser with a go that is older than the tools module needs.
# A vet tool must be built with the toolchain that runs "go vet" or the export data does not
# match. Letting the toolchain switch happen would do exactly that: the tool would be built
# with the newer go while "go vet" keeps running on the older one. An order only prerequisite
# of the binary, so that the check runs before the build and before every use without making
# the binary itself out of date.
vetlock-toolchain:
	@if [ "$(VETLOCK_GO_CURRENT)" != "$(VETLOCK_GO_REQUIRED)" ] && \
		[ "$$(printf '%s\n%s\n' "$(VETLOCK_GO_CURRENT)" "$(VETLOCK_GO_REQUIRED)" | sort -V | head -1)" = "$(VETLOCK_GO_CURRENT)" ]; then \
		echo "vet-lock needs go $(VETLOCK_GO_REQUIRED) or later, found go $(VETLOCK_GO_CURRENT)"; \
		echo "  the analyser must be built with the same toolchain that runs \"go vet\": upgrade go"; \
		exit 1; \
	fi

# Run lint against the previous commit for PR and branch build
# In dev setup look at all changes on top of master
lint: $(GOLANGCI_LINT_BIN)
	@echo "running golangci-lint"
	@"${GOLANGCI_LINT_BIN}" run

# Check the lock annotations. Every package below pkg is analysed, a new package is picked up
# without a change here: annotations that are missing or wrong in it fail the check as soon as it
# is added. Note that the "+checklocks:" requirements of a function are only enforced for callers
# in the listed packages.
VETLOCK_PACKAGES := $(REPO)/...
# Only the non test files of a package are analysed. "go vet" has no option to skip the test
# variant of a package so the file list of each package is passed instead. Inferred locks are
# turned off: those are guesses based on how often a field happens to be used under a lock,
# they are not the documented intent and they change as unrelated code changes.
# The vet tool runs several analyses and every one of them is named on the command line. Naming
# them is not the same as taking the default: an analysis added by a later version of the tool
# then has to be adopted deliberately instead of appearing as a wall of findings on an unrelated
# change. Each analysis has its flags under its own name. "lockorder" is deliberately not named
# here: the order in which this code base nests its lock classes is a separate piece of work. The
# "+lockclass" annotations it shares are still carried, because "lockblocking" reports a wait made
# while a CLASSED lock is held and a type without a class leaves it silent.
VETLOCK_ANALYZERS := -checklocks -lockstringer -lockblocking
VETLOCK_FLAGS := $(VETLOCK_ANALYZERS) -checklocks.inferred=false
# The file lists of all packages are collected in a single "go list" run and a failure to list
# aborts the target: a package that cannot be loaded must not be silently skipped. Every package
# is then analysed, a failure only remembered, so that one bad package does not hide the findings
# of the ones behind it. The loop keeps the accumulated status inside the pipeline subshell it
# runs in and exits with it, the status of a pipeline is the status of its last command.
vetlock: $(VETLOCK_BIN)
	@echo "running vet-lock"
	@filelists=$$("$(GO)" list -f '{{if .GoFiles}}{{$$dir := .Dir}}{{range .GoFiles}}{{$$dir}}/{{.}} {{end}}{{end}}' $(VETLOCK_PACKAGES)) || exit 1; \
	printf '%s\n' "$$filelists" | { \
		status=0; \
		while read -r gofiles; do \
			[ -n "$$gofiles" ] || continue; \
			"$(GO)" vet "-vettool=$(BASE_DIR)$(VETLOCK_BIN)" $(VETLOCK_FLAGS) $$gofiles || status=1; \
		done; \
		exit $$status; \
	}
# Prove that the analysis still detects anything at all. The canary is a file with known
# violations that no build compiles, it is passed explicitly which makes the analysis ignore
# its build constraint. It is checked together with the locking package as it uses the locks
# defined there. Both the exit code and the messages are checked: a canary that fails to build
# or is not found would otherwise look exactly like a violation that was caught. Every class of
# violation is required separately, one per analysis plus the extra checklocks ones, as they are
# detected independently: an analysis that stops reporting or that is dropped from the command
# line above would otherwise leave the canary green on the strength of the other messages.
# The number of diagnostics is checked against the number of messages as well, so that the cases
# the canary holds to be clean stay clean: a report that grows is a false positive the fixture
# says must never be raised, and the messages above would keep matching through it.
	@canary="$$("$(GO)" list -f '{{$$dir := .Dir}}{{range .GoFiles}}{{$$dir}}/{{.}} {{end}}' $(REPO)/locking) $(BASE_DIR)$(VETLOCK_CANARY)"; \
	report=$$("$(GO)" vet "-vettool=$(BASE_DIR)$(VETLOCK_BIN)" $(VETLOCK_FLAGS) $$canary 2>&1); \
	found=$$?; \
	missing=""; \
	for message in $(VETLOCK_CANARY_MESSAGES); do \
		printf '%s' "$$report" | grep -q "$$message" || missing="$$missing $$message"; \
	done; \
	count=$$(printf '%s\n' "$$report" | grep -c -E '^.+:[0-9]+:[0-9]+: ' || true); \
	expected=$$(set -- $(VETLOCK_CANARY_MESSAGES); echo $$#); \
	if [ $$found -eq 0 ] || [ -n "$$missing" ]; then \
		echo "vetlock canary failed: analyzer did not detect a known violation:$$missing"; \
		printf '%s\n' "$$report"; \
		exit 1; \
	fi; \
	if [ "$$count" -ne "$$expected" ]; then \
		echo "vetlock canary failed: expected $$expected diagnostics, got $$count"; \
		printf '%s\n' "$$report"; \
		exit 1; \
	fi

# Check scripts
ALLSCRIPTS := $(shell find . -not \( -path ./"${TOOLS_DIR}" -prune \) -not \( -path ./"${BUILD_DIR}" -prune \) -name '*.sh')
check_scripts: $(SHELLCHECK_BIN)
	@echo "running shellcheck"
	@"$(SHELLCHECK_BIN)" ${ALLSCRIPTS}

# This is a bit convoluted but using a recursive grep on linux fails to write anything when run
# from the Makefile. That caused the pull-request license check run from the github action to
# always pass. The syntax for find is slightly different too but that at least works in a similar
# way on both Mac and Linux. Excluding all .git* files from the checks.
LICENSE_CHECK_OUT := $(BUILD_DIR)/license-check.txt
license-check:
	@echo "checking license headers:"
ifeq (darwin,$(OS))
	$(shell mkdir -p "${BUILD_DIR}" && find -E . -not \( -path './.git*' -prune \) -not \( -path ./"${BUILD_DIR}" -prune \) -not \( -path ./"${TOOLS_DIR}" -prune \) -regex ".*\.(go|sh|md|yaml|yml|mod)" -exec grep -L "Licensed to the Apache Software Foundation" {} \; > "${LICENSE_CHECK_OUT}")
else
	$(shell mkdir -p "${BUILD_DIR}" && find . -not \( -path './.git*' -prune \) -not \( -path ./"${BUILD_DIR}" -prune \) -not \( -path ./"${TOOLS_DIR}" -prune \) -regex ".*\.\(go\|sh\|md\|yaml\|yml\|mod\)" -exec grep -L "Licensed to the Apache Software Foundation" {} \; > "${LICENSE_CHECK_OUT}")
endif
	@if [ -s "${LICENSE_CHECK_OUT}" ]; then \
		echo "following files are missing license header:" ; \
		cat "${LICENSE_CHECK_OUT}" ; \
		exit 1; \
	fi
	@echo "  all OK"

# Check that we use pseudo versions in master
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
commands: $(BUILD_DIR)/simplescheduler $(BUILD_DIR)/schedulerclient $(BUILD_DIR)/queueconfigchecker

$(BUILD_DIR)/simplescheduler: go.mod go.sum $(shell find cmd pkg)
	@echo "building example scheduler"
	@mkdir -p "${BUILD_DIR}"
	"$(GO)" build $(RACE) -a -ldflags '-extldflags "-static"' -o "${BUILD_DIR}/simplescheduler" ./cmd/simplescheduler

$(BUILD_DIR)/schedulerclient: go.mod go.sum $(shell find cmd pkg)
	@echo "building example client"
	@mkdir -p "${BUILD_DIR}"
	"$(GO)" build $(RACE) -a -ldflags '-extldflags "-static"' -o "${BUILD_DIR}/schedulerclient" ./cmd/schedulerclient

$(BUILD_DIR)/queueconfigchecker: go.mod go.sum $(shell find cmd pkg)
	@echo "building queueconfigchecker"
	@mkdir -p "${BUILD_DIR}"
	"$(GO)" build $(RACE) -a -ldflags '-extldflags "-static"' -o "${BUILD_DIR}/queueconfigchecker" ./cmd/queueconfigchecker

# Build binaries for dev and test
build: commands

# Run the tests after building
test: export DEADLOCK_DETECTION_ENABLED = true
test: export DEADLOCK_TIMEOUT_SECONDS = 10
test: export DEADLOCK_EXIT = true
test:
	@echo "running unit tests"
	@mkdir -p "${BUILD_DIR}"
	"$(GO)" clean -testcache
	"$(GO)" test ./... $(RACE) -tags deadlock -coverprofile="${BUILD_DIR}/coverage.txt" -covermode=atomic
	"$(GO)" vet $(REPO)...

# Run benchmarks
bench:
	@echo "running benchmarks"
	"$(GO)" clean -testcache
	"$(GO)" test -v -run '^Benchmark' -bench . ./pkg/...

# Generate FSM graphs (dot/png)
fsm_graph:
	@echo "generating FSM graphs"
	"$(GO)" clean -testcache
	"$(GO)" test -tags graphviz -run 'Test.*FsmGraph' ./pkg/scheduler/objects
	scripts/generate-fsm-graph-images.sh

# Remove generated build artifacts
clean:
	@echo "cleaning up caches and output"
	"$(GO)" clean -cache -testcache -r
	@echo "removing generated files"
	@rm -rf "${BUILD_DIR}"

# Remove all generated content
distclean: clean
	@echo "removing tools"
	@rm -rf "${TOOLS_DIR}"
