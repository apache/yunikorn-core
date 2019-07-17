# YuniKorn Scheduler build

This build section describes two parts of the build.
- [build for an integrated image](#Integrated-image-build)
- [build for this repository](#Core-component-build)

In the current setup the integrated build is part of the kubernetes shim `yunikorn-k8shim`.

### Development Environment setup

Read the [environment setup guide](env-setup.md) first to setup Docker and Kubernetes development environment.

### Build dependencies

The dependencies in the projects are managed using [go modules](https://blog.golang.org/using-go-modules).
Go Modules require at least Go version 1.11 to be installed on the development system.

If you want to modify one of the projects locally and build with your local dependencies you will need to change the module file. 
Changing dependencies uses mod `replace` directives as explained in the [local build document](build-local.md).  

Prerequisite:
- Go 1.11+

## Integrated image build

The image build requires all components to be build into a single executable that can be deployed and run.
This build is currently implemented as part of the kubernetes shim. The following set of build commands are part of the `yunikorn-k8s-shim` build implementation.

Start the integrated build process by pulling the `yunikorn-k8shim` repository:
```bash
mkdir $HOME/yunikorn-k8shim/
cd $HOME/yunikorn-k8shim/
export GOPATH=$HOME/yunikorn-k8shim/
go get github.com/cloudera/yunikorn-k8shim
```
At this point you have an environment that will allow you to build an integrated image for the YuniKorn scheduler.

Note that if the GOPATH is not set the code repository will be located in the standard location for all other go projects: `$HOME/go`

### Build image steps

Building a docker image can be triggered by following command.

```
make image
```

The image with the build in configuration can be deployed directly on kubernetes. Some sample deployments that can be used are found under [deployments](https://github.com/cloudera/yunikorn-k8shim/tree/master/deployments/scheduler) directory.
For the deployment that uses a config map you need to set up the ConfigMap in kubernetes.  
How to deploy the scheduler with a ConfigMap is explained in the [scheduler configuration deployment](configure-scheduler.md) document.

The image build command will first build the integrated executable and then create the docker image.
The default image tags are not be suitable for deployments to an accessible repository as it uses a hardcoded user and would push to [DockerHub](https://hub.docker.com/r/yunikorn/yunikorn-scheduler-k8s).
You *must* update the `TAG` variable in the `Makefile` to push to an accessible repository.
When you update the image tag be aware that the deployment examples given will also need to be updated to reflect the same change.

### Build the web UI

Example deployments reference the [YuniKorn web UI](https://github.com/cloudera/yunikorn-web). 
The YuniKorn web UI has its own specific requirements for the build. The project has specific requirements for the build follow the steps in the README to prepare a development environment and build how to build the projects.
The scheduler is fully functional without the web UI. 

### Locally run the integrated scheduler

When you have a local development environment setup you can run the scheduler in your local kubernetes environment.
This has been tested in a Docker desktop with docker for desktop and Minikube. See the [environment setup guide](env-setup.md) for further details.

```
make run
```
It will connect with the kubernetes cluster using the users configured configuration located in `$HOME/.kube/config`.

You can also use the same approach to run the scheduler locally but connecting to a remote kubernetes cluster,
as long as the `$HOME/.kube/config` file is pointing to that remote cluster.

### How to use 

The simplest way to run YuniKorn is to leverage our pre-built docker images.
YuniKorn could be easily deployed to Kubernetes with a yaml file, running as a customized scheduler.
Then you can run workloads with this scheduler. Example deployments are described in [here](user-guide.md).

## Core component build

The scheduler core, this repository build, by itself does not provide a functional scheduler. 
It just builds the core scheduler functionality without any resource managers or shims.
A functional scheduler must have at least one resource manager that registers.

### Build dependencies
The dependencies in the project are managed using [go modules](https://blog.golang.org/using-go-modules).   

Prerequisite:
- Go 1.11+

### Build steps
The core component contains two command line tools: the `simplescheduler` and the `schedulerclient`.
The two command line tools are currently only provided as examples.

Building the example command line tools:
```
make example
```  

Run all unit tests for the core component: 
```
make test
```
Any changes made to the core code should not cause any existing tests to fail.