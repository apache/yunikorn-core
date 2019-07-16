## Build docker image (without conf file)

Under project root, run command

```
make image_map
```

This command will build an image and push this image to [DockerHub](https://hub.docker.com/r/yunikorn/yunikorn-scheduler-k8s).

**Note** the default build uses a hardcoded user. You *must* update the `IMAGE_TAG` variable in the `Makefile` to push to a different repository. 

## Create ConfigMap

This must be done before deploying the scheduler.

```
// 1) download configuration file
$ curl -o queues.yaml https://raw.githubusercontent.com/cloudera/yunikorn-k8shim/master/conf/queues.yaml

// 2) create configmap
$ kubectl create configmap yunikorn-configs --from-file=queues.yaml
configmap/yunikorn-configs created

// 3) check configmap
$ kubectl describe configmaps yunikorn-configs
```

**Note** if name of the ConfigMap is changed the volume in the scheduler yaml file must be updated to reference the new name otherwise the changes to the configuration will not be picked up. 

## Attach ConfigMap Volume to Scheduler Pod

This is done in the scheduler yaml file, please look at [scheduler-v0.3.35.yaml](../deployments/scheduler/scheduler-v0.3.35.yaml)
for reference.


## Deploy the Scheduler

```
kubectl create -f deployments/scheduler/scheduler-v0.3.35.yaml
```




