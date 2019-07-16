# Deployment of YuniKorn using a ConfigMap

## Build docker image (without conf file)

Under project root of the `yunikorn-k8s-shim`, run the command to build an image using the map for the configuration:
```
make image_map
```

This command will build an image. The image will be tagged with a default version and image tag.

**Note** the default build uses a hardcoded user and tag. You *must* update the `IMAGE_TAG` variable in the `Makefile` to push to an appropriate repository. 

## Create the ConfigMap

This must be done before deploying the scheduler. It requires a correctly setup kubernetes environment.
This kubernetes environment can be either local or remote. 

- download configuration file if not available on the node to add to kubernetes:
```
curl -o queues.yaml https://raw.githubusercontent.com/cloudera/yunikorn-k8shim/master/conf/queues.yaml
```
- create ConfigMap in kubernetes:
```
kubectl create configmap yunikorn-configs --from-file=queues.yaml
```
- check if the ConfigMap was created correctly:
```
kubectl describe configmaps yunikorn-configs
```

**Note** if name of the ConfigMap is changed the volume in the scheduler yaml file must be updated to reference the new name otherwise the changes to the configuration will not be picked up. 

## Attach ConfigMap Volume to the Scheduler Pod

The ConfigMap is attached to the scheduler as a special volume. First step is to specify where to mount it in the pod:
```yaml
  volumeMounts:
    - name: config-volume
      mountPath: /etc/yunikorn/
```
Second step is t link the mount point back to the configuration map created in kubernetes:
```yaml
  volumes:
    - name: config-volume
      configMap:
        name: yunikorn-configs
``` 

Both steps are part of the scheduler yaml file, an example can be seen at [scheduler-v0.3.5.yaml](https://github.com/cloudera/yunikorn-k8shim/blob/master/deployments/scheduler/scheduler-v0.3.5.yaml)
for reference.


## Deploy the Scheduler
With the mapped config and the docker image build with the `image_map` target from the make the scheduler can be deployed: 
```
kubectl create -f deployments/scheduler/scheduler-v0.3.35.yaml
```




