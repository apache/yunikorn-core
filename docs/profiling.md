# Profiling Scheduler

Use [pprof](https://github.com/google/pprof) to do CPU, Memory profiling can help you understand the runtime status of yunikorn scheduler. Profiling instruments have been
added to yunikorn rest service, we can easily retrieve and analyze them from HTTP
endpoints.

## Retrieve CPU (pprof) top

At this step, ensure you already have yunikorn running, it can be either running from
local via a `make run` command, or deployed as a pod running inside of K8s. Then run

```
go tool pprof http://localhost:9080/debug/pprof/profile
```

The profile data will be saved on local file system, once that is done, it enters into
the interactive mode. Now you can run profiling commands, such as

```
(pprof) top
Showing nodes accounting for 14380ms, 44.85% of 32060ms total
Dropped 145 nodes (cum <= 160.30ms)
Showing top 10 nodes out of 106
      flat  flat%   sum%        cum   cum%
    2130ms  6.64%  6.64%     2130ms  6.64%  __tsan_read
    1950ms  6.08% 12.73%     1950ms  6.08%  __tsan::MetaMap::FreeRange
    1920ms  5.99% 18.71%     1920ms  5.99%  __tsan::MetaMap::GetAndLock
    1900ms  5.93% 24.64%     1900ms  5.93%  racecall
    1290ms  4.02% 28.67%     1290ms  4.02%  __tsan_write
    1090ms  3.40% 32.06%     3270ms 10.20%  runtime.mallocgc
    1080ms  3.37% 35.43%     1080ms  3.37%  __tsan_func_enter
    1020ms  3.18% 38.62%     1120ms  3.49%  runtime.scanobject
    1010ms  3.15% 41.77%     1010ms  3.15%  runtime.nanotime
     990ms  3.09% 44.85%      990ms  3.09%  __tsan::DenseSlabAlloc::Refill
```

you can type command such as `web` or `gif` to get a graph that helps you better
understand the overall performance on critical code paths. You can get something
like below:

![CPU Profiling](images/cpu_profile.jpg)

Note, in order to use these
options, you need to install the virtualization tool `graphviz` first, if you are using Mac, simply run `brew install graphviz`, for more info please refer [here](https://graphviz.gitlab.io/).

## Memory Profiling

Similarly you can run

```
go tool pprof http://localhost:9080/debug/pprof/profile/heap
```
