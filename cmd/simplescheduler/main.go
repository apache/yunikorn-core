// ref: https://github.com/kubernetes-csi/drivers/blob/master/app/hostpathplugin/main.go

package main

import (
    "flag"
    "os"
)

func init() {
    flag.Set("logtostderr", "true")
}

var (
    endpoint = flag.String("endpoint", "unix://tmp/csi.sock", "CSI endpoint")
)

func main() {
    flag.Parse()

    handle()
    os.Exit(0)
}

func handle() {
    scheduler := &SimpleScheduler{}
    scheduler.Run("tcp://localhost:3333")
}
