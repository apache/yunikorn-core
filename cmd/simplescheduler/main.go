// ref: https://github.com/kubernetes-csi/drivers/blob/master/app/hostpathplugin/main.go

package main

import (
    "flag"
    "github.com/leftnoteasy/uscheduler/pkg/simplescheduler-this-is-junk-code"
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
    scheduler := &simplescheduler_this_is_junk_code.SimpleScheduler{}
    scheduler.Run("tcp://localhost:3333")
}
