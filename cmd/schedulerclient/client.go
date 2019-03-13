/*
 *
 * Copyright 2015 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package main

import (
    "context"
    "github.infra.cloudera.com/yunikorn/scheduler-interface/lib/go/si"
    "io"
    "log"
    "time"

    "google.golang.org/grpc"
)

const (
    address = "localhost:3333"
)

func main() {
    // Set up a connection to the server.
    conn, err := grpc.Dial(address, grpc.WithInsecure())
    if err != nil {
        log.Fatalf("did not connect: %v", err)
    }
    defer conn.Close()
    c := si.NewSchedulerClient(conn)

    ctx, cancel := context.WithTimeout(context.Background(), time.Hour * 100000)
    defer cancel()
    _, err = c.RegisterResourceManager(ctx, &si.RegisterResourceManagerRequest{})
    if err != nil {
        log.Fatalf("could not greet: %v", err)
    }
    log.Printf("Responded")

    stream, err := c.Update(ctx)
    done := make(chan bool)

    // Connect to server and send streaming
    // first goroutine sends requests
    go func() {
        for i := 1; i <= 10; i++ {
            req := si.UpdateRequest{}
            if err := stream.Send(&req); err != nil {
                log.Fatalf("can not send %v", err)
            }

            log.Print("Send request")
            time.Sleep(time.Millisecond * 100)
        }
        //if err := stream.CloseSend(); err != nil {
        //   log.Println(err)
        //}
    }()

    // second goroutine receives data from stream
    // and saves result in max variable
    //
    // if stream is finished it closes done channel
    go func() {
        for {
            _, err := stream.Recv()
            if err == io.EOF {
                close(done)
                return
            }
            if err != nil {
                log.Fatalf("can not receive %v", err)
            }
            log.Printf("Responded by server")
        }
    }()

    // third goroutine closes done channel
    // if context is done
    go func() {
        <-ctx.Done()
        if err := ctx.Err(); err != nil {
            log.Println(err)
        }
        close(done)
    }()

    <-done
    log.Printf("Finished")
}
