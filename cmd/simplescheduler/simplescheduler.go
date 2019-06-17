/*
Copyright 2019 Cloudera, Inc.  All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"context"
	"github.infra.cloudera.com/yunikorn/scheduler-interface/lib/go/si"
	"io"
	"log"
	"time"
)

type SimpleScheduler struct {
}

func (scheduler *SimpleScheduler) Run(endpoint string) {
	//// Create GRPC servers
	//ss := newSchedulerServer()
	//s := common.NewNonBlockingGRPCServer()
	//s.Start(endpoint, ss)
	//s.Wait()
}

func newSchedulerServer() (si.SchedulerServer) {
	return &SimpleScheduler{}
}

func (scheduler *SimpleScheduler) RegisterResourceManager(ctx context.Context, in *si.RegisterResourceManagerRequest) (*si.RegisterResourceManagerResponse, error) {
	log.Printf("Received registeration")

	return &(si.RegisterResourceManagerResponse{}), nil
}

func (scheduler *SimpleScheduler) Update(conn si.Scheduler_UpdateServer) error {
	log.Println("start new server")
	ctx := conn.Context()

	for {

		// exit if context is done
		// or continue
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// receive data from stream
		_, err := conn.Recv()

		log.Printf("Requested recved")

		if err == io.EOF {
			// return will close stream from server side
			log.Println("exit")
			return nil
		}
		if err != nil {
			log.Printf("receive error %v", err)
			continue
		}

		// Send response to stream
		resp := si.UpdateResponse{}

		time.Sleep(2 * time.Second)

		if err := conn.Send(&resp); err != nil {
			log.Printf("send error %v", err)
			return err
		}

		log.Printf("Responded")
	}
}

