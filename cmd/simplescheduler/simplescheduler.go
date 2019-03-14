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

