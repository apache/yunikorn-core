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

package common

import (
    "fmt"
    "github.com/golang/glog"
    "github.com/cloudera/scheduler-interface/lib/go/si"
    "golang.org/x/net/context"
    "google.golang.org/grpc"
    "net"
    "os"
    "strings"
    "sync"
)

// Defines Non blocking GRPC server interfaces
type NonBlockingGRPCServer interface {
    // Start services at the endpoint
    Start(endpoint string, ss si.SchedulerServer)
    // Waits for the service to stop
    Wait()
    // Stops the service gracefully
    Stop()
    // Stops the service forcefully
    ForceStop()
}

func NewNonBlockingGRPCServer() NonBlockingGRPCServer {
    return &nonBlockingGRPCServer{}
}

// NonBlocking server
type nonBlockingGRPCServer struct {
    wg     sync.WaitGroup
    server *grpc.Server
}

func (s *nonBlockingGRPCServer) Start(endpoint string, ss si.SchedulerServer) {

    s.wg.Add(1)

    go s.serve(endpoint, ss)

    return
}

func (s *nonBlockingGRPCServer) Wait() {
    s.wg.Wait()
}

func (s *nonBlockingGRPCServer) Stop() {
    s.server.GracefulStop()
}

func (s *nonBlockingGRPCServer) ForceStop() {
    s.server.Stop()
}

func ParseEndpoint(ep string) (string, string, error) {
    if strings.HasPrefix(strings.ToLower(ep), "unix://") || strings.HasPrefix(strings.ToLower(ep), "tcp://") {
        s := strings.SplitN(ep, "://", 2)
        if s[1] != "" {
            return s[0], s[1], nil
        }
    }
    return "", "", fmt.Errorf("Invalid endpoint: %v", ep)
}

// Logging unary interceptor function to log every RPC call
func logGRPC(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
    glog.V(3).Infof("GRPC call: %s", info.FullMethod)
    glog.V(5).Infof("GRPC request: %+v", req)
    resp, err := handler(ctx, req)
    if err != nil {
        glog.Errorf("GRPC error: %v", err)
    } else {
        glog.V(5).Infof("GRPC response: %+v", resp)
    }
    return resp, err
}

// Returns unary interceptor that will be used to intercept the execution of a unary RPC on the gRPC server
func withServerUnaryInterceptor() grpc.ServerOption {
    return grpc.UnaryInterceptor(logGRPC)
}

func (s *nonBlockingGRPCServer) serve(endpoint string, ss si.SchedulerServer) {

    proto, addr, err := ParseEndpoint(endpoint)
    if err != nil {
        glog.Fatal(err.Error())
    }

    if proto == "unix" {
        addr = "/" + addr
        if err := os.Remove(addr); err != nil && !os.IsNotExist(err) {
            glog.Fatalf("Failed to remove %s, error: %s", addr, err.Error())
        }
    }

    listener, err := net.Listen(proto, addr)
    if err != nil {
        glog.Fatalf("Failed to listen: %v", err)
    }

    server := grpc.NewServer(withServerUnaryInterceptor())
    s.server = server

    if ss != nil {
        si.RegisterSchedulerServer(server, ss)
    }

    glog.Infof("Listening for connections on address: %#v", listener.Addr())

    if err := server.Serve(listener); err != nil {
        glog.Fatalf("failed to serve: %v", err)
    }

}
