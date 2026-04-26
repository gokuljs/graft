package main

import (
	"context"
	"fmt"
	"log"
	"net"

	pb "github.com/gokuljs/graft/proto"

	"google.golang.org/grpc"
)

// server implements the Greeter service
type server struct {
	pb.UnimplementedGreeterServer
}

// SayHello implements the SayHello RPC
func (s *server) SayHello(ctx context.Context, req *pb.HelloRequest) (*pb.HelloResponse, error) {
	message := fmt.Sprintf("Hello %s!", req.Name)
	return &pb.HelloResponse{Message: message}, nil
}

func main() {
	// Listen on TCP port 50051
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	// Create a gRPC server
	s := grpc.NewServer()

	// Register the Greeter service
	pb.RegisterGreeterServer(s, &server{})
	log.Println("Server is running on port 50051...")

	// Start serving
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
