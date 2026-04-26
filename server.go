package main

import (
	"fmt"
	"log"
	"net"
	"sync"

	"google.golang.org/grpc"
)

func startServer(id int, wg *sync.WaitGroup) {
	defer wg.Done()
	port := 6000 + id
	addr := fmt.Sprintf(":%d", port)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("Failed to start the server on port %d: %v", port, err)
	}
	log.Printf("Server %d started on port %d", id, port)
	s := grpc.NewServer()
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Server %d failed to serve: %v", id, err)
	}
}

func main() {
	defaultNumServers := 5
	var wg sync.WaitGroup
	for i := 0; i < defaultNumServers; i++ {
		wg.Add(1)
		go startServer(i, &wg)
	}
	wg.Wait()
	log.Println("All servers started")
}
