package main

import (
	"fmt"
	"log"
	"net"
	"sync"

	pb "github.com/gokuljs/graft/proto"
	"github.com/gokuljs/graft/raft"
	"google.golang.org/grpc"
)

func startServer(id int, numServers int, wg *sync.WaitGroup) {
	defer wg.Done()
	port := 6000 + id
	addr := fmt.Sprintf(":%d", port)
	var peerIds []int
	peerAddrs := make(map[int]string)
	for i := 0; i < numServers; i++ {
		peerAddrs[i] = fmt.Sprintf("localhost:%d", 6000+i)
		// because this is used to send message to other servers thats why so your storing server ids of rest of the server
		if i != id {
			peerIds = append(peerIds, i)
		}
	}
	raftServer := raft.NewServer(id, peerIds, peerAddrs)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("Failed to start the server on port %d: %v", port, err)
	}
	log.Printf("Server %d started on port %d", id, port)
	s := grpc.NewServer()
	pb.RegisterRaftServer(s, raftServer)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Server %d failed to serve: %v", id, err)
	}
}

func main() {
	defaultNumServers := 5
	var wg sync.WaitGroup
	for i := 0; i < defaultNumServers; i++ {
		wg.Add(1)
		go startServer(i, defaultNumServers, &wg)
	}
	wg.Wait()
	log.Println("All servers started")
}
