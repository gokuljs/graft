package raft

import (
	"context"
	"log"
	"math/rand"
	"sync"
	"time"

	pb "github.com/gokuljs/graft/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

func (s State) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

type Server struct {
	pb.UnimplementedRaftServer
	mu                 sync.Mutex
	id                 int
	state              State
	term               int
	electionResetEvent time.Time
	peerIds            []int
	votedFor           int
	peerAddrs          map[int]string
}

func NewServer(id int, peerIds []int, peerAddrs map[int]string) *Server {
	s := &Server{
		id:                 id,
		state:              Follower,
		term:               0,
		electionResetEvent: time.Now(),
		peerIds:            peerIds,
		votedFor:           -1,
		peerAddrs:          peerAddrs,
	}
	log.Printf("[Server %d] started as Follower", id)
	go s.runElectionTimer()
	return s
}

func (s *Server) electionTimeout() time.Duration {
	return time.Duration(rand.Intn(150)+150) * time.Millisecond
}

func (s *Server) runElectionTimer() {
	// running the election timer
	timeoutDuration := s.electionTimeout()
	// Capture current term to detect if a new election starts
	s.mu.Lock()
	termStarted := s.term
	s.mu.Unlock()
	// Poll every 10ms to check election conditions
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		<-ticker.C // Block until ticker fires
		s.mu.Lock()

		// Leaders don't need election timers
		if s.state != Candidate && s.state != Follower {
			s.mu.Unlock()
			return
		}

		// Stop this timer if a new election already started
		if termStarted != s.term {
			s.mu.Unlock()
			return
		}

		// Check if enough time passed to trigger election
		elapsed := time.Since(s.electionResetEvent)
		if elapsed >= timeoutDuration {
			s.startElection()
			s.mu.Unlock()
			return
		}
		s.mu.Unlock()
	}
}

func (s *Server) startElection() {
	// start the election by becoming a candidate
	s.state = Candidate
	s.term++
	savedCurrentTerm := s.term
	s.electionResetEvent = time.Now()
	s.votedFor = s.id
	votesReceived := 1
	log.Printf("[Server %d] becomes Candidate (term=%d)", s.id, savedCurrentTerm)

	for _, peerId := range s.peerIds {
		go func(peerId int) {
			reply, err := s.call(peerId, savedCurrentTerm)
			if err != nil {
				return
			}
			s.mu.Lock()
			defer s.mu.Unlock()
			if s.state != Candidate {
				return
			}
			if int(reply.Term) > savedCurrentTerm {
				s.becomeFollower(int(reply.Term))
				return
			}
			if reply.VoteGranted {
				votesReceived++
				if votesReceived*2 > len(s.peerIds)+1 {
					s.startLeader()
				}
			}
		}(peerId)
	}
	go s.runElectionTimer()
}

func (s *Server) becomeFollower(term int) {
	s.state = Follower
	s.term = term
	s.votedFor = -1
	s.electionResetEvent = time.Now()
	log.Printf("[Server %d] becomes Follower (term=%d)", s.id, term)
	go s.runElectionTimer()
}

func (s *Server) startLeader() {
	// start the leader by becoming a leader
	s.state = Leader
	log.Printf("[Server %d] becomes LEADER (term=%d)", s.id, s.term)
}

func (s *Server) call(peerId int, term int) (*pb.RequestVoteReply, error) {
	addr := s.peerAddrs[peerId]
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))

	if err != nil {
		return nil, err
	}
	defer conn.Close()
	// Create gRPC client
	client := pb.NewRaftClient(conn)
	// Make the actual network call
	reply, err := client.RequestVote(context.Background(), &pb.RequestVoteRequest{
		Term:        int32(term),
		CandidateId: int32(s.id),
	})
	return reply, err
}

func (s *Server) RequestVote(ctx context.Context, args *pb.RequestVoteRequest) (*pb.RequestVoteReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if int(args.Term) > s.term {
		s.becomeFollower(int(args.Term))
	}
	reply := &pb.RequestVoteReply{}
	if s.term == int(args.Term) && (s.votedFor == -1 || s.votedFor == int(args.CandidateId)) {
		reply.VoteGranted = true
		s.votedFor = int(args.CandidateId)
		s.electionResetEvent = time.Now()
	} else {
		reply.VoteGranted = false
	}
	reply.Term = int32(s.term)
	return reply, nil
}
