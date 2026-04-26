package raft

import (
	"log"
	"math/rand"
	"sync"
	"time"
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
	mu                 sync.Mutex
	id                 int
	state              State
	term               int
	electionResetEvent time.Time
}

func NewServer(id int) *Server {
	s := &Server{
		id:                 id,
		state:              Follower,
		term:               0,
		electionResetEvent: time.Now(),
	}
	log.Printf("Server %d created in %s state", id, s.state)
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
	log.Printf("[Server %d] Election timer started (%v), term=%d", s.id, timeoutDuration, termStarted)

	// Poll every 10ms to check election conditions
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		<-ticker.C // Block until ticker fires
		s.mu.Lock()

		// Leaders don't need election timers
		if s.state != Candidate && s.state != Follower {
			log.Printf("[Server %d] Election timer: state=%s, stopping", s.id, s.state)
			s.mu.Unlock()
			return
		}

		// Stop this timer if a new election already started
		if termStarted != s.term {
			log.Printf("[Server %d] Election timer: term changed from %d to %d, stopping", s.id, termStarted, s.term)
			s.mu.Unlock()
			return
		}

		// Check if enough time passed to trigger election
		elapsed := time.Since(s.electionResetEvent)
		if elapsed >= timeoutDuration {
			log.Printf("[Server %d] Election timeout! (elapsed=%v)", s.id, elapsed)
			s.startElection()
			s.mu.Unlock()
			return
		}
		s.mu.Unlock()
	}
}

func (s *Server) startElection() {
	s.state = Candidate
	s.term++
	s.electionResetEvent = time.Now()
	log.Printf("[Server %d] Becomes Candidate (term=%d)", s.id, s.term)
	go s.runElectionTimer()
}
