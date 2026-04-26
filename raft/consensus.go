package raft

import "log"

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
	id    int
	state State
	term  int
}

func NewServer(id int) *Server {
	s := &Server{
		id:    id,
		state: Follower,
		term:  0,
	}
	log.Printf("Server %d created in %s state", id, s.state)
	return s
}
