package main

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/nyu-distributed-systems-fa18/starter-code-lab2/pb"
)

// Messages that can be passed from the Raft RPC server to the main loop for AppendEntries
type AppendEntriesInput struct {
	arg      *pb.AppendEntriesArgs
	response chan pb.AppendEntriesRet
}

// Messages that can be passed from the Raft RPC server to the main loop for VoteInput
type VoteInput struct {
	arg      *pb.RequestVoteArgs
	response chan pb.RequestVoteRet
}

// Struct off of which we shall hang the Raft service
type Raft struct {
	AppendChan chan AppendEntriesInput
	VoteChan   chan VoteInput
}

func (r *Raft) AppendEntries(ctx context.Context, arg *pb.AppendEntriesArgs) (*pb.AppendEntriesRet, error) {
	c := make(chan pb.AppendEntriesRet)
	r.AppendChan <- AppendEntriesInput{arg: arg, response: c}
	result := <-c
	return &result, nil
}

func (r *Raft) RequestVote(ctx context.Context, arg *pb.RequestVoteArgs) (*pb.RequestVoteRet, error) {
	c := make(chan pb.RequestVoteRet)
	r.VoteChan <- VoteInput{arg: arg, response: c}
	result := <-c
	return &result, nil
}

// Compute a random duration in milliseconds
func randomDuration(r *rand.Rand, low int, up int) time.Duration {
	// Constant
	DurationMax := up
	DurationMin := low
	return time.Duration(r.Intn(DurationMax-DurationMin)+DurationMin) * time.Millisecond
}

// Restart the supplied timer using a random timeout based on function above
func restartTimer(timer *time.Timer, r *rand.Rand, low int, up int) {
	stopped := timer.Stop()
	// If stopped is false that means someone stopped before us, which could be due to the timer going off before this,
	// in which case we just drain notifications.
	if !stopped {
		// Loop for any queued notifications
		for len(timer.C) > 0 {
			<-timer.C
		}

	}
	timer.Reset(randomDuration(r, low, up))
}

// Launch a GRPC service for this Raft peer.
func RunRaftServer(r *Raft, port int) {
	// Convert port to a string form
	portString := fmt.Sprintf(":%d", port)
	// Create socket that listens on the supplied port
	c, err := net.Listen("tcp", portString)
	if err != nil {
		// Note the use of Fatalf which will exit the program after reporting the error.
		log.Fatalf("Could not create listening socket %v", err)
	}
	// Create a new GRPC server
	s := grpc.NewServer()

	pb.RegisterRaftServer(s, r)
	log.Printf("Going to listen on port %v", port)

	// Start serving, this will block this function and only return when done.
	if err := s.Serve(c); err != nil {
		log.Fatalf("Failed to serve %v", err)
	}
}

func connectToPeer(peer string) (pb.RaftClient, error) {
	backoffConfig := grpc.DefaultBackoffConfig
	// Choose an aggressive backoff strategy here.
	backoffConfig.MaxDelay = 500 * time.Millisecond
	conn, err := grpc.Dial(peer, grpc.WithInsecure(), grpc.WithBackoffConfig(backoffConfig))
	// Ensure connection did not fail, which should not happen since this happens in the background
	if err != nil {
		return pb.NewRaftClient(nil), err
	}
	return pb.NewRaftClient(conn), nil
}

type Role int

const (
	CANDIDATE Role = 0
	FOLLOWER  Role = 1
	LEADER    Role = 2
)

// The main service loop. All modifications to the KV store are run through here.
func serve(s *KVStore, r *rand.Rand, peers *arrayPeers, id string, port int) {
	// persistent server attributes
	var currentTerm int64 = 0
	var votedFor string = ""
	var logs []pb.Entry
	// volatile server attributes
	//var commitIndex int64 = 0
	//var lastApplied int64 = 0
	var lastLogTerm int64 = 0
	var lastLogIndex int64 = 0
	var role = FOLLOWER
	var voteCount int64 = 0 // received votes counted
	var majority int64 = 0  // majority votes
	var headCount int64 = 1 // raft servers counted, self included
	raft := Raft{AppendChan: make(chan AppendEntriesInput), VoteChan: make(chan VoteInput)}
	// Start in a Go routine so it doesn't affect us.
	go RunRaftServer(&raft, port)

	peerClients := make(map[string]pb.RaftClient)

	for _, peer := range *peers {
		client, err := connectToPeer(peer)
		if err != nil {
			log.Fatalf("Failed to connect to GRPC server %v", err)
		}
		headCount++
		peerClients[peer] = client
		log.Printf("Connected to %v", peer)
	}
	// count head
	majority = headCount/2 + 1

	type AppendResponse struct {
		ret  *pb.AppendEntriesRet
		err  error
		peer string
	}

	type VoteResponse struct {
		ret  *pb.RequestVoteRet
		err  error
		peer string
	}
	appendResponseChan := make(chan AppendResponse)
	voteResponseChan := make(chan VoteResponse)

	// Create a timer and start running it
	timer := time.NewTimer(randomDuration(r, 1000, 4000))

	// heartbeat timer
	timerHeartBeat := time.NewTimer(randomDuration(r, 1000, 2000))

	// Run forever handling inputs from various channels
	for {
		select {
		// send heartbeat periodically
		case <-timerHeartBeat.C:
			if role == LEADER {
				for p, c := range peerClients {
					// Send in parallel so we don't wait for each client.
					go func(c pb.RaftClient, p string) {
						// send heartbeats
						ret, err := c.AppendEntries(context.Background(), &pb.AppendEntriesArgs{Term: currentTerm, LeaderID: id})
						// get response
						appendResponseChan <- AppendResponse{ret: ret, err: err, peer: p}
					}(c, p)
				}
			}
			restartTimer(timerHeartBeat, r, 1000, 2000)
		case <-timer.C:
			// The timer went off.
			log.Printf("Timeout")
			// voted for myself first
			votedFor = id
			voteCount = 1
			// increment current term
			//if role != CANDIDATE {
			currentTerm++
			//}
			// turn to candidate state
			role = CANDIDATE
			for p, c := range peerClients {
				// Send in parallel so we don't wait for each client.
				go func(c pb.RaftClient, p string) {
					// this tells the other raft servers about the vote request
					ret, err := c.RequestVote(context.Background(), &pb.RequestVoteArgs{Term: currentTerm, CandidateID: id})
					// this receives the response from others
					voteResponseChan <- VoteResponse{ret: ret, err: err, peer: p}
				}(c, p)
			}
			// This will also take care of any pesky timeouts that happened while processing the operation.
			restartTimer(timer, r, 1000, 4000)
		case op := <-s.C:
			// We received an operation from a client
			// TODO: Figure out if you can actually handle the request here. If not use the Redirect result to send the
			// client elsewhere.
			// TODO: Use Raft to make sure it is safe to actually run the command.
			// if this is the leader, add it to the log
			log.Printf("Request received %v, command %v", id, op.command)
			if role == LEADER {
				log.Printf("Entry added to the leader %v, command %v", id, op.command)
				logs = append(logs, pb.Entry{Term:currentTerm, Index:lastLogIndex + 1, Cmd:&op.command})
				lastLogIndex++
				//for p, c := range peerClients {
					// Send in parallel so we don't wait for each client.
					//go func(c pb.RaftClient, p string) {
						// send heartbeats
						// TODO: need refinement
						//ret, err := c.AppendEntries(context.Background(), &pb.AppendEntriesArgs{Term: currentTerm, LeaderID: id, PrevLogIndex:lastLogIndex, PrevLogTerm:lastLogTerm, })
						// get response
						//appendResponseChan <- AppendResponse{ret: ret, err: err, peer: p}
					//}(c, p)
				//}
			}
			// This command commits the operation
			s.HandleCommand(op)
		case ae := <-raft.AppendChan:
			// We received an AppendEntries request from a Raft peer
			// TODO figure out what to do here, what we do is entirely wrong.
			if ae.arg.Term < currentTerm {
				ae.response <- pb.AppendEntriesRet{Term: ae.arg.Term, Success: false}
			}
			role = FOLLOWER
			currentTerm = ae.arg.Term
			votedFor = ""
			log.Printf("Received append entry from %v", ae.arg.LeaderID)
			ae.response <- pb.AppendEntriesRet{Term: ae.arg.Term, Success: true}
			// This will also take care of any pesky timeouts that happened while processing the operation.
			restartTimer(timer, r, 1000, 4000)
		case vr := <-raft.VoteChan:
			// We received a RequestVote RPC from a raft peer
			// TODO: Fix this.
			log.Printf("Received vote request from %v", vr.arg.CandidateID)
			if vr.arg.Term < currentTerm {
				vr.response <- pb.RequestVoteRet{Term: vr.arg.Term, VoteGranted: false}
			} else {
				if (votedFor == "" || votedFor == vr.arg.CandidateID) && (vr.arg.LasLogTerm > lastLogTerm || vr.arg.LasLogTerm == lastLogTerm && vr.arg.LastLogIndex >= lastLogIndex) {
					vr.response <- pb.RequestVoteRet{Term: vr.arg.Term, VoteGranted: true}
					// change the votedFor
					votedFor = vr.arg.CandidateID
				} else {
					vr.response <- pb.RequestVoteRet{Term: vr.arg.Term, VoteGranted: false}
				}
			}
		case vr := <-voteResponseChan:
			// We received a response to a previou vote request.
			// TODO: Fix this
			if vr.err != nil {
				// Do not do Fatalf here since the peer might be gone but we should survive.
				log.Printf("Error calling RPC %v", vr.err)
			} else {
				log.Printf("Got response to vote request from %v", vr.peer)
				log.Printf("Peers %s granted %v term %v", vr.peer, vr.ret.VoteGranted, vr.ret.Term)
			}
			// check whether there are enough to be voted
			if vr.ret.VoteGranted && vr.ret.Term == currentTerm {
				voteCount++
				// get elected
				if voteCount >= majority {
					log.Printf("Elected %v, term is %v", id, currentTerm)
					role = LEADER
					// send heartbeat
					for p, c := range peerClients {
						// Send in parallel so we don't wait for each client.
						go func(c pb.RaftClient, p string) {
							// send heartbeats
							ret, err := c.AppendEntries(context.Background(), &pb.AppendEntriesArgs{Term: currentTerm, LeaderID: id})
							// get response
							appendResponseChan <- AppendResponse{ret: ret, err: err, peer: p}
						}(c, p)
					}
				}
			}
		case ar := <-appendResponseChan:
			// We received a response to a previous AppendEntries RPC call
			log.Printf("Got append entries response from %v, %v", ar.peer, ar.ret.Success)
		}
	}
	log.Printf("Strange to arrive here")
}
