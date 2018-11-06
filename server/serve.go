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

func searchCommand(e *pb.Entry, ops []*InputChannelType, s *KVStore) {
	index := -1
	for i, op := range ops {
		if e.Cmd == &op.command {
			index = i
		}
	}
	if index != -1 {
		s.HandleCommand(*ops[index])
	} else {
		s.CommitLog(e)
	}
}

func min(x int64, y int64) int64 {
	if x < y {
		return x
	}
	return y
}

type Role int

const (
	CANDIDATE Role = 0
	FOLLOWER  Role = 1
	LEADER    Role = 2
)

type AppendResponse struct {
	ret  *pb.AppendEntriesRet
	err  error
	peer string
	op   InputChannelType
}

type VoteResponse struct {
	ret  *pb.RequestVoteRet
	err  error
	peer string
}

func sendHeartBeat(peerClients map[string]pb.RaftClient, logs *[]*pb.Entry,
	prevLogIndex *int64, prevLogTerm *int64, currentTerm *int64,
	id *string, nextIndex *map[string]int64, commitIndex *int64,
	appendResponseChan *chan AppendResponse) {
	for p, c := range peerClients {
		go func(c pb.RaftClient, p string) {
			// send heartbeats
			*prevLogIndex = int64(len((*logs)) - 1)
			log.Printf("In heartbeat: prevLogIndex %v, length of log is %v", *prevLogIndex, len(*logs))
			// if the log is empty
			if len((*logs)) >= 1 {
				*prevLogTerm = (*logs)[len((*logs))-1].Term
				ret, err := c.AppendEntries(
					context.Background(),
					&pb.AppendEntriesArgs{
						Term:         *currentTerm,
						LeaderID:     *id,
						PrevLogIndex: (*nextIndex)[p] - 1,
						PrevLogTerm:  (*logs)[(*nextIndex)[p]-1].Term,
						Entries:      (*logs)[0:0],
						LeaderCommit: *commitIndex,
					})
				// get response
				log.Printf("HeatBeat sent from %v to %v", id, p)
				*appendResponseChan <- AppendResponse{ret: ret, err: err, peer: p}
			} else {
				ret, err := c.AppendEntries(
					context.Background(),
					&pb.AppendEntriesArgs{
						Term:         *currentTerm,
						LeaderID:     *id,
						PrevLogIndex: (*nextIndex)[p] - 1,
						PrevLogTerm:  0,
						Entries:      (*logs)[0:0],
						LeaderCommit: *commitIndex,
					})
				// get response
				log.Printf("HeatBeat sent from %v to %v", id, p)
				*appendResponseChan <- AppendResponse{ret: ret, err: err, peer: p}
			}
		}(c, p)
	}
}

func sendLogEntries(peerClients map[string]pb.RaftClient, peer *string, logs *[]*pb.Entry,
	prevLogIndex *int64, prevLogTerm *int64, currentTerm *int64,
	id *string, nextIndex *map[string]int64, commitIndex *int64,
	appendResponseChan *chan AppendResponse) {
	go func(c pb.RaftClient, p string) {
		// send heartbeats
		if len(*logs) >= 1 {
			*prevLogIndex = int64(len(*logs) - 1)
			*prevLogTerm = (*logs)[len(*logs)-1].Term
		}
		ret, err := c.AppendEntries(
			context.Background(),
			&pb.AppendEntriesArgs{
				Term:         *currentTerm,
				LeaderID:     *id,
				PrevLogIndex: (*nextIndex)[p] - 1,
				PrevLogTerm:  (*logs)[(*nextIndex)[p]-1].Term,
				Entries:      (*logs)[(*nextIndex)[p]:],
				LeaderCommit: *commitIndex,
			})
		// get response
		*appendResponseChan <- AppendResponse{ret: ret, err: err, peer: p}
	}(peerClients[*peer], *peer)
}

func followerUpdate(term *int64, currentTerm *int64, role *Role, votedFor *string, voteCount *int64) {
	*currentTerm = *term
	*role = FOLLOWER
	*votedFor = ""
	*voteCount = 0
}

// The main service loop. All modifications to the KV store are run through here.
func serve(s *KVStore, r *rand.Rand, peers *arrayPeers, id string, port int) {
	// heartbeat lower and upper bound
	lbHeartBeat :=  100
	ubHeartBeat :=  200
	lbTimer :=  1000
	ubTimer :=  2000
	// persistent server attributes
	var currentTerm int64 = 0
	var votedFor string = ""
	var leaderID string = ""
	var logs []*pb.Entry
	matchIndex := make(map[string]int64)
	nextIndex := make(map[string]int64)
	//ops := make(chan *InputChannelType)
	var ops []*InputChannelType
	var op InputChannelType
	// volatile server attributes
	var commitIndex int64 = 0
	var lastApplied int64 = 0
	var lastLogTerm int64 = 0
	var lastLogIndex int64 = 0
	var role = FOLLOWER
	var voteCount int64 = 0 // received votes counted
	var majority int64 = 0  // majority votes
	var headCount int64 = 1 // raft servers counted, self included
	var prevLogIndex int64
	var prevLogTerm int64
	appendResponseChan := make(chan AppendResponse)
	voteResponseChan := make(chan VoteResponse)
	peerClients := make(map[string]pb.RaftClient)
	// Create a timer and start running it
	timer := time.NewTimer(randomDuration(r, lbTimer, ubTimer))
	// heartbeat timer
	timerHeartBeat := time.NewTimer(randomDuration(r, lbHeartBeat, ubHeartBeat))
	raft := Raft{AppendChan: make(chan AppendEntriesInput), VoteChan: make(chan VoteInput)}

	// Start in a Go routine so it doesn't affect us.
	go RunRaftServer(&raft, port)

	// Initialization
	for _, peer := range *peers {
		client, err := connectToPeer(peer)
		if err != nil {
			log.Fatalf("Failed to connect to GRPC server %v", err)
		}
		headCount++
		peerClients[peer] = client
		// initialize matchIndex
		matchIndex[peer] = 0
		log.Printf("Connected to %v", peer)
	}
	// count head
	majority = headCount/2 + 1

	// Run forever handling inputs from various channels
	for {
		// what a leader should do
		if role == LEADER {
			// check nextIndex
			log.Printf("In leader: nextIndex %v", nextIndex)
			// send hearbeats
			sendHeartBeat(peerClients, &logs, &prevLogIndex, &prevLogTerm, &currentTerm,
				&id, &nextIndex, &commitIndex, &appendResponseChan)
			// update commitIndex
			if int64(len(logs)-1) > commitIndex {
				for i := int64(1); i <= int64(len(logs)-1)-commitIndex; i++ {
					j := i + commitIndex
					var count int64 = 0
					for p := range peerClients {
						if matchIndex[p] >= j && logs[j].Term == currentTerm {
							count++
						}
					}
					if count >= majority {
						commitIndex = j
						// add op only once
						ops = append(ops, &op)
					}
				}
			}
		}
		// always check commit
		if commitIndex > lastApplied {
			for i := int64(1); i <= commitIndex-lastApplied; i++ {
				j := i + lastApplied
				searchCommand(logs[j], ops, s)
			}
			lastApplied = commitIndex
		}

		select {
		// send heartbeat periodically
		case <-timerHeartBeat.C:
			if role == LEADER {
				sendHeartBeat(peerClients, &logs, &prevLogIndex, &prevLogTerm, &currentTerm,
					&id, &nextIndex, &commitIndex, &appendResponseChan)
				restartTimer(timerHeartBeat, r, lbHeartBeat, ubHeartBeat)
			} else {
				timerHeartBeat.Stop()
			}
		case <-timer.C:
			// The timer went off.
			log.Printf("Timeout")
			// turn to candidate state
			role = CANDIDATE
			// voted for myself first
			votedFor = id
			voteCount = 1
			// increment current term
			currentTerm++
			for p, c := range peerClients {
				// Send in parallel so we don't wait for each client.
				go func(c pb.RaftClient, p string) {
					if len(logs) >= 1 {
						lastLogIndex = int64(len(logs) - 1)
						lastLogTerm = logs[len(logs)-1].Term
					}
					// this tells the other raft servers about the vote request
					ret, err := c.RequestVote(
						context.Background(),
						&pb.RequestVoteArgs{
							Term:         currentTerm,
							CandidateID:  id,
							LastLogIndex: lastLogIndex,
							LasLogTerm:   lastLogTerm,
						})
					// this receives the response from others
					voteResponseChan <- VoteResponse{ret: ret, err: err, peer: p}
				}(c, p)
			}
			// This will also take care of any pesky timeouts that happened while processing the operation.
			restartTimer(timer, r, lbTimer, ubTimer)
		case op = <-s.C:
			// We received an operation from a client
			// TODO: Figure out if you can actually handle the request here. If not use the Redirect result to send the
			// client elsewhere.
			// TODO: Use Raft to make sure it is safe to actually run the command.
			// if this is the leader, add it to the log
			log.Printf("Client request received %v, command %v", id, op.command)
			if role == LEADER {
				log.Printf("Client Entry added to the leader %v, command %v", id, op.command)
				if len(logs) >= 1 {
					lastLogIndex = int64(len(logs) - 1)
					lastLogTerm = logs[len(logs)-1].Term
				}
				// add to local log
				entry := &pb.Entry{
					Term:  currentTerm,
					Index: lastLogIndex + 1,
					Cmd:   &op.command,
				}
				logs = append(logs, entry)
				// tell the other clients
				for p, c := range peerClients {
					go func(c pb.RaftClient, p string) {
						ret, err := c.AppendEntries(
							context.Background(),
							&pb.AppendEntriesArgs{
								Term:         currentTerm,
								LeaderID:     id,
								PrevLogIndex: lastLogIndex,
								PrevLogTerm:  lastLogTerm,
								Entries:      []*pb.Entry{entry},
								LeaderCommit: commitIndex,
							})
						appendResponseChan <- AppendResponse{ret: ret, err: err, peer: p, op: op}
					}(c, p)
				}
			} else {
				// send redirect information
				log.Printf("In client rquest, %v is redirected to leader %v", op.command, leaderID)
				op.response <- pb.Result{Result:&pb.Result_Redirect{Redirect:&pb.Redirect{Server:leaderID}}}
			}
		case ae := <-raft.AppendChan:
			// We received an AppendEntries request from a Raft peer
			// TODO figure out what to do here, what we do is entirely wrong.
			//log.Printf("AppendEntries request from leader %v received", ae.arg.LeaderID)
			// things that have to be done anyway
			if ae.arg.Term > currentTerm {
				followerUpdate(&ae.arg.Term, &currentTerm, &role, &votedFor, &voteCount)
				leaderID = ae.arg.LeaderID
				//log.Printf("AppendEntries request from new leader %v received", ae.arg.LeaderID)
			}
			// reject if term is smaller, outdated
			if ae.arg.Term < currentTerm {
				log.Printf(
					"AppendEntries request from %s rejected, term %v is smaller than %v",
					ae.arg.LeaderID,
					ae.arg.Term,
					currentTerm,
				)
				ae.response <- pb.AppendEntriesRet{Term: currentTerm, Success: false}
				continue
			}
			// This will also take care of any pesky timeouts that happened while processing the operation.
			restartTimer(timer, r, lbTimer, ubTimer)
			//log.Printf("In append: prevLogIndex %v, length of logs %v", ae.arg.PrevLogIndex, len(logs))
			// reject if log doesn't match
			if int64(len(logs)-1) <= ae.arg.PrevLogIndex-1 || (ae.arg.PrevLogIndex > 0 && logs[ae.arg.PrevLogIndex].Term != ae.arg.PrevLogTerm) {
				log.Printf(
					"AppendEntries request from %s rejected, prevLogTerm %v doesn't match %v",
					ae.arg.LeaderID,
					ae.arg.PrevLogTerm,
					ae.arg.PrevLogIndex,
				)
				ae.response <- pb.AppendEntriesRet{Term: ae.arg.Term, Success: false}
				continue
			}
			// handle conflicts when log matches
			// delete everything follows it
			if int64(len(logs)-1) <= ae.arg.PrevLogIndex+int64(len(ae.arg.Entries)) {
				logs = logs[0 : ae.arg.PrevLogIndex+1]
				logs = append(logs, ae.arg.Entries...)
			} else {
				for i := 1; i <= len(ae.arg.Entries); i++ {
					j := int(ae.arg.PrevLogIndex) + i
					if logs[j] == ae.arg.Entries[i] {
						continue
					} else {
						logs = logs[0 : ae.arg.PrevLogIndex+1]
						logs = append(logs, ae.arg.Entries...)
						break
					}
				}
			}
			// update commit index
			if ae.arg.LeaderCommit > commitIndex {
				commitIndex = min(ae.arg.LeaderCommit, int64(len(logs)-1))
			}
			// don't respond if hearbeat to overcount the vote
			if len(ae.arg.Entries) != 0 {
				ae.response <- pb.AppendEntriesRet{Term: ae.arg.Term, Success: true}
				log.Printf(
					"AppendEntries consistency guaranteed between %v and leader %s",
					id,
					ae.arg.LeaderID,
				)
			} else {
				//log.Printf(
				//	"In append: Heartbeat received from leader %v, follower %s, no response",
				//	ae.arg.LeaderID,
				//	id,
				//)
			}
		case vr := <-raft.VoteChan:
			// We received a RequestVote RPC from a raft peer
			// TODO: Fix this.
			if vr.arg.Term > currentTerm {
				followerUpdate(&vr.arg.Term, &currentTerm, &role, &votedFor, &voteCount)
				log.Printf("Vote Request from newer candidate %v received", vr.arg.CandidateID)
			}
			log.Printf("Vote Request from %v received ", vr.arg.CandidateID)
			if vr.arg.Term < currentTerm {
				log.Printf("Vote Request from %v rejected, term outdated", vr.arg.CandidateID)
				vr.response <- pb.RequestVoteRet{Term: currentTerm, VoteGranted: false}
				continue
			}
			if (votedFor == "" || votedFor == vr.arg.CandidateID) && (vr.arg.LasLogTerm > lastLogTerm || vr.arg.LasLogTerm == lastLogTerm && vr.arg.LastLogIndex >= lastLogIndex) {
				vr.response <- pb.RequestVoteRet{Term: vr.arg.Term, VoteGranted: true}
				log.Printf("Vote Request from %v granted, term %v", vr.arg.CandidateID, vr.arg.Term)
				// change the votedFor
				votedFor = vr.arg.CandidateID
				continue
			}
			log.Printf("Vote Request from %v rejected, log not up-to-date", vr.arg.CandidateID)
			vr.response <- pb.RequestVoteRet{Term: vr.arg.Term, VoteGranted: false}
		case vr := <-voteResponseChan:
			// We received a response to a previous vote request.
			// TODO: Fix this
			if vr.err != nil {
				// Do not do Fatalf here since the peer might be gone but we should survive.
				log.Printf("Error calling RPC %v", vr.err)
				continue
			} else {
				log.Printf("Vote Response from %v", vr.peer)
				log.Printf("Vote Response, Peers %s granted %v term %v", vr.peer, vr.ret.VoteGranted, vr.ret.Term)
			}
			if vr.ret.Term > currentTerm {
				followerUpdate(&vr.ret.Term, &currentTerm, &role, &votedFor, &voteCount)
				log.Printf("Vote Response from newer candidate %v term %v received", vr.peer, vr.ret.Term)
				continue
			}
			// check whether there are enough to be voted
			if vr.ret.VoteGranted && vr.ret.Term == currentTerm {
				voteCount++
				// adlready elected
				if role == LEADER {
					log.Printf("Already elected %v, term is %v", id, currentTerm)
					continue
				}
				// get elected
				if voteCount >= majority {
					log.Printf("ELECTED %v, term is %v", id, currentTerm)
					role = LEADER
					// send heartbeat
					sendHeartBeat(peerClients, &logs, &prevLogIndex, &prevLogTerm, &currentTerm,
						&id, &nextIndex, &commitIndex, &appendResponseChan)
					log.Printf("In election: HeatBeat sent from %v", id)
					for p, _ := range peerClients {
						// reinitialization of nextIndex upon new leader
						nextIndex[p] = int64(len(logs))
					}
					log.Printf("In election: prevLogIndex %v, length of log is %v", prevLogIndex, len(logs))
					log.Printf("In election: NextIndex %v \n after %v elected", nextIndex, id)
					restartTimer(timerHeartBeat, r, lbHeartBeat, ubHeartBeat)
				}
			}
		case ar := <-appendResponseChan:
			// We received a response to a previous AppendEntries RPC call
			if ar.err != nil {
				// Do not do Fatalf here since the peer might be gone but we should survive.
				log.Printf("Error calling RPC %v", ar.err)
				continue
			} else {
				log.Printf("AppendEntries response from %v, result %v", ar.peer, ar.ret.Success)
			}
			// update leader
			if ar.ret.Term > currentTerm {
				currentTerm = ar.ret.Term
				role = FOLLOWER
				votedFor = ""
				voteCount = 0
				log.Printf("AppendEntries response from newer candidate %v term %v received, quit leadership", ar.peer, ar.ret.Term)
				continue
			}
			// success
			if ar.ret.Success {
				if int64(len(logs)-1) >= nextIndex[ar.peer] {
					nextIndex[ar.peer] = int64(len(logs))
					matchIndex[ar.peer] = int64(len(logs) - 1)
				} else {
					nextIndex[ar.peer]++
					matchIndex[ar.peer]++
				}
			} else {
				// fail trys again
				// notice that when nextIndex reacher 0, replicate all the logs
				nextIndex[ar.peer]--
				if len(logs) != 0 && int64(len(logs)-1) >= nextIndex[ar.peer] {
					sendLogEntries(peerClients, &ar.peer, &logs, &prevLogIndex, &prevLogTerm, &currentTerm,
						&id, &nextIndex, &commitIndex, &appendResponseChan)
				}

			}
		}
	}
	log.Printf("Strange to arrive here")
}
