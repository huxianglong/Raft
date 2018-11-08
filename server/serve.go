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

var ifFakeCmds bool = false

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
		//log.Printf("In searchCommand, response is %v",
		//	(*ops[index]).response)
		go s.HandleCommand(*ops[index])
		// TODO: this is just temporary, waste the last channel
		//log.Printf("Stuck here, after HandleCommand.")
		if ifFakeCmds {
			res := <-(*ops[index]).response
			// TODO: this is only approproate for set
			log.Printf("Client got response key: \"%v\" value:\"%v\"", res.GetKv().Key, res.GetKv().Value)
		} else {
			log.Printf("Client response sent: %v", *ops[index])
		}
		// delete the element without
		ops[index] = ops[len(ops)-1]
		ops = ops[:len(ops)-1]
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
	currentTerm *int64,
	id *string, nextIndex *map[string]int64, commitIndex *int64,
	appendResponseChan *chan AppendResponse) {
	// the heartbeat has to be empty
	for p, c := range peerClients {
		go func(c pb.RaftClient, p string) {
			// send heartbeats
			//log.Printf("In heartbeat: prevLogIndex %v, length of log is %v", *prevLogIndex, len(*logs))
			// if the log is empty
			var prevIndex int64
			var prevTerm int64
			prevIndex = (*nextIndex)[p] - 1
			if ((*nextIndex)[p] - 1) > int64(len(*logs)-1) {
				log.Fatalf("In sendHeartBeat, nextIndex %v, log length %v", *nextIndex, len(*logs))
			}
			if len(*logs) >= 1 {
				if (*nextIndex)[p] >= 1 {
					prevTerm = (*logs)[(*nextIndex)[p]-1].Term
				} else {
					prevTerm = 0
				}
			} else {
				prevTerm = 0
			}
			//log.Printf("log length %v, nextIndex %v", len(*logs), *nextIndex)
			ret, err := c.AppendEntries(
				context.Background(),
				&pb.AppendEntriesArgs{
					Term:         *currentTerm,
					LeaderID:     *id,
					PrevLogIndex: prevIndex,
					PrevLogTerm:  prevTerm,
					Entries:      (*logs)[0:0],
					LeaderCommit: *commitIndex,
				})
			//log.Printf("HeatBeat sent from %v to %v", id, p)
			*appendResponseChan <- AppendResponse{ret: ret, err: err, peer: p}
		}(c, p)
	}
}

func sendLogEntries(peerClients map[string]pb.RaftClient, peer *string, logs *[]*pb.Entry,
	currentTerm *int64,
	id *string, nextIndex *map[string]int64, commitIndex *int64,
	appendResponseChan *chan AppendResponse) {
	go func(c pb.RaftClient, p string) {
		var prevTerm int64
		prevIndex := (*nextIndex)[p] - 1
		if ((*nextIndex)[p] - 1) > int64(len(*logs)-1) {
			log.Fatalf("In sendLogEntries, nextIndex %v, log length %v", *nextIndex, len(*logs))
		}
		if (*nextIndex)[p] > 0 {
			prevTerm = (*logs)[(*nextIndex)[p]-1].Term
		} else {
			prevTerm = int64(0)
		}
		ret, err := c.AppendEntries(
			context.Background(),
			&pb.AppendEntriesArgs{
				Term:         *currentTerm,
				LeaderID:     *id,
				PrevLogIndex: prevIndex,
				PrevLogTerm:  prevTerm,
				Entries:      (*logs)[(*nextIndex)[p]:],
				LeaderCommit: *commitIndex,
			})
		if prevIndex == int64(-1) && prevTerm != int64(0) {
			log.Fatalf("In sendLogEntries, prevIndex is -1 but prevTerm is %v", prevTerm)
		}
		log.Printf("AppendEntries again request sent from %v, PrevLogIndex %v, PrevLogTerm %v, Entries length %v, commitIndex %v",
			*id, prevIndex, prevTerm, int64(len(*logs))-(*nextIndex)[p], *commitIndex)
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

func fakeCommand(i int, v int64) InputChannelType {
	var command pb.Command
	switch i {
	case 1:
		command = pb.Command{Operation: pb.Op_SET,
			Arg: &pb.Command_Set{Set: &pb.KeyValue{Key: "1", Value: fmt.Sprintf("%v", v)}}}
	case 2:
		command = pb.Command{Operation: pb.Op_GET, Arg: &pb.Command_Get{Get: &pb.Key{Key: "1"}}}
	}
	c := make(chan pb.Result)
	return InputChannelType{command: command, response: c}
}

// The main service loop. All modifications to the KV store are run through here.
func serve(s *KVStore, r *rand.Rand, peers *arrayPeers, id string, port int) {
	// whether there is fake commands
	// heartbeat lower and upper bound
	lbHeartBeat := 100 / 2
	ubHeartBeat := 200 / 2
	lbTimer := 2000
	ubTimer := 4000
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
	var commitIndex int64 = -1
	var lastApplied int64 = -1
	var lastLogTerm int64 = 0
	var lastLogIndex int64 = 0
	var role = FOLLOWER
	var voteCount int64 = 0 // received votes counted
	var majority int64 = 0  // majority votes
	var headCount int64 = 1 // raft servers counted, self included
	appendResponseChan := make(chan AppendResponse)
	voteResponseChan := make(chan VoteResponse)
	peerClients := make(map[string]pb.RaftClient)
	// create a print timer to print heartbeat
	//timerPrint := time.NewTimer()
	// Create a timer and start running it
	timer := time.NewTimer(randomDuration(r, lbTimer, ubTimer))
	// heartbeat timer
	timerHeartBeat := time.NewTimer(randomDuration(r, lbHeartBeat, ubHeartBeat))
	raft := Raft{AppendChan: make(chan AppendEntriesInput), VoteChan: make(chan VoteInput)}

	// Start in a Go routine so it doesn't affect us.
	go RunRaftServer(&raft, port)

	// timer runs in parallel
	go func() {
		// send heartbeat periodically
		for {
			<-timerHeartBeat.C
			if role == LEADER {
				sendHeartBeat(peerClients, &logs, &currentTerm,
					&id, &nextIndex, &commitIndex, &appendResponseChan)
				restartTimer(timerHeartBeat, r, lbHeartBeat, ubHeartBeat)
			} else {
				timerHeartBeat.Stop()
			}
		}
	}()

	// tells the program still runs
	go func() {
		for {
			time.Sleep(5 * time.Second)
			log.Printf("ALIVE. The election timer %v, leader %v, log length %v",
				timer.C, leaderID, len(logs),
			)
		}
	}()

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
			log.Printf("In commit, id %v, commitIndex %v, lastApplied %v", id, commitIndex, lastApplied)
			for i := int64(1); i <= commitIndex-lastApplied; i++ {
				j := i + lastApplied
				log.Printf("Committing... %v %v log %v %v", role, id, j, logs[j])
				searchCommand(logs[j], ops, s)
				log.Printf("Committed... %v %v log %v %v", role, id, j, logs[j])
			}
			lastApplied = commitIndex
		}

		select {
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
				} else {
					lastLogIndex = -1
					lastLogTerm = 0
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
					log.Printf("ApeendEntries single request sent from %v to %v, PrevLogIndex %v, PrevLogTerm %v, Entries %v",
						id, p, lastLogIndex, lastLogTerm, entry)
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
				op.response <- pb.Result{Result: &pb.Result_Redirect{Redirect: &pb.Redirect{Server: leaderID}}}
			}
		case ae := <-raft.AppendChan:
			// We received an AppendEntries request from a Raft peer
			// TODO figure out what to do here, what we do is entirely wrong.
			//log.Printf("AppendEntries request from leader %v received", ae.arg.LeaderID)
			// things that have to be done anyway
			if ae.arg.Term >= currentTerm {
				leaderID = ae.arg.LeaderID
			}
			if ae.arg.Term > currentTerm {
				followerUpdate(&ae.arg.Term, &currentTerm, &role, &votedFor, &voteCount)
				restartTimer(timer, r, lbTimer, ubTimer)
				//log.Printf("AppendEntries request from new leader %v received", ae.arg.LeaderID)
			}
			// reject if term is smaller, outdated
			if ae.arg.Term < currentTerm {
				log.Printf(
					"AppendEntries request from %s rejected by %v, term %v is smaller than %v",
					ae.arg.LeaderID,
					id,
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
			if ae.arg.PrevLogIndex >= 0 {
				if int64(len(logs)-1) >= ae.arg.PrevLogIndex {
					if logs[ae.arg.PrevLogIndex].Term != ae.arg.PrevLogTerm {
						log.Printf(
							"AppendEntries request from %s rejected by %v, prevLogTerm %v at %v doesn't match",
							ae.arg.LeaderID,
							id,
							ae.arg.PrevLogTerm,
							ae.arg.PrevLogIndex,
							logs[ae.arg.PrevLogIndex].Term,
						)
						ae.response <- pb.AppendEntriesRet{Term: ae.arg.Term, Success: false}
						continue
					} else {
						// copy entries
					}
				} else {
					log.Printf(
						"AppendEntries request from %s rejected by %v, prevLogIndex %v beyond last log index %v",
						ae.arg.LeaderID,
						id,
						ae.arg.PrevLogIndex,
						len(logs)-1,
					)
					ae.response <- pb.AppendEntriesRet{Term: ae.arg.Term, Success: false}
					continue
				}
			} else {
				// copy everything
			}
			// handle conflicts when log matches
			// delete everything follows it
			if int64(len(logs)-1) <= ae.arg.PrevLogIndex+int64(len(ae.arg.Entries)) {
				logs = logs[0 : ae.arg.PrevLogIndex+1]
				logs = append(logs, ae.arg.Entries...)
			} else {
				for i := 1; i <= len(ae.arg.Entries); i++ {
					j := int(ae.arg.PrevLogIndex) + i
					// index notice here
					if logs[j] == ae.arg.Entries[i-1] {
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
					"AppendEntries consistency guaranteed between %v and leader %s, log length %v, last term %v",
					id,
					ae.arg.LeaderID,
					len(logs),
					logs[len(logs)-1].Term,
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
				restartTimer(timer, r, lbTimer, ubTimer)
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
				restartTimer(timer, r, lbTimer, ubTimer)
				log.Printf("Vote Response from newer candidate %v term %v received", vr.peer, vr.ret.Term)
				continue
			}
			// check whether there are enough to be voted
			if vr.ret.VoteGranted && vr.ret.Term == currentTerm {
				voteCount++
				// adlready elected
				if role == LEADER {
					log.Printf("Already elected %v, term is %v", id, currentTerm)
					sendHeartBeat(peerClients, &logs, &currentTerm,
						&id, &nextIndex, &commitIndex, &appendResponseChan)
					log.Printf("In election: HeatBeat sent from %v", id)
					continue
				}
				// get elected
				if voteCount >= majority {
					if ifFakeCmds {
						log.Printf("ELECTED %v, term is %v, fake command sent", id, currentTerm)
						go func() { s.C <- fakeCommand(1, currentTerm) }()
					} else {
						log.Printf("ELECTED %v, term is %v", id, currentTerm)
					}
					role = LEADER
					leaderID = id
					// stop its own timer
					timer.Stop()
					// send heartbeat
					sendHeartBeat(peerClients, &logs, &currentTerm,
						&id, &nextIndex, &commitIndex, &appendResponseChan)
					log.Printf("In election: HeatBeat sent from %v", id)
					for p, _ := range peerClients {
						// reinitialization of nextIndex upon new leader
						nextIndex[p] = int64(len(logs))
					}
					log.Printf("In election: NextIndex %v \n after %v elected, log length %v", nextIndex, id, len(logs))
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
				if nextIndex[ar.peer] >= 1 {
					nextIndex[ar.peer]--
				} else {
					log.Printf("ERROR!! The nextIndex of %v is 0 but still cannot match", ar.peer)
				}
				if len(logs) != 0 && int64(len(logs)-1) >= nextIndex[ar.peer] {
					sendLogEntries(peerClients, &ar.peer, &logs, &currentTerm,
						&id, &nextIndex, &commitIndex, &appendResponseChan)
				}

			}
		}
	}
	log.Printf("Strange to arrive here")
}
