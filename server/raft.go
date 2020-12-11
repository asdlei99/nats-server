// Copyright 2020 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"sync"
	"time"
)

type RaftNode interface {
	Propose(entry []byte) error
	Applied(index uint64)
	State() RaftState
	Leader() bool
	GroupLeader() string
	ID() string
	Group() string
	Peers() []*Peer
	ApplyC() <-chan *CommittedEntry
	LeadChangeC() <-chan bool
	QuitC() <-chan struct{}
	Stop()
}

type WAL interface {
	StoreMsg(subj string, hdr, msg []byte) (uint64, int64, error)
	LoadMsg(seq uint64) (subj string, hdr, msg []byte, ts int64, err error)
	RemoveMsg(seq uint64) (bool, error)
	Compact(seq uint64) (uint64, error)
	State() StreamState
	Stop() error
}

type Peer struct {
	ID   string
	Last time.Time
}

type RaftState uint8

// Allowable states for a NATS Consensus Group.
const (
	Follower RaftState = iota
	Leader
	Candidate
	Observer
	Closed
)

type raft struct {
	sync.RWMutex
	group   string
	sd      string
	id      string
	wal     WAL
	state   RaftState
	csz     int
	qn      int
	peers   map[string]int64
	acks    map[uint64]map[string]struct{}
	elect   *time.Timer
	term    uint64
	pterm   uint64
	pindex  uint64
	commit  uint64
	applied uint64
	leader  string
	vote    string
	hash    string
	s       *Server
	c       *client

	// Subjects for votes, updates, config changes.
	vsubj  string
	vreply string
	asubj  string
	areply string
	csubj  string
	creply string

	// Channels
	propc    chan []byte
	applyc   chan *CommittedEntry
	sendq    chan *pubMsg
	quit     chan struct{}
	reqs     chan *voteRequest
	votes    chan *voteResponse
	resp     chan *appendEntryResponse
	leadc    chan bool
	stepdown chan string
}

const (
	minElectionTimeout = 250 * time.Millisecond
	maxElectionTimeout = 2 * minElectionTimeout
	hbInterval         = 200 * time.Millisecond
)

const (
	raftVoteSubj   = "$SYS.NRG.%s.%s.V"
	raftAppendSubj = "$SYS.NRG.%s.%s.A"
	raftChangeSubj = "$SYS.NRG.%s.%s.C"
	raftReplySubj  = "$SYS.NRG.%s.%s"
)

func (s *Server) newRaftGroup(name, storeDir string, log WAL) (RaftNode, error) {
	s.mu.Lock()
	if s.sys == nil || s.sys.sendq == nil {
		s.mu.Unlock()
		return nil, ErrNoSysAccount
	}
	sendq := s.sys.sendq
	hash := s.sys.shash
	s.mu.Unlock()

	// TODO(dlc) - Need better way for seed based configs etc.
	expected := s.configuredRoutes()
	if expected < 2 {
		return nil, errors.New("raft: cluster too small")
	}

	n := &raft{
		id:       hash[:idLen],
		group:    name,
		sd:       storeDir,
		wal:      log,
		state:    Follower,
		elect:    time.NewTimer(randElectionTimeout()),
		csz:      expected,
		qn:       expected/2 + 1,
		hash:     hash,
		peers:    make(map[string]int64),
		acks:     make(map[uint64]map[string]struct{}),
		s:        s,
		c:        s.createInternalSystemClient(),
		sendq:    sendq,
		quit:     make(chan struct{}),
		reqs:     make(chan *voteRequest, 4),
		votes:    make(chan *voteResponse, 8),
		resp:     make(chan *appendEntryResponse, 32),
		propc:    make(chan []byte, 8),
		applyc:   make(chan *CommittedEntry, 8),
		leadc:    make(chan bool, 4),
		stepdown: make(chan string),
	}

	if term, vote, err := n.readTermVote(); err != nil && term > 0 {
		n.term = term
		n.vote = vote
	}

	if state := log.State(); state.Msgs > 0 {
		// TODO(dlc) - Recover state here.
		fmt.Printf("fs is %+v\n", state)
	}

	if err := n.createInternalSubs(); err != nil {
		n.shutdown()
		return nil, err
	}

	// FIXME(dlc) - Check peers and setup?
	// Make sure to track ourselves.
	n.trackPeer(n.id)

	s.registerRaftNode(n.group, n)
	s.startGoRoutine(n.run)
	n.updateLeadChange(n.State() == Leader)

	return n, nil
}

// Server will track all raft nodes.
func (s *Server) registerRaftNode(group string, n RaftNode) {
	s.rnMu.Lock()
	defer s.rnMu.Unlock()
	if s.raftNodes == nil {
		s.raftNodes = make(map[string]RaftNode)
	}
	s.raftNodes[group] = n
}

func (s *Server) unregisterRaftNode(group string) {
	s.rnMu.Lock()
	defer s.rnMu.Unlock()
	if s.raftNodes != nil {
		delete(s.raftNodes, group)
	}
}

func (s *Server) lookupRaftNode(group string) RaftNode {
	s.rnMu.RLock()
	defer s.rnMu.RUnlock()
	var n RaftNode
	if s.raftNodes != nil {
		n = s.raftNodes[group]
	}
	return n
}

// Formal API

var errProposalFailed = errors.New("raft: proposal failed")
var errNotLeader = errors.New("raft: not leader")

// Propose will propose a new entry to the group.
// This should only be called on the leader.
func (n *raft) Propose(entry []byte) error {
	n.RLock()
	if n.state != Leader {
		n.RUnlock()
		return errNotLeader
	}
	propc := n.propc
	n.RUnlock()

	n.debug("PROPOSE CALLED\n")

	select {
	case propc <- entry:
	default:
		return errProposalFailed
	}
	return nil
}

// Applied is to be called when the FSM has applied the committed entries.
func (n *raft) Applied(index uint64) {
	n.Lock()
	// FIXME(dlc) - Check spec on error conditions.
	n.debug("Updating applied to %d!\n", index)
	n.applied = index
	n.Unlock()
}

// Leader returns if we are the leader for our group.
func (n *raft) Leader() bool {
	if n == nil {
		return false
	}
	n.RLock()
	isLeader := n.state == Leader
	n.RUnlock()
	return isLeader
}

// GroupLeader returns the current leader of the group.
func (n *raft) GroupLeader() string {
	if n == nil {
		return noLeader
	}
	n.RLock()
	defer n.RUnlock()
	return n.leader
}

// State return the current state for this node.
func (n *raft) State() RaftState {
	n.RLock()
	state := n.state
	n.RUnlock()
	return state
}

func (n *raft) ID() string {
	n.RLock()
	defer n.RUnlock()
	return n.id
}

func (n *raft) Group() string {
	n.RLock()
	defer n.RUnlock()
	return n.group
}

func (n *raft) Peers() []*Peer {
	n.RLock()
	defer n.RUnlock()
	if n.state != Leader {
		return nil
	}
	var peers []*Peer
	for id, ts := range n.peers {
		peers = append(peers, &Peer{ID: id, Last: time.Unix(0, ts)})
	}
	return peers
}

func (n *raft) Stop() {
	n.shutdown()
}

func (n *raft) ApplyC() <-chan *CommittedEntry { return n.applyc }
func (n *raft) LeadChangeC() <-chan bool       { return n.leadc }
func (n *raft) QuitC() <-chan struct{}         { return n.quit }

func (n *raft) shutdown() {
	n.Lock()
	close(n.quit)
	n.c.closeConnection(InternalClient)
	n.state = Closed
	s, g := n.s, n.group
	n.Unlock()
	s.unregisterRaftNode(g)
}

func (n *raft) newInbox(cn string) string {
	var b [replySuffixLen]byte
	rn := rand.Int63()
	for i, l := 0, rn; i < len(b); i++ {
		b[i] = digits[l%base]
		l /= base
	}
	return fmt.Sprintf(raftReplySubj, n.hash, b[:])
}

func (n *raft) createInternalSubs() error {
	cn := n.s.ClusterName()
	n.vsubj, n.vreply = fmt.Sprintf(raftVoteSubj, cn, n.group), n.newInbox(cn)
	n.asubj, n.areply = fmt.Sprintf(raftAppendSubj, cn, n.group), n.newInbox(cn)
	n.csubj, n.creply = fmt.Sprintf(raftChangeSubj, cn, n.group), n.newInbox(cn)

	// Votes
	if _, err := n.s.sysSubscribe(n.vreply, n.handleVoteResponse); err != nil {
		return err
	}
	if _, err := n.s.sysSubscribe(n.vsubj, n.handleVoteRequest); err != nil {
		return err
	}
	// AppendEntry
	if _, err := n.s.sysSubscribe(n.areply, n.handleAppendEntryResponse); err != nil {
		return err
	}
	if _, err := n.s.sysSubscribe(n.asubj, n.handleAppendEntry); err != nil {
		return err
	}
	// TODO(dlc) change events.
	return nil
}

func randElectionTimeout() time.Duration {
	delta := rand.Int63n(int64(maxElectionTimeout - minElectionTimeout))
	return (minElectionTimeout + time.Duration(delta))
}

// Lock should be held.
func (n *raft) resetElectionTimeout() {
	et := randElectionTimeout()
	if n.elect == nil {
		n.elect = time.NewTimer(et)
	} else {
		n.elect.Reset(et)
	}
}

func (n *raft) run() {
	s := n.s
	defer s.grWG.Done()

	for s.isRunning() {
		switch n.State() {
		case Follower:
			n.runAsFollower()
		case Candidate:
			n.runAsCandidate()
		case Leader:
			n.runAsLeader()
		}
	}
}

func (n *raft) debug(format string, args ...interface{}) {
	//	nf := fmt.Sprintf("%v [%s:%s  %s]\t%s", time.Now(), n.s.Name(), n.id, n.group, format)
	nf := fmt.Sprintf("[%s:%s  %s]\t%s", n.s.Name(), n.id, n.group, format)
	fmt.Printf(nf, args...)
}

func (n *raft) runAsFollower() {
	n.debug("FOLLOWER!\n")
	for {
		select {
		case <-n.s.quitCh:
			return
		case <-n.quit:
			return
		case <-n.elect.C:
			n.switchToCandidate()
			return
		case vreq := <-n.reqs:
			if err := n.processVoteRequest(vreq); err != nil {
				return
			}
		case newLeader := <-n.stepdown:
			n.debug("Got an new leader as a follower, stepping down! %v\n")
			n.switchToFollower(newLeader)
			return
		}
	}
}

// CommitEntry is handed back to the user to apply a commit to their FSM.
type CommittedEntry struct {
	Index   uint64
	Entries [][]byte
}

type appendEntry struct {
	leader    string
	term      uint64
	commit    uint64
	prevTerm  uint64
	prevIndex uint64
	entries   [][]byte
	// internal use only.
	reply string
	buf   []byte
}

func (ae *appendEntry) String() string {
	return fmt.Sprintf("&{leader:%s term:%d commit:%d pterm:%d pindex:%d entries: %d}",
		ae.leader, ae.term, ae.term, ae.prevTerm, ae.prevIndex, len(ae.entries))
}

// appendEntryResponse is our response to a received appendEntry.
type appendEntryResponse struct {
	term    uint64
	index   uint64
	peer    string
	success bool
}

// We want to make sure this does not change from system changing length of syshash.
const idLen = 8
const appendEntryResponseLen = 24 + 1

func (ar *appendEntryResponse) encode() []byte {
	var buf [appendEntryResponseLen]byte
	var le = binary.LittleEndian
	le.PutUint64(buf[0:], ar.term)
	le.PutUint64(buf[8:], ar.index)
	copy(buf[16:], ar.peer)

	if ar.success {
		buf[24] = 1
	} else {
		buf[24] = 0
	}
	return buf[:appendEntryResponseLen]
}

func (n *raft) decodeAppendEntryResponse(msg []byte) *appendEntryResponse {
	if len(msg) != appendEntryResponseLen {
		return nil
	}
	var le = binary.LittleEndian
	ar := &appendEntryResponse{
		term:  le.Uint64(msg[0:]),
		index: le.Uint64(msg[8:]),
		peer:  string(msg[16 : 16+idLen]),
	}
	ar.success = msg[24] == 1
	return ar
}

func (n *raft) runAsLeader() {
	n.debug("LEADER!\n")

	n.sendHeartbeat()

	const hbInterval = 250 * time.Millisecond
	hb := time.NewTicker(hbInterval)
	defer hb.Stop()

	for {
		select {
		case <-n.s.quitCh:
			return
		case <-n.quit:
			return
		case b := <-n.propc:
			var entries [][]byte
			entries = append(entries, b)
			// Gather more if available, limit to 3 atm. TODO(dlc)
		gather:
			for x := 0; x < 3; x++ {
				select {
				case b := <-n.propc:
					entries = append(entries, b)
				default:
					break gather
				}
			}
			n.debug("Gathered %d entries\n", len(entries))
			n.sendAppendEntry(entries)
		case <-hb.C:
			n.sendHeartbeat()
		case vresp := <-n.votes:
			if vresp.term > n.term {
				n.switchToFollower(noLeader)
				return
			}
		case vreq := <-n.reqs:
			if err := n.processVoteRequest(vreq); err != nil {
				n.switchToFollower(noLeader)
				return
			}
		case newLeader := <-n.stepdown:
			n.debug("Received an entry from another leader!\n")
			n.switchToFollower(newLeader)
			return
		case ar := <-n.resp:
			n.trackPeer(ar.peer)
			n.debug("Got an AE Response! %+v\n", ar)
			n.trackResponse(ar)
		}
	}
}

// Apply will update our commit index and apply the entry to the apply chan.
// lock should be held.
func (n *raft) apply(index uint64) {
	n.commit = index
	if n.state == Leader {
		n.debug("We have consensus on %d!!\n", index)
		delete(n.acks, index)
	}
	// FIXME(dlc) - Can keep this in memory if this too slow.
	_, _, msg, _, err := n.wal.LoadMsg(index)
	if err != nil {
		panic(fmt.Sprintf("Could not read in from wal for %d\n", index))
	}
	ae := n.decodeAppendEntry(msg, _EMPTY_)
	ae.buf = nil
	// We will block here placing the commit entry on purpose.
	n.applyc <- &CommittedEntry{index, ae.entries}
}

func (n *raft) trackResponse(ar *appendEntryResponse) {
	n.Lock()
	// FIXME(dlc) - process non-success.
	if !ar.success || ar.index <= n.commit {
		n.Unlock()
		return
	}

	var sendHB bool
	results := n.acks[ar.index]
	results[ar.peer] = struct{}{}
	if nr := len(results); nr >= n.qn {
		n.apply(ar.index)
		sendHB = len(n.propc) == 0
	}
	n.Unlock()

	if sendHB {
		n.debug("Nothing waiting so sending HB for commit update now!\n")
		n.sendHeartbeat()
	}
}

func (n *raft) trackPeer(peer string) {
	n.Lock()
	n.peers[peer] = time.Now().UnixNano()
	n.Unlock()
}

func (n *raft) runAsCandidate() {
	n.debug("CANDIDATE!\n")
	// TODO(dlc) - drain responses?

	// Send out for votes.
	n.requestVote()

	// We vote for ourselves.
	votes := 1

	for {
		select {
		case <-n.s.quitCh:
			return
		case <-n.quit:
			return
		case <-n.elect.C:
			n.switchToCandidate()
			return
		case vresp := <-n.votes:
			n.debug("VoteResponse is %+v\n", vresp)
			if vresp.granted && n.term >= vresp.term {
				votes++
				n.debug("Num Votes is now %d\n", votes)
				if n.wonElection(votes) {
					// Become LEADER if we have won.
					n.switchToLeader()
					return
				}
			}
		case vreq := <-n.reqs:
			if err := n.processVoteRequest(vreq); err != nil {
				n.switchToFollower(noLeader)
				return
			}
		case newLeader := <-n.stepdown:
			n.switchToFollower(newLeader)
			return
		}
	}
}

const appendEntryBaseLen = idLen + 4*8 + 2

func (ae *appendEntry) encode() []byte {
	var elen int
	for _, e := range ae.entries {
		elen += len(e) + 4 // 4 is for size.
	}
	var le = binary.LittleEndian
	buf := make([]byte, appendEntryBaseLen+elen)
	copy(buf[:idLen], ae.leader)
	le.PutUint64(buf[8:], ae.term)
	le.PutUint64(buf[16:], ae.commit)
	le.PutUint64(buf[24:], ae.prevTerm)
	le.PutUint64(buf[32:], ae.prevIndex)
	le.PutUint16(buf[40:], uint16(len(ae.entries)))
	wi := 42
	for _, b := range ae.entries {
		le.PutUint32(buf[wi:], uint32(len(b)))
		wi += 4
		copy(buf[wi:], b)
		wi += len(b)
	}
	return buf[:wi]
}

// This can not be used post the wire level callback since we do not copy.
func (n *raft) decodeAppendEntry(msg []byte, reply string) *appendEntry {
	if len(msg) < appendEntryBaseLen {
		return nil
	}

	var le = binary.LittleEndian
	ae := &appendEntry{
		leader:    string(msg[:idLen]),
		term:      le.Uint64(msg[8:]),
		commit:    le.Uint64(msg[16:]),
		prevTerm:  le.Uint64(msg[24:]),
		prevIndex: le.Uint64(msg[32:]),
	}
	// Decode Entries.
	ne, ri := int(le.Uint16(msg[40:])), 42
	for i := 0; i < ne; i++ {
		le := int(le.Uint32(msg[ri:]))
		ri += 4
		ae.entries = append(ae.entries, msg[ri:ri+le])
		ri += int(le)
	}
	ae.reply = reply
	ae.buf = msg
	return ae
}

// handleAppendEntry handles and append entry from the wire. We can't rely on msg being available
// past this callback so will do a bunch of procesing here to avoid copies, channels etc.
func (n *raft) handleAppendEntry(sub *subscription, c *client, subject, reply string, msg []byte) {
	ae := n.decodeAppendEntry(msg, reply)
	n.Lock()

	n.debug("Processing AE INLINE %+v\n", ae)

	// Preprocessing based on current state.
	switch n.state {
	case Follower:
		if n.leader != ae.leader {
			n.leader = ae.leader
			n.vote = noVote
		}
	case Leader:
		if ae.term > n.term {
			n.Unlock()
			n.stepdown <- ae.leader
			return
		}
	case Candidate:
		if ae.term >= n.term {
			n.term = ae.term
			n.vote = noVote
			n.writeTermVote()
			n.Unlock()
			n.stepdown <- ae.leader
			return
		}
	}

	// More generic processing here.

	// Ignore old terms.
	if ae.term < n.term {
		n.Unlock()
		ar := appendEntryResponse{n.term, n.pindex, n.id, false}
		n.sendRPC(ae.reply, _EMPTY_, ar.encode())
		return
	}

	// Reset the election timer.
	n.resetElectionTimeout()

	// If this term is greater than ours.
	if ae.term > n.term {
		n.term = ae.term
		n.vote = noVote
		n.writeTermVote()
	}

	// TODO(dlc) - Do both catchup and delete new behaviors frm spec.
	if ae.prevTerm != n.pterm || ae.prevIndex != n.pindex {
		n.debug("DID NOT MATCH %d %d with %d %d\n", ae.prevTerm, ae.prevIndex, n.pterm, n.pindex)
		n.Unlock()
		return
	}

	// Save to our WAL if we have entries.
	if len(ae.entries) > 0 {
		if err := n.storeToWAL(msg); err != nil {
			panic("Error storing!\n")
		}
	}

	// Apply anytyhing we need here.
	if ae.commit > n.commit {
		for index := n.commit + 1; index <= ae.commit; index++ {
			n.apply(index)
		}
	}

	// Success.
	ar := appendEntryResponse{n.term, n.pindex, n.id, true}
	n.Unlock()

	// Send our response.
	n.sendRPC(ae.reply, _EMPTY_, ar.encode())
}

// handleAppendEntryResponse just places the decoded response on the appropriate channel.
func (n *raft) handleAppendEntryResponse(sub *subscription, c *client, subject, reply string, msg []byte) {
	select {
	case n.resp <- n.decodeAppendEntryResponse(msg):
	default:
		n.s.Errorf("Failed to place add entry response on chan for %q", n.group)
	}
}

func (n *raft) buildAppendEntry(entries [][]byte) *appendEntry {
	return &appendEntry{n.id, n.term, n.commit, n.pterm, n.pindex, entries, _EMPTY_, nil}
}

// lock should be held.
func (n *raft) storeToWAL(buf []byte) error {
	seq, _, err := n.wal.StoreMsg(_EMPTY_, nil, buf)
	if err != nil {
		return err
	}
	n.pterm = n.term
	n.pindex = seq
	return nil
}

func (n *raft) sendAppendEntry(entries [][]byte) {
	n.Lock()
	defer n.Unlock()
	buf := n.buildAppendEntry(entries).encode()
	// If we have entries store this in our wal.
	if len(entries) > 0 {
		if err := n.storeToWAL(buf); err != nil {
			panic("Error storing!\n")
		}
		// We count ourselves.
		n.acks[n.pindex] = map[string]struct{}{n.id: struct{}{}}
		n.debug("Placing ourselves in acks: %+v\n", n.acks)
	}
	n.sendRPC(n.asubj, n.areply, buf)
}

func (n *raft) sendHeartbeat() {
	n.debug("Sending HB\n")
	n.sendAppendEntry(nil)
}

type voteRequest struct {
	term      uint64
	lastTerm  uint64
	lastIndex uint64
	candidate string
	// internal only.
	reply string
}

const voteRequestLen = 24 + idLen

func (vr *voteRequest) encode() []byte {
	var buf [voteRequestLen]byte
	var le = binary.LittleEndian
	le.PutUint64(buf[0:], vr.term)
	le.PutUint64(buf[8:], vr.lastTerm)
	le.PutUint64(buf[16:], vr.lastIndex)
	copy(buf[24:24+idLen], vr.candidate)

	return buf[:voteRequestLen]
}

func (n *raft) decodeVoteRequest(msg []byte, reply string) *voteRequest {
	if len(msg) != voteRequestLen {
		return nil
	}
	// Need to copy for now b/c of candidate.
	msg = append(msg[:0:0], msg...)

	var le = binary.LittleEndian
	return &voteRequest{
		term:      le.Uint64(msg[0:]),
		lastTerm:  le.Uint64(msg[8:]),
		lastIndex: le.Uint64(msg[16:]),
		candidate: string(msg[24 : 24+idLen]),
		reply:     reply,
	}
}

const termVoteFile = "tav.dat"
const termVoteLen = idLen + 8

// readTermVote will read the largest term and who we voted from to stable storage.
// Lock should be held.
func (n *raft) readTermVote() (term uint64, voted string, err error) {
	buf, err := ioutil.ReadFile(path.Join(n.sd, termVoteFile))
	if err != nil {
		return 0, noVote, err
	}
	if len(buf) < termVoteLen {
		return 0, noVote, nil
	}
	var le = binary.LittleEndian
	term = le.Uint64(buf[0:])
	voted = string(buf[8:])
	return term, voted, nil
}

// writeTermVote will record the largest term and who we voted for to stable storage.
// Lock should be held.
func (n *raft) writeTermVote() error {
	tvf := path.Join(n.sd, termVoteFile)
	if _, err := os.Stat(tvf); err != nil && !os.IsNotExist(err) {
		return err
	}

	var buf [termVoteLen]byte
	var le = binary.LittleEndian
	le.PutUint64(buf[0:], n.term)
	copy(buf[8:], n.vote)
	if err := ioutil.WriteFile(tvf, buf[:], 0644); err != nil {
		return err
	}
	return nil
}

// voteResponse is a response to a vote request.
type voteResponse struct {
	term    uint64
	granted bool
}

const voteResponseLen = 8 + 1

func (vr *voteResponse) encode() []byte {
	var buf [voteResponseLen]byte
	var le = binary.LittleEndian
	le.PutUint64(buf[0:], vr.term)
	if vr.granted {
		buf[8] = 1
	} else {
		buf[8] = 0
	}
	return buf[:voteResponseLen]
}

func (n *raft) decodeVoteResponse(msg []byte) *voteResponse {
	if len(msg) != voteResponseLen {
		return nil
	}
	var le = binary.LittleEndian
	vr := &voteResponse{term: le.Uint64(msg[0:])}
	vr.granted = msg[8] == 1
	return vr
}

func (n *raft) handleVoteResponse(sub *subscription, c *client, _, reply string, msg []byte) {
	vr := n.decodeVoteResponse(msg)
	if vr == nil {
		n.s.Errorf("Received malformed vote response for %q", n.group)
		return
	}
	select {
	case n.votes <- vr:
	default:
		n.s.Errorf("Failed to place vote response on chan for %q", n.group)
	}
}

var errShouldStepDown = errors.New("raft: stepdown required")

func (n *raft) processVoteRequest(vr *voteRequest) error {
	vresp := voteResponse{n.term, false}
	var err error

	n.debug("Received a voteRequest %+v\n", vr)

	n.Lock()
	if vr.term >= n.term {
		// Only way we get to yes is through here.
		n.term = vr.term
		if n.pterm == vr.lastTerm && n.pindex == vr.lastIndex {
			if n.vote == noVote || n.vote == vr.candidate {
				vresp.granted = true
				n.vote = vr.candidate
			}
		}
		if !vresp.granted {
			err = errShouldStepDown
		}
	}
	// Save off our highest term and vote.
	n.writeTermVote()
	// Reset ElectionTimeout
	n.resetElectionTimeout()
	n.Unlock()

	n.debug("Responded to VoteRequest with %+v\n", vresp)

	n.sendReply(vr.reply, vresp.encode())
	return err
}

func (n *raft) handleVoteRequest(sub *subscription, c *client, subject, reply string, msg []byte) {
	vr := n.decodeVoteRequest(msg, reply)
	if vr == nil {
		n.s.Errorf("Received malformed vote request for %q", n.group)
		return
	}
	select {
	case n.reqs <- vr:
	default:
		n.s.Errorf("Failed to place vote request on chan for %q", n.group)
	}
}

func (n *raft) requestVote() {
	n.Lock()
	if n.state != Candidate {
		panic("raft requestVote not from candidate")
	}
	n.vote = n.id
	n.writeTermVote()
	vr := voteRequest{n.term, n.pterm, n.pindex, n.id, _EMPTY_}
	subj, reply := n.vsubj, n.vreply
	n.Unlock()

	// Now send it out.
	n.sendRPC(subj, reply, vr.encode())
}

func (n *raft) sendRPC(subject, reply string, msg []byte) {
	n.sendq <- &pubMsg{nil, subject, reply, nil, msg, false}
}

func (n *raft) sendReply(subject string, msg []byte) {
	n.sendq <- &pubMsg{nil, subject, _EMPTY_, nil, msg, false}
}

func (n *raft) wonElection(votes int) bool {
	return votes >= n.quorumNeeded()
}

// Return the quorum size for a given cluster config.
func (n *raft) quorumNeeded() int {
	n.RLock()
	qn := n.qn
	n.RUnlock()
	return qn
}

// Lock should be held.
func (n *raft) updateLeadChange(isLeader bool) {
	select {
	case n.leadc <- isLeader:
	case <-n.leadc:
		// We had an old value not consumed.
		select {
		case n.leadc <- isLeader:
		default:
			n.s.Errorf("Failed to post lead change to %v for %q", isLeader, n.group)
		}
	}
}

// Lock should be held.
func (n *raft) switchState(state RaftState) {
	if n.state == Closed {
		return
	}

	// Reset the election timer.
	n.resetElectionTimeout()

	if n.state == Leader && state != Leader {
		n.updateLeadChange(false)
	} else if state == Leader && n.state != Leader {
		n.updateLeadChange(true)
	}

	n.state = state
	n.vote = noVote
	n.writeTermVote()
}

const noLeader = _EMPTY_
const noVote = _EMPTY_

func (n *raft) switchToFollower(leader string) {
	n.debug("Switching to follower!\n")
	n.Lock()
	defer n.Unlock()
	n.leader = leader
	n.switchState(Follower)
}

func (n *raft) switchToCandidate() {
	n.debug("Switching to candidate!\n")
	n.Lock()
	defer n.Unlock()
	// Increment the term.
	n.term++
	// Clear current Leader.
	n.leader = noLeader
	n.resetElectionTimeout()
	n.switchState(Candidate)
}

func (n *raft) switchToLeader() {
	n.debug("Switching to leader!\n")
	n.Lock()
	defer n.Unlock()
	n.leader = n.id
	n.switchState(Leader)
}
