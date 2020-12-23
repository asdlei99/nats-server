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
	Snapshot(snap []byte) error
	Applied(index uint64)
	State() RaftState
	Leader() bool
	Current() bool
	GroupLeader() string
	StepDown() error
	Campaign() error
	ID() string
	Group() string
	Peers() []*Peer
	ProposeAddPeer(peer string) error
	ProposeRemovePeer(peer string) error
	ApplyC() <-chan *CommittedEntry
	LeadChangeC() <-chan bool
	QuitC() <-chan struct{}
	Stop()
}

type WAL interface {
	StoreMsg(subj string, hdr, msg []byte) (uint64, int64, error)
	LoadMsg(index uint64) (subj string, hdr, msg []byte, ts int64, err error)
	RemoveMsg(index uint64) (bool, error)
	Compact(index uint64) (uint64, error)
	State() StreamState
	Stop() error
}

type Peer struct {
	ID   string
	Last time.Time
	// FIXME(dlc) - Track last term and index too
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

	// Subjects for votes, updates, replays.
	vsubj  string
	vreply string
	asubj  string
	areply string

	// For when we need to catch up as a follower.
	catchup  *subscription
	progress map[string]chan uint64

	// Channels
	propc    chan *Entry
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

type RaftConfig struct {
	Name  string
	Store string
	Log   WAL
}

var (
	errProposalFailed = errors.New("raft: proposal failed")
	errNotLeader      = errors.New("raft: not leader")
	errNilCfg         = errors.New("raft: no config given")
	errUnknownPeer    = errors.New("raft: unknown peer")
	errCorruptPeers   = errors.New("raft: corrupt peer state")
	errStepdownFailed = errors.New("raft: stepdown failed")
)

// This will bootstrap a raftNode by writing its config into the store directory.
func (s *Server) bootstrapRaftNode(cfg *RaftConfig, knownPeers []string, allPeersKnown bool) error {
	if cfg == nil {
		return errNilCfg
	}
	// Check validity of peers if presented.
	for _, p := range knownPeers {
		if len(p) != idLen {
			return fmt.Errorf("raft: illegal peer: %q", p)
		}
	}
	expected := len(knownPeers)
	// We need to adjust this is all peers are not known.
	if !allPeersKnown {
		if expected < 2 {
			expected = 2
		}
		if ncr := s.configuredRoutes(); expected < ncr {
			expected = ncr
		}
	}

	return writePeerState(cfg.Store, &peerState{knownPeers, expected})
}

// startRaftNode will start the raft node.
func (s *Server) startRaftNode(cfg *RaftConfig) (RaftNode, error) {
	if cfg == nil {
		return nil, errNilCfg
	}
	s.mu.Lock()
	if s.sys == nil || s.sys.sendq == nil {
		s.mu.Unlock()
		return nil, ErrNoSysAccount
	}
	sendq := s.sys.sendq
	hash := s.sys.shash
	s.mu.Unlock()

	ps, err := readPeerState(cfg.Store)
	if err != nil {
		return nil, err
	}
	if ps == nil || ps.clusterSize < 2 {
		return nil, errors.New("raft: cluster too small")
	}
	n := &raft{
		id:       hash[:idLen],
		group:    cfg.Name,
		sd:       cfg.Store,
		wal:      cfg.Log,
		state:    Follower,
		elect:    time.NewTimer(randElectionTimeout()),
		csz:      ps.clusterSize,
		qn:       ps.clusterSize/2 + 1,
		hash:     hash,
		peers:    make(map[string]int64),
		acks:     make(map[uint64]map[string]struct{}),
		s:        s,
		c:        s.createInternalSystemClient(),
		sendq:    sendq,
		quit:     make(chan struct{}),
		reqs:     make(chan *voteRequest, 4),
		votes:    make(chan *voteResponse, 8),
		resp:     make(chan *appendEntryResponse, 64),
		propc:    make(chan *Entry, 8),
		applyc:   make(chan *CommittedEntry, 64),
		leadc:    make(chan bool, 4),
		stepdown: make(chan string),
	}

	if term, vote, err := n.readTermVote(); err != nil && term > 0 {
		n.term = term
		n.vote = vote
	}

	if state := n.wal.State(); state.Msgs > 0 {
		// TODO(dlc) - Recover our state here.
		if last, err := n.loadLastEntry(); err == nil {
			n.debug("Recovered %+v\n", state)
			n.debug("Loaded last ae from index %d: %+v\n", state.LastSeq, last)
			//n.pterm = last.term
			//n.pindex = state.LastSeq
		}
		// Replay the log.
		// Since doing this in place we need to make sure we have enough room on the applyc.
		if uint64(cap(n.applyc)) < state.Msgs {
			n.debug("Resizing the applyc to %d from %d\n", state.Msgs, cap(n.applyc))
			n.applyc = make(chan *CommittedEntry, state.Msgs)
		}
		for index := state.FirstSeq; index <= state.LastSeq; index++ {
			ae, err := n.loadEntry(index)
			if err != nil {
				n.debug("Could not load WAL entry %d on startup!\n")
				panic("err loading index")
			}
			n.debug("Processing %d on startup\n", index)
			n.processAppendEntry(ae, nil)
		}
	}

	if err := n.createInternalSubs(); err != nil {
		n.shutdown()
		return nil, err
	}

	// Make sure to track ourselves.
	n.trackPeer(n.id)
	// Track known peers
	for _, peer := range ps.knownPeers {
		n.trackPeer(peer)
	}

	s.registerRaftNode(n.group, n)
	s.startGoRoutine(n.run)

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

func (s *Server) shutdownRaftNodes() {
	var nodes []RaftNode
	s.rnMu.RLock()
	for _, n := range s.raftNodes {
		nodes = append(nodes, n)
	}
	s.rnMu.RUnlock()
	for _, node := range nodes {
		if node.Leader() {
			node.StepDown()
		}
		node.Stop()
	}
}

// Formal API

// Propose will propose a new entry to the group.
// This should only be called on the leader.
func (n *raft) Propose(data []byte) error {
	n.RLock()
	if n.state != Leader {
		n.RUnlock()
		return errNotLeader
	}
	propc := n.propc
	n.RUnlock()

	n.debug("PROPOSE CALLED\n")

	select {
	case propc <- &Entry{EntryNormal, data}:
	default:
		return errProposalFailed
	}
	return nil
}

// ProposeAddPeer is called to add a peer to the group.
func (n *raft) ProposeAddPeer(peer string) error {
	n.RLock()
	if n.state != Leader {
		n.RUnlock()
		return errNotLeader
	}
	propc := n.propc
	n.RUnlock()

	n.debug("ProposeAddPeer called for %q!\n", peer)

	select {
	case propc <- &Entry{EntryAddPeer, []byte(peer)}:
	default:
		return errProposalFailed
	}
	return nil
}

// ProposeRemovePeer is called to remove a peer from the group.
func (n *raft) ProposeRemovePeer(peer string) error {
	return errors.New("No Impl")
}

// Applied is to be called when the FSM has applied the committed entries.
func (n *raft) Applied(index uint64) {
	n.Lock()
	// FIXME(dlc) - Check spec on error conditions, storage
	n.debug("Updating applied to %d!\n", index)
	n.applied = index
	// FIXME(dlc) - Can be more efficient here.
	if ae, err := n.loadEntry(index); ae != nil && err == nil {
		// Check to see if we have a snapshot here.
		// Snapshots will be by themselves but we range anyway.
		for _, e := range ae.entries {
			if e.Type == EntrySnapshot {
				n.debug("Found snapshot entry: compacting log to index %d\n", index)
				n.wal.Compact(index)
			}
		}
	}
	n.Unlock()
}

// Snapshot is used to snapshot the fsm. This can only be called from a leader.
// For now these are assumed to be small and will be placed into the log itself.
// TODO(dlc) - For meta and consumers this is straightforward, and for streams sans the messages this is as well.
func (n *raft) Snapshot(snap []byte) error {
	n.RLock()
	if n.state != Leader {
		n.RUnlock()
		return errNotLeader
	}
	propc := n.propc
	n.RUnlock()

	n.debug("SNAPSHOT called with %d bytes, applied is %d!\n", len(snap), n.applied)

	select {
	case propc <- &Entry{EntrySnapshot, snap}:
	default:
		return errProposalFailed
	}

	return nil
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

// Current returns if we are the leader for our group or an up to date follower.
func (n *raft) Current() bool {
	if n == nil {
		return false
	}
	n.RLock()
	isCurrent := n.state == Leader
	if !isCurrent && n.catchup == nil {
		const okInterval = int64(hbInterval) * 2
		// We are not in a catchup state, check last time we heard from the leader.
		// If within a hbInterval*2 consider us good.
		ts := time.Now().UnixNano()
		if lts := n.peers[n.leader]; lts > 0 && (ts-lts) <= okInterval {
			isCurrent = true
		}
	}
	n.RUnlock()
	return isCurrent
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

// StepDown will have a leader stepdown and optionally do a leader transfer.
func (n *raft) StepDown() error {
	n.Lock()

	if n.state != Leader {
		n.Unlock()
		return errNotLeader
	}

	n.debug("Being asked to setpdown!\n")
	// See if we have up to date followers.
	nowts := time.Now().UnixNano()
	maybeLeader := noLeader
	for peer, ts := range n.peers {
		// If not us and alive and caughtup.
		if peer != n.id && (nowts-ts) < int64(hbInterval*2) {
			if n.s.getRouteByHash([]byte(peer)) != nil {
				n.debug("Looking at %q which is %v behind\n", peer, time.Duration(nowts-ts))
				maybeLeader = peer
				break
			}
		}
	}
	stepdown := n.stepdown
	n.Unlock()

	if maybeLeader != noLeader {
		n.debug("Stepping down, selected %q\n", maybeLeader)
		n.sendAppendEntry([]*Entry{&Entry{EntryLeaderTransfer, []byte(maybeLeader)}})
	} else {
		// Force us to stepdown here.
		select {
		case stepdown <- noLeader:
		default:
			return errStepdownFailed
		}
	}
	return nil
}

// Campaign will have our node start a leadership vote.
func (n *raft) Campaign() error {
	n.Lock()
	defer n.Unlock()
	return n.campaign()
}

// Campaign will have our node start a leadership vote.
// Lock should be held.
func (n *raft) campaign() error {
	elect := n.elect
	n.elect = nil
	elect.Reset(0)
	return nil
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
	s, g, wal := n.s, n.group, n.wal
	n.Unlock()
	s.unregisterRaftNode(g)
	n.debug("RAFT shutdown called!\n")
	if wal != nil {
		n.debug("Calling stop on WAL!\n")
		wal.Stop()
	}
}

func (n *raft) newInbox(cn string) string {
	var b [replySuffixLen]byte
	rn := rand.Int63()
	for i, l := 0, rn; i < len(b); i++ {
		b[i] = digits[l%base]
		l /= base
	}
	return fmt.Sprintf(raftReplySubj, n.group, n.hash, b[:])
}

const (
	raftVoteSubj   = "$SYS.NRG.%s.%s.V"
	raftAppendSubj = "$SYS.NRG.%s.%s.A"
	raftReplySubj  = "$SYS.NRG.%s.%s.%s"
)

func (n *raft) createInternalSubs() error {
	cn := n.s.ClusterName()
	n.vsubj, n.vreply = fmt.Sprintf(raftVoteSubj, cn, n.group), n.newInbox(cn)
	n.asubj, n.areply = fmt.Sprintf(raftAppendSubj, cn, n.group), n.newInbox(cn)

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
	//nf := fmt.Sprintf("%v [%s:%s  %s]\t%s", time.Now(), n.s.Name(), n.id, n.group, format)
	nf := fmt.Sprintf("[%s:%s  %s]\t%s", n.s, n.id, n.group, format)
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
			n.debug("Got an new leader as a follower! %v\n")
			n.switchToFollower(newLeader)
			return
		}
	}
}

// CommitEntry is handed back to the user to apply a commit to their FSM.
type CommittedEntry struct {
	Index   uint64
	Entries []*Entry
}

type appendEntry struct {
	leader  string
	term    uint64
	commit  uint64
	pterm   uint64
	pindex  uint64
	entries []*Entry
	// internal use only.
	reply string
	buf   []byte
}

type EntryType uint8

const (
	EntryNormal EntryType = iota
	EntrySnapshot
	EntryPeerState
	EntryAddPeer
	EntryRemovePeer
	EntryLeaderTransfer
)

type Entry struct {
	Type EntryType
	Data []byte
}

func (ae *appendEntry) String() string {
	return fmt.Sprintf("&{leader:%s term:%d commit:%d pterm:%d pindex:%d entries: %d}",
		ae.leader, ae.term, ae.term, ae.pterm, ae.pindex, len(ae.entries))
}

const appendEntryBaseLen = idLen + 4*8 + 2

func (ae *appendEntry) encode() []byte {
	var elen int
	for _, e := range ae.entries {
		elen += len(e.Data) + 1 + 4 // 1 is type, 4 is for size.
	}
	var le = binary.LittleEndian
	buf := make([]byte, appendEntryBaseLen+elen)
	copy(buf[:idLen], ae.leader)
	le.PutUint64(buf[8:], ae.term)
	le.PutUint64(buf[16:], ae.commit)
	le.PutUint64(buf[24:], ae.pterm)
	le.PutUint64(buf[32:], ae.pindex)
	le.PutUint16(buf[40:], uint16(len(ae.entries)))
	wi := 42
	for _, e := range ae.entries {
		le.PutUint32(buf[wi:], uint32(len(e.Data)+1))
		wi += 4
		buf[wi] = byte(e.Type)
		wi++
		copy(buf[wi:], e.Data)
		wi += len(e.Data)
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
		leader: string(msg[:idLen]),
		term:   le.Uint64(msg[8:]),
		commit: le.Uint64(msg[16:]),
		pterm:  le.Uint64(msg[24:]),
		pindex: le.Uint64(msg[32:]),
	}
	// Decode Entries.
	ne, ri := int(le.Uint16(msg[40:])), 42
	for i := 0; i < ne; i++ {
		le := int(le.Uint32(msg[ri:]))
		ri += 4
		etype := EntryType(msg[ri])
		ri++
		ae.entries = append(ae.entries, &Entry{etype, msg[ri : ri+le-1]})
		ri += int(le)
	}
	ae.reply = reply
	ae.buf = msg
	return ae
}

// appendEntryResponse is our response to a received appendEntry.
type appendEntryResponse struct {
	term    uint64
	index   uint64
	peer    string
	success bool
	// internal
	reply string
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
	n.debug("Commit is %d\n", n.commit)

	n.sendPeerState()

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
			entries := []*Entry{b}
			if b.Type == EntryNormal {
			gather:
				// Gather more if available, limit to 3 atm. TODO(dlc)
				for x := 0; x < 3; x++ {
					select {
					case e := <-n.propc:
						entries = append(entries, e)
					default:
						break gather
					}
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
			n.trackPeer(vresp.peer)
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
			n.debug("Got an AE Response! %+v\n", ar)
			n.trackPeer(ar.peer)
			if ar.success {
				n.trackResponse(ar)
			} else if ar.reply != _EMPTY_ {
				n.catchupFollower(ar)
			}
		}
	}
}

// Lock should be held.
func (n *raft) loadLastEntry() (ae *appendEntry, err error) {
	return n.loadEntry(n.wal.State().LastSeq)
}

// Lock should be held.
func (n *raft) loadFirstEntry() (ae *appendEntry, err error) {
	return n.loadEntry(n.wal.State().FirstSeq)
}

func (n *raft) runCatchup(peer, subj string, indexUpdatesC <-chan uint64) {
	n.RLock()
	s := n.s
	reply := n.areply
	n.RUnlock()

	defer s.grWG.Done()

	defer func() {
		n.Lock()
		delete(n.progress, peer)
		if len(n.progress) == 0 {
			n.progress = nil
		}
		// Check if this is a new peer and if so go ahead and propose adding them.
		_, ok := n.peers[peer]
		n.Unlock()
		if !ok {
			n.debug("Cacthup done for %q, is a new peer so will add!\n", peer)
			n.ProposeAddPeer(peer)
		}
	}()

	n.debug("Running catchup for %q\n", peer)

	const maxOutstanding = 48 * 1024 * 1024 // 48MB for now.
	next, total, om := uint64(0), 0, make(map[uint64]int)

	sendNext := func() {
		for total <= maxOutstanding {
			next++
			n.debug("CATCHUP Loading %d index\n", next)
			ae, err := n.loadEntry(next)
			if err != nil {
				if err != ErrStoreEOF {
					n.debug("Got an error loading %d index! %v\n", next, err)
				}
				return
			}
			// Update our tracking total.
			om[next] = len(ae.buf)
			total += len(ae.buf)
			n.debug("Should send %+v\n", ae)
			n.debug("Total %d, om is %+v\n", total, om)
			n.sendRPC(subj, reply, ae.buf)
		}
		n.debug("We are full sending catchup entries\n")
	}

	const activityInterval = 500 * time.Millisecond
	timeout := time.NewTicker(activityInterval)
	defer timeout.Stop()

	// Run as long as we are leader and still not caught up.
	for n.Leader() {
		select {
		case <-n.s.quitCh:
			return
		case <-n.quit:
			return
		case <-timeout.C:
			n.debug("Catching up for %q stalled\n", peer)
			return
		case index := <-indexUpdatesC:
			// Update our activity timer.
			timeout.Reset(activityInterval)
			// Update outstanding total.
			total -= om[index]
			delete(om, index)
			n.RLock()
			finished := index >= n.pindex
			n.RUnlock()
			n.debug("Finished: %v, Total %d, om is %+v\n", finished, total, om)
			// Check if we are done.
			if finished {
				return
			}
			// Still have more catching up to do.
			if next < index {
				n.debug("Adjusting next to %d from %d\n", index, next)
				next = index
			}
			sendNext()
		}
	}
}

func (n *raft) catchupFollower(ar *appendEntryResponse) {
	n.debug("Being asked to catch up follower: %q\n", ar.peer)
	n.RLock()
	if n.progress == nil {
		n.progress = make(map[string]chan uint64)
	}
	if _, ok := n.progress[ar.peer]; ok {
		n.debug("Existing entry for catching up %q\n", ar.peer)
		n.RUnlock()
		return
	}
	ae, err := n.loadEntry(ar.index + 1)
	if err != nil {
		ae, err = n.loadFirstEntry()
	}
	if err != nil || ae == nil {
		n.debug("Could not find a starting entry for us! %v\n", err)
		return
	}
	if ae.pindex != ar.index || ae.pterm != ar.term {
		n.debug("Our first entry does not match where they are!\n")
	}
	// Create a chan for delivering updates from responses.
	indexUpdates := make(chan uint64, 128)
	indexUpdates <- ae.pindex
	n.progress[ar.peer] = indexUpdates
	n.RUnlock()

	n.s.startGoRoutine(func() { n.runCatchup(ar.peer, ar.reply, indexUpdates) })
}

func (n *raft) loadEntry(index uint64) (*appendEntry, error) {
	_, _, msg, _, err := n.wal.LoadMsg(index)
	if err != nil {
		return nil, err
	}
	return n.decodeAppendEntry(msg, _EMPTY_), nil
}

// Apply will update our commit index and apply the entry to the apply chan.
// lock should be held.
func (n *raft) apply(index uint64) {
	n.commit = index
	if n.state == Leader {
		delete(n.acks, index)
	}
	// FIXME(dlc) - Can keep this in memory if this too slow.
	ae, err := n.loadEntry(index)
	if err != nil {
		n.debug("Got an error loading %d index! %v\n", index, err)
		return
	}
	ae.buf = nil

	var committed []*Entry
	for _, e := range ae.entries {
		switch e.Type {
		case EntryNormal:
			committed = append(committed, e)
		case EntrySnapshot:
			n.debug("SNAPSHOT ENTRY TO PROCESS! %+v\n", ae)
			committed = append(committed, e)
		case EntryPeerState:
			if ps, err := decodePeerState(e.Data); err == nil {
				n.processPeerState(ps)
			} else {
				n.debug("Got an err!!! %v\n", err)
			}
		case EntryAddPeer:
			newPeer := string(e.Data)
			n.debug("COMMITTED NEW PEER! %q\n", newPeer)
			if _, ok := n.peers[newPeer]; !ok {
				// We are not tracking this one automatically so we need to bump cluster size.
				n.debug("Expanding the clustersize: %d -> %d\n", n.csz, n.csz+1)
				n.csz++
				n.qn = n.csz/2 + 1
				n.peers[newPeer] = time.Now().UnixNano()
			}
			writePeerState(n.sd, &peerState{n.peerNames(), n.csz})
		}
	}
	// Pass to the upper layers if we normal entries.
	if len(committed) > 0 {
		// We will block here placing the commit entry on purpose.
		n.applyc <- &CommittedEntry{index, committed}
	}
}

// Used to track a success response and apply entries.
func (n *raft) trackResponse(ar *appendEntryResponse) {
	n.Lock()

	// If we are tracking this peer as a catchup follower, update that here.
	if indexUpdateC := n.progress[ar.peer]; indexUpdateC != nil {
		n.debug("Trackresponse needs to update our catchup follower!\n")
		indexUpdateC <- ar.index + 1
	}

	// Ignore items already comitted.
	if ar.index <= n.commit {
		n.Unlock()
		return
	}

	// See if we have items to apply.
	var sendHB bool
	if results := n.acks[ar.index]; results != nil {
		results[ar.peer] = struct{}{}
		if nr := len(results); nr >= n.qn {
			// We have a quorum.
			// FIXME(dlc) - Make sure this is next in line.
			n.debug("We have consensus on %d, commit is %d!!\n", ar.index, n.commit)
			if ar.index == n.commit+1 {
				n.debug("Applying %d\n", ar.index)
				n.apply(ar.index)
				sendHB = len(n.propc) == 0
			} else {
				n.debug("\n\n############## CONSENSUS BUT CAN'T COMMIT!\n\n")
			}
		}
	}
	n.Unlock()

	if sendHB {
		n.debug("Nothing waiting so sending HB for commit update now!\n")
		n.sendHeartbeat()
	}
}

// Track interactions with this peer.
func (n *raft) trackPeer(peer string) error {
	n.Lock()
	var needPeerUpdate bool
	if n.state == Leader {
		if _, ok := n.peers[peer]; !ok {
			// This is someone new, if we have registered all of the peers already
			// this is an error.
			if len(n.peers) >= n.csz {
				n.Unlock()
				n.debug("Leader detected a new peer! %q\n", peer)
				return errUnknownPeer
			}
			n.debug("Adding new peer!! %q\n", peer)
			n.debug("csz is %d, peers is %+v\n", n.csz, n.peers)
			needPeerUpdate = true
		}
	}
	n.peers[peer] = time.Now().UnixNano()
	n.Unlock()

	if needPeerUpdate {
		n.sendPeerState()
	}
	return nil
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
			n.trackPeer(vresp.peer)
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

// handleAppendEntry handles an append entry from the wire. We can't rely on msg being available
// past this callback so will do a bunch of processing here to avoid copies, channels etc.
func (n *raft) handleAppendEntry(sub *subscription, c *client, subject, reply string, msg []byte) {
	ae := n.decodeAppendEntry(msg, reply)
	if ae == nil {
		n.debug("BAD AE received!")
		return
	}
	n.processAppendEntry(ae, sub)
}

// processAppendEntry will process an appendEntry.
func (n *raft) processAppendEntry(ae *appendEntry, sub *subscription) {
	n.Lock()

	isOriginal := sub != nil

	// Track leader directly
	if isOriginal && ae.leader != noLeader {
		n.peers[ae.leader] = time.Now().UnixNano()
	}

	n.debug("Processing AE INLINE %+v\n", ae)
	if n.catchup != nil && n.catchup == sub {
		n.debug("AE is catchup! %+v\n", ae)
	}

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
	ar := appendEntryResponse{n.pterm, n.pindex, n.id, false, _EMPTY_}

	// Ignore old terms.
	if ae.term < n.term {
		n.Unlock()
		n.debug("Ignoring old term!")
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

	catchingUp := n.catchup != nil

	// TODO(dlc) - Do both catchup and delete new behaviors from spec.
	if ae.pterm != n.pterm || ae.pindex != n.pindex {
		if catchingUp && n.catchup != sub {
			n.debug("Already catching up, ignoring\n")
			// FIXME(dlc) - Check for stalls when leader stops etc.
		} else {
			// Check if we are catching up and this is a snapshot, if so reset our wal's index.
			if catchingUp && len(ae.entries) > 0 && ae.entries[0].Type == EntrySnapshot {
				n.debug("Should reset index for wal to %d\n", ae.pindex+1)
				n.debug("Old wal state is %+v\n", n.wal.State())
				n.wal.Compact(ae.pindex + 1)
				n.pindex = ae.pindex
				n.commit = ae.pindex
				n.debug("New wal state is %+v\n", n.wal.State())
			} else {
				n.debug("DID NOT MATCH %d %d with %d %d\n", ae.pterm, ae.pindex, n.pterm, n.pindex)
				if n.catchup != nil {
					n.s.sysUnsubscribe(n.catchup)
				}
				inbox := n.newInbox(n.s.ClusterName())
				var err error
				if n.catchup, err = n.s.sysSubscribe(inbox, n.handleAppendEntry); err != nil {
					n.debug("Error subscribing to our inbox for catchup! %v\n", err)
					return
				}
				n.Unlock()
				n.sendRPC(ae.reply, inbox, ar.encode())
				return
			}
		}
	}

	// If we are here we are good, so if we have a catchup we can shut that down.
	if catchingUp && n.catchup != sub {
		n.debug("Cancelling catchup subscription since we are now up to date\n")
		n.s.sysUnsubscribe(n.catchup)
		n.catchup = nil
	}

	// Save to our WAL if we have entries.
	if len(ae.entries) > 0 {
		// Only store if an original which will have sub != nil
		if isOriginal {
			if err := n.storeToWAL(ae); err != nil {
				n.debug("Error storing to WAL: %v\n", err)
				if err == ErrStoreClosed {
					n.Unlock()
					return
				}
				panic("Error storing!\n")
			}
		} else {
			// This is a replay on startup so just take the appendEntry version.
			n.pterm = ae.term
			n.pindex = ae.pindex + 1
		}

		// Check to see if we have any peer related entries to process here.
		for _, e := range ae.entries {
			switch e.Type {
			case EntryLeaderTransfer:
				maybeLeader := string(e.Data)
				if maybeLeader == n.id {
					n.debug("Received transfer request for %q which is US!\n", e.Data)
					n.campaign()
				}
				// These will not have commits follow them.
				n.commit = ae.pindex + 1
			case EntryAddPeer:
				newPeer := string(e.Data)
				if len(newPeer) == idLen {
					// Track directly
					n.peers[newPeer] = time.Now().UnixNano()
				} else {
					n.debug("Bad add peer: %q\n", newPeer)
				}
			}
		}
	}

	// Apply anything we need here.
	if ae.commit > n.commit {
		for index := n.commit + 1; index <= ae.commit; index++ {
			n.debug("Apply commit for %d\n", index)
			n.apply(index)
		}
	}
	n.Unlock()

	// Success. Send our response.
	ar.success = true
	n.sendRPC(ae.reply, _EMPTY_, ar.encode())
}

// Lock should be held.
func (n *raft) processPeerState(ps *peerState) {
	// Update our version of peers to that of the leader.
	n.csz = ps.clusterSize
	n.peers = make(map[string]int64)
	ts := time.Now().UnixNano()
	for _, peer := range ps.knownPeers {
		n.peers[peer] = ts
	}
	n.debug("Update peers from leader to %+v\n", n.peers)
	writePeerState(n.sd, ps)
}

// handleAppendEntryResponse just places the decoded response on the appropriate channel.
func (n *raft) handleAppendEntryResponse(sub *subscription, c *client, subject, reply string, msg []byte) {
	aer := n.decodeAppendEntryResponse(msg)
	if reply != _EMPTY_ {
		aer.reply = reply
	}
	select {
	case n.resp <- aer:
	default:
		n.s.Errorf("Failed to place add entry response on chan for %q", n.group)
	}
}

func (n *raft) buildAppendEntry(entries []*Entry) *appendEntry {
	return &appendEntry{n.id, n.term, n.commit, n.pterm, n.pindex, entries, _EMPTY_, nil}
}

// lock should be held.
func (n *raft) storeToWAL(ae *appendEntry) error {
	if ae.buf == nil {
		panic("nil buffer for appendEntry!")
	}
	seq, _, err := n.wal.StoreMsg(_EMPTY_, nil, ae.buf)
	if err != nil {
		return err
	}
	n.debug("StoreToWAL called, term %d index %d - %q\n", n.term, seq, ae.buf[:32])
	n.debug("WAL state is %+v\n", n.wal.State())
	n.pterm = ae.term
	n.pindex = seq
	return nil
}

func (n *raft) sendAppendEntry(entries []*Entry) {
	n.Lock()
	defer n.Unlock()
	ae := n.buildAppendEntry(entries)
	ae.buf = ae.encode()
	// If we have entries store this in our wal.
	if len(entries) > 0 {
		if err := n.storeToWAL(ae); err != nil {
			panic("Error storing!\n")
		}
		// We count ourselves.
		n.acks[n.pindex] = map[string]struct{}{n.id: struct{}{}}
	}
	n.sendRPC(n.asubj, n.areply, ae.buf)
}

type peerState struct {
	knownPeers  []string
	clusterSize int
}

func encodePeerState(ps *peerState) []byte {
	var le = binary.LittleEndian
	buf := make([]byte, 4+4+(8*len(ps.knownPeers)))
	le.PutUint32(buf[0:], uint32(ps.clusterSize))
	le.PutUint32(buf[4:], uint32(len(ps.knownPeers)))
	wi := 8
	for _, peer := range ps.knownPeers {
		copy(buf[wi:], peer)
		wi += idLen
	}
	return buf
}

func decodePeerState(buf []byte) (*peerState, error) {
	if len(buf) < 8 {
		return nil, errCorruptPeers
	}
	var le = binary.LittleEndian
	ps := &peerState{clusterSize: int(le.Uint32(buf[0:]))}
	expectedPeers := int(le.Uint32(buf[4:]))
	buf = buf[8:]
	for i, ri, n := 0, 0, expectedPeers; i < n && ri < len(buf); i++ {
		ps.knownPeers = append(ps.knownPeers, string(buf[ri:ri+idLen]))
		ri += idLen
	}
	if len(ps.knownPeers) != expectedPeers {
		return nil, errCorruptPeers
	}
	return ps, nil
}

// Lock should be held.
func (n *raft) peerNames() []string {
	var peers []string
	for peer := range n.peers {
		peers = append(peers, peer)
	}
	return peers
}

func (n *raft) currentPeerState() *peerState {
	n.RLock()
	ps := &peerState{n.peerNames(), n.csz}
	n.RUnlock()
	return ps
}

// sendPeerState will send our current peer state to the cluster.
func (n *raft) sendPeerState() {
	n.debug("SEND PEER STATE!\n")
	n.sendAppendEntry([]*Entry{&Entry{EntryPeerState, encodePeerState(n.currentPeerState())}})
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

const peerStateFile = "peers.idx"

// Writes out our peer state.
func writePeerState(sd string, ps *peerState) error {
	psf := path.Join(sd, peerStateFile)
	if _, err := os.Stat(psf); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := ioutil.WriteFile(psf, encodePeerState(ps), 0644); err != nil {
		return err
	}
	return nil
}

func readPeerState(sd string) (ps *peerState, err error) {
	buf, err := ioutil.ReadFile(path.Join(sd, peerStateFile))
	if err != nil {
		return nil, err
	}
	return decodePeerState(buf)
}

const termVoteFile = "tav.idx"
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
	// FIXME(dlc) - NoVote
	copy(buf[8:], n.vote)
	if err := ioutil.WriteFile(tvf, buf[:8+len(n.vote)], 0644); err != nil {
		return err
	}
	return nil
}

// voteResponse is a response to a vote request.
type voteResponse struct {
	term    uint64
	peer    string
	granted bool
}

const voteResponseLen = 8 + 8 + 1

func (vr *voteResponse) encode() []byte {
	var buf [voteResponseLen]byte
	var le = binary.LittleEndian
	le.PutUint64(buf[0:], vr.term)
	copy(buf[8:], vr.peer)
	if vr.granted {
		buf[16] = 1
	} else {
		buf[16] = 0
	}
	return buf[:voteResponseLen]
}

func (n *raft) decodeVoteResponse(msg []byte) *voteResponse {
	if len(msg) != voteResponseLen {
		return nil
	}
	var le = binary.LittleEndian
	vr := &voteResponse{term: le.Uint64(msg[0:]), peer: string(msg[8:16])}
	vr.granted = msg[16] == 1
	return vr
}

func (n *raft) handleVoteResponse(sub *subscription, c *client, _, reply string, msg []byte) {
	vr := n.decodeVoteResponse(msg)
	n.debug("Received a voteResponse %+v\n", vr)
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
	vresp := voteResponse{n.term, n.id, false}
	var err error

	n.debug("Received a voteRequest %+v\n", vr)

	if err := n.trackPeer(vr.candidate); err != nil {
		n.debug("Got err tracking %q: %v\n", vr.candidate, err)
		n.sendReply(vr.reply, vresp.encode())
		return err
	}

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
