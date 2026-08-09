// Package broker is the control plane that lets pachd place pipeline
// workers on remote nodes. It is a tiny broker between the pachd daemon
// (which runs the PPS master and decides how many workers each pipeline
// needs) and one light agent per node (which spawns and kills worker
// processes on command).
//
// It is deliberately NOT a second pachd and NOT a distributed datastore:
//
//   - The data plane is unchanged. Workers already talk to pachd over gRPC
//     (the object API for datum bytes, etcd for task claims), so a remote
//     worker needs no local access to the daemon's disk.
//   - Mutual exclusion on datum processing is already handled by etcd task
//     claims (see src/server/pkg/work): two workers physically cannot claim
//     the same datum. The broker only decides WHICH NODE runs a worker
//     process, and there is exactly one decision-maker (pachd, serialized
//     under the localclient runtime's mutex).
//   - The registry is ephemeral by design. Nodes re-register on startup and
//     are forgotten after a heartbeat timeout; a missing node never blocks
//     the DAG (its workers' datums are re-claimed from etcd and re-run
//     elsewhere).
//
// Protocol: length-prefixed JSON over TCP (4-byte big-endian length + one
// JSON object per message). Both ends are our binaries; the register message
// carries a protocol version so the wire format can evolve. Agents hold a
// long-lived connection: they send heartbeats (which double as worker-status
// reports), the broker pushes spawn/kill commands, and the agent acks.
//
// Concurrency: Server.mu protects the node/worker maps and is only ever held
// for map operations and channel enqueues. Outbound commands go through a
// buffered channel drained by a per-node writer goroutine, so a wedged agent
// can never block the PPS master. The worker-exit callback is invoked by a
// dedicated drain goroutine, never while Server.mu is held.
package broker

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/pachyderm/pachyderm/src/client/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// ProtocolVersion is the wire format version agents must present to
// register. Bump it when the message shapes change.
const ProtocolVersion = 1

// HeartbeatInterval is how often agents report in, and WatchdogInterval how
// often the broker checks for stale nodes. NodeTimeout is how long without a
// heartbeat before a node (and its workers) are considered dead.
const (
	HeartbeatInterval = 5 * time.Second
	WatchdogInterval  = 5 * time.Second
	NodeTimeout       = 15 * time.Second
)

// ErrNoNode is returned by Server.Place when no registered node serves the
// requested tag.
var ErrNoNode = errors.New("broker: no node registered with the requested tag")

// ErrBusy is returned by Server.Place when the node's command queue is full
// (the node is not keeping up with spawn commands).
var ErrBusy = errors.New("broker: node command queue full")

//////////////////////////////////////////////////////////////////////////////
// Wire messages (length-prefixed JSON, discriminated by "type")
//////////////////////////////////////////////////////////////////////////////

type registerMsg struct {
	Type     string   `json:"type"`     // "register"
	Version  int      `json:"version"`  // ProtocolVersion
	Hostname string   `json:"hostname"` // agent's host name (informational)
	Tags     []string `json:"tags"`     // node tags this agent serves
}

type welcomeMsg struct {
	Type        string `json:"type"`         // "welcome"
	NodeID      string `json:"node_id"`      // assigned node ID
	HeartbeatMS int    `json:"heartbeat_ms"` // suggested heartbeat interval
}

type spawnMsg struct {
	Type     string   `json:"type"`              // "spawn"
	CmdID    string   `json:"cmd_id"`            // unique per command (for acks)
	WorkerID string   `json:"worker_id"`         // stable worker identifier (pod name)
	Env      []string `json:"env"`               // resolved environment, K=V pairs
	Image    string   `json:"image,omitempty"`   // pipeline image (docker runtime)
	Runtime  string   `json:"runtime,omitempty"` // "docker" or "process"
}

type killMsg struct {
	Type     string `json:"type"` // "kill"
	CmdID    string `json:"cmd_id"`
	WorkerID string `json:"worker_id"`
}

type ackMsg struct {
	Type     string `json:"type"` // "ack"
	CmdID    string `json:"cmd_id"`
	WorkerID string `json:"worker_id"`
	Error    string `json:"error,omitempty"`
}

// WorkerState is one worker's status as reported by the agent.
type WorkerState struct {
	WorkerID string `json:"worker_id"`
	Running  bool   `json:"running"`
	Error    string `json:"error,omitempty"` // exit error when Running is false
}

type heartbeatMsg struct {
	Type    string        `json:"type"` // "heartbeat"
	NodeID  string        `json:"node_id"`
	Workers []WorkerState `json:"workers"`
}

// Spawn is a decoded spawn command as delivered to the agent.
type Spawn struct {
	CmdID    string
	WorkerID string
	Env      []string
	Image    string
	Runtime  string
}

// Kill is a decoded kill command as delivered to the agent.
type Kill struct {
	CmdID    string
	WorkerID string
}

// readMsg reads one length-prefixed JSON message into 'v'.
func readMsg(r io.Reader, v interface{}) error {
	var n uint32
	if err := binary.Read(r, binary.BigEndian, &n); err != nil {
		return err
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return err
	}
	return json.Unmarshal(buf, v)
}

// writeMsg writes one length-prefixed JSON message.
func writeMsg(w io.Writer, v interface{}) error {
	buf, err := json.Marshal(v)
	if err != nil {
		return err
	}
	var n [4]byte
	binary.BigEndian.PutUint32(n[:], uint32(len(buf)))
	if _, err := w.Write(n[:]); err != nil {
		return err
	}
	_, err = w.Write(buf)
	return err
}

// readEnvelope reads the message type of the next message, returning the
// type string and the raw payload for a typed decode.
func readEnvelope(r io.Reader) (string, []byte, error) {
	var n uint32
	if err := binary.Read(r, binary.BigEndian, &n); err != nil {
		return "", nil, err
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", nil, err
	}
	var env struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(buf, &env); err != nil {
		return "", nil, errors.Wrapf(err, "broker: malformed message")
	}
	return env.Type, buf, nil
}

//////////////////////////////////////////////////////////////////////////////
// Server
//////////////////////////////////////////////////////////////////////////////

// ExitHandler is called (from a dedicated goroutine, never while Server.mu
// is held) for every worker whose node goes away or reports it stopped.
type ExitHandler func(workerID string, exitErr error)

type node struct {
	id       string
	hostname string
	tags     []string
	lastSeen time.Time
	sendCh   chan interface{} // outbound commands; drained by writer goroutine
	workers  map[string]bool
}

// Server is the broker's pachd-side half: a registry of agents plus command
// fan-out. Place/Kill are called by the localclient runtime.
type Server struct {
	mu      sync.Mutex
	nodes   map[string]*node
	workers map[string]string // workerID -> nodeID
	conns   map[*net.Conn]bool
	exitCb  ExitHandler // guarded by s.mu

	exitCh chan workerExit
	closed chan struct{}
	once   sync.Once

	ln net.Listener
}

type workerExit struct {
	workerID string
	err      error
}

// Listen starts the broker on addr and begins accepting agents. The returned
// Server is ready to use; call Close to shut it down.
func Listen(addr string) (*Server, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, errors.Wrapf(err, "broker: could not listen on %s", addr)
	}
	s := &Server{
		nodes:   make(map[string]*node),
		workers: make(map[string]string),
		conns:   make(map[*net.Conn]bool),
		exitCh:  make(chan workerExit, 1024),
		closed:  make(chan struct{}),
		ln:      ln,
	}
	go s.acceptLoop()
	go s.watchdog()
	go s.drain()
	log.Infof("broker: listening on %s", addr)
	return s, nil
}

// Addr returns the address the broker is listening on.
func (s *Server) Addr() string { return s.ln.Addr().String() }

// SetExitHandler installs the callback invoked when a worker stops (node
// loss or the agent reporting it exited). It must be called before agents
// are expected to place workers.
func (s *Server) SetExitHandler(h ExitHandler) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.exitCb = h
}

// Place assigns workerID to a node serving 'tag' and sends it a spawn
// command with the resolved environment. It returns ErrNoNode when no
// matching node is registered.
func (s *Server) Place(tag, workerID string, env []string, image, runtime string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var best *node
	for _, n := range s.nodes {
		if !hasTag(n.tags, tag) {
			continue
		}
		if best == nil || len(n.workers) < len(best.workers) {
			best = n
		}
	}
	if best == nil {
		return ErrNoNode
	}
	// Replacing an existing mapping (worker restart on the same ID) removes
	// it from the old node's accounting.
	if oldID, ok := s.workers[workerID]; ok {
		if old, ok := s.nodes[oldID]; ok {
			delete(old.workers, workerID)
		}
	}
	best.workers[workerID] = true
	s.workers[workerID] = best.id
	select {
	case best.sendCh <- spawnMsg{Type: "spawn", CmdID: newCmdID(), WorkerID: workerID, Env: env, Image: image, Runtime: runtime}:
		return nil
	default:
		// Queue full: roll back so a retry can pick another node.
		delete(best.workers, workerID)
		delete(s.workers, workerID)
		return ErrBusy
	}
}

// Kill sends a kill command for workerID to whichever node runs it
// (best-effort: the worker may already be gone).
func (s *Server) Kill(workerID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	nodeID, ok := s.workers[workerID]
	if !ok {
		return
	}
	delete(s.workers, workerID)
	n, ok := s.nodes[nodeID]
	if !ok {
		return
	}
	delete(n.workers, workerID)
	select {
	case n.sendCh <- killMsg{Type: "kill", CmdID: newCmdID(), WorkerID: workerID}:
	default:
		log.Warnf("broker: kill queue full for %s; worker %s will be reaped on node loss", nodeID, workerID)
	}
}

// handleConn manages one agent connection: registration, then a read loop
// for heartbeats/acks and a writer goroutine for commands.
func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()
	// Registration must be the first message.
	var reg registerMsg
	if err := readMsg(conn, &reg); err != nil {
		log.Errorf("broker: registration read failed: %v", err)
		return
	}
	if reg.Type != "register" {
		log.Errorf("broker: first message was %q, expected register", reg.Type)
		return
	}
	if reg.Version != ProtocolVersion {
		writeMsg(conn, welcomeMsg{Type: "welcome", NodeID: "", HeartbeatMS: int(HeartbeatInterval / time.Millisecond)})
		log.Errorf("broker: agent protocol version %d != %d; refusing", reg.Version, ProtocolVersion)
		return
	}
	if len(reg.Tags) == 0 {
		log.Errorf("broker: agent registered with no tags")
		return
	}
	n := &node{
		id:       fmt.Sprintf("node-%d", time.Now().UnixNano()),
		hostname: reg.Hostname,
		tags:     reg.Tags,
		lastSeen: time.Now(),
		sendCh:   make(chan interface{}, 64),
		workers:  make(map[string]bool),
	}
	s.mu.Lock()
	s.nodes[n.id] = n
	s.mu.Unlock()
	log.Infof("broker: node %s (%s) registered with tags %v", n.id, reg.Hostname, reg.Tags)
	if err := writeMsg(conn, welcomeMsg{Type: "welcome", NodeID: n.id, HeartbeatMS: int(HeartbeatInterval / time.Millisecond)}); err != nil {
		s.dropNode(n)
		return
	}

	// Writer goroutine: drains the command queue; exits when the channel
	// closes (dropNode) or the connection fails.
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		for msg := range n.sendCh {
			if err := writeMsg(conn, msg); err != nil {
				return
			}
		}
	}()

	// Read loop.
	r := bufio.NewReader(conn)
	for {
		typ, payload, err := readEnvelope(r)
		if err != nil {
			break
		}
		switch typ {
		case "heartbeat":
			var hb heartbeatMsg
			if err := json.Unmarshal(payload, &hb); err != nil {
				continue
			}
			s.mu.Lock()
			if _, ok := s.nodes[hb.NodeID]; !ok {
				s.mu.Unlock()
				break
			}
			n.lastSeen = time.Now()
			s.mu.Unlock()
			s.handleWorkerStates(hb.NodeID, hb.Workers)
		case "ack":
			var ack ackMsg
			if err := json.Unmarshal(payload, &ack); err != nil {
				continue
			}
			if ack.Error != "" {
				log.Warnf("broker: node %s failed command %s for worker %s: %s", n.id, ack.CmdID, ack.WorkerID, ack.Error)
			}
		default:
			log.Warnf("broker: unknown message type %q from %s", typ, n.id)
		}
	}
	// Connection dropped: the node is gone.
	s.dropNode(n)
	<-writerDone
}

// handleWorkerStates reconciles the agent's reported worker status with the
// registry. Workers reported as no longer running (and not being killed by
// us) produce an exit event for the runtime.
func (s *Server) handleWorkerStates(nodeID string, states []WorkerState) {
	var exited []workerExit
	s.mu.Lock()
	n, ok := s.nodes[nodeID]
	if !ok {
		s.mu.Unlock()
		return
	}
	for _, ws := range states {
		if ws.Running {
			continue
		}
		// Only report workers this node was actually assigned.
		if !n.workers[ws.WorkerID] {
			continue
		}
		delete(n.workers, ws.WorkerID)
		delete(s.workers, ws.WorkerID)
		var err error
		if ws.Error != "" {
			err = errors.New(ws.Error)
		}
		exited = append(exited, workerExit{workerID: ws.WorkerID, err: err})
	}
	s.mu.Unlock()
	for _, e := range exited {
		s.enqueueExit(e)
	}
}

// dropNode removes a node (dead connection or watchdog timeout) and reports
// every worker it hosted as exited.
func (s *Server) dropNode(n *node) {
	var exited []workerExit
	s.mu.Lock()
	if _, ok := s.nodes[n.id]; !ok {
		s.mu.Unlock()
		return
	}
	delete(s.nodes, n.id)
	for workerID := range n.workers {
		delete(s.workers, workerID)
		exited = append(exited, workerExit{workerID: workerID, err: errors.New("broker: node lost")})
	}
	n.workers = nil
	close(n.sendCh) // writer goroutine exits
	s.mu.Unlock()
	log.Warnf("broker: node %s (%s) lost; %d worker(s) affected", n.id, n.hostname, len(exited))
	for _, e := range exited {
		s.enqueueExit(e)
	}
}

// enqueueExit queues an exit event for the drain goroutine (non-blocking).
func (s *Server) enqueueExit(e workerExit) {
	select {
	case s.exitCh <- e:
	case <-s.closed:
	default:
		log.Warnf("broker: exit event queue full; dropping exit of worker %s", e.workerID)
	}
}

// drain invokes the exit handler for queued worker exits. It never runs
// while Server.mu is held (exitCb is read under mu, but the callback itself
// is called here, lock-free).
func (s *Server) drain() {
	for {
		select {
		case e := <-s.exitCh:
			s.mu.Lock()
			cb := s.exitCb
			s.mu.Unlock()
			if cb != nil {
				cb(e.workerID, e.err)
			}
		case <-s.closed:
			return
		}
	}
}

// watchdog drops nodes that have not heartbeat within NodeTimeout.
func (s *Server) watchdog() {
	t := time.NewTicker(WatchdogInterval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			var stale []*node
			now := time.Now()
			s.mu.Lock()
			for _, n := range s.nodes {
				if now.Sub(n.lastSeen) > NodeTimeout {
					stale = append(stale, n)
				}
			}
			s.mu.Unlock()
			for _, n := range stale {
				s.dropNode(n)
			}
		case <-s.closed:
			return
		}
	}
}

func (s *Server) acceptLoop() {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			select {
			case <-s.closed:
				return
			default:
			}
			log.Errorf("broker: accept: %v", err)
			continue
		}
		go s.handleConn(conn)
	}
}

// Close shuts the broker down: no new connections, existing nodes lose their
// workers (via the normal node-loss path), and the exit handler is drained.
func (s *Server) Close() error {
	s.once.Do(func() {
		close(s.closed)
		s.ln.Close()
		s.mu.Lock()
		var nodes []*node
		for _, n := range s.nodes {
			nodes = append(nodes, n)
		}
		s.mu.Unlock()
		for _, n := range nodes {
			s.dropNode(n)
		}
	})
	return nil
}

// NodeCount returns the number of registered nodes (for tests and status).
func (s *Server) NodeCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.nodes)
}

// NodesWithTag returns the number of registered nodes serving 'tag'.
func (s *Server) NodesWithTag(tag string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, node := range s.nodes {
		if hasTag(node.tags, tag) {
			n++
		}
	}
	return n
}

func hasTag(tags []string, tag string) bool {
	for _, t := range tags {
		if t == tag {
			return true
		}
	}
	return false
}

func newCmdID() string {
	return fmt.Sprintf("cmd-%d", time.Now().UnixNano())
}

//////////////////////////////////////////////////////////////////////////////
// Client (agent side)
//////////////////////////////////////////////////////////////////////////////

// Client is the agent's connection to the broker. One long-lived connection
// carries heartbeats out and spawn/kill commands in.
type Client struct {
	conn   net.Conn
	wmu    sync.Mutex // serializes writes
	NodeID string
}

// Connect dials the broker, registers with the given tags, and returns a
// ready Client (the welcome message has been consumed; NodeID is set).
func Connect(addr, hostname string, tags []string) (*Client, error) {
	conn, err := net.DialTimeout("tcp", addr, 10*time.Second)
	if err != nil {
		return nil, errors.Wrapf(err, "broker: could not connect to %s", addr)
	}
	c := &Client{conn: conn}
	if err := writeMsg(conn, registerMsg{Type: "register", Version: ProtocolVersion, Hostname: hostname, Tags: tags}); err != nil {
		conn.Close()
		return nil, errors.Wrap(err, "broker: could not register")
	}
	var welcome welcomeMsg
	if err := readMsg(conn, &welcome); err != nil {
		conn.Close()
		return nil, errors.Wrap(err, "broker: could not read welcome")
	}
	if welcome.NodeID == "" {
		conn.Close()
		return nil, errors.New("broker: registration rejected (protocol version mismatch?)")
	}
	c.NodeID = welcome.NodeID
	return c, nil
}

// Heartbeat sends the agent's current worker statuses.
func (c *Client) Heartbeat(workers []WorkerState) error {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	return writeMsg(c.conn, heartbeatMsg{Type: "heartbeat", NodeID: c.NodeID, Workers: workers})
}

// Ack acknowledges a completed command.
func (c *Client) Ack(cmdID, workerID string, err error) error {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	msg := ackMsg{Type: "ack", CmdID: cmdID, WorkerID: workerID}
	if err != nil {
		msg.Error = err.Error()
	}
	return writeMsg(c.conn, msg)
}

// Recv blocks until the next command (spawn or kill) arrives.
func (c *Client) Recv() (interface{}, error) {
	typ, payload, err := readEnvelope(c.conn)
	if err != nil {
		return nil, err
	}
	switch typ {
	case "spawn":
		var m spawnMsg
		if err := json.Unmarshal(payload, &m); err != nil {
			return nil, err
		}
		return &Spawn{CmdID: m.CmdID, WorkerID: m.WorkerID, Env: m.Env, Image: m.Image, Runtime: m.Runtime}, nil
	case "kill":
		var m killMsg
		if err := json.Unmarshal(payload, &m); err != nil {
			return nil, err
		}
		return &Kill{CmdID: m.CmdID, WorkerID: m.WorkerID}, nil
	default:
		return nil, errors.Errorf("broker: unexpected message type %q", typ)
	}
}

// Close shuts down the connection.
func (c *Client) Close() error { return c.conn.Close() }
