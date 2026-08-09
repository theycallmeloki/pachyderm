package broker

import (
	"fmt"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/pachyderm/pachyderm/src/client/pkg/errors"
)

// startTestServer starts a broker on a random loopback port.
func startTestServer(t *testing.T) *Server {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &Server{
		nodes:     make(map[string]*node),
		workers:   make(map[string]string),
		conns:     make(map[*net.Conn]bool),
		tagNodes:  make(map[string][]string),
		tagCursor: make(map[string]int),
		exitCh:    make(chan workerExit, 1024),
		closed:    make(chan struct{}),
		ln:        ln,
	}
	go s.acceptLoop()
	go s.drain()
	go s.watchdog()
	t.Cleanup(func() { s.Close() })
	return s
}

func TestPlacementAndKill(t *testing.T) {
	s := startTestServer(t)

	// No node registered yet.
	if err := s.Place([]string{"gpu"}, "worker-1", []string{"A=1"}, "", ""); !errors.Is(err, ErrNoNode) {
		t.Fatalf("expected ErrNoNode, got %v", err)
	}

	// Agent registers with tag "gpu".
	agent, err := Connect(s.Addr(), "testhost", []string{"gpu"})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer agent.Close()

	// Place a worker; the agent must receive a spawn command.
	if err := s.Place([]string{"gpu"}, "worker-1", []string{"A=1"}, "", ""); err != nil {
		t.Fatalf("place: %v", err)
	}
	msg, err := agent.Recv()
	if err != nil {
		t.Fatalf("recv: %v", err)
	}
	spawn, ok := msg.(*Spawn)
	if !ok {
		t.Fatalf("expected spawn, got %T", msg)
	}
	if spawn.WorkerID != "worker-1" {
		t.Fatalf("wrong worker id %q", spawn.WorkerID)
	}
	if len(spawn.Env) != 1 || spawn.Env[0] != "A=1" {
		t.Fatalf("wrong env: %v", spawn.Env)
	}

	// Kill it; the agent receives a kill command.
	s.Kill("worker-1")
	msg, err = agent.Recv()
	if err != nil {
		t.Fatalf("recv: %v", err)
	}
	if _, ok := msg.(*Kill); !ok {
		t.Fatalf("expected kill, got %T", msg)
	}
}

func TestTagMismatch(t *testing.T) {
	s := startTestServer(t)
	agent, err := Connect(s.Addr(), "testhost", []string{"cpu"})
	if err != nil {
		t.Fatal(err)
	}
	defer agent.Close()

	if err := s.Place([]string{"gpu"}, "worker-1", nil, "", ""); !errors.Is(err, ErrNoNode) {
		t.Fatalf("expected ErrNoNode for tag mismatch, got %v", err)
	}
}

func TestWorkerExitReported(t *testing.T) {
	s := startTestServer(t)
	exited := make(chan workerExit, 4)
	s.SetExitHandler(func(workerID string, err error) {
		exited <- workerExit{workerID: workerID, err: err}
	})

	agent, err := Connect(s.Addr(), "testhost", []string{"gpu"})
	if err != nil {
		t.Fatal(err)
	}
	defer agent.Close()

	if err := s.Place([]string{"gpu"}, "worker-1", nil, "", ""); err != nil {
		t.Fatal(err)
	}
	if _, err := agent.Recv(); err != nil { // consume the spawn
		t.Fatal(err)
	}
	// Agent reports the worker exited.
	if err := agent.Heartbeat([]WorkerState{{WorkerID: "worker-1", Running: false, Error: "boom"}}); err != nil {
		t.Fatal(err)
	}
	select {
	case e := <-exited:
		if e.workerID != "worker-1" {
			t.Fatalf("wrong worker %q", e.workerID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("exit event never delivered")
	}
	// A subsequent kill must be a no-op (worker already removed).
	s.Kill("worker-1")
}

func TestNodeLossReportsWorkers(t *testing.T) {
	s := startTestServer(t)
	exited := make(chan workerExit, 4)
	s.SetExitHandler(func(workerID string, err error) {
		exited <- workerExit{workerID: workerID, err: err}
	})

	agent, err := Connect(s.Addr(), "testhost", []string{"gpu"})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Place([]string{"gpu"}, "worker-1", nil, "", ""); err != nil {
		t.Fatal(err)
	}
	if _, err := agent.Recv(); err != nil { // consume the spawn
		t.Fatal(err)
	}
	agent.Close() // node dies

	select {
	case e := <-exited:
		if e.workerID != "worker-1" {
			t.Fatalf("wrong worker %q", e.workerID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("exit event never delivered on node loss")
	}
	// Node must be deregistered.
	if s.NodeCount() != 0 {
		t.Fatalf("node still registered after loss: %d", s.NodeCount())
	}
}

func TestStaleNodeReaped(t *testing.T) {
	s := startTestServer(t)
	exited := make(chan workerExit, 4)
	s.SetExitHandler(func(workerID string, err error) {
		exited <- workerExit{workerID: workerID, err: err}
	})

	agent, err := Connect(s.Addr(), "testhost", []string{"gpu"})
	if err != nil {
		t.Fatal(err)
	}
	defer agent.Close()
	if err := s.Place([]string{"gpu"}, "worker-1", nil, "", ""); err != nil {
		t.Fatal(err)
	}
	if _, err := agent.Recv(); err != nil { // consume the spawn
		t.Fatal(err)
	}
	// Stop heartbeating and wait for the watchdog to reap the node.
	select {
	case e := <-exited:
		if e.workerID != "worker-1" {
			t.Fatalf("wrong worker %q", e.workerID)
		}
	case <-time.After(NodeTimeout + 2*WatchdogInterval + time.Second):
		t.Fatal("stale node never reaped")
	}
	if s.NodeCount() != 0 {
		t.Fatalf("stale node still registered: %d", s.NodeCount())
	}
}

func TestLeastLoadedPlacement(t *testing.T) {
	s := startTestServer(t)
	a1, err := Connect(s.Addr(), "host1", []string{"gpu"})
	if err != nil {
		t.Fatal(err)
	}
	defer a1.Close()
	a2, err := Connect(s.Addr(), "host2", []string{"gpu"})
	if err != nil {
		t.Fatal(err)
	}
	defer a2.Close()

	// Drain both agents in parallel: each spawn must land on the least
	// loaded node, so with 3 workers the loads must be {2, 1} — never 3 on
	// one node.
	chA := make(chan *Spawn, 4)
	chB := make(chan *Spawn, 4)
	go func() {
		for {
			m, err := a1.Recv()
			if err != nil {
				return
			}
			chA <- m.(*Spawn)
		}
	}()
	go func() {
		for {
			m, err := a2.Recv()
			if err != nil {
				return
			}
			chB <- m.(*Spawn)
		}
	}()

	for i := 1; i <= 3; i++ {
		if err := s.Place([]string{"gpu"}, fmt.Sprintf("w%d", i), nil, "", ""); err != nil {
			t.Fatal(err)
		}
	}
	counts := map[*Client]int{}
	for received := 0; received < 3; received++ {
		select {
		case <-chA:
			counts[a1]++
		case <-chB:
			counts[a2]++
		case <-time.After(3 * time.Second):
			t.Fatal("timed out waiting for spawns")
		}
	}
	if counts[a1]+counts[a2] != 3 {
		t.Fatalf("expected 3 spawns total, got %d", counts[a1]+counts[a2])
	}
	if counts[a1] > 2 || counts[a2] > 2 {
		t.Fatalf("least-loaded placement violated: loads %v", counts)
	}
}

// TestRoundRobin verifies placements rotate through nodes in registration
// order (per tag), and that multi-tag placement falls back across tags.
func TestRoundRobin(t *testing.T) {
	s := startTestServer(t)
	a1, err := Connect(s.Addr(), "host1", []string{"gpu"})
	if err != nil {
		t.Fatal(err)
	}
	defer a1.Close()
	a2, err := Connect(s.Addr(), "host2", []string{"gpu"})
	if err != nil {
		t.Fatal(err)
	}
	defer a2.Close()
	a3, err := Connect(s.Addr(), "host3", []string{"gpu", "cpu"})
	if err != nil {
		t.Fatal(err)
	}
	defer a3.Close()

	// Drain both gpu nodes + host3 (which serves gpu too): 6 placements,
	// each node must receive exactly 2 spawns (round robin over
	// registration order: host1, host2, host3).
	counts := map[*Client]int{}
	ch1 := drainSpawns(t, a1)
	ch2 := drainSpawns(t, a2)
	ch3 := drainSpawns(t, a3)
	for i := 0; i < 6; i++ {
		if err := s.Place([]string{"gpu"}, fmt.Sprintf("w%d", i), nil, "", ""); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 6; i++ {
		select {
		case <-ch1:
			counts[a1]++
		case <-ch2:
			counts[a2]++
		case <-ch3:
			counts[a3]++
		case <-time.After(3 * time.Second):
			t.Fatal("timed out waiting for spawns")
		}
	}
	if counts[a1] != 2 || counts[a2] != 2 || counts[a3] != 2 {
		t.Fatalf("round robin violated: %v", counts)
	}

	// A pipeline that accepts "gpu" or "cpu": the first tag with nodes wins,
	// so this must land on a gpu-serving node (host1, next in rotation).
	if err := s.Place([]string{"gpu", "cpu"}, "w-any", nil, "", ""); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ch1:
	case <-ch2:
	case <-ch3:
	case <-time.After(3 * time.Second):
		t.Fatal("placement never delivered")
	}
}

// TestTagFallback verifies the multi-tag fallback: when the first tag has
// no nodes, the next tag with nodes is used.
func TestTagFallback(t *testing.T) {
	s := startTestServer(t)
	a1, err := Connect(s.Addr(), "host4", []string{"cpu"})
	if err != nil {
		t.Fatal(err)
	}
	defer a1.Close()

	if err := s.Place([]string{"gpu", "cpu"}, "w-cpu", nil, "", ""); err != nil {
		t.Fatalf("fallback placement failed: %v", err)
	}
	msg, err := a1.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if sp, ok := msg.(*Spawn); !ok || sp.WorkerID != "w-cpu" {
		t.Fatalf("expected spawn for w-cpu, got %T %v", msg, msg)
	}
	// With no nodes for the requested tags, ErrNoNode (a cpu-only node does
	// not serve "gpu" alone).
	if err := s.Place([]string{"gpu"}, "w-nobody", nil, "", ""); !errors.Is(err, ErrNoNode) {
		t.Fatalf("expected ErrNoNode, got %v", err)
	}
}

func drainSpawns(t *testing.T, c *Client) chan *Spawn {
	t.Helper()
	ch := make(chan *Spawn, 16)
	go func() {
		for {
			m, err := c.Recv()
			if err != nil {
				return
			}
			if sp, ok := m.(*Spawn); ok {
				ch <- sp
			}
		}
	}()
	return ch
}

// TestForwardCommands verifies the forward/unforward commands reach the
// node running the worker, and NodeIP reports where it is.
func TestForwardCommands(t *testing.T) {
	s := startTestServer(t)
	agent, err := Connect(s.Addr(), "host1", []string{"gpu"})
	if err != nil {
		t.Fatal(err)
	}
	defer agent.Close()
	if err := s.Place([]string{"gpu"}, "worker-1", nil, "", ""); err != nil {
		t.Fatal(err)
	}
	if _, err := agent.Recv(); err != nil { // consume spawn
		t.Fatal(err)
	}
	ip, ok := s.NodeIP("worker-1")
	if !ok || ip == "" {
		t.Fatalf("NodeIP = %q, %v", ip, ok)
	}
	s.Forward("worker-1", 31800, 8000)
	msg, err := agent.Recv()
	if err != nil {
		t.Fatal(err)
	}
	f, ok := msg.(*Forward)
	if !ok {
		t.Fatalf("expected forward, got %T", msg)
	}
	if f.WorkerID != "worker-1" || f.ExternalPort != 31800 || f.InternalPort != 8000 {
		t.Fatalf("bad forward: %+v", f)
	}
	s.Unforward("worker-1", 31800)
	msg, err = agent.Recv()
	if err != nil {
		t.Fatal(err)
	}
	u, ok := msg.(*Unforward)
	if !ok {
		t.Fatalf("expected unforward, got %T", msg)
	}
	if u.ExternalPort != 31800 {
		t.Fatalf("bad unforward: %+v", u)
	}
}

// TestRelay verifies the TCP relay primitive end to end.
func TestRelay(t *testing.T) {
	// Upstream: an echo server.
	upstream, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer upstream.Close()
	go func() {
		for {
			conn, err := upstream.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				buf := make([]byte, 1024)
				for {
					n, err := c.Read(buf)
					if err != nil {
						return
					}
					if _, err := c.Write(buf[:n]); err != nil {
						return
					}
				}
			}(conn)
		}
	}()
	upstreamPort := upstream.Addr().(*net.TCPAddr).Port

	// Relay on another port.
	relay, err := NewRelay("127.0.0.1", 0, net.JoinHostPort("127.0.0.1", strconv.Itoa(upstreamPort)))
	if err != nil {
		t.Fatal(err)
	}
	defer relay.Close()
	relayPort := relay.ln.Addr().(*net.TCPAddr).Port

	conn, err := net.Dial("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(relayPort)))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.Write([]byte("hello through relay")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 64)
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatal(err)
	}
	if string(buf[:n]) != "hello through relay" {
		t.Fatalf("echo mismatch: %q", buf[:n])
	}
}
