package broker

import (
	"fmt"
	"net"
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
		nodes:   make(map[string]*node),
		workers: make(map[string]string),
		conns:   make(map[*net.Conn]bool),
		exitCh:  make(chan workerExit, 1024),
		closed:  make(chan struct{}),
		ln:      ln,
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
	if err := s.Place("gpu", "worker-1", []string{"A=1"}, "", ""); !errors.Is(err, ErrNoNode) {
		t.Fatalf("expected ErrNoNode, got %v", err)
	}

	// Agent registers with tag "gpu".
	agent, err := Connect(s.Addr(), "testhost", []string{"gpu"})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer agent.Close()

	// Place a worker; the agent must receive a spawn command.
	if err := s.Place("gpu", "worker-1", []string{"A=1"}, "", ""); err != nil {
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

	if err := s.Place("gpu", "worker-1", nil, "", ""); !errors.Is(err, ErrNoNode) {
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

	if err := s.Place("gpu", "worker-1", nil, "", ""); err != nil {
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
	if err := s.Place("gpu", "worker-1", nil, "", ""); err != nil {
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
	if err := s.Place("gpu", "worker-1", nil, "", ""); err != nil {
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
		if err := s.Place("gpu", fmt.Sprintf("w%d", i), nil, "", ""); err != nil {
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
