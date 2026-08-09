package localetcd

import (
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"
)

// freePort allocates an ephemeral port and releases it (etcd's embed does
// not support port 0 for the client URL, so the tests pick a real port).
func freePort(t *testing.T) uint16 {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := uint16(ln.Addr().(*net.TCPAddr).Port)
	ln.Close()
	return port
}

// TestStartBindsHost verifies the bind host parameter: etcd must answer on
// the configured address (loopback here), and the returned client must work.
func TestStartBindsHost(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "etcd")
	env, err := Start(dir, "127.0.0.1", freePort(t))
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer env.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = env.EtcdClient.Put(ctx, "k", "v")
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	resp, err := env.EtcdClient.Get(ctx, "k")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if len(resp.Kvs) != 1 || string(resp.Kvs[0].Value) != "v" {
		t.Fatalf("bad value: %v", resp.Kvs)
	}
}

// TestStartBindAll verifies that binding 0.0.0.0 (the multi-node
// configuration) still answers on loopback, which is what the daemon's own
// client dials.
func TestStartBindAll(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "etcd")
	env, err := Start(dir, "0.0.0.0", freePort(t))
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer env.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := env.EtcdClient.Put(ctx, "k2", "v2"); err != nil {
		t.Fatalf("put over 0.0.0.0 bind: %v", err)
	}
}
