package broker

import (
	"testing"
	"time"
)

func TestTxtMap(t *testing.T) {
	m := txtMap([]string{"version=1.12.5", "etcd_port=2379", "broker_port=30660", "no-equals"})
	if m["version"] != "1.12.5" {
		t.Fatalf("version = %q", m["version"])
	}
	if m["etcd_port"] != "2379" {
		t.Fatalf("etcd_port = %q", m["etcd_port"])
	}
	if m["broker_port"] != "30660" {
		t.Fatalf("broker_port = %q", m["broker_port"])
	}
	if _, ok := m["no-equals"]; ok {
		t.Fatalf("no-equals should not be a key")
	}
}

func TestTxtInt(t *testing.T) {
	m := map[string]string{"etcd_port": "2379", "junk": "abc"}
	if got := txtInt(m, "etcd_port", 1); got != 2379 {
		t.Fatalf("etcd_port = %d", got)
	}
	if got := txtInt(m, "junk", 1); got != 1 {
		t.Fatalf("junk should fall back to default, got %d", got)
	}
	if got := txtInt(m, "missing", 42); got != 42 {
		t.Fatalf("missing should fall back to default, got %d", got)
	}
}

func TestTargetAddresses(t *testing.T) {
	tg := &Target{IP: "192.168.1.147", PachdPort: 30650, EtcdPort: 2379, BrokerPort: 30660}
	if got := tg.PachdAddress(); got != "192.168.1.147:30650" {
		t.Fatalf("PachdAddress = %q", got)
	}
	if got := tg.BrokerAddress(); got != "192.168.1.147:30660" {
		t.Fatalf("BrokerAddress = %q", got)
	}
}

// TestResolveRoundTrip registers a pachd service and browses for it over
// real mDNS (multicast loopback on the local host). It asserts that the
// resolved target carries the advertised ports and a matching version.
func TestResolveRoundTrip(t *testing.T) {
	if testing.Short() {
		t.Skip("mDNS round trip skipped in short mode")
	}
	const pachdPort, etcdPort, brokerPort = 30651, 2479, 30661
	cleanup, err := Publish("test-host", pachdPort, etcdPort, brokerPort)
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	defer cleanup()

	target, err := Resolve(8 * time.Second)
	if err != nil {
		t.Fatalf("resolve: %v (is mDNS working on this host?)", err)
	}
	if target.PachdPort != pachdPort {
		t.Fatalf("pachd port = %d, want %d", target.PachdPort, pachdPort)
	}
	if target.EtcdPort != etcdPort {
		t.Fatalf("etcd port = %d, want %d", target.EtcdPort, etcdPort)
	}
	if target.BrokerPort != brokerPort {
		t.Fatalf("broker port = %d, want %d", target.BrokerPort, brokerPort)
	}
	if target.IP == "" {
		t.Fatal("resolved target has no IP")
	}
}
