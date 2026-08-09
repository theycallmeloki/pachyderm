// mDNS/DNS-SD discovery for the broker: pachd publishes a _pachd._tcp
// service (with its ports in TXT records), and agents browse for it to find
// the daemon on a trusted LAN without any configuration. Agents that know
// the daemon's address explicitly (--pachd-address / --join) skip discovery.
//
// The design is deliberately simple and matches local mode's trusted-LAN,
// no-auth posture: mDNS is link-local multicast, the version match prevents
// an agent from joining a daemon built from a different fork line, and the
// registration protocol itself still guards the broker (ProtocolVersion).
// Use --join for anything beyond a single broadcast domain.
package broker

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/grandcat/zeroconf"
	log "github.com/sirupsen/logrus"

	"github.com/pachyderm/pachyderm/src/client/pkg/errors"
	"github.com/pachyderm/pachyderm/src/client/version"
)

const (
	// ServiceName is the DNS-SD service type pachd publishes and agents
	// browse.
	ServiceName = "_pachd._tcp"
	// DiscoveryDomain is the mDNS default domain.
	DiscoveryDomain = "local."
)

// VersionString is the version pachd advertises and agents match against
// (for example "1.12.5"). Agents refuse to join a daemon with a different
// version, since the wire protocols may have drifted.
func VersionString() string {
	return fmt.Sprintf("%d.%d.%d", version.MajorVersion, version.MinorVersion, version.MicroVersion)
}

// Publish registers this daemon with DNS-SD so agents can discover it.
// Returns a cleanup func; call it on shutdown (or leave it to process exit,
// after which the service expires via its TTL).
func Publish(hostname string, pachdPort, etcdPort, brokerPort int) (func(), error) {
	text := []string{
		"version=" + VersionString(),
		"pachd_port=" + strconv.Itoa(pachdPort),
		"etcd_port=" + strconv.Itoa(etcdPort),
		"broker_port=" + strconv.Itoa(brokerPort),
	}
	server, err := zeroconf.Register(hostname+"-pachd", ServiceName, DiscoveryDomain, pachdPort, text, nil)
	if err != nil {
		return nil, errors.Wrap(err, "could not publish pachd via mDNS")
	}
	log.Infof("discovery: advertising %s at %s (ports %d/%d/%d)", ServiceName, hostname, pachdPort, etcdPort, brokerPort)
	return func() { server.Shutdown() }, nil
}

// Target is what an agent needs to reach a pachd daemon: its IP and the
// ports of the gRPC API, etcd, and the broker.
type Target struct {
	IP         string
	PachdPort  int
	EtcdPort   int
	BrokerPort int
}

// BrokerAddress returns the broker's host:port for this target.
func (t *Target) BrokerAddress() string {
	return net.JoinHostPort(t.IP, strconv.Itoa(t.BrokerPort))
}

// PachdAddress returns the pachd gRPC host:port for this target.
func (t *Target) PachdAddress() string {
	return net.JoinHostPort(t.IP, strconv.Itoa(t.PachdPort))
}

// Resolve browses the LAN for a pachd daemon, waiting up to timeout, and
// returns the first instance whose advertised version matches this build.
// It returns ErrNoDaemon if none is found.
func Resolve(timeout time.Duration) (*Target, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	entries := make(chan *zeroconf.ServiceEntry, 8)
	resolver, err := zeroconf.NewResolver()
	if err != nil {
		return nil, errors.Wrap(err, "could not create mDNS resolver")
	}
	if err := resolver.Browse(ctx, ServiceName, DiscoveryDomain, entries); err != nil {
		return nil, errors.Wrap(err, "could not browse for pachd")
	}
	want := VersionString()
	var lastErr error
	for {
		select {
		case entry, ok := <-entries:
			if !ok {
				return nil, lastErrOrNoDaemon(lastErr)
			}
			if entry == nil {
				continue
			}
			txt := txtMap(entry.Text)
			if v, ok := txt["version"]; ok && v != want {
				lastErr = errors.Errorf("discovered pachd version %q does not match agent version %q", v, want)
				log.Warnf("discovery: %v", lastErr)
				continue
			}
			ip := ""
			for _, a := range entry.AddrIPv4 {
				if a != nil && !a.IsLoopback() {
					ip = a.String()
					break
				}
			}
			if ip == "" && len(entry.AddrIPv4) > 0 {
				ip = entry.AddrIPv4[0].String()
			}
			if ip == "" {
				lastErr = errors.Errorf("discovered pachd %q has no IPv4 address", entry.Instance)
				continue
			}
			t := &Target{
				IP:         ip,
				PachdPort:  entry.Port,
				EtcdPort:   txtInt(txt, "etcd_port", 2379),
				BrokerPort: txtInt(txt, "broker_port", 30660),
			}
			log.Infof("discovery: resolved pachd %s (%s, etcd %d, broker %d)", entry.Instance, t.PachdAddress(), t.EtcdPort, t.BrokerPort)
			return t, nil
		case <-ctx.Done():
			return nil, lastErrOrNoDaemon(lastErr)
		}
	}
}

// ErrNoDaemon is returned by Resolve when no matching daemon was found in
// time.
var ErrNoDaemon = errors.New("no pachd daemon discovered on the LAN")

func lastErrOrNoDaemon(lastErr error) error {
	if lastErr != nil {
		return errors.Wrapf(lastErr, "%v", ErrNoDaemon)
	}
	return ErrNoDaemon
}

// txtMap converts a TXT record ([]string of "k=v") to a map.
func txtMap(text []string) map[string]string {
	m := make(map[string]string, len(text))
	for _, t := range text {
		for i := 0; i < len(t); i++ {
			if t[i] == '=' {
				m[t[:i]] = t[i+1:]
				break
			}
		}
	}
	return m
}

func txtInt(m map[string]string, key string, def int) int {
	if v, ok := m[key]; ok {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
