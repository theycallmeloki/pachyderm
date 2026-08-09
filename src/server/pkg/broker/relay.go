package broker

import (
	"io"
	"net"
	"strconv"
	"sync"

	"github.com/pachyderm/pachyderm/src/client/pkg/errors"
)

// Relay is a minimal TCP proxy used to expose pipeline services across the
// broker: the daemon listens on its loopback external port and relays to the
// node's relay (node IP : external port), which the agent runs and forwards
// to the worker's internal port. Two hops, one primitive.
type Relay struct {
	ln     net.Listener
	done   chan struct{}
	once   sync.Once
	target string
}

// NewRelay listens on host:port and forwards every accepted connection to
// target (host:port). Close shuts the listener down.
func NewRelay(host string, port int, target string) (*Relay, error) {
	ln, err := net.Listen("tcp", net.JoinHostPort(host, strconv.Itoa(port)))
	if err != nil {
		return nil, errors.Wrapf(err, "could not listen on %s:%d", host, port)
	}
	r := &Relay{
		ln:     ln,
		done:   make(chan struct{}),
		target: target,
	}
	go r.serve()
	return r, nil
}

func (r *Relay) serve() {
	for {
		conn, err := r.ln.Accept()
		if err != nil {
			select {
			case <-r.done:
				return
			default:
				continue // transient accept error; keep serving
			}
		}
		go r.relay(conn)
	}
}

func (r *Relay) relay(conn net.Conn) {
	upstream, err := net.Dial("tcp", r.target)
	if err != nil {
		conn.Close()
		return
	}
	done := make(chan struct{}, 2)
	go func() {
		io.Copy(upstream, conn)
		upstream.Close()
		done <- struct{}{}
	}()
	go func() {
		io.Copy(conn, upstream)
		conn.Close()
		done <- struct{}{}
	}()
	<-done
	upstream.Close()
	conn.Close()
}

// Close shuts the relay down.
func (r *Relay) Close() error {
	r.once.Do(func() {
		close(r.done)
		r.ln.Close()
	})
	return nil
}
