// Command agent runs the broker node agent: the light "kubelet" that lets
// pachd place pipeline workers on this node. It connects to the pachd-side
// broker (src/server/pkg/broker), registers the node tags it serves, and
// spawns/kills worker processes on command. It is deliberately dumb: every
// scheduling decision lives in pachd; the agent only executes.
//
// Workers run as plain processes (no containers, no docker requirement).
// The agent owns their scratch dirs under --local-dir, rewrites the
// host-specific env vars pachd cannot know (scratch roots, pachd address),
// and kills them all if the broker connection drops — kubelet semantics, so
// pachd can re-place the work elsewhere.
package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/pachyderm/pachyderm/src/client/pkg/errors"
	"github.com/pachyderm/pachyderm/src/server/pkg/broker"
	"github.com/pachyderm/pachyderm/src/server/pkg/podrunner"
)

const (
	heartbeatInterval = broker.HeartbeatInterval
	reconnectDelay    = 2 * time.Second
)

type options struct {
	brokerAddr   string
	tags         []string
	localDir     string
	workerBinary string
	pachdAddr    string
	join         string
	etcdPort     int
	brokerPort   int
	discover     bool
	runtime      string
	dockerBin    string
}

func parseOptions() (*options, error) {
	opts := &options{}
	flag.StringVar(&opts.brokerAddr, "broker", os.Getenv("PACH_BROKER_ADDRESS"), "broker address (host:port); defaults to the discovered pachd's broker port")
	tags := flag.String("tag", os.Getenv("PACH_AGENT_TAG"), "comma-separated node tags this agent serves")
	flag.StringVar(&opts.localDir, "local-dir", envOr("PACH_AGENT_DIR", filepath.Join(os.TempDir(), "pach-agent")), "agent data dir (worker scratch, logs, pid file)")
	flag.StringVar(&opts.workerBinary, "worker-binary", os.Getenv("PACH_WORKER_BINARY"), "path to the pachd worker binary")
	flag.StringVar(&opts.pachdAddr, "pachd-address", os.Getenv("PACH_PEER_ADDRESS"), "pachd address (host:port) workers connect to; defaults to pachd's PEER_PORT (loopback)")
	flag.StringVar(&opts.join, "join", os.Getenv("PACH_JOIN"), "join a specific pachd host (host or host:port); skips mDNS discovery")
	flag.IntVar(&opts.etcdPort, "etcd-port", envInt("PACH_ETCD_PORT", 2379), "pachd's etcd port (used with --join/--pachd-address)")
	flag.IntVar(&opts.brokerPort, "broker-port", envInt("PACH_BROKER_PORT", 30660), "pachd's broker port (used when --broker is not set)")
	flag.BoolVar(&opts.discover, "discover", envBool("PACH_AGENT_DISCOVER", true), "discover pachd via mDNS when no address is given")
	flag.StringVar(&opts.runtime, "runtime", envOr("PACH_WORKER_RUNTIME", "docker"), "how workers run: docker (default) or process")
	flag.StringVar(&opts.dockerBin, "docker", envOr("PACH_DOCKER_BIN", "docker"), "path to the docker binary (docker runtime)")
	flag.Parse()
	if *tags == "" {
		return nil, errors.New("at least one node tag is required (--tag or PACH_AGENT_TAG)")
	}
	if opts.workerBinary == "" {
		return nil, errors.New("worker binary is required (--worker-binary or PACH_WORKER_BINARY)")
	}
	for _, t := range strings.Split(*tags, ",") {
		if t = strings.TrimSpace(t); t != "" {
			opts.tags = append(opts.tags, t)
		}
	}
	if len(opts.tags) == 0 {
		return nil, errors.New("no valid tags in --tag")
	}
	return opts, nil
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envBool(key string, def bool) bool {
	if v := os.Getenv(key); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return def
}

// resolveTarget determines how to reach pachd: an explicit --pachd-address
// wins, then --join (a host, pachd port defaulted), then mDNS discovery.
func resolveTarget(opts *options) (*broker.Target, error) {
	if opts.pachdAddr != "" {
		host, portStr, err := net.SplitHostPort(opts.pachdAddr)
		if err != nil {
			return nil, errors.Wrapf(err, "invalid --pachd-address %q", opts.pachdAddr)
		}
		port, err := strconv.Atoi(portStr)
		if err != nil {
			return nil, errors.Wrapf(err, "invalid --pachd-address %q", opts.pachdAddr)
		}
		return &broker.Target{IP: host, PachdPort: port, EtcdPort: opts.etcdPort, BrokerPort: opts.brokerPort}, nil
	}
	if opts.join != "" {
		host := opts.join
		port := 30650
		if h, p, err := net.SplitHostPort(opts.join); err == nil {
			host = h
			if n, err := strconv.Atoi(p); err == nil {
				port = n
			}
		}
		return &broker.Target{IP: host, PachdPort: port, EtcdPort: opts.etcdPort, BrokerPort: opts.brokerPort}, nil
	}
	if opts.discover {
		return broker.Resolve(5 * time.Second)
	}
	return nil, errors.New("no pachd address: set --pachd-address, --join, or enable discovery (--discover)")
}

// workerProc is one worker this agent is running.
type workerProc struct {
	id     string
	proc   *podrunner.Proc
	mu     sync.Mutex
	exited bool
	err    error
}

// agent manages the workers on this node.
type agent struct {
	opts    *options
	pidFile *podrunner.PidFile
	mu      sync.Mutex
	workers map[string]*workerProc
	relays  map[string]*broker.Relay // "workerID/externalPort" -> relay
	client  *broker.Client
	target  *broker.Target // pachd reachability, resolved per session
	localIP string         // address service relays bind (broker conn's local addr)
}

func newAgent(opts *options) *agent {
	return &agent{
		opts:    opts,
		pidFile: podrunner.NewPidFile(opts.localDir),
		workers: make(map[string]*workerProc),
		relays:  make(map[string]*broker.Relay),
	}
}

func (a *agent) logDir() string     { return filepath.Join(a.opts.localDir, "logs") }
func (a *agent) workersDir() string { return filepath.Join(a.opts.localDir, "workers") }

// setEnv replaces or appends key=value in env.
func setEnv(env []string, key, value string) []string {
	prefix := key + "="
	for i, e := range env {
		if strings.HasPrefix(e, prefix) {
			env[i] = prefix + value
			return env
		}
	}
	return append(env, prefix+value)
}

// spawn starts a worker. If one with the same ID is already running (a
// re-placement racing the death report), the old one is killed first.
func (a *agent) spawn(s *broker.Spawn) error {
	a.mu.Lock()
	if old, ok := a.workers[s.WorkerID]; ok {
		a.mu.Unlock()
		a.killWorker(old)
		a.mu.Lock()
	}
	a.mu.Unlock()

	scratch := filepath.Join(a.workersDir(), s.WorkerID)
	// The cache must live OUTSIDE the scratch dir: scratch is the /pfs mount
	// source, and the worker unlinks every entry of the input dir between
	// datums (except .scratch), which would delete a nested cache out from
	// under the /pach-cache bind mount (the daemon's runtime does the same:
	// LocalDir/cache/<name>, not workers/<name>/cache).
	cacheDir := filepath.Join(a.opts.localDir, "cache", s.WorkerID)
	if err := os.MkdirAll(cacheDir, 0777); err != nil {
		return errors.Wrapf(err, "could not create worker cache dir")
	}
	if err := os.MkdirAll(scratch, 0777); err != nil {
		return errors.Wrapf(err, "could not create worker scratch dir")
	}
	logPath := filepath.Join(a.logDir(), s.WorkerID+".jsonl")
	if err := os.MkdirAll(filepath.Dir(logPath), 0777); err != nil {
		return errors.Wrapf(err, "could not create log dir")
	}
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return errors.Wrapf(err, "could not open log file")
	}

	// The env pachd sent was computed for a local worker; the host-specific
	// parts belong to this node. In docker mode pachd already computed the
	// container-correct values (PACH_WORKER_ROOT=/ and PACH_CACHE_ROOT=/pach-cache,
	// matching the mounts below); in process mode the scratch dirs are ours.
	env := s.Env
	a.mu.Lock()
	target := a.target
	a.mu.Unlock()
	if target != nil {
		// Point the worker at pachd over the LAN: pachd computed loopback
		// addresses that only work on the daemon's own host.
		env = setEnv(env, "ETCD_SERVICE_HOST", target.IP)
		env = setEnv(env, "ETCD_SERVICE_PORT", strconv.Itoa(target.EtcdPort))
		env = setEnv(env, "PACH_PEER_ADDRESS", target.PachdAddress())
	}
	runtime := s.Runtime
	if runtime == "" {
		runtime = a.opts.runtime
	}
	var cmd *exec.Cmd
	switch runtime {
	case "docker":
		if s.Image == "" {
			return errors.Errorf("spawn %s: docker runtime requires a pipeline image", s.WorkerID)
		}
		// Clear any container orphaned by a previous agent run (or a crash);
		// `docker run --name` would otherwise conflict forever.
		if err := exec.Command(a.opts.dockerBin, "rm", "-f", s.WorkerID).Run(); err != nil {
			log.Debugf("agent: pre-rm %s (expected when fresh): %v", s.WorkerID, err)
		}
		args := []string{
			"run", "--rm",
			"--name", s.WorkerID,
			"--network", "host",
			"-v", scratch + ":/pfs",
			"-v", cacheDir + ":/pach-cache",
			"-v", a.opts.workerBinary + ":/pach-bin/worker:ro",
		}
		// The worker inspects the image for empty transform fields and needs
		// the socket for that; without it, the inspection fails and the
		// worker refuses to start (same as a k8s worker without the sidecar).
		if _, err := os.Stat("/var/run/docker.sock"); err == nil {
			args = append(args, "-v", "/var/run/docker.sock:/var/run/docker.sock")
		}
		for _, e := range env {
			args = append(args, "-e", e)
		}
		args = append(args, "--entrypoint", "/pach-bin/worker", s.Image)
		cmd = exec.Command(a.opts.dockerBin, args...)
	default: // process
		env = setEnv(env, "PACH_WORKER_ROOT", scratch)
		env = setEnv(env, "PACH_CACHE_ROOT", cacheDir)
		cmd = exec.Command(a.opts.workerBinary)
		cmd.Env = append(os.Environ(), env...)
	}
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	proc, err := podrunner.Start(s.WorkerID, cmd)
	if err != nil {
		logFile.Close()
		return err
	}
	if err := a.pidFile.Record(proc.Cmd.Process.Pid); err != nil {
		log.Warnf("agent: %v", err)
	}
	w := &workerProc{id: s.WorkerID, proc: proc}
	a.mu.Lock()
	a.workers[s.WorkerID] = w
	a.mu.Unlock()
	log.Infof("agent: started worker %s (pid %d)", s.WorkerID, proc.Cmd.Process.Pid)

	go func() {
		err := proc.Wait()
		a.pidFile.Forget(proc.Cmd.Process.Pid)
		w.mu.Lock()
		w.exited = true
		w.err = err
		w.mu.Unlock()
		a.closeWorkerRelays(s.WorkerID)
		log.Infof("agent: worker %s exited: %v", s.WorkerID, err)
	}()
	return nil
}

func (a *agent) killWorker(w *workerProc) {
	w.mu.Lock()
	alreadyExited := w.exited
	w.mu.Unlock()
	if !alreadyExited {
		if a.opts.runtime == "docker" {
			// Stop the container out-of-band first. The rm -f is the
			// backstop: killing the foreground `docker run` CLI alone can
			// orphan the container if the CLI dies before attaching. It runs
			// in its own process group so a signal to the agent's tree (e.g.
			// a SIGTERM broadcast on shutdown) cannot kill it mid-flight.
			rmCmd := exec.Command(a.opts.dockerBin, "rm", "-f", w.id)
			rmCmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
			if err := rmCmd.Run(); err != nil {
				log.Warnf("agent: docker rm -f %s: %v", w.id, err)
			}
		}
		w.proc.Kill()
	}
	a.pidFile.Forget(w.proc.Cmd.Process.Pid)
	a.mu.Lock()
	if cur, ok := a.workers[w.id]; ok && cur == w {
		delete(a.workers, w.id)
	}
	a.mu.Unlock()
	log.Infof("agent: stopped worker %s", w.id)
}

func (a *agent) kill(s *broker.Kill) error {
	a.mu.Lock()
	w, ok := a.workers[s.WorkerID]
	a.mu.Unlock()
	if ok {
		a.killWorker(w)
	}
	return nil
}

// killAll stops every worker (used on broker disconnect and shutdown).
func (a *agent) killAll() {
	a.mu.Lock()
	workers := make([]*workerProc, 0, len(a.workers))
	for _, w := range a.workers {
		workers = append(workers, w)
	}
	a.mu.Unlock()
	for _, w := range workers {
		a.killWorker(w)
	}
}

// statuses snapshots the current workers for a heartbeat.
func (a *agent) statuses() []broker.WorkerState {
	var out []broker.WorkerState
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, w := range a.workers {
		w.mu.Lock()
		st := broker.WorkerState{WorkerID: w.id, Running: !w.exited}
		if w.err != nil {
			st.Error = w.err.Error()
		}
		w.mu.Unlock()
		out = append(out, st)
	}
	return out
}

// handleForward exposes the worker's internal port on this node: the
// daemon-side relay connects to nodeIP:ExternalPort, and we forward to the
// worker's listener (host networking puts it on loopback).
func (a *agent) handleForward(f *broker.Forward) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	key := fmt.Sprintf("%s/%d", f.WorkerID, f.ExternalPort)
	if _, ok := a.relays[key]; ok {
		return nil // already exposed
	}
	relay, err := broker.NewRelay(a.localIP, f.ExternalPort, net.JoinHostPort("127.0.0.1", strconv.Itoa(f.InternalPort)))
	if err != nil {
		return err
	}
	a.relays[key] = relay
	log.Infof("agent: exposing worker %s service on %s:%d -> :%d", f.WorkerID, a.localIP, f.ExternalPort, f.InternalPort)
	return nil
}

func (a *agent) handleUnforward(u *broker.Unforward) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	key := fmt.Sprintf("%s/%d", u.WorkerID, u.ExternalPort)
	if relay, ok := a.relays[key]; ok {
		relay.Close()
		delete(a.relays, key)
		log.Infof("agent: stopped exposing worker %s service on :%d", u.WorkerID, u.ExternalPort)
	}
	return nil
}

// closeWorkerRelays drops every relay a worker owned (it exited).
func (a *agent) closeWorkerRelays(workerID string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	prefix := workerID + "/"
	for key, relay := range a.relays {
		if strings.HasPrefix(key, prefix) {
			relay.Close()
			delete(a.relays, key)
		}
	}
}

// heartbeatLoop reports worker status every HeartbeatInterval.
func (a *agent) heartbeatLoop() {
	t := time.NewTicker(heartbeatInterval)
	defer t.Stop()
	for range t.C {
		if err := a.client.Heartbeat(a.statuses()); err != nil {
			return // connection is dead; the command loop will notice
		}
	}
}

// runSession handles one broker connection: heartbeats + command dispatch.
func (a *agent) runSession(brokerAddr string, target *broker.Target) error {
	hostname, _ := os.Hostname()
	client, err := broker.Connect(brokerAddr, hostname, a.opts.tags)
	if err != nil {
		return err
	}
	a.client = client
	a.mu.Lock()
	a.target = target
	a.localIP = client.LocalIP()
	a.mu.Unlock()
	defer func() {
		client.Close()
		a.mu.Lock()
		a.target = nil
		a.mu.Unlock()
	}()
	log.Infof("agent: registered with broker %s as %s (tags %v)", brokerAddr, client.NodeID, a.opts.tags)

	go a.heartbeatLoop()
	for {
		msg, err := client.Recv()
		if err != nil {
			return err
		}
		switch m := msg.(type) {
		case *broker.Spawn:
			err := a.spawn(m)
			if ackErr := client.Ack(m.CmdID, m.WorkerID, err); ackErr != nil {
				return ackErr
			}
			if err != nil {
				log.Errorf("agent: spawn %s failed: %v", m.WorkerID, err)
			}
		case *broker.Kill:
			err := a.kill(m)
			if ackErr := client.Ack(m.CmdID, m.WorkerID, err); ackErr != nil {
				return ackErr
			}
		case *broker.Forward:
			err := a.handleForward(m)
			if ackErr := client.Ack(m.CmdID, m.WorkerID, err); ackErr != nil {
				return ackErr
			}
			if err != nil {
				log.Errorf("agent: forward %s failed: %v", m.WorkerID, err)
			}
		case *broker.Unforward:
			err := a.handleUnforward(m)
			if ackErr := client.Ack(m.CmdID, m.WorkerID, err); ackErr != nil {
				return ackErr
			}
		default:
			log.Warnf("agent: unexpected command %T", msg)
		}
	}
}

func main() {
	log.SetLevel(log.InfoLevel)
	opts, err := parseOptions()
	if err != nil {
		fmt.Fprintf(os.Stderr, "agent: %v\n", err)
		os.Exit(1)
	}
	for _, d := range []string{opts.localDir, filepath.Join(opts.localDir, "logs"), filepath.Join(opts.localDir, "workers")} {
		if err := os.MkdirAll(d, 0777); err != nil {
			fmt.Fprintf(os.Stderr, "agent: could not create %s: %v\n", d, err)
			os.Exit(1)
		}
	}
	a := newAgent(opts)
	// Reap workers orphaned by a previous agent run.
	a.pidFile.Reap()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		a.killAll()
		os.Exit(0)
	}()

	// Reconnect forever; a broker restart must not take the node down.
	for {
		target, err := resolveTarget(opts)
		if err != nil {
			log.Errorf("agent: %v; retrying in %s", err, reconnectDelay)
			time.Sleep(reconnectDelay)
			continue
		}
		brokerAddr := opts.brokerAddr
		if brokerAddr == "" {
			brokerAddr = target.BrokerAddress()
		}
		if err := a.runSession(brokerAddr, target); err != nil {
			log.Errorf("agent: broker session ended: %v; reconnecting in %s", err, reconnectDelay)
		}
		// Kubelet semantics: if the broker connection is gone, pachd cannot
		// see or command us, so our workers must not linger (pachd will
		// re-place the work elsewhere once it notices).
		a.killAll()
		time.Sleep(reconnectDelay)
	}
}
