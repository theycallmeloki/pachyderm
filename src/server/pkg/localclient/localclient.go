// Package localclient provides an in-process stand-in for the Kubernetes API
// that pachd uses to manage pipeline workers. It is used by local
// (single-node, k8s-free) deployments so that the PPS master can keep its
// existing ReplicationController-based reconciliation logic while workers are
// actually plain processes on the host.
//
// The public surface is a kubernetes.Interface whose CoreV1 group is backed by
// a Runtime:
//
//   - ReplicationControllers are stored in memory. Creating/updating one with
//     N replicas spawns/kills N worker processes; deleting one kills them all.
//   - Pods mirror the spawned worker processes (name, IP, phase, labels).
//     Watching pods emits ADDED/MODIFIED/DELETED events as processes start,
//     crash (they are restarted, like RestartPolicy: Always), or are killed.
//     Getting logs streams the captured stdout/stderr of the worker process.
//   - Services and Secrets are stored in memory with label-selector support.
//   - Nodes reports a single node, so COEFFICIENT parallelism resolves to the
//     configured constant.
//
// # Suite gating (src/server/pachyderm_test.go vs local mode)
//
// The 165-test suite runs against local mode (docker-mode workers restore
// absolute /pfs paths), and the full-suite baseline is green in one
// invocation. Pipeline services work end to end: the daemon records k8s
// Service/RC/Pod objects it creates (served from the HTTP API for the test
// shim's tu.GetKubeClient) and forwards each pipeline service's external
// port to its worker's container port on loopback (the local stand-in for a
// k8s NodePort service). Pod specs served to tests carry the full pod
// template, so pod-reflection tests see what the controller rendered.
//
// Remaining permanently gated tests, each skipped via tu.LocalMode():
//
//   - pachd restart semantics: TestMissingPipelineSpec,
//     TestNoOutputRepoDoesntCrashPPSMaster (they delete the pachd pod to
//     restart it; local mode has no pod to delete).
//   - k8s scheduling semantics: TestPipelineCrashing (GPU), TestCrashingToStandby.
//   - auth: TestSecretsUnauthenticated, TestLokiLogs (loki logging is not
//     configured in local mode), and the spout auth subtests (auth disabled in
//     local mode). Enterprise has been removed from the fork entirely.
//   - environment: TestCronPipeline (upstream bug: cron file paths embed the
//     machine timezone offset, which PFS path validation rejects),
//     TestSystemResourceRequests (asserts pachd/etcd deployment pods, which
//     local mode does not deploy).
//
// RUN_BAD_TESTS-gated upstream (these all run and pass in local mode when
// RUN_BAD_TESTS is set): TestPipelineFailure, TestService, TestGetLogsWithStats,
// TestChainedPipelinesNoDelay, TestPipelineWithStats*, TestListJobOutput,
// TestUpdateFailedPipeline, TestDatumStatusRestart, TestGarbageCollection,
// TestPipelineVersions, TestDeferredProcessing, TestPipelineHistory,
// TestKeepRepo, TestStatsDeleteAll, TestPipelineWithGitInputMultiPipeline*.
//
// All other API groups are served by the client-go fake clientset.
package localclient

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	log "github.com/sirupsen/logrus"

	autoscalingv1 "k8s.io/api/autoscaling/v1"
	v1 "k8s.io/api/core/v1"
	kubeerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"

	"github.com/pachyderm/pachyderm/src/client"
	"github.com/pachyderm/pachyderm/src/client/pkg/errors"
	"github.com/pachyderm/pachyderm/src/server/pkg/broker"
	"github.com/pachyderm/pachyderm/src/server/pkg/podrunner"
)

// Options configures a Runtime.
type Options struct {
	// WorkerBinary is the path to the pachyderm worker binary that will be
	// spawned for each worker replica.
	WorkerBinary string
	// PachctlBinary is an optional pachctl binary mounted into spout worker
	// containers at /pach-bin/pachctl (the k8s init container provides it
	// there); spout user code calls `pachctl` to write output.
	PachctlBinary string
	// EtcdHost/EtcdPort are where pachd's etcd can be reached; they are
	// injected into worker processes as ETCD_SERVICE_HOST/PORT.
	EtcdHost string
	EtcdPort string
	// PachdPort is the pachd API port; worker processes are pointed at
	// 127.0.0.1:PachdPort via PEER_PORT.
	PachdPort uint16
	// StorageRoot is the daemon's object-store root (PACH_ROOT). Workers get
	// this as their PACH_ROOT too: like the shared hostpath in a local k8s
	// deployment, it is where hashtree chunk indexes are written by the
	// workers' direct object-store clients.
	StorageRoot string
	// WorkerPort is the gRPC port that all worker processes listen on (each
	// worker binds its own loopback IP, so the port can be shared).
	WorkerPort uint16
	// LocalDir is the daemon's data directory. Worker scratch space and logs
	// live under it.
	LocalDir string
	// Runtime selects how workers are run: "process" (default) spawns the
	// worker binary directly on the host; "docker" runs each worker as a
	// container of the pipeline's image with its scratch mounted at /pfs,
	// restoring k8s-style absolute /pfs paths in user code.
	Runtime string
	// DaemonPodName is the name local mode uses for the pachd "pod" (from
	// PACHD_POD_NAME). The debug server and unqualified log queries look
	// pods up under this name.
	DaemonPodName string
	// DaemonHTTPPort is the port the daemon's HTTP API listens on (local mode
	// remaps it into the 306xx range). The runtime forwards the pachd service
	// port 652 (the k8s in-cluster HTTP port) to it so that user code inside
	// workers can reach pachd by its cluster DNS name.
	DaemonHTTPPort int
	// Broker is the node broker (see src/server/pkg/broker). When set, worker
	// replicas whose pod template carries a PACH_NODE_TAG env var are placed
	// on a registered agent serving that tag instead of running locally.
	// Nil disables remote placement.
	Broker *broker.Server
}

// Clientset is a kubernetes.Interface whose CoreV1 resources are backed by a
// Runtime.
type Clientset struct {
	*fake.Clientset
	runtime *Runtime
}

// CoreV1 returns the locally-backed core API group.
func (c *Clientset) CoreV1() v1core.CoreV1Interface {
	return &coreV1{
		CoreV1Interface: c.Clientset.CoreV1(),
		runtime:         c.runtime,
	}
}

// Runtime is the in-memory + process-backed store behind Clientset.
type Runtime struct {
	opts Options

	mu        sync.Mutex
	rcs       map[string]*rcState
	services  map[string]*v1.Service
	secrets   map[string]*v1.Secret
	nextIP    int
	nextPod   int
	watchers  map[int]chan watch.Event
	nextWatch int

	// svcForwards maps "ns/name" of a NodePort pipeline service to the TCP
	// forward that exposes its container port on the host's loopback
	// interface (the local stand-in for the k8s NodePort service).
	svcForwards map[string]io.Closer

	// svcProxyName is the docker container running the pachd HTTP service
	// proxy (port 652 -> daemon HTTP), killed on Close.
	svcProxyName string

	logSrv *httptest.Server

	// dockerBin is the resolved docker CLI path, set when Runtime is docker.
	dockerBin string
}

type rcState struct {
	rc   *v1.ReplicationController
	ns   string
	pods []*podState
}

type podState struct {
	name    string
	ip      string
	cmd     *exec.Cmd
	logPath string
	group   *rcState

	// remote is set when this worker runs on a broker agent (see
	// Options.Broker) rather than as a local process. Remote pods have no
	// local cmd; their lifecycle is driven by broker node reports via
	// HandleRemoteWorkerExit.
	remote bool

	// spawnFailed is set when the pod could not be started at all (as
	// opposed to started and then exited); it selects the failure reason
	// the pipeline controller recognizes (CreateContainerConfigError).
	spawnFailed bool

	mu       sync.Mutex
	exitErr  error
	restarts int
}

// pidFilePath is where the runtime records the PIDs of the worker processes
// it spawns, so that a restarted daemon can reap workers orphaned by a crash.
func (rt *Runtime) pidFilePath() string {
	return filepath.Join(rt.opts.LocalDir, "worker-pids")
}

// NewRuntime creates a Runtime and starts its log server. It first reaps any
// worker processes left over from a previous daemon run (recorded in the
// pid file), so that loopback IPs and ports are free again.
func NewRuntime(opts Options) (*Runtime, error) {
	if opts.WorkerBinary == "" {
		return nil, errors.New("localclient: WorkerBinary is required")
	}
	if opts.Runtime == "" {
		opts.Runtime = "process"
	}
	if opts.Runtime != "process" && opts.Runtime != "docker" {
		return nil, errors.Errorf("localclient: unknown Runtime %q (want \"process\" or \"docker\")", opts.Runtime)
	}
	rt := &Runtime{
		opts:        opts,
		rcs:         make(map[string]*rcState),
		services:    make(map[string]*v1.Service),
		secrets:     make(map[string]*v1.Secret),
		nextIP:      2, // 127.0.0.1 is the daemon itself; workers start at 127.0.0.2
		watchers:    make(map[int]chan watch.Event),
		svcForwards: make(map[string]io.Closer),
	}
	if opts.Runtime == "docker" {
		bin, err := exec.LookPath("docker")
		if err != nil {
			return nil, errors.Wrap(err, "localclient: docker runtime requires the docker CLI")
		}
		rt.dockerBin = bin
	}
	for _, dir := range []string{
		filepath.Join(opts.LocalDir, "workers"),
		filepath.Join(opts.LocalDir, "logs"),
	} {
		if err := os.MkdirAll(dir, 0777); err != nil {
			return nil, errors.Wrapf(err, "could not create %q", dir)
		}
	}
	// Reap leftovers from a previous daemon run BEFORE starting any new
	// containers: the reap removes every pach-local=svc container, which
	// would otherwise kill the service proxy started below.
	rt.reapOrphanedWorkers()
	// Seed a synthetic "pachd" pod so that unqualified log queries (GetLogs
	// with no pipeline/job, i.e. `pachctl logs`) resolve to the daemon.
	pachdLog := filepath.Join(opts.LocalDir, "logs", "pachd.jsonl")
	if f, err := os.OpenFile(pachdLog, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644); err == nil {
		f.Close()
	}
	pachdRC := &rcState{
		rc: &v1.ReplicationController{
			ObjectMeta: metav1.ObjectMeta{Name: "pachd"},
			Spec: v1.ReplicationControllerSpec{
				Template: &v1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{"app": "pachd", "suite": "pachyderm"},
					},
				},
			},
		},
		ns: "default",
	}
	pachdRC.pods = []*podState{{
		name:    "pachd",
		ip:      "127.0.0.1",
		logPath: pachdLog,
		group:   pachdRC,
	}}
	rt.rcs["default/pachd"] = pachdRC
	// Seed the pachd Service (k8s HTTP port 652) and make it reachable:
	// user code that dials pachd.default.svc.cluster.local:652 (resolved by
	// the worker's --add-host entry to loopback) lands on the daemon's HTTP
	// API. Binding 652 directly fails on hosts that block unprivileged
	// binds below 1024 (why local mode remaps ports to the 306xx range), so
	// a root container relays it to the remapped port.
	if opts.DaemonHTTPPort != 0 && opts.DaemonHTTPPort != 652 {
		rt.services["default/pachd"] = &v1.Service{
			ObjectMeta: metav1.ObjectMeta{Name: "pachd", Namespace: "default"},
			Spec: v1.ServiceSpec{
				ClusterIP: "127.0.0.1",
				Ports:     []v1.ServicePort{{Name: "http", Port: 652}},
			},
		}
		rt.startPachdServiceProxy()
	}
	// The debug server reads the daemon's own logs under its pod name
	// (PACHD_POD_NAME); mirror it as a second pod sharing the same file.
	if opts.DaemonPodName != "" && opts.DaemonPodName != "pachd" {
		daemonLog := filepath.Join(opts.LocalDir, "logs", opts.DaemonPodName+".jsonl")
		if f, err := os.OpenFile(daemonLog, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644); err == nil {
			f.Close()
		}
		daemonRC := &rcState{
			rc: &v1.ReplicationController{
				ObjectMeta: metav1.ObjectMeta{Name: opts.DaemonPodName},
				Spec: v1.ReplicationControllerSpec{
					Template: &v1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{"app": opts.DaemonPodName, "suite": "pachyderm"},
						},
					},
				},
			},
			ns: "default",
		}
		daemonRC.pods = []*podState{{
			name:    opts.DaemonPodName,
			ip:      "127.0.0.1",
			logPath: daemonLog,
			group:   daemonRC,
		}}
		rt.rcs["default/"+opts.DaemonPodName] = daemonRC
	}
	rt.logSrv = httptest.NewServer(http.HandlerFunc(rt.handleLogs))
	return rt, nil
}

// pachdServiceProxyImage runs the pachd HTTP service proxy (k8s port 652 ->
// the daemon's remapped HTTP port) as a root container, since unprivileged
// binds below 1024 are blocked on many hosts. It must be present locally;
// docker-mode workers already require local images.
const pachdServiceProxyImage = "python:latest"

// pachdServiceProxyScript is a minimal TCP relay; args: <listen> <target>.
const pachdServiceProxyScript = `import socket, threading, sys
srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
srv.bind(('0.0.0.0', int(sys.argv[1])))
srv.listen(32)
def relay(c):
    try:
        u = socket.create_connection(('127.0.0.1', int(sys.argv[2])), timeout=10)
    except Exception:
        c.close(); return
    def pump(src, dst):
        try:
            while True:
                d = src.recv(65536)
                if not d: break
                dst.sendall(d)
        except Exception:
            pass
        finally:
            try: dst.shutdown(socket.SHUT_WR)
            except Exception: pass
    threading.Thread(target=pump, args=(c, u), daemon=True).start()
    threading.Thread(target=pump, args=(u, c), daemon=True).start()
while True:
    conn, _ = srv.accept()
    threading.Thread(target=relay, args=(conn,), daemon=True).start()
`

// startPachdServiceProxy launches the pachd HTTP service proxy container
// (only possible in docker mode: process-mode workers run unprivileged and
// cannot bind port 652).
func (rt *Runtime) startPachdServiceProxy() {
	if rt.opts.Runtime != "docker" {
		log.Warnf("localclient: pachd service DNS (port 652) cannot be proxied in process mode")
		return
	}
	if out, err := exec.Command(rt.dockerBin, "image", "inspect", pachdServiceProxyImage).CombinedOutput(); err != nil {
		log.Warnf("localclient: %s not available (%v); pachd service DNS (port 652) will not resolve: %s",
			pachdServiceProxyImage, err, strings.TrimSpace(string(out)))
		return
	}
	scriptDir := filepath.Join(rt.opts.LocalDir, "svc")
	if err := os.MkdirAll(scriptDir, 0777); err != nil {
		log.Warnf("localclient: could not create %q: %v", scriptDir, err)
		return
	}
	// Unique per daemon: a stale file or directory left by a previous run
	// (e.g. docker auto-creating the mount source as root) must not block
	// this run's proxy.
	scriptPath := filepath.Join(scriptDir, fmt.Sprintf("proxy-%d.py", os.Getpid()))
	if err := ioutil.WriteFile(scriptPath, []byte(pachdServiceProxyScript), 0644); err != nil {
		log.Warnf("localclient: could not write service proxy script: %v", err)
		return
	}
	name := fmt.Sprintf("pachd-http-svc-%d", os.Getpid())
	cmd := exec.Command(rt.dockerBin, "run", "-d", "--rm", "--network", "host",
		"--name", name, "--label", "pach-local=svc",
		"-v", scriptPath+":/proxy.py:ro",
		pachdServiceProxyImage, "python", "/proxy.py",
		strconv.Itoa(652), strconv.Itoa(rt.opts.DaemonHTTPPort))
	if out, err := cmd.CombinedOutput(); err != nil {
		log.Warnf("localclient: could not start pachd service proxy: %v: %s", err, strings.TrimSpace(string(out)))
		return
	}
	rt.svcProxyName = name
	log.Infof("localclient: pachd HTTP service proxy listening on 652 (-> %d)", rt.opts.DaemonHTTPPort)
}

// reapOrphanedWorkers kills worker processes from a previous daemon run and
// clears the pid file.
func (rt *Runtime) reapOrphanedWorkers() {
	data, err := ioutil.ReadFile(rt.pidFilePath())
	if err != nil {
		os.Remove(rt.pidFilePath()) // ignore errors
		return
	}
	os.Remove(rt.pidFilePath())
	for _, line := range strings.Split(string(data), "\n") {
		pid, err := strconv.Atoi(strings.TrimSpace(line))
		if err != nil || pid <= 0 {
			continue
		}
		// Only kill processes that look like our workers (the pid file is
		// local to this daemon, but be conservative).
		if cmdline, err := ioutil.ReadFile(fmt.Sprintf("/proc/%d/cmdline", pid)); err == nil &&
			strings.Contains(string(cmdline), "worker") {
			syscall.Kill(pid, syscall.SIGKILL)
			log.Infof("localclient: reaped orphaned worker %d", pid)
		}
	}
	// In docker mode, a hard-killed daemon also leaves containers behind (the
	// docker CLI wrapper is dead, so the pid file cannot name them); remove
	// every container we have ever labelled.
	if rt.opts.Runtime == "docker" {
		for _, label := range []string{"pach-local=worker", "pach-local=svc"} {
			out, err := exec.Command(rt.dockerBin, "ps", "-aq", "--filter", "label="+label).Output()
			if err != nil {
				log.Warnf("localclient: could not list orphaned containers: %v", err)
				continue
			}
			for _, id := range strings.Fields(string(out)) {
				if err := exec.Command(rt.dockerBin, "rm", "-f", id).Run(); err != nil {
					log.Warnf("localclient: could not remove orphaned container %s: %v", id, err)
				}
			}
		}
	}
}

// recordPID appends a worker PID to the pid file.
func (rt *Runtime) recordPID(pid int) {
	f, err := os.OpenFile(rt.pidFilePath(), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Warnf("localclient: could not record worker pid: %v", err)
		return
	}
	fmt.Fprintf(f, "%d\n", pid)
	f.Close()
}

func (rt *Runtime) forgetPID(pid int) {
	data, err := ioutil.ReadFile(rt.pidFilePath())
	if err != nil {
		return
	}
	var lines []string
	for _, line := range strings.Split(string(data), "\n") {
		if line != strconv.Itoa(pid) {
			lines = append(lines, line)
		}
	}
	ioutil.WriteFile(rt.pidFilePath(), []byte(strings.Join(lines, "\n")), 0644)
}

// allocIP returns the next free 127.0.0.x address, probing that nothing else
// is already bound to ip:workerPort. Loopback addresses are released when
// workers die, so once the counter passes 254 it wraps back to .2: the probe
// skips any address a live worker still holds.
func (rt *Runtime) allocIP() string {
	for {
		if rt.nextIP > 254 {
			rt.nextIP = 2
		}
		ip := fmt.Sprintf("127.0.0.%d", rt.nextIP)
		rt.nextIP++
		ln, err := net.Listen("tcp", net.JoinHostPort(ip, strconv.Itoa(int(rt.opts.WorkerPort))))
		if err == nil {
			ln.Close()
			return ip
		}
	}
}

// NewClientset returns a kubernetes.Interface backed by rt.
func NewClientset(rt *Runtime) *Clientset {
	return &Clientset{
		Clientset: fake.NewSimpleClientset(),
		runtime:   rt,
	}
}

// Close shuts down the runtime, killing all workers and the log server.
func (rt *Runtime) Close() {
	if rt.svcProxyName != "" {
		if err := exec.Command(rt.dockerBin, "kill", rt.svcProxyName).Run(); err != nil {
			log.Warnf("localclient: could not stop pachd service proxy %s: %v", rt.svcProxyName, err)
		}
		rt.svcProxyName = ""
	}
	rt.mu.Lock()
	for key, f := range rt.svcForwards {
		delete(rt.svcForwards, key)
		f.Close()
	}
	rt.mu.Unlock()
	rt.mu.Lock()
	rcs := make([]*rcState, 0, len(rt.rcs))
	for _, g := range rt.rcs {
		rcs = append(rcs, g)
	}
	rt.mu.Unlock()
	for _, g := range rcs {
		rt.deleteRC(g.ns, g.rc.Name)
	}
	if rt.logSrv != nil {
		rt.logSrv.Close()
	}
}

func (rt *Runtime) serverURL() *url.URL {
	u, _ := url.Parse(rt.logSrv.URL)
	return u
}

// kubeListPrefix is the path prefix for the local-mode k8s object store,
// served by the daemon's HTTP API so that suite tests running in a separate
// process (via tu.GetKubeClient) can reflect on the objects the daemon
// actually created: services, replication controllers, and pods.
const kubeListPrefix = "/v1/local/kube/"

// KubeHTTPHandler wraps an HTTP handler (the pachd HTTP API) with the
// local-mode k8s object store. GETs under kubeListPrefix are served from the
// runtime's in-memory stores; the scale subresource under the same prefix is
// writable so the autoscaling monitor and suite tests can change an RC's
// replica count (which reconcileLocked turns into worker spawns/kills).
// Everything else passes through to inner.
func (rt *Runtime) KubeHTTPHandler(inner http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, kubeListPrefix) {
			inner.ServeHTTP(w, r)
			return
		}
		if r.Method == http.MethodPost {
			rt.serveScale(w, r)
			return
		}
		if r.Method != http.MethodGet {
			inner.ServeHTTP(w, r)
			return
		}
		kind := strings.TrimPrefix(r.URL.Path, kubeListPrefix)
		ns := r.URL.Query().Get("namespace")
		if ns == "" {
			ns = "default"
		}
		opts := metav1.ListOptions{LabelSelector: r.URL.Query().Get("labelSelector")}
		var (
			obj interface{}
			err error
		)
		switch kind {
		case "services":
			obj, err = rt.listServices(ns, opts)
		case "rcs":
			obj, err = rt.listRCs(ns, opts)
		case "pods":
			obj, err = rt.listPods(ns, opts)
		default:
			http.NotFound(w, r)
			return
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(obj); err != nil {
			log.Warnf("localclient: could not encode kube %s list: %v", kind, err)
		}
	})
}

// serveScale handles POST /v1/local/kube/rcs/<name>/scale with a JSON body of
// {"replicas": N}, mirroring the k8s Scale subresource. It is used by the
// suite's autoscaling test to drive replica counts from outside the daemon.
func (rt *Runtime) serveScale(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, kubeListPrefix)
	parts := strings.Split(rest, "/")
	if len(parts) != 3 || parts[0] != "rcs" || parts[2] != "scale" {
		http.NotFound(w, r)
		return
	}
	var req struct {
		Replicas int32 `json:"replicas"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	rc, err := rt.getRC(r.URL.Query().Get("namespace"), parts[1])
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	rc.Spec.Replicas = &req.Replicas
	updated, err := rt.updateRC(r.URL.Query().Get("namespace"), rc)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(rcToScale(updated)); err != nil {
		log.Warnf("localclient: could not encode scale response: %v", err)
	}
}

//////////////////////////////////////////////////////////////////////////////
// CoreV1 overrides

type coreV1 struct {
	v1core.CoreV1Interface
	runtime *Runtime
}

func (c *coreV1) ReplicationControllers(ns string) v1core.ReplicationControllerInterface {
	return &rcs{
		ReplicationControllerInterface: c.CoreV1Interface.ReplicationControllers(ns),
		runtime:                        c.runtime,
		ns:                             ns,
	}
}

func (c *coreV1) Pods(ns string) v1core.PodInterface {
	return &pods{
		PodInterface: c.CoreV1Interface.Pods(ns),
		runtime:      c.runtime,
		ns:           ns,
	}
}

func (c *coreV1) Nodes() v1core.NodeInterface {
	return &nodes{
		NodeInterface: c.CoreV1Interface.Nodes(),
		runtime:       c.runtime,
	}
}

func (c *coreV1) Services(ns string) v1core.ServiceInterface {
	return &services{
		ServiceInterface: c.CoreV1Interface.Services(ns),
		runtime:          c.runtime,
		ns:               ns,
	}
}

func (c *coreV1) Secrets(ns string) v1core.SecretInterface {
	return &secrets{
		SecretInterface: c.CoreV1Interface.Secrets(ns),
		runtime:         c.runtime,
		ns:              ns,
	}
}

//////////////////////////////////////////////////////////////////////////////
// ReplicationControllers

type rcs struct {
	v1core.ReplicationControllerInterface
	runtime *Runtime
	ns      string
}

func (r *rcs) Create(rc *v1.ReplicationController) (*v1.ReplicationController, error) {
	return r.runtime.createRC(r.ns, rc)
}

func (r *rcs) Update(rc *v1.ReplicationController) (*v1.ReplicationController, error) {
	return r.runtime.updateRC(r.ns, rc)
}

func (r *rcs) Delete(name string, opts *metav1.DeleteOptions) error {
	return r.runtime.deleteRC(r.ns, name)
}

func (r *rcs) List(opts metav1.ListOptions) (*v1.ReplicationControllerList, error) {
	return r.runtime.listRCs(r.ns, opts)
}

// GetScale reports the RC's current replica count (the autoscaling monitor
// reads it before deciding to scale up).
func (r *rcs) GetScale(name string, _ metav1.GetOptions) (*autoscalingv1.Scale, error) {
	rc, err := r.runtime.getRC(r.ns, name)
	if err != nil {
		return nil, err
	}
	return rcToScale(rc), nil
}

// UpdateScale sets the RC's desired replica count; reconcileLocked spawns or
// kills worker processes to match, so autoscaling genuinely changes the
// number of local workers.
func (r *rcs) UpdateScale(name string, scale *autoscalingv1.Scale) (*autoscalingv1.Scale, error) {
	rc, err := r.runtime.getRC(r.ns, name)
	if err != nil {
		return nil, err
	}
	rc.Spec.Replicas = &scale.Spec.Replicas
	if _, err := r.runtime.updateRC(r.ns, rc); err != nil {
		return nil, err
	}
	return rcToScale(rc), nil
}

func rcToScale(rc *v1.ReplicationController) *autoscalingv1.Scale {
	replicas := int32(0)
	if rc.Spec.Replicas != nil {
		replicas = *rc.Spec.Replicas
	}
	return &autoscalingv1.Scale{
		ObjectMeta: metav1.ObjectMeta{Name: rc.Name, Namespace: rc.Namespace},
		Spec:       autoscalingv1.ScaleSpec{Replicas: replicas},
	}
}

func (rt *Runtime) createRC(ns string, rc *v1.ReplicationController) (*v1.ReplicationController, error) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	key := ns + "/" + rc.Name
	if _, ok := rt.rcs[key]; ok {
		return nil, kubeerrors.NewAlreadyExists(schema.GroupResource{Group: "", Resource: "replicationcontrollers"}, rc.Name)
	}
	g := &rcState{
		rc: rc.DeepCopy(),
		ns: ns,
	}
	rt.rcs[key] = g
	rt.reconcileLocked(g)
	return rt.rcs[key].rc.DeepCopy(), nil
}

func (rt *Runtime) getRC(ns string, name string) (*v1.ReplicationController, error) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	key := ns + "/" + name
	g, ok := rt.rcs[key]
	if !ok {
		return nil, kubeerrors.NewNotFound(schema.GroupResource{Group: "", Resource: "replicationcontrollers"}, name)
	}
	return g.rc.DeepCopy(), nil
}

func (rt *Runtime) updateRC(ns string, rc *v1.ReplicationController) (*v1.ReplicationController, error) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	key := ns + "/" + rc.Name
	g, ok := rt.rcs[key]
	if !ok {
		return nil, kubeerrors.NewNotFound(schema.GroupResource{Group: "", Resource: "replicationcontrollers"}, rc.Name)
	}
	g.rc = rc.DeepCopy()
	rt.reconcileLocked(g)
	return g.rc.DeepCopy(), nil
}

func (rt *Runtime) deleteRC(ns string, name string) error {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	key := ns + "/" + name
	g, ok := rt.rcs[key]
	if !ok {
		return nil
	}
	delete(rt.rcs, key)
	for _, p := range g.pods {
		rt.killPodLocked(g, p)
	}
	return nil
}

func (rt *Runtime) listRCs(ns string, opts metav1.ListOptions) (*v1.ReplicationControllerList, error) {
	sel, err := labels.Parse(opts.LabelSelector)
	if err != nil {
		return nil, errors.Wrapf(err, "invalid label selector %q", opts.LabelSelector)
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	list := &v1.ReplicationControllerList{}
	for _, g := range rt.rcs {
		if g.ns != ns || !sel.Matches(labels.Set(g.rc.Labels)) {
			continue
		}
		list.Items = append(list.Items, *g.rc.DeepCopy())
	}
	return list, nil
}

// reconcileLocked spawns or kills worker processes so that the RC has exactly
// the desired number of replicas. Callers must hold rt.mu.
func (rt *Runtime) reconcileLocked(g *rcState) {
	want := 0
	if g.rc.Spec.Replicas != nil {
		want = int(*g.rc.Spec.Replicas)
	}
	for len(g.pods) < want {
		if err := rt.spawnPodLocked(g); err != nil {
			log.Errorf("localclient: could not spawn worker for %q: %v", g.rc.Name, err)
			return
		}
	}
	for len(g.pods) > want {
		p := g.pods[len(g.pods)-1]
		rt.killPodLocked(g, p)
	}
}

// workerEnv computes the environment for a worker replica, overriding the k8s
// Downward API and k8s-injected service env vars with local values. In docker
// mode the scratch dir is mounted at /pfs and the daemon's storage root at
// /pach, and the worker runs with rootPath "/" exactly as in a k8s pod.
func (rt *Runtime) workerEnv(g *rcState, name, ip string) ([]string, error) {
	template := &g.rc.Spec.Template.Spec
	var userEnv []v1.EnvVar
	for _, c := range template.Containers {
		if c.Name == client.PPSWorkerUserContainerName {
			userEnv = c.Env
		}
	}
	env, err := rt.resolveEnv(g.ns, name, ip, userEnv)
	if err != nil {
		return nil, err
	}
	env = setEnv(env, "PPS_WORKER_IP", ip)
	env = setEnv(env, "PPS_POD_NAME", name)
	env = setEnv(env, "PPS_WORKER_GRPC_PORT", strconv.Itoa(int(rt.opts.WorkerPort)))
	env = setEnv(env, "PEER_PORT", strconv.Itoa(int(rt.opts.PachdPort)))
	env = setEnv(env, "ETCD_SERVICE_HOST", rt.opts.EtcdHost)
	env = setEnv(env, "ETCD_SERVICE_PORT", rt.opts.EtcdPort)
	if rt.opts.Runtime == "docker" {
		env = setEnv(env, "PACH_WORKER_ROOT", "/")
		// The cache must live outside /pfs: the worker unlinks every entry of
		// the input dir between datums (k8s keeps the cache on a separate
		// emptyDir volume for the same reason).
		env = setEnv(env, "PACH_CACHE_ROOT", "/pach-cache")
		// PACH_ROOT must be the same absolute path the daemon uses: the
		// worker sends block paths to the daemon's object API verbatim
		// (DirectObjWriter), and the local backend resolves them as-is.
		env = setEnv(env, "PACH_ROOT", rt.opts.StorageRoot)
	} else {
		scratch := filepath.Join(rt.opts.LocalDir, "workers", name)
		env = setEnv(env, "PACH_WORKER_ROOT", scratch)
		env = setEnv(env, "PACH_CACHE_ROOT", filepath.Join(scratch, "cache"))
		env = setEnv(env, "PACH_ROOT", rt.opts.StorageRoot)
	}
	env = append(env, "PACH_LOCAL_WORKER=1")
	// Spout pipelines run `pachctl` in the user container; point pachctl at
	// the config secret mounted at /pachctl (the driver appends /pach-bin to
	// the user code's PATH so the binary is findable).
	if spoutPod(g) {
		env = setEnv(env, "PACH_CONFIG", "/pachctl/config.json")
	}
	return env, nil
}

// spoutPod reports whether the pod template belongs to a spout pipeline,
// detected by the pachctl config secret the pps master adds to spout RCs.
func spoutPod(g *rcState) bool {
	for _, vol := range g.rc.Spec.Template.Spec.Volumes {
		if vol.Secret != nil && strings.HasPrefix(vol.Secret.SecretName, "spout-pachctl-secret-") {
			return true
		}
	}
	return false
}

// workerCmd builds the command that runs one worker replica. In process mode
// it is the worker binary itself; in docker mode it is `docker run` with the
// pipeline's image, the scratch dir mounted at /pfs, the daemon's storage
// root at /pach, and the worker binary at /pach-bin/worker.
func (rt *Runtime) workerCmd(g *rcState, name, ip string, env []string) (*exec.Cmd, error) {
	if rt.opts.Runtime != "docker" {
		return exec.Command(rt.opts.WorkerBinary), nil
	}
	image := ""
	for _, c := range g.rc.Spec.Template.Spec.Containers {
		if c.Name == client.PPSWorkerUserContainerName {
			image = c.Image
		}
	}
	if image == "" {
		return nil, errors.Errorf("localclient: no image found for worker %q", name)
	}
	scratch := filepath.Join(rt.opts.LocalDir, "workers", name)
	// The cache mount must NOT be nested inside the /pfs mount: the worker
	// unlinks every entry of the input dir between datums (except .scratch),
	// which would empty a nested cache. It lives alongside workers/ instead.
	cacheDir := filepath.Join(rt.opts.LocalDir, "cache", name)
	args := []string{
		"run", "--rm",
		"--name", name,
		"--network", "host",
		// Map the k8s pachd service DNS name to loopback so user code that
		// talks to pachd by its in-cluster hostname (e.g. the reprocess-spec
		// test, which wgets pachd over HTTP) works in local mode. The worker
		// shares the host network, and docker resolves the name via a
		// container-scoped /etc/hosts entry without touching the host's.
		"--add-host", fmt.Sprintf("pachd.%s.svc.cluster.local:127.0.0.1", g.ns),
		// Run as the image's default user (usually root): the worker
		// resolves Transform.User against the container's /etc/passwd and
		// drops to that user (chowning inputs) around user code, which
		// requires the worker itself to be root. Root also bypasses the
		// 0700 daemon-owned storage and scratch dirs.
		"--label", "pach-local=worker",
		"-v", scratch + ":/pfs",
		"-v", cacheDir + ":/pach-cache",
		// Bind the daemon's storage root at its real path so that absolute
		// block paths (PACH_ROOT/block/...) resolve identically in the
		// container and on the daemon host.
		"-v", rt.opts.StorageRoot + ":" + rt.opts.StorageRoot,
		"-v", rt.opts.WorkerBinary + ":/pach-bin/worker:ro",
	}
	// The worker's TLS stack (e.g. go-git cloning git inputs) needs a
	// current CA bundle; base images ship stale or no roots.
	if _, err := os.Stat("/etc/ssl/certs/ca-certificates.crt"); err == nil {
		args = append(args, "-v", "/etc/ssl/certs/ca-certificates.crt:/etc/ssl/certs/ca-certificates.crt:ro")
	}
	// The worker recovers the image's ENTRYPOINT/USER/WORKDIR for empty
	// transform fields via docker inspection, exactly like a k8s worker pod
	// (which has the dockerd sidecar's socket); mount the host socket so the
	// same fallback works here.
	if _, err := os.Stat("/var/run/docker.sock"); err == nil {
		args = append(args, "-v", "/var/run/docker.sock:/var/run/docker.sock")
	}
	// Secret volume mounts (transform.Secrets with a MountPath): materialize
	// each secret's data as files and bind them read-only into the container.
	// The storage-backend secret is deployment plumbing, not a pipeline
	// secret - skip it (it is never created in local mode). Spout pipelines
	// additionally get the pachctl config secret (created by the pps master)
	// mounted at /pachctl, and the pachctl binary at /pach-bin/pachctl so the
	// user code can run `pachctl` (the k8s init container does the same).
	podSpec := g.rc.Spec.Template.Spec
	isSpout := false
	var userMounts []v1.VolumeMount
	for _, c := range podSpec.Containers {
		if c.Name == client.PPSWorkerUserContainerName {
			userMounts = c.VolumeMounts
		}
	}
	for _, vm := range userMounts {
		for _, vol := range podSpec.Volumes {
			if vol.Name != vm.Name || vol.Secret == nil {
				continue
			}
			if vol.Secret.SecretName == client.StorageSecretName {
				continue
			}
			isSpout = isSpout || strings.HasPrefix(vol.Secret.SecretName, "spout-pachctl-secret-")
			hostDir := filepath.Join(rt.opts.LocalDir, "secrets", vol.Secret.SecretName)
			if err := rt.materializeSecret(g.ns, vol.Secret.SecretName, hostDir); err != nil {
				return nil, err
			}
			args = append(args, "-v", hostDir+":"+vm.MountPath+":ro")
		}
	}
	if isSpout && rt.opts.PachctlBinary != "" {
		args = append(args, "-v", rt.opts.PachctlBinary+":/pach-bin/pachctl:ro")
	}
	for _, e := range env {
		args = append(args, "-e", e)
	}
	// --entrypoint overrides the image's ENTRYPOINT (e.g. the test
	// entrypoint image's `cp ...`); the worker binary is the process.
	args = append(args, "--entrypoint", "/pach-bin/worker", image)
	return exec.Command(rt.dockerBin, args...), nil
}

// materializeSecret writes a secret's data as files under dir (one file per
// key), so the worker container can bind-mount them at the pipeline's
// MountPath. It fails if the secret is absent, matching k8s's
// CreateContainerConfigError for a missing secret. Callers must hold rt.mu
// (it is invoked from workerCmd under the spawn lock, like resolveEnv).
func (rt *Runtime) materializeSecret(ns, name, dir string) error {
	secret, ok := rt.secrets[ns+"/"+name]
	if !ok {
		return errors.Errorf("secret %q not found while mounting secret volume", name)
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		return errors.EnsureStack(err)
	}
	for k, v := range secret.Data {
		if err := ioutil.WriteFile(filepath.Join(dir, k), v, 0644); err != nil {
			return errors.EnsureStack(err)
		}
	}
	return nil
}

// preflightImage ensures the worker's image is available before spawning:
// already-pulled images are free (a local inspect), real missing images are
// pulled, and nonexistent images fail fast so the pod records the failure at
// spawn time rather than crash-looping through docker run.
func (rt *Runtime) preflightImage(g *rcState) error {
	image := ""
	for _, c := range g.rc.Spec.Template.Spec.Containers {
		if c.Name == client.PPSWorkerUserContainerName {
			image = c.Image
		}
	}
	if image == "" {
		return nil
	}
	if err := exec.Command(rt.dockerBin, "image", "inspect", image).Run(); err == nil {
		return nil // already present
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, rt.dockerBin, "pull", image).CombinedOutput()
	if err != nil {
		return errors.Errorf("could not pull image %q: %v: %s", image, err, strings.TrimSpace(string(out)))
	}
	return nil
}

// spawnPodLocked starts one worker process for g. Callers must hold rt.mu.
func (rt *Runtime) spawnPodLocked(g *rcState) error {
	idx := rt.nextPod
	rt.nextPod++
	name := fmt.Sprintf("%s-%04d", g.rc.Name, idx)
	ip := rt.allocIP()

	env, err := rt.workerEnv(g, name, ip)
	if err != nil {
		rt.addFailedPodLocked(g, name, ip, "", len(nodeTags(g)) > 0 && rt.opts.Broker != nil, err)
		return err
	}

	// Remote placement: workers whose pod template carries PACH_NODE_TAG run
	// on a broker agent serving that tag (see Options.Broker). The agent
	// owns the worker's scratch dirs and execs the worker binary itself.
	if tags := nodeTags(g); len(tags) > 0 && rt.opts.Broker != nil {
		logPath := filepath.Join(rt.opts.LocalDir, "logs", name+".jsonl")
		if err := os.MkdirAll(filepath.Dir(logPath), 0777); err != nil {
			return errors.Wrapf(err, "could not create log dir")
		}
		// Touch an empty local log file so pod log queries succeed (the real
		// output lives on the agent; streaming it is a later phase).
		if err := ioutil.WriteFile(logPath, nil, 0644); err != nil {
			return errors.Wrapf(err, "could not create log file")
		}
		if err := rt.opts.Broker.Place(tags, name, env, workerImage(g), rt.opts.Runtime); err != nil {
			rt.addFailedPodLocked(g, name, ip, logPath, true, errors.Wrapf(err, "no agent available for node tags %v", tags))
			return err
		}
		p := &podState{
			name:    name,
			ip:      ip,
			logPath: logPath,
			group:   g,
			remote:  true,
		}
		g.pods = append(g.pods, p)
		rt.emitLocked(g, watch.Added, p.podObj(g.ns))
		log.Infof("localclient: placed worker %s on agent for tags %v", name, tags)
		rt.reconcileServiceForwardLocked(g)
		return nil
	}

	scratch := filepath.Join(rt.opts.LocalDir, "workers", name)
	if err := os.MkdirAll(filepath.Join(scratch, client.PPSInputPrefix), 0777); err != nil {
		return errors.Wrapf(err, "could not create worker scratch dir")
	}
	if rt.opts.Runtime == "docker" {
		if err := os.MkdirAll(filepath.Join(rt.opts.LocalDir, "cache", name), 0777); err != nil {
			return errors.Wrapf(err, "could not create worker cache dir")
		}
	} else {
		if err := os.MkdirAll(filepath.Join(scratch, "cache"), 0777); err != nil {
			return errors.Wrapf(err, "could not create worker cache dir")
		}
	}
	logPath := filepath.Join(rt.opts.LocalDir, "logs", name+".jsonl")
	if err := os.MkdirAll(filepath.Dir(logPath), 0777); err != nil {
		return errors.Wrapf(err, "could not create log dir")
	}
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return errors.Wrapf(err, "could not open log file %q", logPath)
	}

	// env was already resolved above (shared with the remote-placement
	// branch); the local and remote paths diverge only in how they run it.
	cmd, err := rt.workerCmd(g, name, ip, env)
	if err != nil {
		logFile.Close()
		rt.addFailedPodLocked(g, name, ip, logPath, false, err)
		return err
	}
	if rt.opts.Runtime == "docker" {
		if err := rt.preflightImage(g); err != nil {
			logFile.Close()
			rt.addFailedPodLocked(g, name, ip, logPath, false, err)
			return err
		}
	}
	cmd.Env = append(os.Environ(), env...)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	proc, err := podrunner.Start(name, cmd)
	if err != nil {
		logFile.Close()
		rt.addFailedPodLocked(g, name, ip, logPath, false, err)
		return errors.Wrapf(err, "could not start worker process for %q", name)
	}

	p := &podState{
		name:    name,
		ip:      ip,
		cmd:     proc.Cmd,
		logPath: logPath,
		group:   g,
	}
	g.pods = append(g.pods, p)
	rt.recordPID(cmd.Process.Pid)
	log.Infof("localclient: started worker %s (%s)", name, ip)
	rt.emitLocked(g, watch.Added, p.podObj(g.ns))
	go rt.monitorPod(g, p)
	return nil
}

// addFailedPodLocked records a worker replica that could not be started, in a
// failed pod state the pipeline controller can observe (so the pipeline goes
// CRASHING instead of the master retrying forever with no visible progress),
// and retries the spawn with backoff until it succeeds or the RC goes away
// (k8s CrashLoopBackOff semantics). Callers must hold rt.mu.
func (rt *Runtime) addFailedPodLocked(g *rcState, name, ip, logPath string, remote bool, spawnErr error) {
	p := &podState{
		name:        name,
		ip:          ip,
		logPath:     logPath,
		group:       g,
		remote:      remote,
		spawnFailed: true,
		exitErr:     spawnErr,
	}
	g.pods = append(g.pods, p)
	rt.emitLocked(g, watch.Added, p.podObj(g.ns))
	log.Errorf("localclient: worker %s failed to start: %v; retrying", name, spawnErr)
	go func() {
		for {
			time.Sleep(2 * time.Second)
			rt.mu.Lock()
			stillWanted := false
			for _, q := range g.pods {
				if q == p {
					stillWanted = true
					break
				}
			}
			if !stillWanted {
				rt.mu.Unlock()
				return
			}
			err := rt.restartPodLocked(g, p)
			rt.mu.Unlock()
			if err == nil {
				return
			}
			log.Errorf("localclient: could not start worker %s (retrying): %v", p.name, err)
		}
	}()
}

// killPodLocked terminates one worker process (its whole process group, so
// user code children die too) and removes it from g. Callers must hold rt.mu.
func (rt *Runtime) killPodLocked(g *rcState, p *podState) {
	if p.remote {
		for i := range g.pods {
			if g.pods[i] == p {
				g.pods = append(g.pods[:i], g.pods[i+1:]...)
				break
			}
		}
		if rt.opts.Broker != nil {
			rt.opts.Broker.Kill(p.name)
			// The worker's service relay dies with it; re-point any
			// remaining service forward at a surviving worker (or back to
			// local semantics when none are left).
			rt.reconcileServiceForwardLocked(g)
		}
		log.Infof("localclient: stopped remote worker %s", p.name)
		rt.emitLocked(g, watch.Deleted, p.podObj(g.ns))
		return
	}
	if p.cmd != nil && p.cmd.Process != nil {
		if rt.opts.Runtime == "docker" {
			// Stop the container first; the foreground `docker run` CLI then
			// exits on its own.
			if err := exec.Command(rt.dockerBin, "kill", p.name).Run(); err != nil {
				log.Warnf("localclient: docker kill %s: %v", p.name, err)
			}
		}
		// Kill the process group so user-code children are not orphaned.
		(&podrunner.Proc{ID: p.name, Cmd: p.cmd}).Kill()
		rt.forgetPID(p.cmd.Process.Pid)
	}
	for i := range g.pods {
		if g.pods[i] == p {
			g.pods = append(g.pods[:i], g.pods[i+1:]...)
			break
		}
	}
	log.Infof("localclient: stopped worker %s", p.name)
	rt.emitLocked(g, watch.Deleted, p.podObj(g.ns))
}

// monitorPod waits for a worker process to exit, reports the failure, and
// restarts it with backoff (matching k8s RestartPolicy: Always).
func (rt *Runtime) monitorPod(g *rcState, p *podState) {
	err := p.cmd.Wait()
	p.mu.Lock()
	p.exitErr = err
	// A docker CLI exit of 125 means the daemon refused to start the
	// container (most commonly an image that cannot be pulled); mirror
	// k8s' ImagePullBackOff so the pipeline controller marks the pipeline
	// CRASHING instead of a worker crash-loop.
	if rt.opts.Runtime == "docker" {
		if ee, ok := err.(*exec.ExitError); ok && ee.ExitCode() == 125 {
			p.spawnFailed = true
		}
	}
	p.mu.Unlock()

	rt.mu.Lock()
	// If the pod was removed while we were waiting, don't restart it.
	stillWanted := false
	for _, q := range g.pods {
		if q == p {
			stillWanted = true
			break
		}
	}
	if stillWanted {
		rt.emitLocked(g, watch.Modified, p.podObj(g.ns)) // Failed
	}
	rt.mu.Unlock()

	if !stillWanted {
		return
	}
	log.Errorf("localclient: worker %s exited: %v; restarting", p.name, err)
	time.Sleep(2 * time.Second)

	rt.mu.Lock()
	defer rt.mu.Unlock()
	// Check again under the lock; the RC may have been deleted or scaled down.
	stillWanted = false
	for _, q := range g.pods {
		if q == p {
			stillWanted = true
			break
		}
	}
	if !stillWanted {
		return
	}
	p.mu.Lock()
	p.restarts++
	p.mu.Unlock()
	if err := rt.restartPodLocked(g, p); err != nil {
		log.Errorf("localclient: could not restart worker %s: %v", p.name, err)
	}
}

// workerImage returns the pipeline's image from the worker pod template (the
// user container), or "" if the pipeline has no image (process-mode workers
// ignore the image entirely).
func workerImage(g *rcState) string {
	for _, c := range g.rc.Spec.Template.Spec.Containers {
		if c.Name == client.PPSWorkerUserContainerName {
			return c.Image
		}
	}
	return ""
}

// nodeTags returns the tags the pipeline requests placement on (the
// PACH_NODE_TAG env var in the worker pod template, comma-separated for
// multiple acceptable nodes), or nil if the pipeline runs locally.
func nodeTags(g *rcState) []string {
	for _, c := range g.rc.Spec.Template.Spec.Containers {
		if c.Name != client.PPSWorkerUserContainerName {
			continue
		}
		for _, e := range c.Env {
			if e.Name == "PACH_NODE_TAG" {
				var tags []string
				for _, t := range strings.Split(e.Value, ",") {
					if t = strings.TrimSpace(t); t != "" {
						tags = append(tags, t)
					}
				}
				return tags
			}
		}
	}
	return nil
}

// HandleRemoteWorkerExit is the broker's worker-exit callback (wired via
// Server.SetExitHandler). It marks the worker's pod Failed and restarts it
// with backoff, mirroring monitorPod's RestartPolicy: Always behavior for
// local workers. It is called from the broker's drain goroutine, so it must
// not hold any broker locks.
func (rt *Runtime) HandleRemoteWorkerExit(workerID string, exitErr error) {
	rt.mu.Lock()
	var g *rcState
	var p *podState
	for _, rc := range rt.rcs {
		for _, pod := range rc.pods {
			if pod.name == workerID {
				g, p = rc, pod
				break
			}
		}
		if g != nil {
			break
		}
	}
	if g == nil || p == nil || !p.remote {
		rt.mu.Unlock()
		return
	}
	p.mu.Lock()
	p.exitErr = exitErr
	p.mu.Unlock()
	rt.emitLocked(g, watch.Modified, p.podObj(g.ns)) // Failed
	rt.mu.Unlock()

	log.Errorf("localclient: remote worker %s exited: %v; restarting", workerID, exitErr)
	time.Sleep(2 * time.Second)

	rt.mu.Lock()
	defer rt.mu.Unlock()
	// Check again under the lock; the RC may have been deleted or scaled down.
	stillWanted := false
	for _, q := range g.pods {
		if q == p {
			stillWanted = true
			break
		}
	}
	if !stillWanted {
		return
	}
	p.mu.Lock()
	p.restarts++
	p.mu.Unlock()
	if err := rt.restartPodLocked(g, p); err != nil {
		log.Errorf("localclient: could not restart remote worker %s: %v", workerID, err)
	}
}

// restartPodLocked starts a fresh process for a crashed pod, reusing its name
// and IP. Callers must hold rt.mu.
func (rt *Runtime) restartPodLocked(g *rcState, p *podState) error {
	if p.remote {
		env, err := rt.workerEnv(g, p.name, p.ip)
		if err != nil {
			return err
		}
		tags := nodeTags(g)
		if rt.opts.Broker == nil || len(tags) == 0 {
			return errors.Errorf("localclient: remote worker %s has no broker to restart on", p.name)
		}
		if err := rt.opts.Broker.Place(tags, p.name, env, workerImage(g), rt.opts.Runtime); err != nil {
			return errors.Wrapf(err, "no agent available for node tags %v", tags)
		}
		p.mu.Lock()
		p.exitErr = nil
		p.spawnFailed = false
		p.mu.Unlock()
		rt.emitLocked(g, watch.Modified, p.podObj(g.ns)) // Running again
		log.Infof("localclient: re-placed remote worker %s on tags %v", p.name, tags)
		return nil
	}
	logFile, err := os.OpenFile(p.logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	env, err := rt.workerEnv(g, p.name, p.ip)
	if err != nil {
		logFile.Close()
		return err
	}
	cmd, err := rt.workerCmd(g, p.name, p.ip, env)
	if err != nil {
		logFile.Close()
		return err
	}
	cmd.Env = append(os.Environ(), env...)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	proc, err := podrunner.Start(p.name, cmd)
	if err != nil {
		logFile.Close()
		return errors.Wrapf(err, "could not restart worker process for %q", p.name)
	}
	p.cmd = proc.Cmd
	p.mu.Lock()
	p.exitErr = nil
	p.spawnFailed = false
	p.mu.Unlock()
	rt.emitLocked(g, watch.Modified, p.podObj(g.ns)) // Running again
	log.Infof("localclient: restarted worker %s", p.name)
	go rt.monitorPod(g, p)
	return nil
}

// podObj builds the v1.Pod that mirrors a worker process.
func (p *podState) podObj(ns string) *v1.Pod {
	phase := v1.PodRunning
	var waiting *v1.ContainerStateWaiting
	p.mu.Lock()
	if p.exitErr != nil {
		phase = v1.PodFailed
		reason := "CrashLoopBackOff"
		if p.spawnFailed {
			reason = "CreateContainerConfigError"
			if strings.Contains(strings.ToLower(p.exitErr.Error()), "image") {
				reason = "ErrImagePull"
			}
		}
		waiting = &v1.ContainerStateWaiting{
			Reason:  reason,
			Message: p.exitErr.Error(),
		}
	}
	p.mu.Unlock()
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        p.name,
			Namespace:   ns,
			Labels:      p.group.rc.Spec.Template.ObjectMeta.Labels,
			Annotations: p.group.rc.Spec.Template.ObjectMeta.Annotations,
		},
		// The pod's spec is exactly the RC's pod template spec (that is what
		// a real pod is created from); serving it lets suite tests that
		// reflect on pod specs (resources, volumes, podspec overrides) see
		// what the pipeline controller rendered.
		Spec: *p.group.rc.Spec.Template.Spec.DeepCopy(),
		Status: v1.PodStatus{
			Phase:  phase,
			PodIP:  p.ip,
			PodIPs: []v1.PodIP{{IP: p.ip}},
			ContainerStatuses: []v1.ContainerStatus{{
				Name:  client.PPSWorkerUserContainerName,
				Ready: phase == v1.PodRunning,
				State: v1.ContainerState{Waiting: waiting},
			}},
		},
	}
}

// resolveEnv converts a pod template's env vars into a process environment,
// resolving k8s Downward-API field refs and (in-memory) secret refs.
// Callers must hold rt.mu (secret lookups are not separately locked).
func (rt *Runtime) resolveEnv(ns, podName, ip string, envVars []v1.EnvVar) ([]string, error) {
	var out []string
	for _, e := range envVars {
		switch {
		case e.Value != "":
			out = append(out, e.Name+"="+e.Value)
		case e.ValueFrom != nil && e.ValueFrom.FieldRef != nil:
			switch e.ValueFrom.FieldRef.FieldPath {
			case "metadata.name":
				out = append(out, e.Name+"="+podName)
			case "metadata.namespace":
				out = append(out, e.Name+"="+ns)
			case "status.podIP":
				out = append(out, e.Name+"="+ip)
			default:
				log.Warnf("localclient: unsupported field ref %q for env var %q; skipping", e.ValueFrom.FieldRef.FieldPath, e.Name)
			}
		case e.ValueFrom != nil && e.ValueFrom.SecretKeyRef != nil:
			ref := e.ValueFrom.SecretKeyRef
			val, ok := rt.lookupSecretLocked(ns, ref.Name, ref.Key)
			if ok {
				out = append(out, e.Name+"="+val)
			} else if ref.Optional == nil || !*ref.Optional {
				return nil, errors.Errorf("secret %q key %q not found while resolving env var %q", ref.Name, ref.Key, e.Name)
			}
		default:
			log.Warnf("localclient: unsupported env var source for %q; skipping", e.Name)
		}
	}
	return out, nil
}

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

//////////////////////////////////////////////////////////////////////////////
// Pods

type pods struct {
	v1core.PodInterface
	runtime *Runtime
	ns      string
}

func (p *pods) Get(name string, opts metav1.GetOptions) (*v1.Pod, error) {
	return p.runtime.getPod(p.ns, name)
}

func (p *pods) List(opts metav1.ListOptions) (*v1.PodList, error) {
	return p.runtime.listPods(p.ns, opts)
}

func (p *pods) Watch(opts metav1.ListOptions) (watch.Interface, error) {
	return p.runtime.watchPods(opts)
}

func (p *pods) GetLogs(name string, opts *v1.PodLogOptions) *rest.Request {
	// The request needs working serializers: on a non-2xx response (e.g. a
	// missing pod) rest.Request decodes the error body, and a nil
	// RenegotiatedDecoder panics the daemon.
	ser := rest.Serializers{
		Encoder: scheme.Codecs.LegacyCodec(schema.GroupVersion{Version: "v1"}),
		Decoder: scheme.Codecs.UniversalDeserializer(),
		RenegotiatedDecoder: func(contentType string, params map[string]string) (runtime.Decoder, error) {
			return scheme.Codecs.UniversalDeserializer(), nil
		},
	}
	req := rest.NewRequest(
		p.runtime.logClient(),
		"GET",
		p.runtime.serverURL(),
		"",
		rest.ContentConfig{GroupVersion: &schema.GroupVersion{Version: "v1"}},
		ser,
		nil, nil, 0,
	)
	return req.AbsPath("api", "v1", "namespaces", p.ns, "pods", name, "log").
		VersionedParams(opts, scheme.ParameterCodec)
}

func (rt *Runtime) getPod(ns, name string) (*v1.Pod, error) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	for _, g := range rt.rcs {
		if g.ns != ns {
			continue
		}
		for _, p := range g.pods {
			if p.name == name {
				return p.podObj(ns), nil
			}
		}
	}
	return nil, kubeerrors.NewNotFound(schema.GroupResource{Group: "", Resource: "pods"}, name)
}

func (rt *Runtime) listPods(ns string, opts metav1.ListOptions) (*v1.PodList, error) {
	sel, err := labels.Parse(opts.LabelSelector)
	if err != nil {
		return nil, errors.Wrapf(err, "invalid label selector %q", opts.LabelSelector)
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	list := &v1.PodList{}
	for _, g := range rt.rcs {
		if g.ns != ns {
			continue
		}
		for _, p := range g.pods {
			obj := p.podObj(ns)
			if sel.Matches(labels.Set(obj.Labels)) {
				list.Items = append(list.Items, *obj)
			}
		}
	}
	return list, nil
}

type podWatcher struct {
	ch   chan watch.Event
	stop func()
}

func (w *podWatcher) ResultChan() <-chan watch.Event { return w.ch }
func (w *podWatcher) Stop()                          { w.stop() }

func (rt *Runtime) watchPods(opts metav1.ListOptions) (watch.Interface, error) {
	if _, err := labels.Parse(opts.LabelSelector); err != nil {
		return nil, errors.Wrapf(err, "invalid label selector %q", opts.LabelSelector)
	}
	rt.mu.Lock()
	id := rt.nextWatch
	rt.nextWatch++
	ch := make(chan watch.Event, 256)
	rt.watchers[id] = ch
	rt.mu.Unlock()
	return &podWatcher{
		ch: ch,
		stop: func() {
			rt.mu.Lock()
			delete(rt.watchers, id)
			close(ch)
			rt.mu.Unlock()
		},
	}, nil
}

// emitLocked broadcasts a pod event to all watchers. Callers must hold rt.mu.
func (rt *Runtime) emitLocked(g *rcState, eventType watch.EventType, pod *v1.Pod) {
	if pod == nil {
		return
	}
	event := watch.Event{Type: eventType, Object: pod}
	for _, ch := range rt.watchers {
		select {
		case ch <- event:
		default: // slow watcher: drop rather than block the daemon
		}
	}
}

//////////////////////////////////////////////////////////////////////////////
// Logs

// logClient adapts http.Client to the rest.HTTPClient interface.
type logClient struct {
	*http.Client
}

func (c *logClient) DoRaw() ([]byte, error) {
	return nil, errors.New("localclient: DoRaw is not supported")
}

func (rt *Runtime) logClient() rest.HTTPClient {
	return &logClient{Client: rt.logSrv.Client()}
}

// handleLogs serves /api/v1/namespaces/<ns>/pods/<pod>/log for worker logs.
// It supports the follow, tailLines, sinceSeconds, and container query params
// that rest.Request encodes from v1.PodLogOptions.
func (rt *Runtime) handleLogs(w http.ResponseWriter, r *http.Request) {
	segments := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	// .../pods/<podName>/log
	if len(segments) < 6 || segments[len(segments)-1] != "log" || segments[len(segments)-3] != "pods" {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	podName := segments[len(segments)-2]
	q := r.URL.Query()
	follow := q.Get("follow") == "true"
	tailLines := -1
	if v := q.Get("tailLines"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			tailLines = n
		}
	}
	var since time.Time
	if v := q.Get("sinceSeconds"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			since = time.Now().Add(-time.Duration(n) * time.Second)
		}
	}
	rt.serveLogs(w, r, podName, follow, tailLines, since)
}

func (rt *Runtime) serveLogs(w http.ResponseWriter, r *http.Request, podName string, follow bool, tailLines int, since time.Time) {
	path := filepath.Join(rt.opts.LocalDir, "logs", podName+".jsonl")
	f, err := os.Open(path)
	if err != nil {
		// A JSON body keeps rest.Request's error decoding (which the
		// debug server exercises) from choking on sniffed text.
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, `{"message":"logs for %q not found"}`, podName)
		return
	}
	defer f.Close()

	flusher, canFlush := w.(http.Flusher)
	writeLines := func() (int, error) {
		data, err := ioutil.ReadAll(f)
		if err != nil {
			return 0, err
		}
		if len(data) == 0 {
			return 0, nil
		}
		lines := strings.Split(strings.TrimSuffix(string(data), "\n"), "\n")
		if tailLines >= 0 && len(lines) > tailLines {
			lines = lines[len(lines)-tailLines:]
		}
		if !since.IsZero() {
			kept := lines[:0]
			for _, line := range lines {
				if lineTS(line).After(since) {
					kept = append(kept, line)
				}
			}
			lines = kept
		}
		if len(lines) == 0 {
			return 0, nil
		}
		n, err := io.WriteString(w, strings.Join(lines, "\n")+"\n")
		if err != nil {
			return n, err
		}
		if canFlush {
			flusher.Flush()
		}
		return n, nil
	}

	if _, err := writeLines(); err != nil {
		return
	}
	if !follow {
		return
	}
	// Follow: poll for appended lines until the client disconnects.
	// start following from the current end of the file so that -f behaves
	// like "logs since now" (kubectl semantics for a running pod).
	offset, _ := f.Seek(0, io.SeekEnd)
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
			st, err := f.Stat()
			if err != nil || st.Size() <= offset {
				continue
			}
			data := make([]byte, st.Size()-offset)
			n, err := f.ReadAt(data, offset)
			if err != nil && err != io.EOF {
				return
			}
			offset += int64(n)
			if n > 0 {
				if _, err := w.Write(data[:n]); err != nil {
					return
				}
				if canFlush {
					flusher.Flush()
				}
			}
		}
	}
}

// lineTS extracts the RFC3339 timestamp from a jsonpb-marshaled
// pps.LogMessage line. Non-JSON lines (e.g. stderr) have no timestamp.
func lineTS(line string) time.Time {
	var msg struct {
		TS string `json:"ts"`
	}
	if err := json.Unmarshal([]byte(line), &msg); err != nil {
		return time.Time{}
	}
	ts, err := time.Parse(time.RFC3339Nano, msg.TS)
	if err != nil {
		return time.Time{}
	}
	return ts
}

//////////////////////////////////////////////////////////////////////////////
// Nodes

type nodes struct {
	v1core.NodeInterface
	runtime *Runtime
}

func (n *nodes) List(opts metav1.ListOptions) (*v1.NodeList, error) {
	return &v1.NodeList{
		Items: []v1.Node{{
			ObjectMeta: metav1.ObjectMeta{
				Name:   "local",
				Labels: map[string]string{"kubernetes.io/hostname": "local"},
			},
			Status: v1.NodeStatus{
				Phase: v1.NodeRunning,
				Addresses: []v1.NodeAddress{{
					Type:    v1.NodeInternalIP,
					Address: "127.0.0.1",
				}},
			},
		}},
	}, nil
}

//////////////////////////////////////////////////////////////////////////////
// Services and Secrets (label-selector aware, in-memory)

type services struct {
	v1core.ServiceInterface
	runtime *Runtime
	ns      string
}

func (s *services) Create(svc *v1.Service) (*v1.Service, error) {
	return s.runtime.createService(s.ns, svc)
}

func (s *services) Delete(name string, opts *metav1.DeleteOptions) error {
	return s.runtime.deleteService(s.ns, name)
}

func (s *services) Get(name string, opts metav1.GetOptions) (*v1.Service, error) {
	return s.runtime.getService(s.ns, name)
}

func (s *services) List(opts metav1.ListOptions) (*v1.ServiceList, error) {
	return s.runtime.listServices(s.ns, opts)
}

func (rt *Runtime) createService(ns string, svc *v1.Service) (*v1.Service, error) {
	// Emulate the k8s API server's defaulting: a service created in
	// namespace ns is stored with that namespace, and gets a ClusterIP.
	// In local mode "the cluster" is the daemon's host, so the ClusterIP
	// is loopback (this is what the pachd HTTP service proxy dials).
	if svc.Namespace == "" {
		svc.Namespace = ns
	}
	if svc.Spec.ClusterIP == "" {
		svc.Spec.ClusterIP = "127.0.0.1"
	}
	rt.mu.Lock()
	key := ns + "/" + svc.Name
	if _, ok := rt.services[key]; ok {
		rt.mu.Unlock()
		return nil, kubeerrors.NewAlreadyExists(schema.GroupResource{Group: "", Resource: "services"}, svc.Name)
	}
	rt.services[key] = svc.DeepCopy()
	rt.mu.Unlock()
	rt.maybeStartServiceForward(key, svc)
	rt.mu.Lock()
	out := rt.services[key].DeepCopy()
	rt.mu.Unlock()
	return out, nil
}

func (rt *Runtime) deleteService(ns, name string) error {
	rt.mu.Lock()
	key := ns + "/" + name
	delete(rt.services, key)
	if f, ok := rt.svcForwards[key]; ok {
		delete(rt.svcForwards, key)
		rt.mu.Unlock()
		f.Close()
		return nil
	}
	rt.mu.Unlock()
	return nil
}

// maybeStartServiceForward exposes a pipeline service's container port on the
// host's loopback interface, standing in for the k8s NodePort service: the
// worker container runs with host networking (so its listener is already on
// the host), and this daemon-side forward makes the pipeline's external port
// reachable at 127.0.0.1:<ExternalPort>. It only forwards services that carry
// a "user-port" (pipeline services); the pachyderm-internal service for each
// pipeline has no user-port and needs no forward.
func (rt *Runtime) maybeStartServiceForward(key string, svc *v1.Service) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	// If the pipeline's RC already exists, its workers may be broker-placed;
	// let the reconcile decide local vs remote forwarding.
	if pipelineName := svc.Labels["pipelineName"]; pipelineName != "" {
		for _, g := range rt.rcs {
			if g.rc.Spec.Template.Labels["pipelineName"] == pipelineName {
				rt.reconcileServiceForwardLocked(g)
				return
			}
		}
	}
	for _, p := range svc.Spec.Ports {
		if p.Name != "user-port" {
			continue
		}
		externalPort := int(p.Port)
		internalPort := externalPort
		if p.TargetPort.IntValue() != 0 {
			internalPort = p.TargetPort.IntValue()
		}
		if externalPort == 0 || internalPort == 0 || externalPort == internalPort {
			return // nothing to forward (same port is already host-reachable)
		}
		if _, ok := rt.svcForwards[key]; ok {
			return
		}
		f, err := newTCPForward(externalPort, internalPort)
		if err != nil {
			log.Warnf("localclient: could not forward service %s port %d->%d: %v", key, externalPort, internalPort, err)
			return
		}
		rt.svcForwards[key] = f
		return
	}
}

// reconcileServiceForwardLocked makes the pipeline's service reachable at
// 127.0.0.1:<external> on the daemon host no matter where the workers run:
// loopback->loopback for local workers, or loopback-><node IP>:<external>
// for broker-placed workers (the node's agent relays that to the worker's
// internal port). Callers must hold rt.mu.
func (rt *Runtime) reconcileServiceForwardLocked(g *rcState) {
	if rt.opts.Broker == nil {
		return
	}
	pipelineName := g.rc.Spec.Template.Labels["pipelineName"]
	if pipelineName == "" {
		return
	}
	var remotePod *podState
	for _, p := range g.pods {
		if p.remote {
			remotePod = p
			break
		}
	}
	for key, svc := range rt.services {
		if svc.Labels["pipelineName"] != pipelineName {
			continue
		}
		for _, port := range svc.Spec.Ports {
			if port.Name != "user-port" {
				continue
			}
			externalPort := int(port.Port)
			internalPort := externalPort
			if port.TargetPort.IntValue() != 0 {
				internalPort = port.TargetPort.IntValue()
			}
			if externalPort == 0 || internalPort == 0 {
				continue
			}
			if remotePod == nil {
				// All-local workers: the plain loopback forward.
				if _, ok := rt.svcForwards[key]; ok {
					continue
				}
				f, err := newTCPForward(externalPort, internalPort)
				if err != nil {
					log.Warnf("localclient: could not forward service %s port %d->%d: %v", key, externalPort, internalPort, err)
					continue
				}
				rt.svcForwards[key] = f
				continue
			}
			// Broker-placed worker: ask its node to expose the port, then
			// relay the daemon's loopback external port to that node.
			nodeIP, ok := rt.opts.Broker.NodeIP(remotePod.name)
			if !ok {
				continue
			}
			rt.opts.Broker.Forward(remotePod.name, externalPort, internalPort)
			if f, ok := rt.svcForwards[key]; ok {
				f.Close()
				delete(rt.svcForwards, key)
			}
			f, err := newTCPForwardTo("127.0.0.1", externalPort,
				net.JoinHostPort(nodeIP, strconv.Itoa(externalPort)))
			if err != nil {
				log.Warnf("localclient: could not forward service %s to node %s port %d: %v", key, nodeIP, externalPort, err)
				continue
			}
			rt.svcForwards[key] = f
		}
	}
}

// tcpForward is a small host-side proxy from a fixed loopback port to a
// worker's listener (which, with host networking, is also on loopback).
type tcpForward struct {
	ln     net.Listener
	done   chan struct{}
	once   sync.Once
	target string
}

func newTCPForward(externalPort, internalPort int) (*tcpForward, error) {
	return newTCPForwardTo("127.0.0.1", externalPort, net.JoinHostPort("127.0.0.1", strconv.Itoa(internalPort)))
}

// newTCPForwardTo is the general form: a loopback (or any) host port
// relaying to an arbitrary target. The remote-worker service relay uses it
// with the node's address as the target.
func newTCPForwardTo(host string, externalPort int, target string) (*tcpForward, error) {
	ln, err := net.Listen("tcp", net.JoinHostPort(host, strconv.Itoa(externalPort)))
	if err != nil {
		return nil, err
	}
	f := &tcpForward{
		ln:     ln,
		done:   make(chan struct{}),
		target: target,
	}
	go f.serve()
	return f, nil
}

func (f *tcpForward) serve() {
	for {
		conn, err := f.ln.Accept()
		if err != nil {
			select {
			case <-f.done:
				return
			default:
				continue // transient accept error; keep serving
			}
		}
		go f.relay(conn)
	}
}

func (f *tcpForward) relay(conn net.Conn) {
	upstream, err := net.Dial("tcp", f.target)
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

func (f *tcpForward) Close() error {
	f.once.Do(func() {
		close(f.done)
		f.ln.Close()
	})
	return nil
}

func (rt *Runtime) getService(ns, name string) (*v1.Service, error) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	svc, ok := rt.services[ns+"/"+name]
	if !ok {
		return nil, kubeerrors.NewNotFound(schema.GroupResource{Group: "", Resource: "services"}, name)
	}
	return svc.DeepCopy(), nil
}

func (rt *Runtime) listServices(ns string, opts metav1.ListOptions) (*v1.ServiceList, error) {
	sel, err := labels.Parse(opts.LabelSelector)
	if err != nil {
		return nil, errors.Wrapf(err, "invalid label selector %q", opts.LabelSelector)
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	list := &v1.ServiceList{}
	for _, svc := range rt.services {
		if svc.Namespace == ns && sel.Matches(labels.Set(svc.Labels)) {
			list.Items = append(list.Items, *svc.DeepCopy())
		}
	}
	return list, nil
}

type secrets struct {
	v1core.SecretInterface
	runtime *Runtime
	ns      string
}

func (s *secrets) Create(secret *v1.Secret) (*v1.Secret, error) {
	return s.runtime.createSecret(s.ns, secret)
}

func (s *secrets) Delete(name string, opts *metav1.DeleteOptions) error {
	return s.runtime.deleteSecret(s.ns, name)
}

func (s *secrets) DeleteCollection(opts *metav1.DeleteOptions, listOpts metav1.ListOptions) error {
	return s.runtime.deleteSecretCollection(s.ns, listOpts)
}

func (s *secrets) Get(name string, opts metav1.GetOptions) (*v1.Secret, error) {
	return s.runtime.getSecret(s.ns, name)
}

func (s *secrets) List(opts metav1.ListOptions) (*v1.SecretList, error) {
	return s.runtime.listSecrets(s.ns, opts)
}

func (rt *Runtime) createSecret(ns string, secret *v1.Secret) (*v1.Secret, error) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	key := ns + "/" + secret.Name
	if _, ok := rt.secrets[key]; ok {
		return nil, kubeerrors.NewAlreadyExists(schema.GroupResource{Group: "", Resource: "secrets"}, secret.Name)
	}
	// The k8s API server defaults an unset type to Opaque and fills in the
	// namespace from the request.
	if secret.Type == "" {
		secret.Type = v1.SecretTypeOpaque
	}
	if secret.Namespace == "" {
		secret.Namespace = ns
	}
	rt.secrets[key] = secret.DeepCopy()
	return rt.secrets[key].DeepCopy(), nil
}

func (rt *Runtime) deleteSecret(ns, name string) error {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	delete(rt.secrets, ns+"/"+name)
	return nil
}

func (rt *Runtime) deleteSecretCollection(ns string, listOpts metav1.ListOptions) error {
	sel, err := labels.Parse(listOpts.LabelSelector)
	if err != nil {
		return errors.Wrapf(err, "invalid label selector %q", listOpts.LabelSelector)
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	for key, secret := range rt.secrets {
		if secret.Namespace == ns && sel.Matches(labels.Set(secret.Labels)) {
			delete(rt.secrets, key)
		}
	}
	return nil
}

func (rt *Runtime) getSecret(ns, name string) (*v1.Secret, error) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	secret, ok := rt.secrets[ns+"/"+name]
	if !ok {
		return nil, kubeerrors.NewNotFound(schema.GroupResource{Group: "", Resource: "secrets"}, name)
	}
	return secret.DeepCopy(), nil
}

func (rt *Runtime) listSecrets(ns string, opts metav1.ListOptions) (*v1.SecretList, error) {
	sel, err := labels.Parse(opts.LabelSelector)
	if err != nil {
		return nil, errors.Wrapf(err, "invalid label selector %q", opts.LabelSelector)
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	list := &v1.SecretList{}
	for _, secret := range rt.secrets {
		if secret.Namespace == ns && sel.Matches(labels.Set(secret.Labels)) {
			list.Items = append(list.Items, *secret.DeepCopy())
		}
	}
	return list, nil
}

// lookupSecretLocked returns the value of a secret key. Callers must hold
// rt.mu.
func (rt *Runtime) lookupSecretLocked(ns, name, key string) (string, bool) {
	secret, ok := rt.secrets[ns+"/"+name]
	if !ok {
		return "", false
	}
	val, ok := secret.Data[key]
	if !ok {
		return "", false
	}
	return string(val), true
}
