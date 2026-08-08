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
// All other API groups are served by the client-go fake clientset.
package localclient

import (
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

	v1 "k8s.io/api/core/v1"
	kubeerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"

	"github.com/pachyderm/pachyderm/src/client"
	"github.com/pachyderm/pachyderm/src/client/pkg/errors"
)

// Options configures a Runtime.
type Options struct {
	// WorkerBinary is the path to the pachyderm worker binary that will be
	// spawned for each worker replica.
	WorkerBinary string
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

	logSrv *httptest.Server
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
	rt := &Runtime{
		opts:     opts,
		rcs:      make(map[string]*rcState),
		services: make(map[string]*v1.Service),
		secrets:  make(map[string]*v1.Secret),
		nextIP:   2, // 127.0.0.1 is the daemon itself; workers start at 127.0.0.2
		watchers: make(map[int]chan watch.Event),
	}
	for _, dir := range []string{
		filepath.Join(opts.LocalDir, "workers"),
		filepath.Join(opts.LocalDir, "logs"),
	} {
		if err := os.MkdirAll(dir, 0777); err != nil {
			return nil, errors.Wrapf(err, "could not create %q", dir)
		}
	}
	rt.reapOrphanedWorkers()
	rt.logSrv = httptest.NewServer(http.HandlerFunc(rt.handleLogs))
	return rt, nil
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
// is already bound to ip:workerPort.
func (rt *Runtime) allocIP() string {
	for {
		ip := fmt.Sprintf("127.0.0.%d", rt.nextIP)
		rt.nextIP++
		ln, err := net.Listen("tcp", net.JoinHostPort(ip, strconv.Itoa(int(rt.opts.WorkerPort))))
		if err == nil {
			ln.Close()
			return ip
		}
		if rt.nextIP > 254 {
			log.Errorf("localclient: ran out of loopback IPs for workers")
			return ip // let the spawn fail loudly
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

// spawnPodLocked starts one worker process for g. Callers must hold rt.mu.
func (rt *Runtime) spawnPodLocked(g *rcState) error {
	idx := rt.nextPod
	rt.nextPod++
	name := fmt.Sprintf("%s-%04d", g.rc.Name, idx)
	ip := rt.allocIP()

	scratch := filepath.Join(rt.opts.LocalDir, "workers", name)
	if err := os.MkdirAll(filepath.Join(scratch, client.PPSInputPrefix), 0777); err != nil {
		return errors.Wrapf(err, "could not create worker scratch dir")
	}
	logPath := filepath.Join(rt.opts.LocalDir, "logs", name+".jsonl")
	if err := os.MkdirAll(filepath.Dir(logPath), 0777); err != nil {
		return errors.Wrapf(err, "could not create log dir")
	}
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return errors.Wrapf(err, "could not open log file %q", logPath)
	}

	template := &g.rc.Spec.Template.Spec
	var userEnv []v1.EnvVar
	for _, c := range template.Containers {
		if c.Name == client.PPSWorkerUserContainerName {
			userEnv = c.Env
		}
	}
	env, err := rt.resolveEnv(g.ns, name, ip, userEnv)
	if err != nil {
		logFile.Close()
		return err
	}
	// Identity + connectivity overrides (these replace the k8s Downward API
	// and k8s-injected service env vars).
	env = setEnv(env, "PPS_WORKER_IP", ip)
	env = setEnv(env, "PPS_POD_NAME", name)
	env = setEnv(env, "PPS_WORKER_GRPC_PORT", strconv.Itoa(int(rt.opts.WorkerPort)))
	env = setEnv(env, "PEER_PORT", strconv.Itoa(int(rt.opts.PachdPort)))
	env = setEnv(env, "ETCD_SERVICE_HOST", rt.opts.EtcdHost)
	env = setEnv(env, "ETCD_SERVICE_PORT", rt.opts.EtcdPort)
	env = setEnv(env, "PACH_WORKER_ROOT", scratch)
	env = setEnv(env, "PACH_CACHE_ROOT", filepath.Join(scratch, "cache"))
	env = setEnv(env, "PACH_ROOT", rt.opts.StorageRoot)
	env = append(env, "PACH_LOCAL_WORKER=1")

	cmd := exec.Command(rt.opts.WorkerBinary)
	cmd.Env = append(os.Environ(), env...)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		logFile.Close()
		return errors.Wrapf(err, "could not start worker process for %q", name)
	}

	p := &podState{
		name:    name,
		ip:      ip,
		cmd:     cmd,
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

// killPodLocked terminates one worker process (its whole process group, so
// user code children die too) and removes it from g. Callers must hold rt.mu.
func (rt *Runtime) killPodLocked(g *rcState, p *podState) {
	if p.cmd != nil && p.cmd.Process != nil {
		// Kill the process group so user-code children are not orphaned.
		syscall.Kill(-p.cmd.Process.Pid, syscall.SIGKILL)
		p.cmd.Process.Kill()
		p.cmd.Wait()
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

// restartPodLocked starts a fresh process for a crashed pod, reusing its name
// and IP. Callers must hold rt.mu.
func (rt *Runtime) restartPodLocked(g *rcState, p *podState) error {
	scratch := filepath.Join(rt.opts.LocalDir, "workers", p.name)
	logFile, err := os.OpenFile(p.logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	template := &g.rc.Spec.Template.Spec
	var userEnv []v1.EnvVar
	for _, c := range template.Containers {
		if c.Name == client.PPSWorkerUserContainerName {
			userEnv = c.Env
		}
	}
	env, err := rt.resolveEnv(g.ns, p.name, p.ip, userEnv)
	if err != nil {
		logFile.Close()
		return err
	}
	env = setEnv(env, "PPS_WORKER_IP", p.ip)
	env = setEnv(env, "PPS_POD_NAME", p.name)
	env = setEnv(env, "PPS_WORKER_GRPC_PORT", strconv.Itoa(int(rt.opts.WorkerPort)))
	env = setEnv(env, "PEER_PORT", strconv.Itoa(int(rt.opts.PachdPort)))
	env = setEnv(env, "ETCD_SERVICE_HOST", rt.opts.EtcdHost)
	env = setEnv(env, "ETCD_SERVICE_PORT", rt.opts.EtcdPort)
	env = setEnv(env, "PACH_WORKER_ROOT", scratch)
	env = setEnv(env, "PACH_CACHE_ROOT", filepath.Join(scratch, "cache"))
	env = setEnv(env, "PACH_ROOT", rt.opts.StorageRoot)
	env = append(env, "PACH_LOCAL_WORKER=1")

	cmd := exec.Command(rt.opts.WorkerBinary)
	cmd.Env = append(os.Environ(), env...)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		logFile.Close()
		return errors.Wrapf(err, "could not restart worker process for %q", p.name)
	}
	p.cmd = cmd
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
		waiting = &v1.ContainerStateWaiting{
			Reason:  "CrashLoopBackOff",
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
	req := rest.NewRequest(
		p.runtime.logClient(),
		"GET",
		p.runtime.serverURL(),
		"",
		rest.ContentConfig{GroupVersion: &schema.GroupVersion{Version: "v1"}},
		rest.Serializers{},
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
		http.Error(w, fmt.Sprintf("logs for %q not found", podName), http.StatusNotFound)
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
				Phase:  v1.NodeRunning,
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
	rt.mu.Lock()
	defer rt.mu.Unlock()
	key := ns + "/" + svc.Name
	if _, ok := rt.services[key]; ok {
		return nil, kubeerrors.NewAlreadyExists(schema.GroupResource{Group: "", Resource: "services"}, svc.Name)
	}
	rt.services[key] = svc.DeepCopy()
	return rt.services[key].DeepCopy(), nil
}

func (rt *Runtime) deleteService(ns, name string) error {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	delete(rt.services, ns+"/"+name)
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

