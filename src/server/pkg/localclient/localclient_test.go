package localclient

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/pachyderm/pachyderm/src/client/pkg/require"
	v1 "k8s.io/api/core/v1"
)

// testRuntime builds a process-mode Runtime in a temp LocalDir. Unit tests
// never touch the daemon or docker; WorkerBinary is only used when an RC is
// actually spawned.
func testRuntime(t *testing.T) (*Runtime, string) {
	dir := t.TempDir()
	rt, err := NewRuntime(Options{
		WorkerBinary: "sh",
		LocalDir:     dir,
		Runtime:      "process",
	})
	require.NoError(t, err)
	t.Cleanup(rt.Close)
	return rt, dir
}

func copyFile(t *testing.T, src, dst string) {
	t.Helper()
	data, err := ioutil.ReadFile(src)
	require.NoError(t, err)
	require.NoError(t, ioutil.WriteFile(dst, data, 0755))
}

func TestRecordForgetPID(t *testing.T) {
	rt, _ := testRuntime(t)
	require.NoError(t, ioutil.WriteFile(rt.pidFilePath(), nil, 0644))
	rt.recordPID(111)
	rt.recordPID(222)
	rt.recordPID(333)
	rt.forgetPID(222)
	data, err := ioutil.ReadFile(rt.pidFilePath())
	require.NoError(t, err)
	require.Equal(t, "111\n333\n", string(data))
	rt.forgetPID(111)
	rt.forgetPID(333)
	data, err = ioutil.ReadFile(rt.pidFilePath())
	require.NoError(t, err)
	require.Equal(t, "", string(data))
}

// TestReapOrphanedWorkers is the crash-recovery contract at the runtime
// level: NewRuntime must kill workers a hard-killed daemon left behind (by
// pid-file lookup, and by docker label when docker mode is on) and clear the
// pid file, while leaving unrelated processes alone.
func TestReapOrphanedWorkers(t *testing.T) {
	dir := t.TempDir()

	sleep, err := exec.LookPath("sleep")
	require.NoError(t, err)

	// A live process whose cmdline contains "worker": exactly what a
	// crashed daemon leaves behind.
	workerPath := filepath.Join(dir, "worker")
	copyFile(t, sleep, workerPath)
	orphan := exec.Command(workerPath, "100")
	require.NoError(t, orphan.Start())

	// A live process that must NOT be reaped: same binary, "worker" nowhere
	// in its cmdline.
	unrelated := exec.Command(sleep, "100")
	require.NoError(t, unrelated.Start())
	defer func() {
		unrelated.Process.Kill()
		unrelated.Wait()
	}()

	// pid file: a dead pid, a garbage line, and the live orphan's pid.
	pidFile := filepath.Join(dir, "worker-pids")
	require.NoError(t, ioutil.WriteFile(pidFile, []byte(fmt.Sprintf(
		"2147483647\nnot-a-pid\n%d\n", orphan.Process.Pid)), 0644))

	rt, err := NewRuntime(Options{WorkerBinary: "sh", LocalDir: dir, Runtime: "process"})
	require.NoError(t, err)
	rt.Close()

	// The orphan was SIGKILLed (Wait observes the signal); the unrelated
	// process is untouched; the pid file is gone.
	require.YesError(t, orphan.Wait())
	require.NoError(t, syscall.Kill(unrelated.Process.Pid, 0))
	_, err = os.Stat(pidFile)
	require.YesError(t, err)
}

func TestAllocIP(t *testing.T) {
	// WorkerPort is zero here, so the probe always succeeds and allocIP
	// hands out sequential loopback addresses starting at .2 (the daemon
	// itself owns .1).
	rt, _ := testRuntime(t)
	require.Equal(t, "127.0.0.2", rt.allocIP())
	require.Equal(t, "127.0.0.3", rt.allocIP())
}

// TestAllocIPWrap is the regression for the suite-killing bug where allocIP
// returned invalid addresses (127.0.0.256+) once the monotonic counter passed
// 254, crash-looping every new worker (a pipeline whose worker can't start
// never gets a job). The counter must wrap back to .2, where the availability
// probe skips addresses live workers still hold.
func TestAllocIPWrap(t *testing.T) {
	rt, _ := testRuntime(t)
	rt.nextIP = 258 // counter from a long daemon lifetime
	first := rt.allocIP()
	second := rt.allocIP()
	require.Equal(t, "127.0.0.2", first)
	require.Equal(t, "127.0.0.3", second)
	require.True(t, rt.nextIP >= 2 && rt.nextIP <= 255)
}

func TestLineTS(t *testing.T) {
	require.Equal(t,
		time.Date(2026, 8, 8, 0, 0, 1, 123456789, time.UTC),
		lineTS(`{"ts":"2026-08-08T00:00:01.123456789Z","message":"x"}`))
	require.Equal(t, time.Time{}, lineTS("plain stderr line"))
	require.Equal(t, time.Time{}, lineTS(`{"message":"no ts"}`))
}

// TestServeLogs pins the filtering semantics GetLogs relies on: full read in
// order, tailLines, since (non-JSON stderr lines have no timestamp and are
// dropped by since filters), and a JSON 404 for a missing pod.
func TestServeLogs(t *testing.T) {
	rt, dir := testRuntime(t)
	pod := "pipeline-v1-0000"
	logs := []string{
		`{"ts":"2026-08-08T00:00:01.000000000Z","message":"one"}`,
		`{"ts":"2026-08-08T00:00:02.000000000Z","message":"two"}`,
		"plain stderr line",
		`{"ts":"2026-08-08T00:00:03.000000000Z","message":"three"}`,
		`{"ts":"2026-08-08T00:00:04.000000000Z","message":"four"}`,
	}
	require.NoError(t, ioutil.WriteFile(
		filepath.Join(dir, "logs", pod+".jsonl"),
		[]byte(strings.Join(logs, "\n")+"\n"), 0644))

	// no filters: every line, in order
	rec := httptest.NewRecorder()
	rt.serveLogs(rec, httptest.NewRequest("GET", "/", nil), pod, false, -1, time.Time{})
	require.Equal(t, strings.Join(logs, "\n")+"\n", rec.Body.String())

	// tailLines: last 2
	rec = httptest.NewRecorder()
	rt.serveLogs(rec, httptest.NewRequest("GET", "/", nil), pod, false, 2, time.Time{})
	require.Equal(t, strings.Join(logs[3:], "\n")+"\n", rec.Body.String())

	// since: only lines after the cutoff; the un-timestamped stderr line is
	// filtered out with the rest
	since := time.Date(2026, 8, 8, 0, 0, 2, 0, time.UTC)
	rec = httptest.NewRecorder()
	rt.serveLogs(rec, httptest.NewRequest("GET", "/", nil), pod, false, -1, since)
	require.Equal(t, strings.Join(logs[3:], "\n")+"\n", rec.Body.String())

	// missing pod: JSON 404 (rest.Request decodes the body)
	rec = httptest.NewRecorder()
	rt.serveLogs(rec, httptest.NewRequest("GET", "/", nil), "missing-pod", false, -1, time.Time{})
	require.Equal(t, http.StatusNotFound, rec.Code)
	require.True(t, strings.Contains(rec.Body.String(), "not found"))
}

// TestHandleLogs covers the URL-shape and query-parameter parsing of the log
// endpoint as rest.Request encodes it.
func TestHandleLogs(t *testing.T) {
	rt, dir := testRuntime(t)
	pod := "pipeline-v1-0000"
	lines := []string{
		`{"ts":"2026-08-08T00:00:01.000000000Z","message":"one"}`,
		`{"ts":"2026-08-08T00:00:02.000000000Z","message":"two"}`,
	}
	require.NoError(t, ioutil.WriteFile(
		filepath.Join(dir, "logs", pod+".jsonl"),
		[]byte(strings.Join(lines, "\n")+"\n"), 0644))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/v1/namespaces/default/pods/"+pod+"/log?tailLines=1", nil)
	rt.handleLogs(rec, req)
	require.Equal(t, lines[1]+"\n", rec.Body.String())

	rec = httptest.NewRecorder()
	req = httptest.NewRequest("GET", "/api/v1/namespaces/default/pods/"+pod+"/log", nil)
	rt.handleLogs(rec, req)
	require.Equal(t, strings.Join(lines, "\n")+"\n", rec.Body.String())
}

// TestGetLogs drives the whole GetLogs path the pps API uses: a
// rest.Request through the runtime's log server. The missing-pod case is the
// regression for the nil-RenegotiatedDecoder panic.
func TestGetLogs(t *testing.T) {
	rt, _ := testRuntime(t)
	// NewRuntime seeds an empty pachd pod log.
	line := `{"ts":"2026-08-08T00:00:01.000000000Z","message":"hello"}`
	require.NoError(t, ioutil.WriteFile(
		filepath.Join(rt.opts.LocalDir, "logs", "pachd.jsonl"),
		[]byte(line+"\n"), 0644))

	cs := NewClientset(rt)
	two := int64(2)
	body, err := cs.CoreV1().Pods("default").GetLogs("pachd", &v1.PodLogOptions{TailLines: &two}).DoRaw()
	require.NoError(t, err)
	require.Equal(t, line+"\n", string(body))

	_, err = cs.CoreV1().Pods("default").GetLogs("no-such-pod", &v1.PodLogOptions{}).DoRaw()
	require.YesError(t, err)
}
