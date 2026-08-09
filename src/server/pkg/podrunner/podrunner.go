// Package podrunner runs a single worker process and manages its lifecycle:
// starting it in its own process group (so user-code children die with it),
// recording its PID for orphan reaping, and killing the whole group on
// shutdown.
//
// It is the process-level half of the local k8s stand-in. The daemon's
// localclient runtime uses it to spawn local workers; the broker agent uses
// it to spawn workers on a remote node. Everything process-shaped (exec,
// process groups, PID files, log capture) lives here so the two callers
// share one implementation.
package podrunner

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"github.com/pachyderm/pachyderm/src/client/pkg/errors"
)

// Proc is a running worker process.
type Proc struct {
	// ID is the worker's stable identifier (the k8s pod name).
	ID string
	// Cmd is the underlying process. Stdout/Stderr are wired by the caller
	// (typically to a per-worker log file) before Start.
	Cmd *exec.Cmd
}

// Start launches cmd in a new process group. It returns once the process has
// started; the caller must eventually call Wait (to reap it) or Kill.
func Start(id string, cmd *exec.Cmd) (*Proc, error) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		return nil, errors.Wrapf(err, "could not start worker process for %q", id)
	}
	return &Proc{ID: id, Cmd: cmd}, nil
}

// Wait blocks until the process exits and returns its exit error (nil on
// clean exit).
func (p *Proc) Wait() error {
	return p.Cmd.Wait()
}

// Kill terminates the whole process group (so user code children are not
// orphaned), then reaps the process.
func (p *Proc) Kill() {
	if p.Cmd.Process == nil {
		return
	}
	// Kill the process group first; SIGKILL the leader afterwards in case the
	// group has already been reparented.
	syscall.Kill(-p.Cmd.Process.Pid, syscall.SIGKILL)
	p.Cmd.Process.Kill()
	p.Cmd.Wait()
}

// PidFile tracks the PIDs of running workers so that a restarted process can
// reap workers orphaned by a crash (they would otherwise linger and hold
// their ports hostage).
type PidFile struct {
	path string
}

// NewPidFile returns a PidFile stored under dir.
func NewPidFile(dir string) *PidFile {
	return &PidFile{path: filepath.Join(dir, "worker-pids")}
}

// Reap kills every PID recorded in the file (workers left over from a
// previous run) and clears it. Missing files are not an error.
func (f *PidFile) Reap() {
	data, err := ioutil.ReadFile(f.path)
	if err != nil {
		return
	}
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		if line == "" {
			continue
		}
		pid, err := strconv.Atoi(line)
		if err != nil {
			continue
		}
		// Kill the process group if it still exists; ignore errors (the
		// worker may already be gone).
		syscall.Kill(-pid, syscall.SIGKILL)
		syscall.Kill(pid, syscall.SIGKILL)
	}
	os.Remove(f.path)
}

// Record appends pid to the file.
func (f *PidFile) Record(pid int) error {
	fh, err := os.OpenFile(f.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return errors.Wrapf(err, "could not record worker pid")
	}
	defer fh.Close()
	if _, err := fmt.Fprintf(fh, "%d\n", pid); err != nil {
		return errors.Wrapf(err, "could not record worker pid")
	}
	return nil
}

// Forget removes pid from the file (best-effort; rewritten in place).
func (f *PidFile) Forget(pid int) {
	data, err := ioutil.ReadFile(f.path)
	if err != nil {
		return
	}
	var kept []string
	want := strconv.Itoa(pid)
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		if line != "" && line != want {
			kept = append(kept, line)
		}
	}
	ioutil.WriteFile(f.path, []byte(strings.Join(kept, "\n")+"\n"), 0644)
}
