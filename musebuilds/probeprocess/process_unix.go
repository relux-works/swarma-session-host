//go:build darwin || linux

package probeprocess

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

type osProcess struct {
	cmd         *exec.Cmd
	out, stderr *os.File
	done        chan Exit
}

func (OSLauncher) Start(ctx context.Context, executable *os.File, env []string) (Process, error) {
	if err := ctx.Err(); err != nil {
		return nil, errors.Join(ErrNotAttempted, err)
	}
	// Darwin's descriptor namespace is not a portable exec API. Keep the
	// verified FD and verify the native execution path immediately before
	// Start. The active same-account pathname race is design section 7's
	// explicit residual, not a claim of atomic descriptor execution.
	checkFD, err := unix.Open(executable.Name(), unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NONBLOCK|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, errors.Join(ErrNotAttempted, ErrIdentityChanged, err)
	}
	check := os.NewFile(uintptr(checkFD), executable.Name())
	defer check.Close()
	wanted, err := executable.Stat()
	if err != nil {
		return nil, errors.Join(ErrNotAttempted, ErrIdentityChanged, err)
	}
	actual, err := check.Stat()
	if err != nil {
		return nil, errors.Join(ErrNotAttempted, ErrIdentityChanged, err)
	}
	if !actual.Mode().IsRegular() || !os.SameFile(wanted, actual) {
		return nil, errors.Join(ErrNotAttempted, ErrIdentityChanged)
	}
	out, outWriter, err := os.Pipe()
	if err != nil {
		return nil, errors.Join(ErrNotAttempted, err)
	}
	errOut, errWriter, err := os.Pipe()
	if err != nil {
		_ = out.Close()
		_ = outWriter.Close()
		return nil, errors.Join(ErrNotAttempted, err)
	}
	cmd := exec.CommandContext(ctx, executable.Name(), "--version")
	cmd.Env = append([]string{}, env...)
	cmd.Stdout, cmd.Stderr = outWriter, errWriter
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.WaitDelay = 5 * time.Second
	p := &osProcess{cmd: cmd, out: out, stderr: errOut, done: make(chan Exit, 1)}
	cmd.Cancel = p.Kill
	if err := ctx.Err(); err != nil {
		_ = out.Close()
		_ = outWriter.Close()
		_ = errOut.Close()
		_ = errWriter.Close()
		return nil, errors.Join(ErrNotAttempted, err)
	}
	err = cmd.Start()
	_ = outWriter.Close()
	_ = errWriter.Close()
	if err != nil {
		_ = out.Close()
		_ = errOut.Close()
		return nil, err
	}
	go func() {
		err := cmd.Wait()
		exit := Exit{Code: -1, Err: err}
		if cmd.ProcessState != nil {
			exit.Code = cmd.ProcessState.ExitCode()
			if status, ok := cmd.ProcessState.Sys().(syscall.WaitStatus); ok && status.Signaled() {
				exit.Signal = status.Signal().String()
			}
		}
		p.done <- exit
	}()
	return p, nil
}

func (p *osProcess) Stdout() io.ReadCloser { return p.out }
func (p *osProcess) Stderr() io.ReadCloser { return p.stderr }
func (p *osProcess) Done() <-chan Exit     { return p.done }
func (p *osProcess) Kill() error {
	if p.cmd.Process == nil {
		return os.ErrProcessDone
	}
	err := syscall.Kill(-p.cmd.Process.Pid, syscall.SIGKILL)
	if errors.Is(err, syscall.ESRCH) {
		return nil
	}
	return err
}
func (p *osProcess) GroupGone() bool {
	return p.cmd.Process != nil && errors.Is(syscall.Kill(-p.cmd.Process.Pid, 0), syscall.ESRCH)
}
