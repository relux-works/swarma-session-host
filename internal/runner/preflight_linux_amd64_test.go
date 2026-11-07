package runner

import (
	"errors"
	"os"
	"syscall"
	"testing"
	"time"
)

func requireLinuxPreflight(t *testing.T) {
	t.Helper()
	if err := preflightLinuxCapture(); err != nil {
		if os.Getenv("SESSION_HOST_REQUIRE_LINUX_PIDFS") == "1" {
			t.Fatal(err)
		}
		t.Skip("LINUX_PIDFS_NOT_QUALIFIED", err)
	}
}

func TestLinuxPreflightFailureCleanupAndDeadline(t *testing.T) {
	requireLinuxPreflight(t)
	stages := []string{"self", "peer", "spawn", "capture", "live", "kill", "zombie", "wait", "reaped"}
	for _, expire := range []bool{false, true} {
		for index, target := range stages {
			label := target
			if expire {
				label += "-expired"
			}
			t.Run(label, func(t *testing.T) {
				now := time.Now()
				var pid, fd, waits int
				fd = -1
				var seen []string
				err := preflightLinuxCaptureWith(linuxPreflightHooks{
					now: func() time.Time { return now },
					stage: func(name string) error {
						seen = append(seen, name)
						if name == target {
							if expire {
								now = now.Add(preflightObservationTimeout)
								return nil
							}
							return errChildBirthUnavailable
						}
						return nil
					},
					capture: func(p, f int) linuxOriginal { pid, fd = p, f; return captureLinuxOriginal(p, f) },
					wait:    func(p *os.Process) (*os.ProcessState, error) { waits++; pid = p.Pid; return p.Wait() },
				})
				if err == nil || len(seen) != index+1 {
					t.Fatalf("gate/stages: %v %v", err, seen)
				}
				expectedWaits := 0
				if index > 2 {
					expectedWaits = 1
				}
				if waits != expectedWaits {
					t.Fatalf("wait attempts=%d want=%d", waits, expectedWaits)
				}
				if pid != 0 && syscall.Kill(pid, 0) != syscall.ESRCH {
					t.Fatal("probe not reaped", pid)
				}
				if fd >= 0 {
					if _, _, e := syscall.Syscall(syscall.SYS_FCNTL, uintptr(fd), syscall.F_GETFD, 0); e != syscall.EBADF {
						t.Fatal("retained fd leaked", fd, e)
					}
				}
			})
		}
	}
}

func TestLinuxPreflightWaitErrorNeverRetries(t *testing.T) {
	requireLinuxPreflight(t)
	waits := 0
	err := preflightLinuxCaptureWith(linuxPreflightHooks{wait: func(p *os.Process) (*os.ProcessState, error) {
		waits++
		state, err := p.Wait()
		if err != nil {
			t.Fatal(err)
		}
		return state, errors.New("injected uncertain wait result")
	}})
	if err == nil || waits != 1 {
		t.Fatalf("error=%v waits=%d", err, waits)
	}
}

func TestLinuxPreflightDeadlineDoesNotResetOrGrantLateSuccess(t *testing.T) {
	requireLinuxPreflight(t)
	now := time.Now()
	var seen []string
	err := preflightLinuxCaptureWith(linuxPreflightHooks{
		now: func() time.Time { return now },
		stage: func(name string) error {
			seen = append(seen, name)
			if name == "self" {
				now = now.Add(900 * time.Millisecond)
			}
			if name == "peer" {
				now = now.Add(100 * time.Millisecond)
			}
			return nil
		},
	})
	if err == nil || len(seen) != 2 {
		t.Fatal("budget reset", err, seen)
	}
	now = time.Now()
	waits := 0
	err = preflightLinuxCaptureWith(linuxPreflightHooks{
		now: func() time.Time { return now },
		wait: func(p *os.Process) (*os.ProcessState, error) {
			waits++
			state, err := p.Wait()
			now = now.Add(preflightObservationTimeout)
			return state, err
		},
	})
	if err == nil || waits != 1 {
		t.Fatal("late wait granted success/retried", err, waits)
	}
}

func TestLinuxPreflightPartialCaptureCleanup(t *testing.T) {
	requireLinuxPreflight(t)
	waits := 0
	err := preflightLinuxCaptureWith(linuxPreflightHooks{
		capture: func(pid, fd int) linuxOriginal {
			return linuxOriginal{err: errChildBirthUnavailable, release: (&linuxFDSet{fds: []int{fd}}).close}
		},
		wait: func(p *os.Process) (*os.ProcessState, error) { waits++; return p.Wait() },
	})
	if err == nil || waits != 1 {
		t.Fatal(err, waits)
	}
}

func TestLinuxV2SelectedPreflightOnceBeforeListen(t *testing.T) {
	requireLinuxPreflight(t)
	cfg := testConfig(t, "echo")
	cfg.IdentifyV2 = true
	calls := 0
	s, err := startWithPreflight(cfg, captureChildBirth, waitOriginalChild, nil, func() error {
		calls++
		if _, err := os.Lstat(cfg.Socket); !os.IsNotExist(err) {
			t.Fatal("Listen reached before preflight", err)
		}
		return preflightLinuxCapture()
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	trackTestRunner(t, s)
	cfg.IdentifyV2 = false // modifying caller's copy cannot change this lifetime
	for range 3 {
		r := v2Call(t, s)
		if r.Result != "verified" {
			t.Fatal(r)
		}
	}
	if calls != 1 || !s.cfg.IdentifyV2 {
		t.Fatal("selection/probe lifetime", calls, s.cfg.IdentifyV2)
	}
}
