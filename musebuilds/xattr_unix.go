//go:build darwin || linux

package musebuilds

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"

	"golang.org/x/sys/unix"
)

// xattrCopyBound caps one extended-attribute value during the freeze
// copy. Values past the bound refuse rather than truncate.
const xattrCopyBound = 1024 * 1024

// captureXattrs takes one descriptor-bound attribute snapshot before copying.
// A total bound prevents a large set of individually small values from
// consuming unbounded memory. No source pathname is reopened.
func captureXattrs(ctx context.Context, file *os.File) (map[string][]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	names, err := listXattrsFD(file)
	if err != nil {
		if runtime.GOOS != "darwin" && errors.Is(err, unix.EOPNOTSUPP) {
			return map[string][]byte{}, nil
		}
		return nil, err
	}
	attrs := make(map[string][]byte, len(names))
	total := 0
	for _, name := range names {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		value, err := readXattrFD(file, name)
		if err != nil {
			return nil, err
		}
		total += len(name) + len(value)
		if total > xattrCopyBound {
			return nil, fmt.Errorf("attribute snapshot past bound")
		}
		attrs[name] = value
	}
	return attrs, ctx.Err()
}

func copyXattrs(ctx context.Context, attrs map[string][]byte, dst *os.File) error {
	for name, value := range attrs {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := unix.Fsetxattr(int(dst.Fd()), name, value, 0); err != nil {
			return fmt.Errorf("xattr %q: %w", name, err)
		}
	}
	return nil
}

// A staged quarantine flag refuses independently of equality. Its lookup is
// fgetxattr on the same descriptor used to copy, hash and publish.
func verifyStagedXattrs(ctx context.Context, want map[string][]byte, staged *os.File) error {
	quarantined, err := quarantineFDCheck(staged)
	if err != nil {
		return err
	}
	if quarantined {
		return errQuarantinedCopy
	}
	got, err := captureXattrs(ctx, staged)
	if err != nil {
		return err
	}
	if len(got) != len(want) {
		return fmt.Errorf("staged attributes differ from the source snapshot")
	}
	for name, value := range want {
		found, ok := got[name]
		if !ok || !bytes.Equal(found, value) {
			return fmt.Errorf("staged xattr %q disagrees with source snapshot", name)
		}
	}
	return nil
}

func listXattrsFD(file *os.File) ([]string, error) {
	buf := make([]byte, 4096)
	var size int
	for {
		got, err := unix.Flistxattr(int(file.Fd()), buf)
		if err == nil {
			size = got
			break
		}
		if !errors.Is(err, unix.ERANGE) {
			if errors.Is(err, unix.EOPNOTSUPP) {
				return nil, err
			}
			return nil, fmt.Errorf("list xattrs: %w", err)
		}
		if len(buf) >= xattrCopyBound {
			return nil, fmt.Errorf("xattr list past the bound")
		}
		buf = make([]byte, len(buf)*2)
	}
	return splitXattrNames(buf, size), nil
}

func splitXattrNames(buf []byte, size int) []string {
	var names []string
	start := 0
	for i := 0; i < size; i++ {
		if buf[i] == 0 {
			if i > start {
				names = append(names, string(buf[start:i]))
			}
			start = i + 1
		}
	}
	return names
}

func readXattrFD(file *os.File, name string) ([]byte, error) {
	buf := make([]byte, 4096)
	var size int
	for {
		got, err := unix.Fgetxattr(int(file.Fd()), name, buf)
		if err == nil {
			size = got
			break
		}
		if !errors.Is(err, unix.ERANGE) {
			return nil, fmt.Errorf("xattr %q: %w", name, err)
		}
		if len(buf) >= xattrCopyBound {
			return nil, fmt.Errorf("xattr %q past the bound", name)
		}
		buf = make([]byte, len(buf)*2)
	}
	return append([]byte(nil), buf[:size]...), nil
}
