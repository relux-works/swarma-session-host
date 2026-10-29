// Package localipc contains the private, bounded transport of the fixture prototype.
package localipc

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
)

const MaxFrame = 64 << 10

func Read(reader *bufio.Reader, value any) error {
	line, err := reader.ReadSlice('\n')
	if err != nil {
		return err
	}
	return Decode(bytes.NewReader(line), value)
}

func Write(writer io.Writer, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if len(data)+1 > MaxFrame {
		return errors.New("frame_too_large")
	}
	data = append(data, '\n')
	n, err := writer.Write(data)
	if err == nil && n != len(data) {
		return io.ErrShortWrite
	}
	return err
}

// Decode accepts exactly one closed JSON object per line, with a fixed byte cap.
func Decode(reader io.Reader, value any) error {
	data, err := io.ReadAll(io.LimitReader(reader, MaxFrame+1))
	if err != nil {
		return err
	}
	if len(data) > MaxFrame {
		return errors.New("frame_too_large")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return errors.New("invalid_frame")
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return errors.New("invalid_frame")
	}
	return nil
}

// Listen refuses occupied paths; it never removes somebody else's socket.
func Listen(path string) (*net.UnixListener, error) {
	if !filepath.IsAbs(path) || len(path) > 100 {
		return nil, fmt.Errorf("invalid_socket_path")
	}
	parent, err := os.Lstat(filepath.Dir(path))
	if err != nil || !parent.IsDir() || parent.Mode().Perm()&0077 != 0 {
		return nil, errors.New("socket_directory_not_private")
	}
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(path, 0600); err != nil {
		listener.Close()
		return nil, err
	}
	return listener, nil
}
