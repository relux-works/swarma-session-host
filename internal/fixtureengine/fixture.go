// Package fixtureengine implements synthetic children, not agent sessions.
package fixtureengine

import (
	"bufio"
	"bytes"
	"errors"
	"io"

	"github.com/relux-works/swarma-session-host/internal/fixtureadapters"
	"github.com/relux-works/swarma-session-host/internal/localipc"
)

func Run(protocol string, input io.Reader, output io.Writer) error {
	if protocol != "json" && protocol != "line" {
		return errors.New("unsupported_fixture_protocol")
	}
	scanner := bufio.NewScanner(input)
	scanner.Buffer(make([]byte, 1024), 4096)
	for scanner.Scan() {
		line := scanner.Bytes()
		if protocol == "json" {
			var envelope fixtureadapters.Envelope
			if err := localipc.Decode(bytes.NewReader(line), &envelope); err != nil || envelope.Kind != "notice" {
				return errors.New("invalid_fixture_input")
			}
			envelope.Kind = "applied"
			if err := localipc.Write(output, envelope); err != nil {
				return err
			}
		} else {
			if bytes.Count(line, []byte{'\t'}) != 4 {
				return errors.New("invalid_fixture_input")
			}
			data := append(append([]byte("applied\t"), line...), '\n')
			n, err := output.Write(data)
			if err != nil {
				return err
			}
			if n != len(data) {
				return io.ErrShortWrite
			}
		}
	}
	return scanner.Err()
}
