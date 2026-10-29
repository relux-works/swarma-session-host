// Package fixtureadapters supplies two synthetic protocols to exercise plugins.
// Neither adapter implements a real agent harness.
package fixtureadapters

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"

	"github.com/relux-works/swarma-session-host/internal/localipc"
	"github.com/relux-works/swarma-session-host/internal/sessionhost"
)

type JSON struct{}

func (JSON) Name() string { return "json-fixture" }

type Envelope struct {
	Kind      string                `json:"kind"`
	Notice    sessionhost.Notice    `json:"notice"`
	Selection sessionhost.Selection `json:"selection"`
}

func (JSON) Encode(n sessionhost.Notice, s sessionhost.Selection) ([]byte, error) {
	data, err := json.Marshal(Envelope{Kind: "notice", Notice: n, Selection: s})
	return append(data, '\n'), err
}

func (JSON) Applied(output []byte, n sessionhost.Notice, s sessionhost.Selection) (bool, error) {
	lines := bytes.Split(output, []byte{'\n'})
	for _, line := range lines[:len(lines)-1] {
		var envelope Envelope
		if err := localipc.Decode(bytes.NewReader(line), &envelope); err != nil || envelope.Kind != "applied" {
			return false, errors.New("invalid_fixture_ack")
		}
		if envelope.Notice == n && envelope.Selection == s {
			return true, nil
		}
	}
	return false, nil
}

// Lines uses a distinct wire encoding. Adding it requires no core changes.
type Lines struct{}

func (Lines) Name() string { return "line-fixture" }

func (Lines) Encode(n sessionhost.Notice, s sessionhost.Selection) ([]byte, error) {
	fields := []string{n.ID, n.Reference, s.Vendor, s.Model, s.Reasoning}
	for _, field := range fields {
		if strings.ContainsAny(field, "\t\r\n") {
			return nil, errors.New("invalid_fixture_field")
		}
	}
	return []byte(strings.Join(fields, "\t") + "\n"), nil
}

func (a Lines) Applied(output []byte, n sessionhost.Notice, s sessionhost.Selection) (bool, error) {
	expected, err := a.Encode(n, s)
	if err != nil {
		return false, err
	}
	lines := bytes.Split(output, []byte{'\n'})
	for _, line := range lines[:len(lines)-1] {
		if !bytes.HasPrefix(line, []byte("applied\t")) || bytes.Count(line, []byte{'\t'}) != 5 {
			return false, errors.New("invalid_fixture_ack")
		}
		if bytes.Equal(bytes.TrimPrefix(line, []byte("applied\t")), bytes.TrimSuffix(expected, []byte{'\n'})) {
			return true, nil
		}
	}
	return false, nil
}
