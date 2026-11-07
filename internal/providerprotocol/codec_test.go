package providerprotocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"strings"
	"testing"
)

func wire(s string) []byte {
	b := make([]byte, 4, len(s)+4)
	binary.BigEndian.PutUint32(b, uint32(len(s)))
	return append(b, []byte(s)...)
}

func TestClosedCodec(t *testing.T) {
	valid := `{"version":1,"id":1,"type":"hello"}`
	cases := map[string][]byte{
		"unknown-version":   wire(`{"version":2,"id":1,"type":"hello"}`),
		"missing-version":   wire(`{"id":1,"type":"hello"}`),
		"zero-id":           wire(`{"version":1,"id":0,"type":"hello"}`),
		"float-id":          wire(`{"version":1,"id":1.0,"type":"hello"}`),
		"unknown-type":      wire(`{"version":1,"id":1,"type":"trigger"}`),
		"wrong-case":        wire(`{"Version":1,"id":1,"type":"hello"}`),
		"duplicate":         wire(`{"version":1,"id":1,"id":2,"type":"hello"}`),
		"escaped-duplicate": wire(`{"version":1,"id":1,"\u0069d":2,"type":"hello"}`),
		"unknown-key":       wire(`{"version":1,"id":1,"type":"hello","origin":"private"}`),
		"null-body":         wire(`{"version":1,"id":1,"type":"hello","body":null}`),
		"extra-body":        wire(`{"version":1,"id":1,"type":"hello","body":{}}`),
		"missing-body":      wire(`{"version":1,"id":1,"type":"prepared"}`),
		"missing-field":     wire(`{"version":1,"id":1,"type":"refused","body":{}}`),
		"null-field":        wire(`{"version":1,"id":1,"type":"refused","body":{"reason":null}}`),
		"unknown-reason":    wire(`{"version":1,"id":1,"type":"refused","body":{"reason":"private"}}`),
		"nested-duplicate":  wire(`{"version":1,"id":1,"type":"refused","body":{"reason":"origin-unavailable","reason":"lease-unavailable"}}`),
		"nested-unknown":    wire(`{"version":1,"id":1,"type":"refused","body":{"reason":"origin-unavailable","origin":"private"}}`),
		"nested-wrong-case": wire(`{"version":1,"id":1,"type":"refused","body":{"Reason":"origin-unavailable"}}`),
		"array":             wire(`{"version":1,"id":1,"type":"refused","body":[]}`),
		"trailing-value":    wire(valid + `{}`),
		"invalid-utf8":      wire("{\"version\":1,\"id\":1,\"type\":\"\xff\"}"),
		"truncated-header":  {0, 0},
		"truncated-body":    append([]byte{0, 0, 0, 100}, []byte(valid)...),
		"zero-length":       {0, 0, 0, 0},
		"oversize":          {0, 0, 128, 1},
	}
	for name, b := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := ReadFrame(bytes.NewReader(b))
			if err == nil {
				t.Fatal("accepted invalid frame")
			}
			if name == "unknown-version" && !errors.Is(err, ErrVersion) {
				t.Fatal(err)
			}
			if strings.Contains(err.Error(), "private") {
				t.Fatal("payload in diagnostic")
			}
		})
	}
	// Boundary and non-prefetch control: the next protocol remains untouched.
	padded := valid + strings.Repeat(" ", MaxFrame-len(valid))
	r := bytes.NewReader(append(wire(padded), wire(valid)...))
	if _, err := ReadFrame(r); err != nil {
		t.Fatal(err)
	}
	if r.Len() != len(wire(valid)) {
		t.Fatal("read crossed a frame boundary")
	}
	if _, err := ReadFrame(r); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadFrame(r); !errors.Is(err, ErrUnresolved) {
		t.Fatal(err)
	}
}

type fragmentedWriter struct{ bytes.Buffer }

func (w *fragmentedWriter) Write(p []byte) (int, error) {
	if len(p) > 3 {
		p = p[:3]
	}
	return w.Buffer.Write(p)
}

type zeroWriter struct{}

func (zeroWriter) Write([]byte) (int, error) { return 0, nil }

type failedWriter struct{}

func (failedWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

func TestWriteBoundsAndPartialWrites(t *testing.T) {
	f, err := NewFrame(1, "hello", nil)
	if err != nil {
		t.Fatal(err)
	}
	var w fragmentedWriter
	if err := WriteFrame(&w, f); err != nil {
		t.Fatal(err)
	}
	if got, err := ReadFrame(&w); err != nil || got.Type != "hello" {
		t.Fatal(got.Type, err)
	}
	for _, w := range []io.Writer{zeroWriter{}, failedWriter{}} {
		if err := WriteFrame(w, f); !errors.Is(err, ErrUnresolved) {
			t.Fatal(err)
		}
	}
	if _, err := NewFrame(1, "prepared", References{MemberRef: strings.Repeat("x", 513), ConnectionRef: "connection", ConfigurationRef: "configuration", ProfileLockRef: "profile"}); err == nil {
		t.Fatal("oversized reference")
	}
	if _, err := NewFrame(1, "hello", map[string]string{"padding": strings.Repeat("x", MaxFrame)}); err == nil {
		t.Fatal("oversized body")
	}
}

func TestCanonicalLeaseExpiry(t *testing.T) {
	for _, s := range []string{"2026-11-07T11:00:00+00:00", "2026-11-07T11:00:00.0000000000Z", "2026-11-07T11:00:00.1+01:00"} {
		e := enrolled(bootstrap())
		e.LeaseExpiresAt = s
		if _, err := NewFrame(1, "enrollment", e); err == nil {
			t.Fatal("accepted noncanonical expiry")
		}
	}
}

func FuzzReadFrame(f *testing.F) {
	f.Add(wire(`{"version":1,"id":1,"type":"hello"}`))
	f.Add([]byte{0, 0, 0, 0})
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) > MaxFrame+4 {
			t.Skip()
		}
		frame, err := ReadFrame(bytes.NewReader(b))
		if err != nil {
			return
		}
		var out bytes.Buffer
		if err := WriteFrame(&out, frame); err != nil {
			t.Fatal(err)
		}
		if _, err := ReadFrame(&out); err != nil {
			t.Fatal(err)
		}
	})
}
