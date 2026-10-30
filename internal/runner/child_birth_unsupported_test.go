//go:build !darwin || !cgo

package runner

import "testing"

// The unsupported capture is an explicit refusal, not a synthetic birth tuple.
func TestChildBirthUnsupported(t *testing.T) {
	birth, err := captureChildBirth(123)
	if err != errChildBirthUnsupported || birth != (childBirth{}) {
		t.Fatalf("BIRTH_UNSUPPORTED: birth=%+v err=%v", birth, err)
	}
}
