//go:build !darwin || !cgo

package runner

func captureChildBirth(int) (childBirth, error) {
	return childBirth{}, errChildBirthUnsupported
}
