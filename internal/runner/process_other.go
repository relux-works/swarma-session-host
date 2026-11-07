//go:build !linux || !amd64

package runner

import "os"

func preflightIdentifyV2() error { return errChildBirthUnsupported }

func startOriginalProcess(selected bool, binary string, args []string, attr *os.ProcAttr) (*os.Process, linuxOriginal, error) {
	if selected {
		return nil, linuxOriginal{err: errChildBirthUnsupported}, errChildBirthUnsupported
	}
	p, err := os.StartProcess(binary, args, attr)
	return p, linuxOriginal{err: errChildBirthUnsupported}, err
}
