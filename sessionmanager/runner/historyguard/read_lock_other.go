//go:build !unix

package historyguard

import "context"

func ReadLock(context.Context, string) (func(), error) { return nil, ErrUncertain }
