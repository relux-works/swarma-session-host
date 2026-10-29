//go:build !unix

package historyguard

import "context"

func Lock(context.Context, string, bool) (func(), error) { return nil, ErrUncertain }
