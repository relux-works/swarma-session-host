package musebuilds

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
)

// readMetadataDocument opens a freeze metadata document without
// following a trailing symlink and without blocking on a FIFO,
// requires a regular file within bound, optionally requires
// current-user ownership, and reads it with a context check per
// chunk. The size bound is enforced from fstat BEFORE reading, and
// again during the read so growth past the bound refuses instead of
// allocating. Absence (ENOENT on open) is (nil, nil, false, nil);
// every other failure is an error, never absence.
func readMetadataDocument(ctx context.Context, path string, bound int64, wantOwner bool) (raw []byte, info os.FileInfo, present bool, err error) {
	file, err := openMetadataFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil, false, nil
		}
		return nil, nil, false, err
	}
	defer func() { _ = file.Close() }()
	return readMetadataFD(ctx, file, bound, wantOwner)
}

func readMetadataFD(ctx context.Context, file *os.File, bound int64, wantOwner bool) (raw []byte, info os.FileInfo, present bool, err error) {
	path := file.Name()
	info, err = file.Stat()
	if err != nil {
		return nil, nil, false, err
	}
	if !info.Mode().IsRegular() {
		return nil, nil, false, fmt.Errorf("%s is not a regular file", path)
	}
	if info.Size() > bound {
		return nil, nil, false, fmt.Errorf("%s is %d bytes, past the %d bound", path, info.Size(), bound)
	}
	if wantOwner {
		if err := checkStoreOwner(path, info); err != nil {
			return nil, nil, false, err
		}
	}
	var buf bytes.Buffer
	chunk := make([]byte, 32*1024)
	reader := io.LimitReader(file, bound+1)
	var total int64
	for {
		if err := ctx.Err(); err != nil {
			return nil, nil, false, err
		}
		n, rerr := reader.Read(chunk)
		if n > 0 {
			total += int64(n)
			if total > bound {
				return nil, nil, false, fmt.Errorf("%s grew past the %d bound", path, bound)
			}
			_, _ = buf.Write(chunk[:n])
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			return nil, nil, false, rerr
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, false, err
	}
	return buf.Bytes(), info, true, nil
}
