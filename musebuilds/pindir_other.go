//go:build !unix

package musebuilds

import (
	"context"
	"errors"
	"os"
	"time"
)

type pinnedDir struct {
	file *os.File
	path string
}

var errPinnedUnsupported = errors.New("pinned publication needs unix")

func openDirPath(context.Context, string, bool) (*pinnedDir, error) { return nil, errPinnedUnsupported }
func openPinnedDir(string) (*pinnedDir, error)                      { return nil, errPinnedUnsupported }
func (d *pinnedDir) close() error                                   { return errPinnedUnsupported }
func (d *pinnedDir) mkdirChild(string) (bool, error)                { return false, errPinnedUnsupported }
func (d *pinnedDir) openChildDir(string) (*pinnedDir, error)        { return nil, errPinnedUnsupported }
func (d *pinnedDir) entries() ([]os.DirEntry, error)                { return nil, errPinnedUnsupported }
func (d *pinnedDir) openFile(string, int, os.FileMode) (*os.File, error) {
	return nil, errPinnedUnsupported
}
func (d *pinnedDir) openChildFile(string) (*os.File, error)          { return nil, errPinnedUnsupported }
func (d *pinnedDir) childExists(string) (bool, error)                { return false, errPinnedUnsupported }
func renamePinnedChild(*pinnedDir, *pinnedDir, string, string) error { return errPinnedUnsupported }
func (d *pinnedDir) chmodChild(string, os.FileMode) error            { return errPinnedUnsupported }
func (d *pinnedDir) chmodSelf(os.FileMode) error                     { return errPinnedUnsupported }
func (d *pinnedDir) sync() error                                     { return errPinnedUnsupported }
func (d *pinnedDir) chtimesChild(string, time.Time) error            { return errPinnedUnsupported }
