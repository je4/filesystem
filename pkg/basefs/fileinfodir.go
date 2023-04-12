package basefs

import (
	"io/fs"
	"time"
)

// NewFileInfoDir creates a new FileInfo for a directory.
func NewFileInfoDir(base string) *fileInfoDir {
	return &fileInfoDir{
		base: base,
	}
}

type fileInfoDir struct {
	base string
}

func (fid *fileInfoDir) Name() string {
	return fid.base
}

func (fid *fileInfoDir) Size() int64 {
	return 0
}

func (fid *fileInfoDir) Mode() fs.FileMode {
	return fs.ModeDir | 0755
}

func (fid *fileInfoDir) ModTime() time.Time {
	return time.Time{}
}

func (fid *fileInfoDir) IsDir() bool {
	return true
}

func (fid *fileInfoDir) Sys() any {
	return nil
}

var (
	_ fs.FileInfo = (*fileInfoDir)(nil)
)
