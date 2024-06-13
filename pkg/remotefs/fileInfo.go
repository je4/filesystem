package remotefs

import (
	"io/fs"
	"time"
)

type fileInfo struct {
	Name_    string      `json:"name"`
	Size_    int64       `json:"size"`
	Mode_    fs.FileMode `json:"mode"`
	ModTime_ string      `json:"modTime"`
	IsDir_   bool        `json:"isDir"`
}

func (f *fileInfo) Name() string {
	return f.Name_
}

func (f *fileInfo) Size() int64 {
	return f.Size_
}

func (f *fileInfo) Mode() fs.FileMode {
	return f.Mode_
}

func (f *fileInfo) ModTime() time.Time {
	t, _ := time.Parse(time.RFC3339, f.ModTime_)
	return t
}

func (f *fileInfo) IsDir() bool {
	return f.IsDir_
}

func (f *fileInfo) Sys() any {
	return nil
}

var _ fs.FileInfo = (*fileInfo)(nil)
