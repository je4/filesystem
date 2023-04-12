package zipfs

import (
	"github.com/je4/filesystem/v2/pkg/basefs"
	"io"
	"io/fs"
)

func NewFile(info fs.FileInfo, rc io.ReadCloser, mutex *basefs.Mutex) fs.File {
	return &file{
		ReadCloser: rc,
		fi:         info,
		mutex:      mutex,
	}
}

type file struct {
	io.ReadCloser
	fi    fs.FileInfo
	mutex *basefs.Mutex
}

func (f *file) Stat() (fs.FileInfo, error) {
	return f.fi, nil
}

func (f *file) Close() error {
	defer f.mutex.Unlock()
	return f.ReadCloser.Close()
}

var (
	_ fs.File       = (*file)(nil)
	_ io.ReadCloser = (*file)(nil)
)
