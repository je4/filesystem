package zipasfolder

import (
	"emperror.dev/errors"
	"io"
	"io/fs"
)

func NewFile(fileInfo fs.FileInfo, rc io.ReadCloser, lock *mutex) *File {
	return &File{
		ReadCloser: rc,
		lock:       lock,
		fileInfo:   fileInfo,
	}
}

type File struct {
	io.ReadCloser
	lock     *mutex
	fileInfo fs.FileInfo
}

func (rcm *File) Stat() (fs.FileInfo, error) {
	return rcm.fileInfo, nil
}

func (rcm *File) Close() error {
	defer rcm.lock.Unlock()
	return errors.WithStack(rcm.ReadCloser.Close())
}

var _ fs.File = &File{}
