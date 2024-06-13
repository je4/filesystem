package remotefs

import (
	"emperror.dev/errors"
	"io"
	"io/fs"
	"time"
)

type file struct {
	d    *remoteFSRW
	name string
	rc   io.ReadCloser
}

func (f *file) Read(p []byte) (n int, err error) {
	return f.rc.Read(p)
}

func (f *file) Close() error {
	return f.rc.Close()
}

func (f *file) Stat() (info fs.FileInfo, err error) {
	return f.d.Stat(f.name)
}

type fileWrite struct {
	d    *remoteFSRW
	name string
	wc   io.WriteCloser
	done chan error
}

func (f *fileWrite) Write(p []byte) (n int, err error) {
	return f.wc.Write(p)
}

func (f *fileWrite) Close() error {
	if err := f.wc.Close(); err != nil {
		return err
	}
	select {
	case err := <-f.done:
		return err
	case <-time.After(3 * time.Second):
		return errors.New("timeout")
	}
}

var _ fs.File = (*file)(nil)
