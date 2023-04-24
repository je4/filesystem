package zipfs

import (
	"emperror.dev/errors"
	"fmt"
	"github.com/je4/filesystem/v2/pkg/writefs"
	"io"
	"io/fs"
)

func NewFSFile(baseFS fs.FS, path string) (*fsFile, error) {
	stat, err := fs.Stat(baseFS, path)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot stat file '%s'", path)
	}
	fp, err := baseFS.Open(path)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot open file '%s'", path)
	}
	fpAt, ok := fp.(io.ReaderAt)
	if !ok {
		return nil, errors.Errorf("cannot cast reader of file '%s' to io.ReaderAt", path)
	}
	zfs, err := NewFS(fpAt, stat.Size())

	return &fsFile{
		zipFS: zfs,
		fp:    fp,
	}, nil
}

type fsFile struct {
	*zipFS
	fp io.Closer
}

func (f *fsFile) Close() error {
	return errors.Wrapf(f.fp.Close(), "cannot close file '%v'", f.fp)
}

var (
	_ fs.FS              = (*fsFile)(nil)
	_ writefs.CloseFS    = (*fsFile)(nil)
	_ fs.ReadDirFS       = (*zipFS)(nil)
	_ fs.ReadFileFS      = (*zipFS)(nil)
	_ fs.StatFS          = (*zipFS)(nil)
	_ fs.SubFS           = (*zipFS)(nil)
	_ writefs.IsLockedFS = (*zipFS)(nil)
	_ OpenRawZipFS       = (*zipFS)(nil)
	_ fmt.Stringer       = (*zipFS)(nil)
)
