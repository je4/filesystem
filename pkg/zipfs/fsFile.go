package zipfs

import (
	"emperror.dev/errors"
	"fmt"
	"github.com/je4/filesystem/v3/pkg/writefs"
	"github.com/je4/utils/v2/pkg/zLogger"
	"io"
	"io/fs"
)

type OpenRawZipCloserFS interface {
	OpenRawZipFS
	io.Closer
}

func NewFSFile(baseFS fs.FS, path string, logger zLogger.ZLogger) (*fsFile, error) {
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
	zfs, err := NewFS(fpAt, stat.Size(), fmt.Sprintf("fsFile(%v/%s)", baseFS, path), logger)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot open zipfs for file '%s'", path)
	}
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
	_ fs.FS           = (*fsFile)(nil)
	_ writefs.CloseFS = (*fsFile)(nil)
	_ fs.ReadDirFS    = (*fsFile)(nil)
	_ fs.ReadFileFS   = (*fsFile)(nil)
	_ fs.StatFS       = (*fsFile)(nil)
	//_ fs.SubFS           = (*fsFile)(nil)
	_ writefs.IsLockedFS = (*fsFile)(nil)
	_ OpenRawZipFS       = (*fsFile)(nil)
	_ fmt.Stringer       = (*fsFile)(nil)
)
