package zipasfolder

import (
	"emperror.dev/errors"
	"github.com/je4/filesystem/v3/pkg/zipfs"
	"github.com/je4/utils/v2/pkg/zLogger"
	"io"
	"io/fs"
)

func NewZipFSCloser(zipFile fs.File, filename string, logger zLogger.ZLogger) (fs.FS, error) {
	readerAt, ok := zipFile.(io.ReaderAt)
	if !ok {
		return nil, errors.New("zipFile does not implement io.ReaderAt")
	}
	zstat, err := zipFile.Stat()
	if err != nil {
		return nil, errors.Wrap(err, "cannot stat zip file")
	}
	zfs, err := zipfs.NewFS(readerAt, zstat.Size(), filename, logger)
	return &zipFSCloser{
		FS:      zfs,
		zipFile: zipFile,
		logger:  logger,
	}, nil
}

type zipFSCloser struct {
	fs.FS
	zipFile fs.File
	logger  zLogger.ZLogger
}

func (zipFS *zipFSCloser) Stat(name string) (fs.FileInfo, error) {
	statFS, ok := zipFS.FS.(fs.StatFS)
	if !ok {
		return nil, errors.New("s3FSRW does not implement StatFS")
	}
	return statFS.Stat(name)
}

func (zipFS *zipFSCloser) ReadDir(name string) ([]fs.DirEntry, error) {
	readDirFS, ok := zipFS.FS.(fs.ReadDirFS)
	if !ok {
		return nil, errors.New("s3FSRW does not implement ReadDirFS")
	}
	return readDirFS.ReadDir(name)
}

func (zipFS *zipFSCloser) Close() error {
	return errors.WithStack(zipFS.zipFile.Close())
}

var (
	_ fs.FS        = &zipFSCloser{}
	_ fs.ReadDirFS = &zipFSCloser{}
	_ fs.StatFS    = &zipFSCloser{}
)
