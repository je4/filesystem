package zipfsrw

import (
	"bufio"
	"emperror.dev/errors"
	"fmt"
	"github.com/je4/filesystem/v3/pkg/writefs"
	"github.com/je4/filesystem/v3/pkg/zipfs"
	"io"
	"io/fs"
)

// NewZipFSRW creates a new ReadWriteFS
// If the file does not exist, it will be created on the first write operation.
// If the file exists, it will be opened and read.
// Changes will be written to an additional file and then renamed to the original file.
// additional writers will added via io.MultiWriter
// additional writers will not be closed
func NewFSFile(baseFS fs.FS, path string, noCompression bool, writers ...io.Writer) (*fsFile, error) {
	writerPath := path

	var zipFS zipfs.OpenRawZipFS

	if xfs, err := zipfs.NewFSFile(baseFS, path); err != nil {
		if errors.Cause(err) != fs.ErrNotExist {
			return nil, errors.Wrapf(err, "cannot open zip file '%s'", path)
		}
	} else {
		zipFS = xfs
		writerPath = path + ".tmp"
	}

	// create new file
	zipFP, err := writefs.Create(baseFS, writerPath)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot create zip file '%s'", writerPath)
	}
	// add a buffer to the file
	zipFPBuffer := bufio.NewWriterSize(zipFP, 1024*1024)

	var mainWriter io.Writer
	if len(writers) > 0 {
		mainWriter = io.MultiWriter(append(writers, zipFPBuffer)...)
	} else {
		mainWriter = zipFPBuffer
	}

	zipFSRWBase, err := NewFS(mainWriter, zipFS, noCompression, fmt.Sprintf("fsFile(%v/%s)", baseFS, path))
	if err != nil {
		return nil, errors.Wrap(err, "cannot create zipFSRW")
	}

	return &fsFile{
		zipFSRW:     zipFSRWBase,
		path:        path,
		writerPath:  writerPath,
		baseFS:      baseFS,
		zipFP:       zipFP,
		zipFPBuffer: zipFPBuffer,
		zipFS:       zipFS,
	}, nil
}

type fsFile struct {
	*zipFSRW
	baseFS fs.FS
	//fp          fs.File
	zipFP       writefs.FileWrite
	zipFPBuffer *bufio.Writer
	path        string
	writerPath  string
	zipFS       zipfs.OpenRawZipFS
}

func (zfsrw *fsFile) String() string {
	return fmt.Sprintf("fsFile(%v/%s)", zfsrw.baseFS, zfsrw.path)
}

func (zfsrw *fsFile) Close() error {
	var errs = []error{}

	if err := zfsrw.zipFSRW.Close(); err != nil {
		errs = append(errs, errors.WithStack(err))
	}
	if err := zfsrw.zipFPBuffer.Flush(); err != nil {
		errs = append(errs, errors.WithStack(err))
	}
	if err := zfsrw.zipFP.Close(); err != nil {
		errs = append(errs, errors.WithStack(err))
	}

	if zfsrw.zipFS != nil {
		if err := writefs.Close(zfsrw.zipFS); err != nil {
			errs = append(errs, errors.WithStack(err))
		}
	}

	if zfsrw.HasChanged() && zfsrw.path != zfsrw.writerPath {
		if err := writefs.Remove(zfsrw.baseFS, zfsrw.path); err != nil {
			errs = append(errs, errors.WithStack(err))
		}
		if err := writefs.Rename(zfsrw.baseFS, zfsrw.writerPath, zfsrw.path); err != nil {
			errs = append(errs, errors.WithStack(err))
		}
	}

	if len(errs) > 0 {
		return errors.Combine(errs...)
	}

	return nil
}
