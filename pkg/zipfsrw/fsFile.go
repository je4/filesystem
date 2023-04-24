package zipfsrw

import (
	"bufio"
	"emperror.dev/errors"
	"fmt"
	"github.com/je4/filesystem/v2/pkg/writefs"
	"github.com/je4/filesystem/v2/pkg/zipfs"
	"io/fs"
)

// NewZipFSRW creates a new ReadWriteFS
// If the file does not exist, it will be created on the first write operation.
// If the file exists, it will be opened and read.
// Changes will be written to an additional file and then renamed to the original file.
func NewFSFile(baseFS fs.FS, path string) (writefs.ReadWriteFS, error) {
	newpath := path

	var zfs zipfs.OpenRawZipFS

	if xfs, err := zipfs.NewFSFile(baseFS, path); err != nil {
		if errors.Cause(err) != fs.ErrNotExist {
			return nil, errors.Wrapf(err, "cannot open zip file '%s'", path)
		}
	} else {
		zfs = xfs
		newpath = path + ".tmp"
	}

	// create new file
	fp, err := writefs.Create(baseFS, newpath)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot create zip file '%s'", newpath)
	}
	// add a buffer to the file
	newFPBuffer := bufio.NewWriterSize(fp, 1024*1024)

	zipFSRWBase, err := NewFS(newFPBuffer, zfs)
	if err != nil {
		return nil, errors.Wrap(err, "cannot create zipFSRW")
	}

	return &fsFile{
		zipFSRW:     zipFSRWBase,
		name:        path,
		tmpName:     newpath,
		baseFS:      baseFS,
		zipFP:       fp,
		zipFPBuffer: newFPBuffer,
	}, nil
}

type fsFile struct {
	*zipFSRW
	baseFS fs.FS
	//fp          fs.File
	zipFP       writefs.FileWrite
	zipFPBuffer *bufio.Writer
	name        string
	tmpName     string
}

func (zfsrw *fsFile) String() string {
	return fmt.Sprintf("fsFile(%s)", zfsrw.name)
}

func (zfsrw *fsFile) Close() error {
	var errs = []error{}

	if err := zfsrw.zipFSRW.Close(); err != nil {
		errs = append(errs, err)
	}
	if err := zfsrw.zipFPBuffer.Flush(); err != nil {
		errs = append(errs, err)
	}
	if err := zfsrw.zipFP.Close(); err != nil {
		errs = append(errs, err)
	}
	if zfsrw.HasChanged() && zfsrw.name != zfsrw.tmpName {
		if err := writefs.Remove(zfsrw.baseFS, zfsrw.name); err != nil {
			errs = append(errs, err)
		}
		if err := writefs.Rename(zfsrw.baseFS, zfsrw.tmpName, zfsrw.name); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return errors.WithStack(errors.Combine(errs...))
	}

	return nil
}
