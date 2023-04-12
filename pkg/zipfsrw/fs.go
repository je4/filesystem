package zipfsrw

import (
	"archive/zip"
	"bufio"
	"emperror.dev/errors"
	"github.com/je4/filesystem/v2/pkg/basefs"
	"github.com/je4/filesystem/v2/pkg/readwritefs"
	"github.com/je4/filesystem/v2/pkg/writefs"
	"github.com/je4/filesystem/v2/pkg/zipfs"
	"golang.org/x/exp/slices"
	"io"
	"io/fs"
)

// NewZipFSRW creates a new ReadWriteFS
// If the file does not exist, it will be created on the first write operation.
// If the file exists, it will be opened and read.
// Changes will be written to an additional file and then renamed to the original file.
func NewZipFSRW(baseFS readwritefs.ReadWriteFS, path string) (readwritefs.ReadWriteFS, error) {
	var zfs zipfs.OpenRawZipFS
	var fp fs.File
	// if target file exists, open it and create a zipfs
	stat, err := fs.Stat(baseFS, path)
	if err == nil {
		fp, err = baseFS.Open(path)
		if err != nil {
			return nil, errors.Wrapf(err, "cannot open zip file '%s'", path)
		}
		fpwat, ok := fp.(io.ReaderAt)
		if !ok {
			return nil, errors.Errorf("cannot cast file '%s' to io.WriterAt", path)
		}
		_zfs, err := zipfs.NewFS(fpwat, stat.Size())
		if err != nil {
			return nil, errors.Wrapf(err, "cannot open zip file '%s'", path)
		}
		zfs, ok = _zfs.(zipfs.OpenRawZipFS)
		if !ok {
			return nil, errors.Errorf("cannot cast zip file '%s' to zipfs.OpenRawZipFS", path)
		}
	}
	newpath := path
	if zfs != nil {
		newpath = newpath + ".tmp"
	}
	// create new file
	newfp, err := baseFS.Create(newpath)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot create zip file '%s'", newpath)
	}
	// add a buffer to the file
	newFPBuffer := bufio.NewWriterSize(newfp, 1024*1024)
	// create a new zip writer
	zipWriter := zip.NewWriter(newFPBuffer)

	return &zipFSRW{
		baseFS:      baseFS,
		zipWriter:   zipWriter,
		zfs:         zfs,
		fp:          fp,
		zipFP:       newfp,
		zipFPBuffer: newFPBuffer,
		newFiles:    []string{},
	}, nil
}

type zipFSRW struct {
	baseFS      readwritefs.ReadWriteFS
	zfs         zipfs.OpenRawZipFS
	fp          fs.File
	zipWriter   *zip.Writer
	zipFP       writefs.FileWrite
	zipFPBuffer *bufio.Writer
	newFiles    []string
}

func (zfsrw *zipFSRW) Close() error {
	var errs = []error{}
	if zfsrw.fp != nil {
		if err := zfsrw.fp.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if zfsrw.zipWriter != nil && zfsrw.zipFP != nil {
		if err := zfsrw.zipWriter.Close(); err != nil {
			errs = append(errs, err)
		}
		if err := zfsrw.zipFPBuffer.Flush(); err != nil {
			errs = append(errs, err)
		}
		if err := zfsrw.zipFP.Close(); err != nil {
			errs = append(errs, err)
		}

	}
	if len(errs) > 0 {
		return errors.WithStack(errors.Combine(errs...))
	}

	return nil
}

func (zfsrw *zipFSRW) Open(name string) (fs.File, error) {
	name = clearPath(name)
	if slices.Contains(zfsrw.newFiles, name) {
		return nil, errors.Wrapf(fs.ErrPermission, "file '%s' is not yet written to disk", name)
	}
	if zfsrw.zfs == nil {
		return nil, errors.WithStack(fs.ErrNotExist)
	}
	fp, err := zfsrw.zfs.Open(name)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot open file '%s'", name)
	}
	return fp, nil
}

func (zfsrw *zipFSRW) Create(path string) (writefs.FileWrite, error) {
	path = clearPath(path)
	fp, err := zfsrw.zipWriter.Create(path)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot create file '%s'", path)
	}
	zfsrw.newFiles = append(zfsrw.newFiles, path)
	return basefs.NewNopWriteCloser(fp), nil
}

var (
	_ readwritefs.ReadWriteFS = &zipFSRW{}
	_ basefs.CloserFS         = &zipFSRW{}
)
