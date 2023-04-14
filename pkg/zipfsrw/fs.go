// Package zipfsrw provides a functionality to create and update content of a zip file
package zipfsrw

import (
	"archive/zip"
	"bufio"
	"emperror.dev/errors"
	"github.com/je4/filesystem/v2/pkg/basefs"
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
func NewZipFSRW(baseFS fs.FS, path string) (writefs.ReadWriteFS, error) {
	var fpat io.ReaderAt
	var size int64
	var fp fs.File
	var ok bool
	newpath := path
	// if target file exists, open it and create a zipfs
	stat, err := fs.Stat(baseFS, path)
	if err == nil {
		size = stat.Size()
		fp, err = baseFS.Open(path)
		if err != nil {
			return nil, errors.Wrapf(err, "cannot open zip file '%s'", path)
		}
		fpat, ok = fp.(io.ReaderAt)
		if !ok {
			return nil, errors.Errorf("cannot cast file '%s' to io.WriterAt", path)
		}
		newpath = newpath + ".tmp"
	}
	// create new file
	newfp, err := writefs.Create(baseFS, newpath)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot create zip file '%s'", newpath)
	}
	// add a buffer to the file
	newFPBuffer := bufio.NewWriterSize(newfp, 1024*1024)

	zipFSRWBase, err := NewZipFSRWBase(newFPBuffer, fpat, size)
	if err != nil {
		return nil, errors.Wrap(err, "cannot create zipFSRWBase")
	}

	return &zipFSRW{
		zipFSRWBase: zipFSRWBase,
		name:        path,
		tmpName:     newpath,
		baseFS:      baseFS,
		fp:          fp,
		zipFP:       newfp,
		zipFPBuffer: newFPBuffer,
	}, nil
}

type zipFSRW struct {
	*zipFSRWBase
	baseFS      fs.FS
	fp          fs.File
	zipFP       writefs.FileWrite
	zipFPBuffer *bufio.Writer
	name        string
	tmpName     string
}

func (zfsrw *zipFSRW) Close() error {
	var errs = []error{}

	if err := zfsrw.zipFSRWBase.Close(); err != nil {
		errs = append(errs, err)
	}
	if err := zfsrw.zipFPBuffer.Flush(); err != nil {
		errs = append(errs, err)
	}
	if err := zfsrw.zipFP.Close(); err != nil {
		errs = append(errs, err)
	}
	if zfsrw.fp != nil {
		if err := zfsrw.fp.Close(); err != nil {
			errs = append(errs, err)
		}
		if zfsrw.HasChanged() {
			if err := writefs.Remove(zfsrw.baseFS, zfsrw.name); err != nil {
				errs = append(errs, err)
			}
			if err := writefs.Rename(zfsrw.baseFS, zfsrw.tmpName, zfsrw.name); err != nil {
				errs = append(errs, err)
			}
		} else {
			if err := writefs.Remove(zfsrw.baseFS, zfsrw.tmpName); err != nil {
				errs = append(errs, err)
			}
		}

	}
	if len(errs) > 0 {
		return errors.WithStack(errors.Combine(errs...))
	}

	return nil
}

func NewZipFSRWBase(writer io.Writer, reader io.ReaderAt, size int64) (*zipFSRWBase, error) {
	zipWriter := zip.NewWriter(writer)
	var orzfs zipfs.OpenRawZipFS
	if reader != nil {
		zfs, err := zipfs.NewFS(reader, size)
		if err != nil {
			return nil, errors.Wrapf(err, "cannot open zip file")
		}
		orzfs = zfs.(zipfs.OpenRawZipFS)
	}
	return &zipFSRWBase{
		zfs:       orzfs,
		zipWriter: zipWriter,
		newFiles:  []string{},
	}, nil
}

type zipFSRWBase struct {
	zfs       zipfs.OpenRawZipFS
	zipWriter *zip.Writer
	newFiles  []string
}

func (zfsrw *zipFSRWBase) HasChanged() bool {
	return len(zfsrw.newFiles) > 0
}

func (zfsrw *zipFSRWBase) Close() error {
	var errs = []error{}

	// copy old compressed files to new zip file
	if zfsrw.zfs != nil {
		zipReader := zfsrw.zfs.GetZipReader()
		for _, f := range zipReader.File {
			if !slices.Contains(zfsrw.newFiles, f.Name) {
				rc, err := f.OpenRaw()
				if err != nil {
					errs = append(errs, err)
					break
				}
				w, err := zfsrw.zipWriter.CreateRaw(&f.FileHeader)
				if err != nil {
					errs = append(errs, err)
					break
				}
				if _, err := io.Copy(w, rc); err != nil {
					errs = append(errs, err)
					break
				}
			}
		}
	}

	if err := zfsrw.zipWriter.Close(); err != nil {
		errs = append(errs, err)
	}
	return nil
}

func (zfsrw *zipFSRWBase) Open(name string) (fs.File, error) {
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

func (zfsrw *zipFSRWBase) Create(path string) (writefs.FileWrite, error) {
	path = clearPath(path)
	fp, err := zfsrw.zipWriter.Create(path)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot create file '%s'", path)
	}
	zfsrw.newFiles = append(zfsrw.newFiles, path)
	return basefs.NewNopWriteCloser(fp), nil
}

var (
	_ writefs.ReadWriteFS = &zipFSRW{}
	_ writefs.CloseFS     = &zipFSRW{}
)
