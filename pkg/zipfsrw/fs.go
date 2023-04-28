// Package zipfsrw provides a functionality to create and update content of a zip file
package zipfsrw

import (
	"archive/zip"
	"emperror.dev/errors"
	"fmt"
	"github.com/je4/filesystem/v2/pkg/writefs"
	"github.com/je4/filesystem/v2/pkg/zipfs"
	"golang.org/x/exp/slices"
	"io"
	"io/fs"
)

func NewFS(writer io.Writer, zipFS zipfs.OpenRawZipFS, noCompression bool) (*zipFSRW, error) {
	zipWriter := zip.NewWriter(writer)
	return &zipFSRW{
		zipReader:     zipFS,
		zipWriter:     zipWriter,
		newFiles:      []string{},
		noCompression: noCompression,
	}, nil
}

type zipFSRW struct {
	zipReader     zipfs.OpenRawZipFS
	zipWriter     *zip.Writer
	newFiles      []string
	noCompression bool
}

func (zfsrw *zipFSRW) Stat(name string) (fs.FileInfo, error) {
	if zfsrw.zipReader != nil {
		return fs.Stat(zfsrw.zipReader, name)
	}
	return nil, fmt.Errorf("write only zip file")
}

func (zfsrw *zipFSRW) String() string {
	return "zipFSRW"
}

func (zfsrw *zipFSRW) HasChanged() bool {
	return len(zfsrw.newFiles) > 0
}

func (zfsrw *zipFSRW) Close() error {
	var errs = []error{}

	// copy old compressed files to new zip file
	if zfsrw.zipReader != nil {
		zipReader := zfsrw.zipReader.GetZipReader()
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
	return errors.Combine(errs...)
}

func (zfsrw *zipFSRW) Open(name string) (fs.File, error) {
	name = clearPath(name)
	if slices.Contains(zfsrw.newFiles, name) {
		return nil, errors.Wrapf(fs.ErrPermission, "file '%s' is not yet written to disk", name)
	}
	if zfsrw.zipReader == nil {
		return nil, errors.WithStack(fs.ErrNotExist)
	}
	fp, err := zfsrw.zipReader.Open(name)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot open file '%s'", name)
	}
	return fp, nil
}

func (zfsrw *zipFSRW) Create(path string) (writefs.FileWrite, error) {
	path = clearPath(path)
	header := &zip.FileHeader{
		Name: path,
	}
	if zfsrw.noCompression {
		header.Method = zip.Store
	} else {
		header.Method = zip.Deflate
	}
	fp, err := zfsrw.zipWriter.CreateHeader(header)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot create file '%s'", path)
	}
	zfsrw.newFiles = append(zfsrw.newFiles, path)
	return writefs.NewNopWriteCloser(fp), nil
}

func (zfsrw *zipFSRW) ReadDir(name string) ([]fs.DirEntry, error) {
	if zfsrw.zipReader == nil {
		return []fs.DirEntry{}, nil
	}
	return fs.ReadDir(zfsrw.zipReader, name)
}

func (zfsrw *zipFSRW) Sub(name string) (fs.FS, error) {
	return writefs.NewSubFS(zfsrw, name), nil
}

var (
	_ writefs.ReadWriteFS = &zipFSRW{}
	_ writefs.CloseFS     = &zipFSRW{}
	_ fmt.Stringer        = &zipFSRW{}
	_ fs.ReadDirFS        = &zipFSRW{}
	_ fs.FS               = &zipFSRW{}
	_ fs.SubFS            = &zipFSRW{}
	_ fs.StatFS           = &zipFSRW{}
)
var (
	_ writefs.ReadWriteFS = &fsFile{}
	_ writefs.CloseFS     = &fsFile{}
	_ fmt.Stringer        = &fsFile{}
	_ fs.ReadDirFS        = &fsFile{}
	_ fs.FS               = &fsFile{}
	_ fs.SubFS            = &fsFile{}
	_ fs.StatFS           = &fsFile{}
)
