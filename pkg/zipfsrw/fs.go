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

func NewFS(writer io.Writer, orzfs zipfs.OpenRawZipFS, noCompression bool) (*zipFSRW, error) {
	zipWriter := zip.NewWriter(writer)
	return &zipFSRW{
		zfs:           orzfs,
		zipWriter:     zipWriter,
		newFiles:      []string{},
		noCompression: noCompression,
	}, nil
}

type zipFSRW struct {
	zfs           zipfs.OpenRawZipFS
	zipWriter     *zip.Writer
	newFiles      []string
	noCompression bool
}

func (zfsrw *zipFSRW) HasChanged() bool {
	return len(zfsrw.newFiles) > 0
}

func (zfsrw *zipFSRW) Close() error {
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

		if err := writefs.Close(zfsrw.zfs); err != nil {
			errs = append(errs, err)
		}
	}

	if err := zfsrw.zipWriter.Close(); err != nil {
		errs = append(errs, err)
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

var (
	_ writefs.ReadWriteFS = &fsFile{}
	_ writefs.CloseFS     = &fsFile{}
	_ fmt.Stringer        = &fsFile{}
)
