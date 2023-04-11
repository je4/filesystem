package zipasfolder

import (
	"archive/zip"
	"emperror.dev/errors"
	"golang.org/x/exp/slices"
	"io/fs"
	"strings"
)

func NewZIPFS(zipReader *zip.Reader, zipFile fs.File) *ZIPFS {
	return &ZIPFS{
		zipReader: zipReader,
		zipFile:   zipFile,
		lock:      NewMutex(),
	}
}

type ZIPFS struct {
	zipReader *zip.Reader
	zipFile   fs.File
	lock      *mutex
}

func (zipFS *ZIPFS) Stat(name string) (fs.FileInfo, error) {
	zipFS.lock.Lock()
	defer zipFS.lock.Unlock()
	for _, f := range zipFS.zipReader.File {
		if f.Name == name {
			_, err := f.Open()
			if err != nil {

				return nil, errors.WithStack(err)
			}
			return f.FileInfo(), nil
		}
	}
	return nil, fs.ErrNotExist
}

func (zipFS *ZIPFS) ReadDir(name string) ([]fs.DirEntry, error) {
	zipFS.lock.Lock()
	defer zipFS.lock.Unlock()
	if name == "." {
		name = ""
	}
	name = strings.Trim(name, "/")
	var result []fs.DirEntry
	for _, f := range zipFS.zipReader.File {
		if strings.HasPrefix(f.Name, name) {
			parts := strings.Split(strings.Trim(f.Name[len(name):], "/"), "/")
			if len(parts) == 1 {
				result = append(result, NewZIPFSDirEntry(f.FileInfo()))
				continue
			}
			result = append(result, NewZIPFSDirEntry(NewZIPFSFileInfoDir(parts[0])))
		}
	}
	slices.SortFunc(result, func(i, j fs.DirEntry) bool {
		return i.Name() < j.Name()
	})
	return slices.CompactFunc(result, func(i, j fs.DirEntry) bool {
		return i.Name() == j.Name()
	}), nil
}

func (zipFS *ZIPFS) IsLocked() bool {
	return zipFS.lock.IsLocked()
}

func (zipFS *ZIPFS) Close() error {
	return errors.WithStack(zipFS.zipFile.Close())
}

func (zipFS *ZIPFS) Open(name string) (fs.File, error) {
	zipFS.lock.Lock()
	for _, f := range zipFS.zipReader.File {
		if f.Name == name {
			rc, err := f.Open()
			if err != nil {
				zipFS.lock.Unlock()
				return nil, errors.WithStack(err)
			}
			return NewFile(f.FileInfo(), rc, zipFS.lock), nil
		}
	}
	return nil, fs.ErrNotExist
}

var (
	_ fs.FS        = &ZIPFS{}
	_ fs.ReadDirFS = &ZIPFS{}
	_ fs.StatFS    = &ZIPFS{}
)
