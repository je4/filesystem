package zipfs

import (
	"archive/zip"
	"emperror.dev/errors"
	"fmt"
	"github.com/je4/filesystem/v2/pkg/writefs"
	"golang.org/x/exp/slices"
	"io"
	"io/fs"
	"strings"
)

type OpenRawZipFS interface {
	fs.FS
	OpenRaw(name string) (fs.File, error)
	GetZipReader() *zip.Reader
}

// NewFS creates a new fs.FS from a readerAt and size
// it implements fs.FS, fs.ReadDirFS, fs.ReadFileFS, fs.StatFS, fs.SubFS, basefs.IsLockedFS
func NewFS(r io.ReaderAt, size int64) (fs fs.FS, err error) {
	zipReader, err := zip.NewReader(r, size)
	if err != nil {
		return nil, err
	}
	return &zipFS{
		Reader: zipReader,
		mutex:  writefs.NewMutex(),
	}, nil
}

type zipFS struct {
	*zip.Reader
	mutex *writefs.Mutex
}

func (zfs *zipFS) String() string {
	return fmt.Sprintf("zipFS(%d files)", len(zfs.File))
}

func (zfs *zipFS) GetZipReader() *zip.Reader {
	return zfs.Reader
}

func (zfs *zipFS) Sub(dir string) (fs.FS, error) {
	return fs.Sub(zfs, dir)
}

func (zfs *zipFS) Stat(name string) (fs.FileInfo, error) {
	name = clearPath(name)
	for _, f := range zfs.File {
		if f.Name == name {
			return f.FileInfo(), nil
		}
	}
	return nil, fs.ErrNotExist
}

func (zfs *zipFS) ReadFile(name string) ([]byte, error) {
	name = clearPath(name)
	for _, f := range zfs.File {
		if f.Name == name {
			rc, err := f.Open()
			if err != nil {
				return nil, err
			}
			defer rc.Close()
			return io.ReadAll(rc)
		}
	}
	return nil, fs.ErrNotExist
}

func (zfs *zipFS) ReadDir(name string) ([]fs.DirEntry, error) {
	name = clearPath(name)
	var result []fs.DirEntry
	for _, f := range zfs.File {
		if strings.HasPrefix(f.Name, name) {
			parts := strings.Split(strings.Trim(f.Name[len(name):], "/"), "/")
			if len(parts) == 1 {
				result = append(result, writefs.NewDirEntry(f.FileInfo()))
				continue
			}
			result = append(result, writefs.NewDirEntry(writefs.NewFileInfoDir(parts[0])))
		}
	}
	slices.SortFunc(result, func(i, j fs.DirEntry) bool {
		return i.Name() < j.Name()
	})
	return slices.CompactFunc(result, func(i, j fs.DirEntry) bool {
		return i.Name() == j.Name()
	}), nil

}

func (zfs *zipFS) Open(name string) (fs.File, error) {
	for _, f := range zfs.File {
		if f.Name == name {
			w, err := f.Open()
			if err != nil {
				return nil, errors.Wrapf(err, "failed to open file '%s'", name)
			}
			zfs.mutex.Lock()
			return NewFile(f.FileInfo(), w, zfs.mutex), nil
		}
	}
	return nil, fs.ErrNotExist
}

func (zfs *zipFS) OpenRaw(name string) (fs.File, error) {
	for _, f := range zfs.File {
		if f.Name == name {
			w, err := f.OpenRaw()
			if err != nil {
				return nil, errors.Wrapf(err, "failed to open file '%s'", name)
			}
			zfs.mutex.Lock()
			return NewFile(f.FileInfo(), writefs.NewNopReadCloser(w), zfs.mutex), nil
		}
	}
	return nil, fs.ErrNotExist
}

func (zfs *zipFS) IsLocked() bool {
	return zfs.mutex.IsLocked()
}

var (
	_ fs.FS              = (*zipFS)(nil)
	_ fs.ReadDirFS       = (*zipFS)(nil)
	_ fs.ReadFileFS      = (*zipFS)(nil)
	_ fs.StatFS          = (*zipFS)(nil)
	_ fs.SubFS           = (*zipFS)(nil)
	_ writefs.IsLockedFS = (*zipFS)(nil)
	_ OpenRawZipFS       = (*zipFS)(nil)
	_ fmt.Stringer       = (*zipFS)(nil)
)
