package s3fsrw

import (
	"fmt"
	"github.com/je4/filesystem/v2/pkg/writefs"
	"io"
	"io/fs"
	"path/filepath"
	"strings"
)

type SubFS struct {
	*s3FSRW
	pathPrefix string
}

func NewSubFS(fs *s3FSRW, pathPrefix string) (*SubFS, error) {
	sfs := &SubFS{
		s3FSRW:     fs,
		pathPrefix: strings.TrimRight(filepath.ToSlash(filepath.Clean(pathPrefix)), "/") + "/",
	}
	return sfs, nil
}

func (s3SubFS *SubFS) String() string {
	return fmt.Sprintf("%s/%s", s3SubFS.s3FSRW.String(), s3SubFS.pathPrefix)
}

func (s3SubFS *SubFS) Open(name string) (fs.File, error) {
	name = filepath.ToSlash(filepath.Join(s3SubFS.pathPrefix, filepath.Clean(name)))
	return s3SubFS.s3FSRW.Open(name)
}

func (s3SubFS *SubFS) Create(name string) (io.WriteCloser, error) {
	name = filepath.ToSlash(filepath.Join(s3SubFS.pathPrefix, filepath.Clean(name)))
	return s3SubFS.s3FSRW.Create(name)
}

func (s3SubFS *SubFS) Remove(name string) error {
	name = filepath.ToSlash(filepath.Join(s3SubFS.pathPrefix, filepath.Clean(name)))
	return s3SubFS.s3FSRW.Remove(name)
}

func (s3SubFS *SubFS) ReadDir(path string) ([]fs.DirEntry, error) {
	path = filepath.ToSlash(filepath.Join(s3SubFS.pathPrefix, filepath.Clean(path)))
	return s3SubFS.s3FSRW.ReadDir(path)
}

func (s3SubFS *SubFS) ReadFile(name string) ([]byte, error) {
	name = filepath.ToSlash(filepath.Join(s3SubFS.pathPrefix, filepath.Clean(name)))
	return s3SubFS.s3FSRW.ReadFile(name)
}

func (s3SubFS *SubFS) WalkDir(path string, fn fs.WalkDirFunc) error {
	path = filepath.ToSlash(filepath.Join(s3SubFS.pathPrefix, filepath.Clean(path)))
	prefix := strings.TrimRight(s3SubFS.pathPrefix, "/") + "/"
	return s3SubFS.s3FSRW.WalkDir(path, func(path string, d fs.DirEntry, err error) error {
		return fn(strings.TrimPrefix(path, prefix), d, err)
	})
}

func (s3SubFS *SubFS) Stat(path string) (fs.FileInfo, error) {
	path = filepath.ToSlash(filepath.Join(s3SubFS.pathPrefix, filepath.Clean(path)))
	return s3SubFS.s3FSRW.Stat(path)
}

func (s3SubFS *SubFS) SubFSRW(path string) (fs.FS, error) {
	name := filepath.ToSlash(filepath.Join(s3SubFS.pathPrefix, filepath.Clean(path)))
	if name == "." {
		name = ""
	}
	if name == "" {
		return s3SubFS, nil
	}
	return s3SubFS.s3FSRW.Sub(name)
}

func (s3SubFS *SubFS) SubFS(path string) (fs.FS, error) {
	name := filepath.ToSlash(filepath.Join(s3SubFS.pathPrefix, filepath.Clean(path)))
	if name == "." {
		name = ""
	}
	if name == "" {
		return s3SubFS, nil
	}
	return s3SubFS.s3FSRW.Sub(name)
}
func (s3SubFS *SubFS) HasContent() bool {
	return s3SubFS.s3FSRW.hasContent(s3SubFS.pathPrefix)
}

// check interface satisfaction
var (
	_ writefs.ReadWriteFS = &s3FSRW{}
	_ writefs.MkDirFS     = &s3FSRW{}
	_ writefs.RenameFS    = &s3FSRW{}
	_ writefs.RemoveFS    = &s3FSRW{}
	_ fs.ReadDirFS        = &s3FSRW{}
	_ fs.ReadFileFS       = &s3FSRW{}
	_ fs.StatFS           = &s3FSRW{}
	_ fs.SubFS            = &s3FSRW{}
)
