package s3fsrw

import (
	"fmt"
	"github.com/je4/filesystem/v2/pkg/writefs"
	"io/fs"
	"path/filepath"
	"strings"
)

type subFS struct {
	*s3FSRW
	pathPrefix string
}

func NewSubFS(fs *s3FSRW, pathPrefix string) (*subFS, error) {
	sfs := &subFS{
		s3FSRW:     fs,
		pathPrefix: strings.TrimRight(filepath.ToSlash(filepath.Clean(pathPrefix)), "/") + "/",
	}
	return sfs, nil
}

func (s3SubFS *subFS) String() string {
	return fmt.Sprintf("%s/%s", s3SubFS.s3FSRW.String(), s3SubFS.pathPrefix)
}

func (s3SubFS *subFS) Open(name string) (fs.File, error) {
	name = filepath.ToSlash(filepath.Join(s3SubFS.pathPrefix, filepath.Clean(name)))
	return s3SubFS.s3FSRW.Open(name)
}

func (s3SubFS *subFS) Create(name string) (writefs.FileWrite, error) {
	name = filepath.ToSlash(filepath.Join(s3SubFS.pathPrefix, filepath.Clean(name)))
	return s3SubFS.s3FSRW.Create(name)
}

func (s3SubFS *subFS) Remove(name string) error {
	name = filepath.ToSlash(filepath.Join(s3SubFS.pathPrefix, filepath.Clean(name)))
	return s3SubFS.s3FSRW.Remove(name)
}

func (s3SubFS *subFS) ReadDir(path string) ([]fs.DirEntry, error) {
	path = filepath.ToSlash(filepath.Join(s3SubFS.pathPrefix, filepath.Clean(path)))
	return s3SubFS.s3FSRW.ReadDir(path)
}

func (s3SubFS *subFS) ReadFile(name string) ([]byte, error) {
	name = filepath.ToSlash(filepath.Join(s3SubFS.pathPrefix, filepath.Clean(name)))
	return s3SubFS.s3FSRW.ReadFile(name)
}

func (s3SubFS *subFS) WalkDir(path string, fn fs.WalkDirFunc) error {
	path = filepath.ToSlash(filepath.Join(s3SubFS.pathPrefix, filepath.Clean(path)))
	prefix := strings.TrimRight(s3SubFS.pathPrefix, "/") + "/"
	return s3SubFS.s3FSRW.WalkDir(path, func(path string, d fs.DirEntry, err error) error {
		return fn(strings.TrimPrefix(path, prefix), d, err)
	})
}

func (s3SubFS *subFS) Stat(path string) (fs.FileInfo, error) {
	path = filepath.ToSlash(filepath.Join(s3SubFS.pathPrefix, filepath.Clean(path)))
	return s3SubFS.s3FSRW.Stat(path)
}

func (s3SubFS *subFS) SubFSRW(path string) (fs.FS, error) {
	name := filepath.ToSlash(filepath.Join(s3SubFS.pathPrefix, filepath.Clean(path)))
	if name == "." {
		name = ""
	}
	if name == "" {
		return s3SubFS, nil
	}
	return s3SubFS.s3FSRW.Sub(name)
}

func (s3SubFS *subFS) SubFS(path string) (fs.FS, error) {
	name := filepath.ToSlash(filepath.Join(s3SubFS.pathPrefix, filepath.Clean(path)))
	if name == "." {
		name = ""
	}
	if name == "" {
		return s3SubFS, nil
	}
	return s3SubFS.s3FSRW.Sub(name)
}
func (s3SubFS *subFS) HasContent() bool {
	return s3SubFS.s3FSRW.hasContent(s3SubFS.pathPrefix)
}

// check interface satisfaction
var (
	_ fs.FS            = &subFS{}
	_ writefs.CreateFS = &subFS{}
	_ writefs.MkDirFS  = &subFS{}
	_ writefs.RenameFS = &subFS{}
	_ writefs.RemoveFS = &subFS{}
	_ fs.ReadDirFS     = &subFS{}
	_ fs.ReadFileFS    = &subFS{}
	_ fs.StatFS        = &subFS{}
	_ fs.SubFS         = &subFS{}
)
