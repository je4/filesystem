package writefs

import (
	"errors"
	"io/fs"
	"path/filepath"
)

type subFS struct {
	fsys ReadWriteFS
	dir  string
}

func (sfs *subFS) Rename(oldPath, newPath string) error {
	return Rename(
		sfs.fsys,
		filepath.ToSlash(filepath.Join(sfs.dir, oldPath)),
		filepath.ToSlash(filepath.Join(sfs.dir, newPath)),
	)
}

func (sfs *subFS) Remove(path string) error {
	return Remove(sfs.fsys, filepath.ToSlash(filepath.Join(sfs.dir, path)))
}

func NewSubFS(fsys ReadWriteFS, dir string) *subFS {
	return &subFS{
		fsys: fsys,
		dir:  dir,
	}
}

func (sfs *subFS) Open(name string) (fs.File, error) {
	return sfs.fsys.Open(filepath.ToSlash(filepath.Join(sfs.dir, name)))
}

func (sfs *subFS) ReadDir(name string) ([]fs.DirEntry, error) {
	return fs.ReadDir(sfs.fsys, filepath.ToSlash(filepath.Join(sfs.dir, name)))
}

func (sfs *subFS) ReadFile(name string) ([]byte, error) {
	return fs.ReadFile(sfs.fsys, filepath.ToSlash(filepath.Join(sfs.dir, name)))
}

func (sfs *subFS) Stat(name string) (fs.FileInfo, error) {
	return fs.Stat(sfs.fsys, filepath.ToSlash(filepath.Join(sfs.dir, name)))
}

func (sfs *subFS) Sub(dir string) (fs.FS, error) {
	return NewSubFS(sfs.fsys, filepath.ToSlash(filepath.Join(sfs.dir, dir))), nil
}

func (sfs *subFS) Create(path string) (FileWrite, error) {
	return sfs.fsys.Create(filepath.ToSlash(filepath.Join(sfs.dir, path)))
}

func (sfs *subFS) MkDir(path string) error {
	mkdirFS, ok := sfs.fsys.(MkDirFS)
	if !ok {
		return errors.New("fs does not support MkDir")
	}
	return mkdirFS.MkDir(filepath.ToSlash(filepath.Join(sfs.dir, path)))
}

var (
	_ fs.FS         = &subFS{}
	_ CreateFS      = &subFS{}
	_ MkDirFS       = &subFS{}
	_ RenameFS      = &subFS{}
	_ RemoveFS      = &subFS{}
	_ fs.ReadDirFS  = &subFS{}
	_ fs.ReadFileFS = &subFS{}
	_ fs.StatFS     = &subFS{}
	_ fs.SubFS      = &subFS{}
)
