package zipasfolder

import (
	"errors"
	"github.com/je4/filesystem/v2/pkg/fsrw"
	"io/fs"
	"path/filepath"
)

type subFS struct {
	fsys fsrw.FSRW
	dir  string
}

func NewSubFS(fsys fsrw.FSRW, dir string) *subFS {
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

func (sfs *subFS) Sub(dir string) (fsrw.FSRW, error) {
	return NewSubFS(sfs.fsys, filepath.ToSlash(filepath.Join(sfs.dir, dir))), nil
}

func (sfs *subFS) Create(path string) (fsrw.FileW, error) {
	return sfs.fsys.Create(filepath.ToSlash(filepath.Join(sfs.dir, path)))
}

func (sfs *subFS) MkDir(path string) error {
	mkdirFS, ok := sfs.fsys.(fsrw.MkDirFSRW)
	if !ok {
		return errors.New("fs does not support MkDir")
	}
	return mkdirFS.MkDir(filepath.ToSlash(filepath.Join(sfs.dir, path)))
}
