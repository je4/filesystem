package osfsrw

import (
	"emperror.dev/errors"
	"github.com/je4/filesystem/v2/pkg/writefs"
	"io/fs"
	"os"
	"path/filepath"
)

func NewFS(dir string) (*osFSRW, error) {
	stat, err := os.Stat(dir)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if !stat.IsDir() {
		return nil, errors.Errorf("not a directory: %s", dir)
	}
	return &osFSRW{
		dir: dir,
	}, nil
}

type osFSRW struct {
	dir string
}

func (d *osFSRW) Sub(dir string) (fs.FS, error) {
	return NewFS(filepath.Join(d.dir, dir))
}

func (d *osFSRW) Remove(path string) error {
	return errors.WithStack(os.Remove(filepath.Join(d.dir, path)))
}

func (d *osFSRW) Rename(oldPath, newPath string) error {
	return errors.WithStack(os.Rename(filepath.Join(d.dir, oldPath), filepath.Join(d.dir, newPath)))
}

func (d *osFSRW) Open(name string) (fs.File, error) {
	fp, err := os.Open(filepath.Join(d.dir, name))
	return fp, errors.WithStack(err)
}

func (d *osFSRW) Stat(name string) (fs.FileInfo, error) {
	fi, err := os.Stat(filepath.Join(d.dir, name))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, errors.WithStack(fs.ErrNotExist)
		}
		return nil, errors.WithStack(err)
	}
	return fi, nil
}

func (d *osFSRW) Create(path string) (writefs.FileWrite, error) {
	w, err := os.Create(filepath.Join(d.dir, path))
	return w, errors.WithStack(err)
}

func (d *osFSRW) MkDir(path string) error {
	return errors.WithStack(os.Mkdir(filepath.Join(d.dir, path), 0777))
}

func (d *osFSRW) ReadDir(name string) ([]fs.DirEntry, error) {
	de, err := os.ReadDir(filepath.Join(d.dir, name))
	return de, errors.WithStack(err)
}

func (d *osFSRW) ReadFile(name string) ([]byte, error) {
	data, err := os.ReadFile(filepath.Join(d.dir, name))
	return data, errors.WithStack(err)
}

var (
	_ writefs.CreateFS    = &osFSRW{}
	_ writefs.ReadWriteFS = &osFSRW{}
	_ writefs.MkDirFS     = &osFSRW{}
	_ writefs.RenameFS    = &osFSRW{}
	_ writefs.RemoveFS    = &osFSRW{}
	_ fs.ReadDirFS        = &osFSRW{}
	_ fs.ReadFileFS       = &osFSRW{}
	_ fs.StatFS           = &osFSRW{}
	_ fs.SubFS            = &osFSRW{}
)
