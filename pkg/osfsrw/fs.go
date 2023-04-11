package osfsrw

import (
	"github.com/je4/filesystem/v2/pkg/fsrw"
	"io/fs"
	"os"
	"path/filepath"
)

func NewOSFSRW(dir string) fsrw.FSRW {
	return &osFSRW{
		dir: dir,
	}
}

type osFSRW struct {
	dir string
}

func (d *osFSRW) Open(name string) (fs.File, error) {
	return os.Open(filepath.Join(d.dir, name))
}

func (d *osFSRW) Stat(name string) (fs.FileInfo, error) {
	return os.Stat(filepath.Join(d.dir, name))
}

func (d *osFSRW) Create(path string) (fsrw.FileW, error) {
	return os.Create(filepath.Join(d.dir, path))
}

func (d *osFSRW) MkDir(path string) error {
	return os.Mkdir(filepath.Join(d.dir, path), 0777)
}

func (d *osFSRW) ReadDir(name string) ([]fs.DirEntry, error) {
	return os.ReadDir(filepath.Join(d.dir, name))
}

func (d *osFSRW) ReadFile(name string) ([]byte, error) {
	return os.ReadFile(filepath.Join(d.dir, name))
}

var (
	_ fsrw.FSRW     = &osFSRW{}
	_ fs.ReadDirFS  = &osFSRW{}
	_ fs.ReadFileFS = &osFSRW{}
	_ fs.StatFS     = &osFSRW{}
)
