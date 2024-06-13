package vfsrw

import (
	"emperror.dev/errors"
	"fmt"
	"github.com/je4/filesystem/v3/pkg/writefs"
	"github.com/je4/utils/v2/pkg/zLogger"
	"io"
	"io/fs"
	"strings"
)

func NewFS(config Config, logger zLogger.ZLogger) (*vFSRW, error) {
	var toClose = []io.Closer{}
	var closeAll = func() {
		// iterate in reverse order
		last := len(toClose) - 1
		for i := range toClose {
			c := toClose[last-i]
			c.Close()
		}
	}

	vfs := &vFSRW{fss: map[string]fs.FS{}}

	_logger := logger.With().Str("module", "vfsrw").Logger()
	logger = &_logger

	for _, cfg := range config {
		switch strings.ToLower(cfg.Type) {
		case "os":
			if cfg.OS == nil {
				closeAll()
				return nil, errors.Errorf("no os section for filesystem '%s'", cfg.Name)
			}
			xFS, err := newOS(cfg.OS, logger)
			if err != nil {
				closeAll()
				return nil, errors.Wrapf(err, "cannot create osfs in '%s'", cfg.Name)
			}
			if closer, ok := xFS.(io.Closer); ok {
				toClose = append(toClose, closer)
			}
			vfs.fss[cfg.Name] = xFS
		case "sftp":
			if cfg.SFTP == nil {
				closeAll()
				return nil, errors.Errorf("no sftp section for filesystem '%s'", cfg.Name)
			}
			xFS, err := newSFTP(cfg.SFTP, logger)
			if err != nil {
				closeAll()
				return nil, errors.Wrapf(err, "cannot create sftpfsrw in '%s'", cfg.Name)
			}
			if closer, ok := xFS.(io.Closer); ok {
				toClose = append(toClose, closer)
			}
			vfs.fss[cfg.Name] = xFS
		case "s3":
			if cfg.S3 == nil {
				closeAll()
				return nil, errors.Errorf("no s3 section for filesystem '%s'", cfg.Name)
			}
			xFS, err := newS3(cfg.S3, logger)
			if err != nil {
				closeAll()
				return nil, errors.Wrapf(err, "cannot create s3fsrw in '%s'", cfg.Name)
			}
			if closer, ok := xFS.(io.Closer); ok {
				toClose = append(toClose, closer)
			}
			vfs.fss[cfg.Name] = xFS
		case "remote":
			if cfg.Remote == nil {
				closeAll()
				return nil, errors.Errorf("no Remote section for filesystem '%s'", cfg.Name)
			}
			xFS, err := newS3(cfg.S3, logger)
			if err != nil {
				closeAll()
				return nil, errors.Wrapf(err, "cannot create s3fsrw in '%s'", cfg.Name)
			}
			if closer, ok := xFS.(io.Closer); ok {
				toClose = append(toClose, closer)
			}
			vfs.fss[cfg.Name] = xFS
		}
	}
	return vfs, nil
}

type vFSRW struct {
	fss map[string]fs.FS
}

func (vfs *vFSRW) Close() error {
	var errs = []error{}
	for _, fs := range vfs.fss {
		if closer, ok := fs.(io.Closer); ok {
			err := closer.Close()
			if err != nil {
				errs = append(errs, errors.WithStack(err))
			}
		}
	}
	return errors.Combine(errs...)
}

func (vfs *vFSRW) Remove(name string) error {
	vFS, path, err := vfs.getFS(name)
	if err != nil {
		return errors.WithStack(err)
	}
	err = writefs.Remove(vFS, path)
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (vfs *vFSRW) Rename(oldPath, newPath string) error {
	name1, _, _ := matchPath(oldPath)
	name2, _, _ := matchPath(newPath)
	if name2 != name1 {
		return errors.Errorf("cannot rename over multiple filesystems %s -> %s", name1, name2)
	}

	vFS, op, err := vfs.getFS(oldPath)
	if err != nil {
		return errors.WithStack(err)
	}
	_, np, err := vfs.getFS(newPath)
	if err != nil {
		return errors.WithStack(err)
	}
	err = writefs.Rename(vFS, op, np)
	if err != nil {
		return errors.WithStack(err)
	}
	return nil

}

func (vfs *vFSRW) MkDir(name string) error {
	vFS, path, err := vfs.getFS(name)
	if err != nil {
		return errors.WithStack(err)
	}
	err = writefs.MkDir(vFS, path)
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (vfs *vFSRW) Create(name string) (writefs.FileWrite, error) {
	vFS, path, err := vfs.getFS(name)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	data, err := writefs.Create(vFS, path)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return data, nil
}

func (vfs *vFSRW) String() string {
	names := []string{}
	for name, _ := range vfs.fss {
		names = append(names, name)
	}
	return fmt.Sprintf("vFSRW(%v)", names)
}

func (vfs *vFSRW) Sub(dir string) (fs.FS, error) {
	return writefs.NewSubFS(vfs, dir), nil
}

func (vfs *vFSRW) Stat(name string) (fs.FileInfo, error) {
	vFS, path, err := vfs.getFS(name)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	data, err := fs.Stat(vFS, path)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return data, nil
}

func (vfs *vFSRW) ReadFile(name string) ([]byte, error) {
	vFS, path, err := vfs.getFS(name)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	data, err := fs.ReadFile(vFS, path)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return data, nil
}

func (vfs *vFSRW) ReadDir(name string) ([]fs.DirEntry, error) {
	vFS, path, err := vfs.getFS(name)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	de, err := fs.ReadDir(vFS, path)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return de, nil
}

func (vfs *vFSRW) Open(vfsPath string) (fs.File, error) {
	vFS, path, err := vfs.getFS(vfsPath)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	fp, err := vFS.Open(path)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return fp, nil
}

func (vfs *vFSRW) getFS(vfsPath string) (fs.FS, string, error) {
	name, path, err := matchPath(vfsPath)
	if err != nil {
		return nil, "", errors.WithStack(err)
	}
	vFS, ok := vfs.fss[name]
	if !ok {
		return nil, "", errors.Errorf("vfs '%s' not configured for path '%s'", name, vfsPath)
	}
	return vFS, path, nil
}

var (
	_ fs.FS         = (*vFSRW)(nil)
	_ fs.ReadDirFS  = (*vFSRW)(nil)
	_ fs.ReadFileFS = (*vFSRW)(nil)
	_ fs.StatFS     = (*vFSRW)(nil)
	_ fs.SubFS      = (*vFSRW)(nil)
	//	_ writefs.IsLockedFS = (*vFSRW)(nil)
	_ fmt.Stringer        = (*vFSRW)(nil)
	_ writefs.ReadWriteFS = (*vFSRW)(nil)
	_ writefs.MkDirFS     = (*vFSRW)(nil)
	_ writefs.RenameFS    = (*vFSRW)(nil)
	_ writefs.RemoveFS    = (*vFSRW)(nil)
	_ writefs.CreateFS    = (*vFSRW)(nil)
)
