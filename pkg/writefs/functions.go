package writefs

import (
	"emperror.dev/errors"
	"io/fs"
)

func MkDir(fsys fs.FS, path string) error {
	if _fsys, ok := fsys.(MkDirFS); ok {
		return _fsys.MkDir(path)
	}
	return errors.Wrapf(fs.ErrInvalid, "fs does not support MkDir")
}

func Rename(fsys fs.FS, oldPath, newPath string) error {
	if _fsys, ok := fsys.(RenameFS); ok {
		return _fsys.Rename(oldPath, newPath)
	}
	return errors.Wrapf(fs.ErrInvalid, "fs does not support Rename")
}

func Create(fsys fs.FS, path string) (FileWrite, error) {
	if _fsys, ok := fsys.(WriteFS); ok {
		return _fsys.Create(path)
	}
	return nil, errors.Wrapf(fs.ErrInvalid, "fs does not support Create")
}

func Delete(fsys fs.FS, path string) error {
	if _fsys, ok := fsys.(DeleteFS); ok {
		return _fsys.Delete(path)
	}
	return errors.Wrapf(fs.ErrInvalid, "fs does not support Delete")
}
