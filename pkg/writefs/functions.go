package writefs

import (
	"emperror.dev/errors"
	"io/fs"
)

var ErrNotImplemented = errors.New("not implemented")

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
	return errors.Wrap(ErrNotImplemented, "Rename")
}

func Create(fsys fs.FS, path string) (FileWrite, error) {
	if _fsys, ok := fsys.(CreateFS); ok {
		return _fsys.Create(path)
	}
	return nil, errors.Wrap(ErrNotImplemented, "Create")
}

func Remove(fsys fs.FS, path string) error {
	if _fsys, ok := fsys.(RemoveFS); ok {
		return _fsys.Remove(path)
	}
	return errors.Wrap(ErrNotImplemented, "Remove")
}

func Close(fsys fs.FS) error {
	if _fsys, ok := fsys.(CloseFS); ok {
		return _fsys.Close()
	}
	return nil
}

func Fullpath(fsys fs.FS, name string) (string, error) {
	if _fsys, ok := fsys.(FullpathFS); ok {
		return _fsys.Fullpath(name)
	}
	return "", errors.Wrap(ErrNotImplemented, "Fullpath")
}

func WriteFile(fsys fs.FS, name string, data []byte) error {
	if _fsys, ok := fsys.(WriteFileFS); ok {
		return _fsys.WriteFile(name, data)
	}
	fp, err := Create(fsys, name)
	if err != nil {
		return errors.Wrapf(err, "cannot create file '%s'", name)
	}
	if _, err := fp.Write(data); err != nil {
		fp.Close()
		return errors.Wrapf(err, "cannot write file '%s'", name)
	}
	if err := fp.Close(); err != nil {
		return errors.Wrapf(err, "cannot close file '%s'", name)
	}
	return nil
}

func HasContent(fsys fs.FS) bool {
	entries, err := fs.ReadDir(fsys, "")
	if err != nil {
		return false
	}
	for _, entry := range entries {
		if entry.Name() != "" {
			return true
		}
	}
	return false
}
