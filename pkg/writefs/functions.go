package writefs

import (
	"emperror.dev/errors"
	"io"
	"io/fs"
	"os"
	"strings"
)

var ErrNotImplemented = errors.NewPlain("not implemented")

func SubFSCreate(fsys fs.FS, path string) (fs.FS, error) {
	if err := MkDir(fsys, path); err != nil {
		if !errors.Is(err, fs.ErrExist) {
			return nil, errors.Wrapf(err, "cannot create directory '%s'", path)
		}
	}
	return fs.Sub(fsys, path)
}

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

func WriteFile(fsys fs.FS, name string, data []byte) (int, error) {
	if _fsys, ok := fsys.(WriteFileFS); ok {
		return _fsys.WriteFile(name, data)
	}
	fp, err := Create(fsys, name)
	if err != nil {
		return 0, errors.Wrapf(err, "cannot create file '%s'", name)
	}
	count, err := fp.Write(data)
	if err != nil {
		fp.Close()
		return 0, errors.Wrapf(err, "cannot write file '%s'", name)
	}
	if err := fp.Close(); err != nil {
		return 0, errors.Wrapf(err, "cannot close file '%s'", name)
	}
	return count, nil
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

func Copy(fs fs.FS, src, dst string) (int64, error) {
	var srcFP io.ReadCloser
	var err error
	if strings.Contains(src, "://") {
		srcFP, err = fs.Open(src)
		if err != nil {
			return 0, errors.Wrapf(err, "cannot open source '%s'", src)
		}
	} else {
		srcFP, err = os.Open(src)
		if err != nil {
			return 0, errors.Wrapf(err, "cannot open source '%s'", src)
		}
	}
	var dstFP io.WriteCloser
	if strings.Contains(dst, "://") {
		dstFP, err = Create(fs, dst)
		if err != nil {
			srcFP.Close()
			return 0, errors.Wrapf(err, "cannot open destination '%s'", dst)
		}
	} else {
		dstFP, err = os.Create(dst)
		if err != nil {
			srcFP.Close()
			return 0, errors.Wrapf(err, "cannot open destination '%s'", dst)
		}
	}
	var errs []error

	num, err := io.Copy(dstFP, srcFP)
	if err != nil {
		errs = append(errs, errors.Wrap(err, "cannot copy data"))
	}
	if err := dstFP.Close(); err != nil {
		errs = append(errs, errors.Wrap(err, "cannot close destination"))
	}
	if err := srcFP.Close(); err != nil {
		errs = append(errs, errors.Wrap(err, "cannot close source"))
	}
	if len(errs) > 0 {
		return 0, errors.Wrap(errors.Combine(errs...), "cannot copy files")
	}
	return num, nil
}
