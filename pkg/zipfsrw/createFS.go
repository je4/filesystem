package zipfsrw

import (
	"emperror.dev/errors"
	"github.com/je4/filesystem/v2/pkg/writefs"
	"io/fs"
	"strings"
)

func NewCreateFSFunc(noCompression bool) writefs.CreateFSFunc {
	return func(f *writefs.Factory, zipFile string) (fs.FS, error) {
		parts := strings.Split(zipFile, "/")
		if len(parts) < 2 {
			return nil, errors.Errorf("invalid zip path: %s", zipFile)
		}
		baseFS, err := f.Get(strings.Join(parts[:len(parts)-1], "/"))
		if err != nil {
			return nil, errors.Wrapf(err, "cannot get base filesystem for '%s'", zipFile)
		}
		zipFS, err := NewFSFile(baseFS, parts[len(parts)-1], noCompression)
		if err != nil {
			return nil, errors.Wrapf(err, "cannot create zip filesystem for '%s'", zipFile)
		}
		return zipFS, nil
	}
}
func createFS(f *writefs.Factory, zipFile string) (fs.FS, error) {
	parts := strings.Split(zipFile, "/")
	if len(parts) < 2 {
		return nil, errors.Errorf("invalid zip path: %s", zipFile)
	}
	baseFS, err := f.Get(strings.Join(parts[:len(parts)-1], "/"))
	if err != nil {
		return nil, errors.Wrapf(err, "cannot get base filesystem for '%s'", zipFile)
	}
	zipFS, err := NewFSFile(baseFS, parts[len(parts)-1], false)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot create zip filesystem for '%s'", zipFile)
	}
	return zipFS, nil
}

var (
	_ writefs.CreateFSFunc = createFS
)
