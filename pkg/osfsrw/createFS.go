package osfsrw

import (
	"emperror.dev/errors"
	"github.com/je4/filesystem/v2/pkg/writefs"
	"io/fs"
	"strings"
)

func CreateFS(f *writefs.Factory, baseFolder string) (fs.FS, error) {
	folder := strings.TrimPrefix(baseFolder, "file://")
	osFS, err := NewFS(folder)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return osFS, nil
}

var (
	_ writefs.CreateFSFunc = CreateFS
)
