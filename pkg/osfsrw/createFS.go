package osfsrw

import (
	"emperror.dev/errors"
	"github.com/je4/filesystem/v3/pkg/writefs"
	"io/fs"
	"strings"
)

func NewCreateFSFunc() writefs.CreateFSFunc {
	return func(f *writefs.Factory, baseFolder string) (fs.FS, error) {
		folder := strings.TrimPrefix(baseFolder, "file://")
		osFS, err := NewFS(folder)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return osFS, nil
	}
}
