package osfsrw

import (
	"emperror.dev/errors"
	"github.com/je4/filesystem/v3/pkg/writefs"
	"github.com/je4/utils/v2/pkg/zLogger"
	"io/fs"
	"strings"
)

func NewCreateFSFunc(logger zLogger.ZLogger) writefs.CreateFSFunc {
	return func(f *writefs.Factory, baseFolder string) (fs.FS, error) {
		folder := strings.TrimPrefix(baseFolder, "file://")
		osFS, err := NewFS(folder, logger)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return osFS, nil
	}
}
