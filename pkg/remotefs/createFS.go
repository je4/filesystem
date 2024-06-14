package remotefs

import (
	"crypto/tls"
	"github.com/je4/filesystem/v3/pkg/writefs"
	"github.com/je4/utils/v2/pkg/zLogger"
	"io"
	"io/fs"
)

func NewCreateFSFunc(tlsConfig *tls.Config, addr string, vfs string, closer []io.Closer, logger zLogger.ZLogger) writefs.CreateFSFunc {
	return func(f *writefs.Factory, baseFolder string) (fs.FS, error) {
		return NewFS(tlsConfig, addr, baseFolder, vfs, closer, logger)
	}
}
