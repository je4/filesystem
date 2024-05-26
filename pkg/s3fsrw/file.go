package s3fsrw

import (
	"emperror.dev/errors"
	"github.com/je4/utils/v2/pkg/zLogger"
	"github.com/minio/minio-go/v7"
	"io/fs"
)

func NewROFile(o *minio.Object, debugInfo string, logger zLogger.ZWrapper) *ROFile {
	return &ROFile{
		Object:    o,
		logger:    logger,
		debugInfo: debugInfo,
	}
}

type ROFile struct {
	*minio.Object
	logger    zLogger.ZWrapper
	debugInfo string
}

func (s3f *ROFile) Close() error {
	s3f.logger.Debugf("closing s3 read-only file: %s", s3f.debugInfo)
	return errors.WithStack(s3f.Object.Close())
}

func (s3f *ROFile) Stat() (fs.FileInfo, error) {
	oInfo, err := s3f.Object.Stat()
	if err != nil {
		return nil, errors.Wrapf(err, "cannot stat '%v'", s3f.Object)
	}
	return NewFileInfo(&oInfo), nil
}

var _ fs.File = &ROFile{}
