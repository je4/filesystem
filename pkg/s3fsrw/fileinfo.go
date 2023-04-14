package s3fsrw

import (
	"github.com/minio/minio-go/v7"
	"io/fs"
	"path/filepath"
	"time"
)

func NewFileInfo(o *minio.ObjectInfo) fs.FileInfo {
	return &fileInfo{o}
}

type fileInfo struct {
	*minio.ObjectInfo
}

func (s3fi fileInfo) String() string {
	return s3fi.Key
}

func (s3fi fileInfo) Name() string {
	return filepath.Base(s3fi.Key)
}

func (s3fi fileInfo) Size() int64 {
	return s3fi.ObjectInfo.Size
}

func (s3fi fileInfo) Mode() fs.FileMode {
	return 0
}

func (s3fi fileInfo) ModTime() time.Time {
	return s3fi.LastModified
}

func (s3fi fileInfo) IsDir() bool {
	return s3fi.ObjectInfo.Size == 0 &&
		s3fi.ObjectInfo.StorageClass == ""
	//	return false
}

func (s3fi fileInfo) Sys() any {
	return nil
}

var _ fs.FileInfo = &fileInfo{}
