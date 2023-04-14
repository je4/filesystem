package s3fsrw

import (
	"emperror.dev/errors"
	"github.com/minio/minio-go/v7"
	"io"
)

func NewUploadInfo(ui minio.UploadInfo, err error) uploadInfo {
	return uploadInfo{
		uploadInfo: ui,
		err:        err,
	}
}

type uploadInfo struct {
	uploadInfo minio.UploadInfo
	err        error
}

func NewWriteCloser() *rwCloser {
	pr, pw := io.Pipe()
	return &rwCloser{
		PipeWriter: pw,
		pr:         pr,
		c:          make(chan uploadInfo),
	}
}

type rwCloser struct {
	*io.PipeWriter
	pr *io.PipeReader
	c  chan uploadInfo
}

func (wc *rwCloser) Close() error {
	errs := []error{}
	//errs = append(errs, wc.pr.Close())
	errs = append(errs, wc.PipeWriter.Close())
	uploadInfo := <-wc.c
	errs = append(errs, uploadInfo.err)
	return errors.Combine(errs...)
}

func (wc *rwCloser) GetReader() io.Reader {
	return wc.pr
}
