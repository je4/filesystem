package s3fsrw

import (
	"emperror.dev/errors"
	"github.com/minio/minio-go/v7"
	"io"
	"sync/atomic"
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
		c:          make(chan uploadInfo, 1),
		isClosed:   atomic.Bool{},
		errs:       []error{},
	}
}

type rwCloser struct {
	*io.PipeWriter
	pr       *io.PipeReader
	c        chan uploadInfo
	isClosed atomic.Bool
	errs     []error
}

func (wc *rwCloser) Write(p []byte) (n int, err error) {
	n, err = wc.PipeWriter.Write(p)
	if err != nil {
		wc.errs = append(wc.errs, err)
	}
	return n, errors.Combine(wc.errs...)
}

func (wc *rwCloser) Close() error {
	if !wc.isClosed.Load() {
		wc.isClosed.Swap(true)
		wc.errs = append(wc.errs, wc.PipeWriter.Close())
		ui := <-wc.c
		wc.errs = append(wc.errs, ui.err)
	}
	return errors.Combine(wc.errs...)
}

func (wc *rwCloser) GetReader() io.Reader {
	return wc.pr
}
