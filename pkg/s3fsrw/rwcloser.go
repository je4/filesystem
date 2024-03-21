package s3fsrw

import (
	"emperror.dev/errors"
	"github.com/je4/utils/v2/pkg/zLogger"
	"github.com/minio/minio-go/v7"
	"io"
	"sync/atomic"
)

func NewUploadInfo(ui *minio.UploadInfo, err error) *uploadInfo {
	return &uploadInfo{
		uploadInfo: ui,
		err:        err,
	}
}

type uploadInfo struct {
	uploadInfo *minio.UploadInfo
	err        error
}

func NewWriteCloser(debugInfo string, logger zLogger.ZWrapper) *rwCloser {
	pr, pw := io.Pipe()
	return &rwCloser{
		PipeWriter: pw,
		pr:         pr,
		c:          make(chan *uploadInfo, 1),
		isClosed:   atomic.Bool{},
		errs:       []error{},
		logger:     logger,
		debugInfo:  debugInfo,
	}
}

type rwCloser struct {
	*io.PipeWriter
	pr         *io.PipeReader
	c          chan *uploadInfo
	isClosed   atomic.Bool
	errs       []error
	uploadInfo *uploadInfo
	logger     zLogger.ZWrapper
	debugInfo  string
}

func (wc *rwCloser) Write(p []byte) (n int, err error) {
	if wc.uploadInfo == nil {
		select {
		case wc.uploadInfo = <-wc.c:
			return 0, errors.Wrapf(errors.Combine(append(wc.errs, wc.uploadInfo.err)...), "cannot write")
		default:
		}
	}
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
		if wc.uploadInfo == nil {
			wc.uploadInfo = <-wc.c
		}
		wc.errs = append(wc.errs, wc.uploadInfo.err)
	}
	wc.logger.Debugf("close s3 pipe: %s", wc.debugInfo)
	return errors.Combine(wc.errs...)
}

func (wc *rwCloser) GetReader() io.Reader {
	return wc.pr
}
