package writefs

import (
	"emperror.dev/errors"
	"io"
	"io/fs"
	"os"
	"strings"
)

func Copy(fs fs.FS, src, dst string) (int64, error) {
	var srcFP io.ReadCloser
	var err error
	if strings.Contains(src, "://") {
		srcFP, err = fs.Open(src)
		if err != nil {
			return 0, errors.Wrapf(err, "cannot open source '%s'", src)
		}
	} else {
		srcFP, err = os.Open(src)
		if err != nil {
			return 0, errors.Wrapf(err, "cannot open source '%s'", src)
		}
	}
	var dstFP io.WriteCloser
	if strings.Contains(dst, "://") {
		dstFP, err = Create(fs, dst)
		if err != nil {
			srcFP.Close()
			return 0, errors.Wrapf(err, "cannot open destination '%s'", dst)
		}
	} else {
		dstFP, err = os.Create(dst)
		if err != nil {
			srcFP.Close()
			return 0, errors.Wrapf(err, "cannot open destination '%s'", dst)
		}
	}
	var errs []error

	num, err := io.Copy(dstFP, srcFP)
	if err != nil {
		errs = append(errs, errors.Wrap(err, "cannot copy data"))
	}
	if err := dstFP.Close(); err != nil {
		errs = append(errs, errors.Wrap(err, "cannot close destination"))
	}
	if err := srcFP.Close(); err != nil {
		errs = append(errs, errors.Wrap(err, "cannot close source"))
	}
	if len(errs) > 0 {
		return 0, errors.Wrap(errors.Combine(errs...), "cannot copy files")
	}
	return num, nil
}
