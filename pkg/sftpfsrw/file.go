package sftpfsrw

import (
	"emperror.dev/errors"
	"github.com/pkg/sftp"
)

type sftpFile struct {
	*sftp.File
	sess *sftpSession
}

func (f *sftpFile) Close() error {
	defer f.sess.sftpFS.closeSession(f.sess)
	if err := f.File.Close(); err != nil {
		return errors.Wrapf(err, "cannot close '%s'", f.Name())
	}
	return nil
}
