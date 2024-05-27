package sftpfsrw

import (
	"emperror.dev/errors"
	"fmt"
	"github.com/je4/filesystem/v3/pkg/writefs"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
	"io"
	"io/fs"
	"path/filepath"
	"time"
)

func NewFS(addr string, config *ssh.ClientConfig, baseDir string, numSessions uint) (*sftpFSRW, error) {
	client, err := ssh.Dial("tcp", addr, config)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot connect to '%s'", addr)
	}

	sftpFS := &sftpFSRW{
		addr:         addr,
		user:         config.User,
		baseDir:      baseDir,
		sshClient:    client,
		sftpSessions: map[uint]*sftpSession{},
		freeSessions: make(chan uint, numSessions),
	}

	for i := uint(0); i < numSessions; i++ {
		session, err := sftp.NewClient(client)
		if err != nil {
			return nil, errors.Wrap(err, "cannot create sftp client")
		}
		sftpFS.sftpSessions[i] = &sftpSession{
			Client: session,
			id:     i,
			sftpFS: sftpFS,
		}
		sftpFS.freeSessions <- i
	}

	return sftpFS, nil
}

type sftpSession struct {
	*sftp.Client
	id     uint
	sftpFS *sftpFSRW
}

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

func (sess *sftpSession) Open(fullpath string) (fs.File, error) {

	fp, err := sess.Client.Open(fullpath)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot open '%s'", fullpath)
	}
	return &sftpFile{
		File: fp,
		sess: sess,
	}, nil
}

func (sess *sftpSession) Create(fullpath string) (writefs.FileWrite, error) {
	fp, err := sess.Client.Create(fullpath)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot open '%s'", fullpath)
	}
	return &sftpFile{
		File: fp,
		sess: sess,
	}, nil
}

type sftpFSRW struct {
	sshClient    *ssh.Client
	sftpSessions map[uint]*sftpSession
	addr         string
	user         string
	baseDir      string
	freeSessions chan uint
}

func (sftpFS *sftpFSRW) Remove(path string) error {
	sess, err := sftpFS.getSession(time.Second * 10)
	if err != nil {
		return errors.Wrapf(err, "cannot get sftp session")
	}
	defer sftpFS.closeSession(sess)
	fullpath := filepath.ToSlash(filepath.Join(sftpFS.baseDir, path))
	return sess.Remove(fullpath)
}

func (sftpFS *sftpFSRW) Rename(oldPath, newPath string) error {
	sess, err := sftpFS.getSession(time.Second * 10)
	if err != nil {
		return errors.Wrapf(err, "cannot get sftp session")
	}
	defer sftpFS.closeSession(sess)
	oldPath = filepath.ToSlash(filepath.Join(sftpFS.baseDir, oldPath))
	newPath = filepath.ToSlash(filepath.Join(sftpFS.baseDir, newPath))
	return sess.Rename(oldPath, newPath)
}

func (sftpFS *sftpFSRW) MkDir(path string) error {
	sess, err := sftpFS.getSession(time.Second * 10)
	if err != nil {
		return errors.Wrapf(err, "cannot get sftp session")
	}
	defer sftpFS.closeSession(sess)
	fullpath := filepath.ToSlash(filepath.Join(sftpFS.baseDir, path))
	return sess.Mkdir(fullpath)
}

func (sftpFS *sftpFSRW) Create(path string) (writefs.FileWrite, error) {
	sess, err := sftpFS.getSession(time.Second * 10)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot get sftp session")
	}
	fullpath := filepath.ToSlash(filepath.Join(sftpFS.baseDir, path))
	fp, err := sess.Create(fullpath)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot create '%s'")
	}
	return fp, nil
}

func (sftpFS *sftpFSRW) ReadDir(name string) ([]fs.DirEntry, error) {
	sess, err := sftpFS.getSession(time.Second * 10)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot get sftp session")
	}
	defer sftpFS.closeSession(sess)
	fullpath := filepath.ToSlash(filepath.Join(sftpFS.baseDir, name))
	dirs, err := sess.ReadDir(fullpath)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot read folder '%s'", fullpath)
	}
	ret := []fs.DirEntry{}
	for _, d := range dirs {
		fi := fs.FileInfoToDirEntry(d)
		if fi == nil {
			continue
		}
		ret = append(ret, fi)
	}
	return ret, err
}

func (sftpFS *sftpFSRW) ReadFile(name string) ([]byte, error) {
	fp, err := sftpFS.Open(name)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot open '%s'", name)
	}
	defer fp.Close()
	data, err := io.ReadAll(fp)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot read from '%s'", name)
	}
	return data, nil
}

func (sftpFS *sftpFSRW) Stat(name string) (fs.FileInfo, error) {
	sess, err := sftpFS.getSession(time.Second * 10)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot get sftp session")
	}
	fullpath := filepath.ToSlash(filepath.Join(sftpFS.baseDir, name))
	fi, err := sess.Stat(fullpath)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot stat '%s'", fullpath)
	}
	return fi, nil
}

func (sftpFS *sftpFSRW) getSession(timeout time.Duration) (*sftpSession, error) {
	select {
	case i, ok := <-sftpFS.freeSessions:
		if !ok {
			return nil, errors.Errorf("error reading from channel")
		}
		return sftpFS.sftpSessions[i], nil
	case <-time.After(timeout):
		return nil, errors.Errorf("timeout reached")
	}
}

func (sftpFS *sftpFSRW) closeSession(sess *sftpSession) {
	sftpFS.freeSessions <- sess.id
}

func (sftpFS *sftpFSRW) Sub(dir string) (fs.FS, error) {
	return writefs.NewSubFS(sftpFS, dir), nil
}

func (sftpFS *sftpFSRW) String() string {
	return fmt.Sprintf("sftp://%s@%s/%s", sftpFS.user, sftpFS.addr, sftpFS.baseDir)
}

func (sftpFS *sftpFSRW) Open(name string) (fs.File, error) {
	sess, err := sftpFS.getSession(time.Second * 10)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot get sftp session")
	}
	fullpath := filepath.ToSlash(filepath.Join(sftpFS.baseDir, name))
	fp, err := sess.Open(fullpath)
	if err != nil {
		sftpFS.closeSession(sess)
		return nil, errors.Wrapf(err, "cannot open '%s'", name)
	}
	return fp, nil
}

func (sftpFS *sftpFSRW) Close() error {
	var errs = []error{}
	close(sftpFS.freeSessions)

	for _, sess := range sftpFS.sftpSessions {
		if err := sess.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return nil
}

var (
	_ fs.FS         = (*sftpFSRW)(nil)
	_ fs.ReadDirFS  = (*sftpFSRW)(nil)
	_ fs.ReadFileFS = (*sftpFSRW)(nil)
	_ fs.StatFS     = (*sftpFSRW)(nil)
	_ fs.SubFS      = (*sftpFSRW)(nil)
	//	_ writefs.IsLockedFS = (*sftpFSRW)(nil)
	_ fmt.Stringer        = (*sftpFSRW)(nil)
	_ writefs.ReadWriteFS = (*sftpFSRW)(nil)
	_ writefs.MkDirFS     = (*sftpFSRW)(nil)
	_ writefs.RenameFS    = (*sftpFSRW)(nil)
	_ writefs.RemoveFS    = (*sftpFSRW)(nil)
)
