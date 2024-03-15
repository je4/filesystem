package vfsrw

import (
	"emperror.dev/errors"
	"github.com/je4/filesystem/v2/pkg/osfsrw"
	"github.com/je4/filesystem/v2/pkg/s3fsrw"
	"github.com/je4/filesystem/v2/pkg/sftpfsrw"
	"github.com/je4/filesystem/v2/pkg/zipasfolder"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"
	"io/fs"
	"os"
	"regexp"
)

func newOS(cfg *OS) (fs.FS, error) {
	rFS, err := osfsrw.NewFS(cfg.BaseDir)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot create new osfsrw")
	}
	if cfg.ZipAsFolderCache == 0 {
		return rFS, nil
	}
	zFS, err := zipasfolder.NewFS(rFS, int(cfg.ZipAsFolderCache))
	if err != nil {
		return nil, errors.Wrapf(err, "cannot create zipasfolder over '%v'", zFS)
	}
	return zFS, nil
}

func newSFTP(cfg *SFTP) (fs.FS, error) {
	if cfg.Sessions <= cfg.ZipAsFolderCache {
		return nil, errors.Errorf("sftp sessions (%v) must be larger than zipasfoldercache (%v)", cfg.Sessions, cfg.ZipAsFolderCache)
	}
	sConfig := &ssh.ClientConfig{
		User: string(cfg.User),
	}
	if len(cfg.PrivateKey) > 0 {
		var signers = []ssh.Signer{}
		for _, keyFile := range cfg.PrivateKey {
			pemBytes, err := os.ReadFile(keyFile)
			if err != nil {
				return nil, errors.Wrapf(err, "cannot read '%s'", keyFile)
			}
			if cfg.Password != "" {
				signer, err := ssh.ParsePrivateKeyWithPassphrase(pemBytes, []byte(cfg.Password))
				if err != nil {
					return nil, errors.Wrapf(err, "cannot parse and decrypt '%s'", keyFile)
				}
				signers = append(signers, signer)
			} else {
				signer, err := ssh.ParsePrivateKey(pemBytes)
				if err != nil {
					return nil, errors.Wrapf(err, "cannot parse '%s'", keyFile)
				}
				signers = append(signers, signer)
			}
		}
		sConfig.Auth = []ssh.AuthMethod{ssh.PublicKeys(signers...)}
	} else {
		// password login
		sConfig.Auth = []ssh.AuthMethod{ssh.Password(string(cfg.Password))}
	}
	if len(cfg.KnownHosts) == 0 {
		sConfig.HostKeyCallback = ssh.InsecureIgnoreHostKey()
	} else {
		hkCallback, err := knownhosts.New(cfg.KnownHosts...)
		if err != nil {
			return nil, errors.Wrapf(err, "cannot read known hosts %v", cfg.KnownHosts)
		}
		sConfig.HostKeyCallback = hkCallback
	}
	rFS, err := sftpfsrw.NewFS(string(cfg.Address), sConfig, cfg.BaseDir, cfg.Sessions)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot create sftpfsrw")
	}
	if cfg.ZipAsFolderCache == 0 {
		return rFS, nil
	}
	zFS, err := zipasfolder.NewFS(rFS, int(cfg.ZipAsFolderCache))
	if err != nil {
		return nil, errors.Wrapf(err, "cannot create zipasfolder over '%v'", zFS)
	}
	return zFS, nil
}

func newS3(cfg *S3, logger zLogger.ZWrapper) (fs.FS, error) {
	rFS, err := s3fsrw.NewFS(string(cfg.Endpoint), string(cfg.AccessKeyID), string(cfg.SecretAccessKey), string(cfg.Region), cfg.UseSSL, logger)
	if err != nil {
		return nil, errors.Wrap(err, "cannot create s3fsrw")
	}
	if cfg.ZipAsFolderCache == 0 {
		return rFS, nil
	}
	zFS, err := zipasfolder.NewFS(rFS, int(cfg.ZipAsFolderCache))
	if err != nil {
		return nil, errors.Wrapf(err, "cannot create zipasfolder over '%v'", zFS)
	}
	return zFS, nil
}

var matchPathRegexp = regexp.MustCompile(`^vfs://?([^/]+)/(.*)$`)

func matchPath(vfsPath string) (name string, path string, err error) {
	matches := matchPathRegexp.FindStringSubmatch(vfsPath)
	if matches == nil {
		err = errors.Errorf("invalid path format '%s'", vfsPath)
		return
	}
	name = matches[1]
	path = matches[2]
	return
}
