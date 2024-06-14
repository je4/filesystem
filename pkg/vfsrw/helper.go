package vfsrw

import (
	"crypto/tls"
	"crypto/x509"
	"emperror.dev/errors"
	"github.com/je4/filesystem/v3/pkg/osfsrw"
	"github.com/je4/filesystem/v3/pkg/remotefs"
	"github.com/je4/filesystem/v3/pkg/s3fsrw"
	"github.com/je4/filesystem/v3/pkg/sftpfsrw"
	"github.com/je4/filesystem/v3/pkg/zipasfolder"
	"github.com/je4/trustutil/v2/pkg/loader"
	"github.com/je4/utils/v2/pkg/zLogger"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"
	"io"
	"io/fs"
	"os"
	"regexp"
)

func newRemote(name string, conf *Remote, logger zLogger.ZLogger) (fs.FS, error) {
	clientCert, clientLoader, err := loader.CreateClientLoader(conf.ClientTLS, logger)
	if err != nil {
		logger.Panic().Msgf("cannot create client loader: %v", err)
	}
	rFS, err := remotefs.NewFS(clientCert, conf.Address, conf.BaseDir, name, []io.Closer{clientLoader}, logger)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot create new osfsrw")
	}
	return rFS, nil
}

func newOS(name string, cfg *OS, logger zLogger.ZLogger) (fs.FS, error) {
	rFS, err := osfsrw.NewFS(cfg.BaseDir, logger)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot create new osfsrw")
	}
	if cfg.ZipAsFolderCache == 0 {
		return rFS, nil
	}
	zFS, err := zipasfolder.NewFS(rFS, int(cfg.ZipAsFolderCache), logger)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot create zipasfolder over '%v'", zFS)
	}
	return zFS, nil
}

func newSFTP(name string, cfg *SFTP, logger zLogger.ZLogger) (fs.FS, error) {
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
	rFS, err := sftpfsrw.NewFS(string(cfg.Address), sConfig, cfg.BaseDir, cfg.Sessions, logger)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot create sftpfsrw")
	}
	if cfg.ZipAsFolderCache == 0 {
		return rFS, nil
	}
	zFS, err := zipasfolder.NewFS(rFS, int(cfg.ZipAsFolderCache), logger)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot create zipasfolder over '%v'", zFS)
	}
	return zFS, nil
}

func newS3(name string, cfg *S3, logger zLogger.ZLogger) (fs.FS, error) {
	var tlsConfig *tls.Config
	switch cfg.CAPEM {
	case "ignore":
		tlsConfig = &tls.Config{InsecureSkipVerify: true}
	case "":
		// no tls
	default:
		tlsConfig = &tls.Config{RootCAs: x509.NewCertPool()}
		if ok := tlsConfig.RootCAs.AppendCertsFromPEM([]byte(cfg.CAPEM)); !ok {
			return nil, errors.New("cannot add root ca to CertPool")
		}
	}

	rFS, err := s3fsrw.NewFS(string(cfg.Endpoint), string(cfg.AccessKeyID), string(cfg.SecretAccessKey), string(cfg.Region), cfg.UseSSL, cfg.Debug, tlsConfig, logger)
	if err != nil {
		return nil, errors.Wrap(err, "cannot create s3fsrw")
	}
	if cfg.ZipAsFolderCache == 0 {
		return rFS, nil
	}
	zFS, err := zipasfolder.NewFS(rFS, int(cfg.ZipAsFolderCache), nil)
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
