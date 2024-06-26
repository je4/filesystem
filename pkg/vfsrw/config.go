package vfsrw

import "github.com/je4/utils/v2/pkg/config"
import trustconfig "github.com/je4/trustutil/v2/pkg/config"

type SFTP struct {
	Address          config.EnvString
	KnownHosts       []string
	BaseDir          string
	Sessions         uint
	User             config.EnvString
	Password         config.EnvString
	PrivateKey       []string
	ZipAsFolderCache uint
}

type OS struct {
	BaseDir          string
	ZipAsFolderCache uint
}

type Remote struct {
	Address   string
	ClientTLS *trustconfig.TLSConfig
	BaseDir   string
}

type S3 struct {
	AccessKeyID      config.EnvString
	SecretAccessKey  config.EnvString
	Endpoint         config.EnvString
	Region           config.EnvString
	UseSSL           bool
	Debug            bool
	CAPEM            string
	BaseUrl          string
	ZipAsFolderCache uint
}

type VFS struct {
	Name   string  `toml:"name"`
	Type   string  `toml:"type"`
	S3     *S3     `toml:"s3,omitempty"`
	OS     *OS     `toml:"os,omitempty"`
	SFTP   *SFTP   `toml:"sftp,omitempty"`
	Remote *Remote `toml:"remote,omitempty"`
}

type Config map[string]*VFS
