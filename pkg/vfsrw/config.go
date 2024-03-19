package vfsrw

import "github.com/je4/utils/v2/pkg/config"

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
	Name string
	Type string
	S3   *S3   `toml:"s3,omitempty"`
	OS   *OS   `toml:"os,omitempty"`
	SFTP *SFTP `toml:"sftp,omitempty"`
}

type Config map[string]*VFS
