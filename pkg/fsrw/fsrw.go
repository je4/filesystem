package fsrw

import "io/fs"

type FSRW interface {
	fs.FS
	Create(path string) (FileW, error)
}

type MkDirFSRW interface {
	FSRW
	MkDir(path string) error
}

type OpenFileFSRW interface {
	FSRW
	OpenFile(name string, flag int, perm fs.FileMode) (FileRW, error)
}

type CloseFSRW interface {
	FSRW
	Close() error
}
