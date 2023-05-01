package writefs

type CreateFS interface {
	Create(path string) (FileWrite, error)
}

type MkDirFS interface {
	MkDir(path string) error
}

type RenameFS interface {
	Rename(oldPath, newPath string) error
}

type RemoveFS interface {
	Remove(path string) error
}

type CloseFS interface {
	Close() error
}

type WriteFileFS interface {
	WriteFile(name string, data []byte) error
}

type FullpathFS interface {
	Fullpath(name string) (string, error)
}
