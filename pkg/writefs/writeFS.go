package writefs

type WriteFS interface {
	Create(path string) (FileWrite, error)
}

type MkDirFS interface {
	WriteFS
	MkDir(path string) error
}

type RenameFS interface {
	WriteFS
	Rename(oldPath, newPath string) error
}

type DeleteFS interface {
	WriteFS
	Delete(path string) error
}
