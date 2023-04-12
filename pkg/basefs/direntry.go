package basefs

import "io/fs"

type dirEntry struct {
	fs.FileInfo
}

// NewDirEntry creates a new DirEntry from a FileInfo.
func NewDirEntry(info fs.FileInfo) *dirEntry {
	return &dirEntry{
		FileInfo: info,
	}
}

func (de *dirEntry) Type() fs.FileMode {
	return de.FileInfo.Mode().Type()
}

func (de *dirEntry) Info() (fs.FileInfo, error) {
	return de.FileInfo, nil
}

var (
	_ fs.DirEntry = (*dirEntry)(nil)
)
