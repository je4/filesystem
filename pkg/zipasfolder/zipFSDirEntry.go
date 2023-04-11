package zipasfolder

import (
	"io/fs"
	"path/filepath"
)

func NewZIPFSDirEntry(info fs.FileInfo) *ZIPFSDirEntry {
	return &ZIPFSDirEntry{
		info: info,
	}
}

type ZIPFSDirEntry struct {
	info fs.FileInfo
}

func (zfsde *ZIPFSDirEntry) Name() string {
	return filepath.Base(zfsde.info.Name())
}

func (zfsde *ZIPFSDirEntry) IsDir() bool {
	return zfsde.info.IsDir()
}

func (zfsde *ZIPFSDirEntry) Type() fs.FileMode {
	return zfsde.info.Mode().Type()
}

func (zfsde *ZIPFSDirEntry) Info() (fs.FileInfo, error) {
	return zfsde.info, nil
}

var (
	_ fs.DirEntry = &ZIPFSDirEntry{}
)
