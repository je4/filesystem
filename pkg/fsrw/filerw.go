package fsrw

import "io/fs"

type FileRW interface {
	fs.File
	FileW
}
