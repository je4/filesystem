package writefs

import (
	"io/fs"
)

type ReadWriteFS interface {
	fs.FS
	CreateFS
}
