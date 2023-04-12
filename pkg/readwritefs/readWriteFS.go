package readwritefs

import (
	"github.com/je4/filesystem/v2/pkg/writefs"
	"io/fs"
)

type ReadWriteFS interface {
	fs.FS
	writefs.WriteFS
}
