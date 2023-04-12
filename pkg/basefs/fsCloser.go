package basefs

import "io/fs"

// CloserFS is a fs.FS that can be closed.
type CloserFS interface {
	fs.FS
	Close() error
}
