package basefs

import "io/fs"

// IsLockedFS is a fs.FS that can be checked for being locked.
type IsLockedFS interface {
	fs.FS
	IsLocked() bool
}
