package zipasfolder

import (
	"path/filepath"
	"strings"
)

func clearPath(path string) string {
	path = strings.Trim(filepath.ToSlash(filepath.Clean(path)), "/")
	if path == "." {
		path = ""
	}
	return path
}
