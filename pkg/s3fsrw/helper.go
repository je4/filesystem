package s3fsrw

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

func extractBucket(path string) (string, string) {
	path = clearPath(path)
	if path == "" {
		return "", ""
	}
	parts := strings.SplitN(path, "/", 2)
	if len(parts) == 1 {
		return parts[0], ""
	}
	return parts[0], parts[1]
}
