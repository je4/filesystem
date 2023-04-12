package main

import (
	"flag"
	"fmt"
	"github.com/je4/filesystem/v2/pkg/basefs"
	"github.com/je4/filesystem/v2/pkg/osfsrw"
	"github.com/je4/filesystem/v2/pkg/zipasfolder"
	"io/fs"
	"path/filepath"
)

var basedir = flag.String("basedir", "", "The base directory to use for the zip file. (default: current directory)")

func recurseDir(fsys fs.FS, name string) {
	files, err := fs.ReadDir(fsys, name)
	if err != nil {
		panic(err)
	}
	for _, file := range files {
		fname := filepath.ToSlash(filepath.Join(name, file.Name()))
		if file.IsDir() {
			fmt.Printf("[d] %s\n", fname)
			recurseDir(fsys, fname)
		} else {
			fi, err := file.Info()
			if err != nil {
				panic(err)
			}
			fmt.Printf("[f] %s [%v]\n", fname, fi.Size())
		}
	}
}

func main() {
	flag.Parse()

	dirFS := osfsrw.NewOSFSRW(*basedir)
	newFS, err := zipasfolder.NewFS(dirFS, 20)
	if err != nil {
		panic(err)
	}
	closeFS, ok := newFS.(basefs.CloserFS)
	if ok {
		defer closeFS.Close()
	}

	recurseDir(newFS, "")
}
