package main

import (
	"emperror.dev/errors"
	"flag"
	"fmt"
	"github.com/je4/filesystem/v2/pkg/osfsrw"
	"github.com/je4/filesystem/v2/pkg/writefs"
	"github.com/je4/filesystem/v2/pkg/zipasfolder"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"path/filepath"
	"time"
)

type FSAbstraction interface {
	GetScheme() string
	GetID() string
	GetName() string
	GetBasepath() string
	OpenRead(fsPath *url.URL) (io.ReadCloser, error)
	OpenCreate(fsPath *url.URL) (io.WriteCloser, error)
}

func NewFSAbstractionZipAsFolder(name, basePath, fsBase string, cacheSize int) (FSAbstraction, error) {
	fsa := &FSAbstractionZipAsFolder{
		name:     name,
		basePath: basePath,
	}
	osfs, err := osfsrw.NewFS(fsBase)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot create osfs")
	}
	fsa.FS, err = zipasfolder.NewFS(osfs, cacheSize)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot create zipasfolder")
	}
	return fsa, nil
}

type FSAbstractionZipAsFolder struct {
	fs.FS
	name     string
	basePath string
}

func (fszas *FSAbstractionZipAsFolder) GetScheme() string {
	return "file"
}

func (fszas *FSAbstractionZipAsFolder) GetID() string {
	return fszas.GetName()
}

func (fszas *FSAbstractionZipAsFolder) GetName() string {
	return fszas.name
}

func (fszas *FSAbstractionZipAsFolder) GetBasepath() string {
	return fszas.basePath
}

func (fszas *FSAbstractionZipAsFolder) OpenRead(fsPath *url.URL) (io.ReadCloser, error) {
	fp, err := fszas.FS.Open(filepath.ToSlash(filepath.Clean(fsPath.Path)))
	if err != nil {
		return nil, errors.Wrapf(err, "cannot open file '%s'", fsPath.String())
	}
	return fp, nil
}

func (fszas *FSAbstractionZipAsFolder) OpenCreate(fsPath *url.URL) (io.WriteCloser, error) {
	return writefs.Create(fszas.FS, filepath.ToSlash(filepath.Clean(fsPath.Path)))
}

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

// C:/temp/ocfl_bang.zip/id_blah-blubb/v1/content/data/collage_files/15/36_39.jpeg
func main0() {
	flag.Parse()

	dirFS, _ := osfsrw.NewFS(*basedir)
	newFS, err := zipasfolder.NewFS(dirFS, 20)
	if err != nil {
		panic(err)
	}
	defer writefs.Close(newFS)
	fs.ReadDir(newFS, "")

	recurseDir(newFS, "")
}

func closeReader(r io.Reader) error {
	rc, ok := r.(io.ReadCloser)
	if !ok {
		return errors.New("cannot cast reader to io.ReadCloser")
	}
	return rc.Close()
}

func serveContent(w http.ResponseWriter, r *http.Request, name string, modTime time.Time, content io.Reader) {
	rs, ok := content.(io.ReadSeeker)
	if ok {
		http.ServeContent(w, r, name, modTime, rs)
		return
	}
	ra := r.Header.Get("Range")
	if len(ra) > 0 {
		w.WriteHeader(http.StatusBadRequest)
		io.WriteString(w, "Range not supported")
		return
	}
	_, err := io.Copy(w, content)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		io.WriteString(w, err.Error())
		return
	}
}

func main() {
	fszas, err := NewFSAbstractionZipAsFolder("test", "temp", "C:/", 20)
	if err != nil {
		panic(err)
	}
	url, err := url.Parse("file://test/ocfl_bang.zip/id_blah-blubb/v1/content/data/collage_files/15/36_39.jpeg")
	fp, err := fszas.OpenRead(url)
	if err != nil {
		panic(err)
	}

	if _, err := io.ReadAll(fp); err != nil {
		panic(err)
	}
	//http.ServeContent()

	defer closeReader(fp)

}
