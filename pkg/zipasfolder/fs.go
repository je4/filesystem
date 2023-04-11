package zipasfolder

import (
	"archive/zip"
	"github.com/bluele/gcache"
	"github.com/je4/filesystem/v2/pkg/fsrw"
	"github.com/pkg/errors"
	"io"
	"io/fs"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

func NewFS(baseFS fsrw.FSRW, cacheSize int) fsrw.FSRW {
	f := &FS{
		baseFS: baseFS,
		zipCache: gcache.New(cacheSize).
			LRU().
			LoaderFunc(func(key interface{}) (interface{}, error) {
				zipFilename, ok := key.(string)
				if !ok {
					return nil, errors.Errorf("cannot cast key %v to string", key)
				}
				zipFile, err := baseFS.Open(zipFilename)
				if err != nil {
					return nil, errors.Wrapf(err, "cannot open zip file '%s'", zipFilename)
				}
				stat, err := zipFile.Stat()
				if err != nil {
					return nil, errors.Wrapf(err, "cannot stat zip file '%s'", zipFilename)
				}
				filesize := stat.Size()
				readerAt, ok := zipFile.(io.ReaderAt)
				if !ok {
					zipFile.Close()
					return nil, errors.Errorf("cannot cast file '%s' to io.ReaderAt", zipFilename)
				}
				zipReader, err := zip.NewReader(readerAt, filesize)
				if err != nil {
					zipFile.Close()
					return nil, errors.Wrapf(err, "cannot create zip reader for '%s'", zipFilename)
				}
				zipFS := NewZIPFS(zipReader, zipFile)
				return zipFS, nil
			}).
			EvictedFunc(func(key, value any) {
				zipFS, ok := value.(*ZIPFS)
				if !ok {
					return
				}
				zipFS.Close()
			}).
			PurgeVisitorFunc(func(key, value any) {
				zipFS, ok := value.(*ZIPFS)
				if !ok {
					return
				}
				zipFS.Close()
			}).
			Build(),
		end: make(chan bool),
	}
	go func() {
		for alive := true; alive; {
			timer := time.NewTimer(time.Minute)
			select {
			case <-f.end:
				timer.Stop()
				alive = false
			case <-timer.C:
				f.ClearUnlocked()
			}
		}
	}()
	return f
}

type FS struct {
	baseFS   fsrw.FSRW
	zipCache gcache.Cache
	lock     sync.RWMutex
	end      chan bool
}

func (fsys *FS) Create(path string) (fsrw.FileW, error) {
	return fsys.baseFS.Create(path)
}

func (fsys *FS) MkDir(path string) error {
	mkdirFS, ok := fsys.baseFS.(fsrw.MkDirFSRW)
	if !ok {
		return errors.New("MkDir not supported")
	}
	return mkdirFS.MkDir(path)
}

func (fsys *FS) Stat(name string) (fs.FileInfo, error) {
	name = strings.TrimPrefix(name, "./")
	name = strings.Trim(name, "/")
	zipFile, zipPath, isZIP := expandZipFile(name)
	if !isZIP {
		statFS, ok := fsys.baseFS.(fs.StatFS)
		if !ok {
			return nil, errors.New("Stat not supported")
		}
		info, err := statFS.Stat(name)
		if err != nil {
			return info, errors.Wrapf(err, "cannot open file '%s'", name)
		}
		return info, nil
	}
	fsys.lock.RLock()
	defer fsys.lock.RUnlock()
	zipFSCache, err := fsys.zipCache.Get(zipFile)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot get zip file '%s'", zipFile)
	}
	zipFS, ok := zipFSCache.(*ZIPFS)
	if !ok {
		return nil, errors.Errorf("cannot cast zip file '%s' to *ZIPFS", zipFile)
	}
	return zipFS.Stat(zipPath)
}

func (fsys *FS) Sub(dir string) (fsrw.FSRW, error) {
	return NewSubFS(fsys, dir), nil
}

func (fsys *FS) ReadFile(name string) ([]byte, error) {
	fp, err := fsys.Open(name)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot open file '%s'", name)
	}
	defer fp.Close()
	return io.ReadAll(fp)
}

func (fsys *FS) ReadDir(name string) ([]fs.DirEntry, error) {
	name = strings.TrimPrefix(name, "./")
	name = strings.Trim(name, "/")
	zipFile, zipPath, isZIP := expandZipFile(name)
	if !isZIP {
		if name == "" {
			name = "."
		}
		entries, err := fs.ReadDir(fsys.baseFS, name)
		//file, err := fsys.baseFS.ReadDir(name)
		if err != nil {
			return entries, errors.Wrapf(err, "cannot open file '%s'", name)
		}
		var result = make([]fs.DirEntry, 0, len(entries))
		for _, entry := range entries {
			fi, err := entry.Info()
			if err != nil {
				return nil, errors.Wrapf(err, "cannot get info for file '%s'", entry.Name())
			}
			if fi.IsDir() || isZipFile(entry.Name()) {
				result = append(result, NewZIPFSDirEntry(NewZIPFSFileInfoDir(entry.Name())))
			} else {
				result = append(result, NewZIPFSDirEntry(fi))
			}
		}
		return result, nil
	}
	fsys.lock.RLock()
	defer fsys.lock.RUnlock()
	zipFSCache, err := fsys.zipCache.Get(zipFile)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot get zip file '%s'", zipFile)
	}
	zipFS, ok := zipFSCache.(*ZIPFS)
	if !ok {
		return nil, errors.Errorf("cannot cast zip file '%s' to *ZIPFS", zipFile)
	}
	return zipFS.ReadDir(zipPath)
}

func (fsys *FS) Open(name string) (fs.File, error) {
	name = strings.TrimPrefix(name, "./")
	name = strings.Trim(name, "/")
	zipFile, zipPath, isZIP := expandZipFile(name)
	if !isZIP {
		file, err := fsys.baseFS.Open(name)
		if err != nil {
			return file, errors.Wrapf(err, "cannot open file '%s'", name)
		}
		return file, nil
	}

	fsys.lock.RLock()
	defer fsys.lock.RUnlock()
	zipFSCache, err := fsys.zipCache.Get(zipFile)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot get zip file '%s'", zipFile)
	}
	zipFS, ok := zipFSCache.(*ZIPFS)
	if !ok {
		return nil, errors.Errorf("cannot cast zip file '%s' to *ZIPFS", zipFile)
	}
	rc, err := zipFS.Open(zipPath)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot open file '%s' in zip file '%s'", zipPath, zipFile)
	}
	return rc, nil
}

func (fsys *FS) Close() error {
	fsys.lock.Lock()
	defer fsys.lock.Unlock()
	fsys.end <- true
	fsys.zipCache.Purge()
	return nil
}

func (fsys *FS) ClearUnlocked() error {
	fsys.lock.Lock()
	defer fsys.lock.Unlock()
	fss := fsys.zipCache.GetALL(false)
	for key, fs := range fss {
		fs, ok := fs.(*ZIPFS)
		if !ok {
			continue
		}
		if !fs.IsLocked() {
			fsys.zipCache.Remove(key)
		}
	}
	return nil
}
func isZipFile(name string) bool {
	return strings.ToLower(filepath.Ext(name)) == ".zip"
}

func expandZipFile(name string) (zipFile string, zipPath string, isZip bool) {
	name = filepath.ToSlash(filepath.Clean(name))
	parts := strings.Split(name, "/")
	for i := len(parts) - 1; i >= 0; i-- {
		if isZipFile(parts[i]) {
			zipFile = strings.Join(parts[:i+1], "/")
			zipPath = strings.Join(parts[i+1:], "/")
			isZip = true
			return
		}
	}
	return
}

var (
	_ fsrw.FSRW     = &FS{}
	_ fs.ReadDirFS  = &FS{}
	_ fs.ReadFileFS = &FS{}
)
