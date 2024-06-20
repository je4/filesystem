package zipasfolder

import (
	"fmt"
	"github.com/bluele/gcache"
	"github.com/je4/filesystem/v3/pkg/writefs"
	"github.com/je4/utils/v2/pkg/zLogger"
	"github.com/pkg/errors"
	"io"
	"io/fs"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// NewFS creates a new zipAsFolderFS which handles zipfiles like folders which are read-only
// it implements readwritefs.ReadWriteFS, fs.ReadDirFS, fs.ReadFileFS, basefs.CloserFS
func NewFS(baseFS fs.FS, cacheSize int, logger zLogger.ZLogger) (*zipAsFolderFS, error) {
	_logger := logger.With().Str("class", "zipAsFolderFS").Logger()
	logger = &_logger
	f := &zipAsFolderFS{
		baseFS: baseFS,
		zipCache: gcache.New(cacheSize).
			LRU().
			LoaderFunc(func(key interface{}) (interface{}, error) {
				zipFilename, ok := key.(string)
				logger.Debug().Msgf("load zip file '%s'", zipFilename)
				if !ok {
					return nil, errors.Errorf("cannot cast key %v to string", key)
				}
				zipFile, err := baseFS.Open(zipFilename)
				if err != nil {
					return nil, errors.Wrapf(err, "cannot open zip file '%s'", zipFilename)
				}
				zipFS, err := NewZipFSCloser(zipFile, zipFilename, logger)
				return zipFS, nil
			}).
			EvictedFunc(func(key, value any) {
				logger.Debug().Msgf("evict zip file '%s'", key)
				zipFS, ok := value.(fs.FS)
				if !ok {
					return
				}
				writefs.Close(zipFS)
			}).
			PurgeVisitorFunc(func(key, value any) {
				logger.Debug().Msgf("purge zip file '%s'", key)
				zipFS, ok := value.(fs.FS)
				if !ok {
					return
				}
				writefs.Close(zipFS)
			}).
			Build(),
		end:    make(chan bool),
		logger: logger,
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
	return f, nil
}

type zipAsFolderFS struct {
	baseFS   fs.FS
	zipCache gcache.Cache
	lock     sync.RWMutex
	end      chan bool
	logger   zLogger.ZLogger
}

func (fsys *zipAsFolderFS) Fullpath(name string) (string, error) {
	return writefs.Fullpath(fsys.baseFS, name)
}

func (fsys *zipAsFolderFS) String() string {
	return fmt.Sprintf("zipAsFolder:%v", fsys.baseFS)
}

// CReate creates a new file
func (fsys *zipAsFolderFS) Create(path string) (writefs.FileWrite, error) {
	path = clearPath(path)
	zipFile, _, isZIP := expandZipFile(path)
	if isZIP {
		return nil, errors.Errorf("cannot create file '%s' in zip file '%s'", path, zipFile)
	}
	return writefs.Create(fsys.baseFS, path)
}

// MkDir creates a new folder
func (fsys *zipAsFolderFS) MkDir(path string) error {
	path = clearPath(path)
	zipFile, _, isZIP := expandZipFile(path)
	if isZIP {
		return errors.Errorf("cannot create folder '%s' in zip file '%s'", path, zipFile)
	}
	return writefs.MkDir(fsys.baseFS, path)
}

// Stat returns the file info for a given path
func (fsys *zipAsFolderFS) Stat(name string) (fs.FileInfo, error) {
	name = strings.TrimPrefix(name, "./")
	name = strings.Trim(name, "/")
	zipFile, zipPath, isZIP := expandZipFile(name)
	if !isZIP {
		info, err := fs.Stat(fsys.baseFS, name)
		if err != nil {
			return info, errors.Wrapf(err, "cannot stat file '%s'", name)
		}
		return info, nil
	}
	fsys.lock.RLock()
	defer fsys.lock.RUnlock()
	zipFSCache, err := fsys.zipCache.Get(zipFile)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot get zip file '%s'", zipFile)
	}

	zipFS, ok := zipFSCache.(fs.FS)
	if !ok {
		return nil, errors.Errorf("cannot cast zip file '%s' to fs.s3FSRW", zipFile)
	}
	return fs.Stat(zipFS, zipPath)
}

// Sub returns a new zipAsFolderFS which is a subfolder of the current zipAsFolderFS
func (fsys *zipAsFolderFS) Sub(dir string) (writefs.ReadWriteFS, error) {
	return writefs.NewSubFS(fsys, dir), nil
}

// ReadFile reads a file from the filesystem
func (fsys *zipAsFolderFS) ReadFile(name string) ([]byte, error) {
	fp, err := fsys.Open(name)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot open file '%s'", name)
	}
	defer fp.Close()
	return io.ReadAll(fp)
}

// ReadDir reads a directory from the filesystem
func (fsys *zipAsFolderFS) ReadDir(name string) ([]fs.DirEntry, error) {
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
				result = append(result, writefs.NewDirEntry(writefs.NewFileInfoDir(entry.Name())))
			} else {
				result = append(result, writefs.NewDirEntry(fi))
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
	zipFS, ok := zipFSCache.(fs.FS)
	if !ok {
		return nil, errors.Errorf("cannot cast zip file '%s' to *ZIPFS", zipFile)
	}
	return fs.ReadDir(zipFS, zipPath)
}

// Open opens a file from the filesystem
func (fsys *zipAsFolderFS) Open(name string) (fs.File, error) {
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
	zipFS, ok := zipFSCache.(fs.FS)
	if !ok {
		return nil, errors.Errorf("cannot cast zip file '%s' to *ZIPFS", zipFile)
	}
	rc, err := zipFS.Open(zipPath)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot open file '%s' in zip file '%s'", zipPath, zipFile)
	}
	return rc, nil
}

// Close closes the filesystem and underlying fs if possible
func (fsys *zipAsFolderFS) Close() error {
	fsys.lock.Lock()
	defer fsys.lock.Unlock()
	fsys.end <- true
	fsys.zipCache.Purge()

	if closer, ok := fsys.baseFS.(io.Closer); ok {
		closer.Close()
	}
	return nil
}

func (fsys *zipAsFolderFS) ClearUnlocked() error {
	fsys.lock.Lock()
	defer fsys.lock.Unlock()
	fsMap := fsys.zipCache.GetALL(false)
	for key, mFS := range fsMap {
		isLockedFS, ok := mFS.(writefs.IsLockedFS)
		if !ok {
			continue
		}
		if !isLockedFS.IsLocked() {
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
	_ writefs.ReadWriteFS = (*zipAsFolderFS)(nil)
	_ writefs.MkDirFS     = (*zipAsFolderFS)(nil)
	_ writefs.CloseFS     = (*zipAsFolderFS)(nil)
	_ writefs.FullpathFS  = (*zipAsFolderFS)(nil)
	_ fs.ReadDirFS        = (*zipAsFolderFS)(nil)
	_ fs.ReadFileFS       = (*zipAsFolderFS)(nil)
	_ fmt.Stringer        = (*zipAsFolderFS)(nil)
)
