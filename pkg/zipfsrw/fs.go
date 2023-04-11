package zipfs

import (
	"archive/zip"
	"bufio"
	"bytes"
	"emperror.dev/errors"
	"encoding/json"
	"fmt"
	"github.com/google/tink/go/keyset"
	"github.com/google/tink/go/tink"
	"github.com/je4/gocfl/v2/pkg/baseFS"
	"github.com/je4/gocfl/v2/pkg/encrypt"
	"github.com/je4/gocfl/v2/pkg/ocfl"
	checksum "github.com/je4/utils/v2/pkg/checksum"
	"github.com/op/go-logging"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

func IsErrNotExist(err error) bool {
	return errors.Cause(err) == fs.ErrNotExist
}

type flusherCloser struct {
	closer io.Closer
	buffer *bufio.Writer
}

func (c flusherCloser) FlushClose() error {
	if c.buffer != nil {
		if err := c.buffer.Flush(); err != nil {
			return errors.WithStack(err)
		}
	}
	if c.closer != nil {
		if err := c.closer.Close(); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

type nopCloserWriter struct {
	io.Writer
}

func (*nopCloserWriter) Close() error { return nil }

type FS struct {
	path, temp string
	factory    *baseFS.Factory
	srcReader  ocfl.CloserAt
	//	dstWriter  io.Writer
	//	dstBufferWriter *bufio.Writer
	r                 *zip.Reader
	w                 *zip.Writer
	noCompression     bool
	noCopy            []string
	logger            *logging.Logger
	closed            *bool
	checksumWriter    *checksum.ChecksumWriter
	checksumAESWriter *checksum.ChecksumWriter
	//	encryptAESWriter       *encrypt.EncryptWriterAESCBC
	//	encryptAESBufferWriter *bufio.Writer
	// encryptFP io.WriteCloser
	flusherCloser []*flusherCloser
	aead          tink.AEAD
	aad           []byte
	handle        *keyset.Handle
}

func NewFS(path string, factory *baseFS.Factory, digestAlgorithms []checksum.DigestAlgorithm, RW bool, noCompression bool, aes bool, aead tink.AEAD, aad []byte, clear bool, logger *logging.Logger) (*FS, error) {
	logger.Debug("instantiating FS")
	pathTemp := path + ".tmp"

	if clear {
		_ = factory.Delete(path)
		if aes {
			_ = factory.Delete(path + ".aes")
		}
	}
	fp, err := factory.Open(path)
	if err != nil {
		if IsErrNotExist(err) {
			pathTemp = path
		} else {
			return nil, errors.Wrapf(err, "cannot open '%s'", path)
		}
	}
	pathAES := pathTemp + ".aes"
	var zipReader baseFS.ReadSeekCloserStat
	var zipReaderAt ocfl.CloserAt
	var fileSize int64
	var ok bool
	if fp != nil {
		zipReader, ok = fp.(baseFS.ReadSeekCloserStat)
		if !ok {
			return nil, errors.Errorf("no FileSeeker for '%s'", path)
		}
		fi, err := zipReader.Stat()
		if err != nil {
			zipReader.Close()
			return nil, errors.Wrapf(err, "cannot stat '%s'", path)
		}
		fileSize = fi.Size()
		zipReaderAt, ok = zipReader.(ocfl.CloserAt)
		if !ok {
			zipReaderAt = &readSeekCloserToCloserAt{readSeeker: zipReader}
		}
	}
	isClosed := false
	zipFS := &FS{
		noCopy:        []string{},
		srcReader:     zipReaderAt,
		logger:        logger,
		closed:        &isClosed,
		path:          path,
		temp:          pathTemp,
		factory:       factory,
		noCompression: noCompression,
		aead:          aead,
		aad:           aad,
		flusherCloser: []*flusherCloser{},
	}
	if /*(zipFS.dstWriter != nil && zipFS.dstWriter != (*os.File)(nil)) && */ zipReaderAt != nil && zipReaderAt != (*os.File)(nil) {
		if zipFS.r, err = zip.NewReader(zipReaderAt, fileSize); err != nil {
			return nil, errors.Wrap(err, "cannot create zip reader")
		}
	}
	var zipFile io.WriteCloser
	if RW {
		zipFile, err = factory.Create(pathTemp)
		if err != nil {
			return nil, errors.Wrapf(err, "cannot create '%s'", pathTemp)
		}
		zipFileBuffer := bufio.NewWriterSize(zipFile, 1024*1024)
		zipFS.flusherCloser = append(zipFS.flusherCloser, &flusherCloser{closer: zipFile, buffer: zipFileBuffer})

		targetWriter := []io.Writer{zipFileBuffer}
		if aes {
			encryptFile, err := factory.Create(pathAES)
			if err != nil {
				return nil, errors.Wrapf(err, "cannot create '%s'", path)
			}
			encryptFileBuffer := bufio.NewWriterSize(encryptFile, 1024*1024)
			zipFS.flusherCloser = append(zipFS.flusherCloser, &flusherCloser{closer: encryptFile, buffer: encryptFileBuffer})

			checksumEncryptFile, err := checksum.NewChecksumWriter(digestAlgorithms, encryptFileBuffer)
			if err != nil {
				return nil, errors.Wrap(err, "cannot create ChecksumWriter")
			}
			zipFS.flusherCloser = append(zipFS.flusherCloser, &flusherCloser{closer: checksumEncryptFile, buffer: nil})
			zipFS.checksumAESWriter = checksumEncryptFile

			encrypter, err := encrypt.NewEncryptWriterAESGCM(checksumEncryptFile, aad, nil)
			if err != nil {
				return nil, errors.Wrap(err, "cannot create ChecksumWriter")
			}
			zipFS.handle = encrypter.GetKeysetHandle()
			zipFS.flusherCloser = append(zipFS.flusherCloser, &flusherCloser{closer: encrypter, buffer: nil})

			targetWriter = append(targetWriter, encrypter)
		}
		checksumWriter, err := checksum.NewChecksumWriter(digestAlgorithms, targetWriter...)
		if err != nil {
			return nil, errors.Wrap(err, "cannot create ChecksumWriter")
		}
		zipFS.flusherCloser = append(zipFS.flusherCloser, &flusherCloser{closer: checksumWriter, buffer: nil})
		zipFS.checksumWriter = checksumWriter

		zipFS.w = zip.NewWriter(checksumWriter)
		zipFS.flusherCloser = append(zipFS.flusherCloser, &flusherCloser{closer: zipFS.w, buffer: nil})
	}
	if zipReaderAt == nil && zipFile == nil {
		return nil, errors.Errorf("cannot open '%s'", path)
	}

	return zipFS, nil
}

func (zipFS *FS) String() string {
	return fmt.Sprintf("zipfs://")
}

func (zipFS *FS) isClosed() bool {
	return *zipFS.closed
}

func (zipFS *FS) IsNotExist(err error) bool {
	return err == fs.ErrNotExist
}

func (zipFS *FS) Close() error {
	var err error
	if zipFS.isClosed() {
		return nil
		//return errors.New("zipFS closed")
	}
	zipFS.logger.Debug("Close ZipFS")
	// check whether we have to copy all stuff
	if zipFS.r != nil && zipFS.w != nil {
		// check whether there's a new version of the file
		for _, zipItem := range zipFS.r.File {
			found := false
			for _, added := range zipFS.noCopy {
				if added == zipItem.Name {
					found = true
					zipFS.logger.Debugf("overwriting %s", added)
					break
				}
			}
			if found {
				continue
			}
			zipFS.logger.Debugf("copying %s", zipItem.Name)
			zipItemReader, err := zipItem.OpenRaw()
			if err != nil {
				return errors.Wrapf(err, "cannot open raw source %s", zipItem.Name)
			}
			header := zipItem.FileHeader
			targetItem, err := zipFS.w.CreateRaw(&header)
			if err != nil {
				return errors.Wrapf(err, "cannot create raw target %s", zipItem.Name)
			}
			if _, err := io.Copy(targetItem, zipItemReader); err != nil {
				return errors.Wrapf(err, "cannot raw copy %s", zipItem.Name)
			}
		}
	}
	finalError := []error{}

	// first flush all buffers in reverse order
	for i := len(zipFS.flusherCloser) - 1; i >= 0; i-- {
		fc := zipFS.flusherCloser[i]
		if err := fc.FlushClose(); err != nil {
			finalError = append(finalError, err)
		}
	}

	var digests = map[checksum.DigestAlgorithm]string{}
	var digestsAES = map[checksum.DigestAlgorithm]string{}
	if zipFS.w != nil {
		if zipFS.checksumWriter != nil {
			digests, err = zipFS.checksumWriter.GetChecksums()
			if err != nil {
				digests = map[checksum.DigestAlgorithm]string{}
				finalError = append(finalError, err)
			}
		}
		if zipFS.checksumAESWriter != nil {
			digestsAES, err = zipFS.checksumAESWriter.GetChecksums()
			if err != nil {
				digestsAES = map[checksum.DigestAlgorithm]string{}
				finalError = append(finalError, err)
			}
		}
	}
	if len(finalError) > 0 {
		return errors.Combine(finalError...)
	}
	for digestAlg, digest := range digests {
		manifestName := fmt.Sprintf("%s.%s", zipFS.path, digestAlg)
		fp, err := zipFS.factory.Create(manifestName)
		if err != nil {
			finalError = append(finalError, errors.Wrapf(err, "cannot create %s", manifestName))
			continue
		}
		if _, err := fp.Write([]byte(fmt.Sprintf("%s *%s\n", digest, filepath.Base(zipFS.path)))); err != nil {
			finalError = append(finalError, errors.Wrapf(err, "cannot write %s", manifestName))
		}
		if err := fp.Close(); err != nil {
			finalError = append(finalError, errors.Wrapf(err, "cannot close %s", manifestName))
		}
	}

	for digestAlg, digest := range digestsAES {
		manifestName := fmt.Sprintf("%s.aes.%s", zipFS.path, digestAlg)
		fp, err := zipFS.factory.Create(manifestName)
		if err != nil {
			finalError = append(finalError, errors.Wrapf(err, "cannot create %s", manifestName))
			continue
		}
		if _, err := fp.Write([]byte(fmt.Sprintf("%s *%s\n", digest, filepath.Base(zipFS.path+".aes")))); err != nil {
			finalError = append(finalError, errors.Wrapf(err, "cannot write %s", manifestName))
		}
		if err := fp.Close(); err != nil {
			finalError = append(finalError, errors.Wrapf(err, "cannot close %s", manifestName))
		}
	}
	if zipFS.handle != nil {
		keyFileName := zipFS.path + ".aes.key.json"
		keyBuf := bytes.NewBuffer(nil)
		wr := keyset.NewBinaryWriter(keyBuf)

		if err := zipFS.handle.Write(wr, zipFS.aead); err != nil {
			finalError = append(finalError, errors.Wrapf(err, "cannot write %s", keyFileName))
		}
		ts := encrypt.KeyStruct{
			EncryptedKey: keyBuf.Bytes(),
			Aad:          zipFS.aad,
		}
		jsonBytes, err := json.Marshal(ts)
		if err != nil {
			finalError = append(finalError, errors.Wrapf(err, "cannot marshal %s", keyFileName))
		} else {
			if err := zipFS.factory.WriteFile(keyFileName, jsonBytes); err != nil {
				finalError = append(finalError, errors.Wrapf(err, "cannot write %s", keyFileName))
			}
		}
	}

	if zipFS.temp != zipFS.path {
		if err := zipFS.factory.Rename(zipFS.temp, zipFS.path); err != nil {
			return errors.Wrap(err, "cannot rename on close")
		}

		if zipFS.checksumAESWriter != nil {
			if err := zipFS.factory.Rename(zipFS.temp+".aes", zipFS.path+".aes"); err != nil {
				return errors.Wrap(err, "cannot rename on close")
			}
		}
	}

	return errors.Combine(finalError...)
}

func (zipFS *FS) Discard() error {
	finalError := []error{}
	if zipFS.w != nil {
		if err := zipFS.w.Flush(); err != nil {
			finalError = append(finalError, err)
		}
		if err := zipFS.w.Close(); err != nil {
			finalError = append(finalError, err)
		}
	}
	return errors.Combine(finalError...)
}

func (zipFS *FS) OpenSeeker(name string) (ocfl.FileSeeker, error) {
	zipFS.logger.Debugf("%s - OpenSeeker(%s)", zipFS.String(), name)
	if zipFS.isClosed() {
		return nil, errors.New("zipFS closed")
	}

	name = filepath.ToSlash(filepath.Clean(name))
	//name = strings.TrimPrefix(name, "./")
	if zipFS.r == nil {
		return nil, fs.ErrNotExist
	}
	name = filepath.ToSlash(name)
	// check whether file is newly created
	for _, newItem := range zipFS.noCopy {
		if newItem == name {
			return nil, fs.ErrInvalid // new files cannot be opened
		}
	}
	for _, zipItem := range zipFS.r.File {
		if zipItem.Name == name {
			finfo, err := NewFileInfoFile(zipItem)
			if err != nil {
				return nil, errors.Wrapf(err, "cannot create zipfs.FileInfo for %s", zipItem.Name)
			}
			f, err := NewFile(finfo)
			if err != nil {
				return nil, errors.Wrapf(err, "cannot create zipfs.File from zipfs.FileInfo for %s", finfo.Name())
			}
			return f, nil
		}
	}
	zipFS.logger.Debugf("%s not found", name)
	return nil, fs.ErrNotExist
}

func (zipFS *FS) Open(name string) (fs.File, error) {
	return zipFS.OpenSeeker(name)
}

func (zipFS *FS) ReadFile(name string) ([]byte, error) {
	fp, err := zipFS.Open(name)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot open '%s'", name)
	}
	defer fp.Close()
	data := bytes.NewBuffer(nil)
	if _, err := io.Copy(data, fp); err != nil {
		return nil, errors.Wrapf(err, "cannot read '%s'", name)
	}
	return data.Bytes(), nil
}

func (zipFS *FS) Create(name string) (io.WriteCloser, error) {
	if zipFS.isClosed() {
		return nil, errors.New("zipFS closed")
	}
	if zipFS.w == nil {
		return nil, errors.New("cannot create file in read-only zipFS")
	}
	name = filepath.ToSlash(filepath.Clean(name))
	zipFS.logger.Debugf("%s", name)
	header := &zip.FileHeader{
		Name:   name,
		Method: zip.Deflate,
	}
	if zipFS.noCompression {
		header.Method = zip.Store
	}
	wc, err := zipFS.w.CreateHeader(header)
	// wc, err := zipFS.w.Create(name)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot create file %s", name)
	}
	zipFS.noCopy = append(zipFS.noCopy, name)
	return &nopCloserWriter{wc}, nil
}

func (zipFS *FS) Delete(name string) error {
	filename := filepath.ToSlash(filepath.Clean(name))
	zipFS.noCopy = append(zipFS.noCopy, filename)
	return nil
}

func (zipFS *FS) HasContent() bool {
	dirEntries, err := zipFS.ReadDir(".")
	if err != nil {
		return false
	}
	return len(dirEntries) > 0
}

func (zipFS *FS) ReadDir(path string) ([]fs.DirEntry, error) {
	if zipFS.isClosed() {
		return nil, errors.New("zipFS closed")
	}

	name := filepath.ToSlash(filepath.Clean(path))
	zipFS.logger.Debugf("%s", name)
	if zipFS.r == nil {
		return []fs.DirEntry{}, nil
	}

	if name == "." {
		name = ""
	}
	// force slash at the end
	if name != "" {
		name = strings.TrimSuffix(filepath.ToSlash(name), "/") + "/"
	}
	var entries = []*DirEntry{}
	var dirs = []string{}
	for _, zipItem := range zipFS.r.File {
		if name != "" && !strings.HasPrefix(zipItem.Name, name) {
			continue
		}
		fname := zipItem.Name
		if name != "" && !strings.HasPrefix(fname, name) {
			continue
		}
		fname = strings.TrimPrefix(fname, name)
		parts := strings.Split(fname, "/")
		// only files have one part
		if len(parts) == 1 {
			var fi *FileInfo
			var err error
			if zipItem.Name == name {
				continue
			}
			if zipItem.FileInfo().IsDir() {
				fi, err = NewFileInfoDir(strings.TrimPrefix(zipItem.Name, name))
				if err != nil {
					return nil, errors.Wrapf(err, "cannot create FileInfo for %s", zipItem.Name)
				}
			} else {
				fi, err = NewFileInfoFile(zipItem)
				if err != nil {
					return nil, errors.Wrapf(err, "cannot create FileInfo for %s", zipItem.Name)
				}
			}
			entries = append(entries, NewDirEntry(fi))
		} else {
			found := false
			for _, d := range dirs {
				if d == parts[0] {
					found = true
				}
			}
			if !found {
				dirs = append(dirs, parts[0])
			}
		}
	}
	for _, d := range dirs {
		fi, err := NewFileInfoDir(d)
		if err != nil {
			return nil, errors.Wrapf(err, "cannot create Fileinfo for %s", d)
		}
		entries = append(entries, NewDirEntry(fi))
	}

	var result = []fs.DirEntry{}
	for _, entry := range entries {
		result = append(result, entry)
	}
	// sort on filename
	sort.Slice(result, func(i, j int) bool {
		return result[i].Name() < result[j].Name()
	})
	return result, nil
}

func (zipFS *FS) WalkDir(path string, fn fs.WalkDirFunc) error {
	if zipFS.isClosed() {
		return errors.New("zipFS closed")
	}
	path = filepath.ToSlash(filepath.Clean(path))
	name := path
	lr := len(name) + 1
	for _, file := range zipFS.r.File {
		if !strings.HasPrefix(file.Name, name) {
			continue
		}
		var fi *FileInfo
		var err error
		if file.FileInfo().IsDir() {
			fi, err = NewFileInfoDir(file.Name[lr:])
			if err != nil {
				return errors.Wrapf(err, "cannot create FileInfo for %s", file.Name)
			}
		} else {
			fi, err = NewFileInfoFile(file)
			if err != nil {
				return errors.Wrapf(err, "cannot create FileInfo for %s", file.Name)
			}
		}
		if err := fn(fmt.Sprintf("%s/%s", path, file.Name[lr:]), NewDirEntry(fi), nil); err != nil {
			return err
		}
	}
	return nil
}

func (zipFS *FS) Stat(path string) (fs.FileInfo, error) {
	if zipFS.r == nil {
		return nil, fs.ErrNotExist
	}
	if zipFS.isClosed() {
		return nil, errors.New("zipFS closed")
	}

	name := filepath.ToSlash(filepath.Clean(path))
	zipFS.logger.Debugf("%s", name)

	// check whether file is newly created
	for _, newItem := range zipFS.noCopy {
		if newItem == name {
			return nil, fs.ErrInvalid // new files cannot be opened
		}
	}
	for _, zipItem := range zipFS.r.File {
		if zipItem.Name == name {
			finfo, err := NewFileInfoFile(zipItem)
			if err != nil {
				return nil, errors.Wrapf(err, "cannot create zipfs.FileInfo for %s", zipItem.Name)
			}
			return finfo, nil
		} else {
			if strings.HasPrefix(zipItem.Name, name) {
				return NewFileInfoDir(name)
			}
		}
	}
	return nil, fs.ErrNotExist
}

func (zipFS *FS) Rename(src, dest string) error {
	_, err := zipFS.Stat(dest)
	if err != nil {
		if !zipFS.IsNotExist(err) {
			return errors.Wrapf(err, "cannot stat '%s'", dest)
		}
	} else {
		if err := zipFS.Delete(dest); err != nil {
			return errors.Wrapf(err, "cannot delete '%s'", dest)
		}
	}
	// now, dest should not exist...

	srcFP, err := zipFS.Open(src)
	if err != nil {
		return errors.Wrapf(err, "cannot open '%s'", src)
	}
	defer srcFP.Close()
	destFP, err := zipFS.Create(dest)
	if err != nil {
		return errors.Wrapf(err, "cannot create '%s'", dest)
	}
	defer destFP.Close()
	if _, err := io.Copy(destFP, srcFP); err != nil {
		return errors.Wrapf(err, "cannot copy '%s' --> '%s'", src, dest)
	}
	return nil
}

func (zipFS *FS) SubFSRW(path string) (ocfl.OCFLFS, error) {
	return NewSubFS(zipFS, path)
}
func (zipFS *FS) SubFS(path string) (ocfl.OCFLFSRead, error) {
	return zipFS.SubFSRW(path)
}

// check interface satisfaction
var (
	_ ocfl.OCFLFS = &FS{}
)
