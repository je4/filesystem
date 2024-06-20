package zipfsrw

import (
	"emperror.dev/errors"
	"fmt"
	"github.com/je4/filesystem/v3/pkg/writefs"
	"github.com/je4/utils/v2/pkg/checksum"
	"github.com/je4/utils/v2/pkg/zLogger"
	"io"
	"io/fs"
	"strings"
)

// NewZipFSRW creates a new ReadWriteFS
// If the file does not exist, it will be created on the first write operation.
// If the file exists, it will be opened and read.
// Changes will be written to an additional file and then renamed to the original file.
func NewFSFileChecksums(baseFS fs.FS, path string, noCompression bool, algs []checksum.DigestAlgorithm, logger zLogger.ZLogger, writers ...io.Writer) (*fsFileChecksums, error) {
	newpath := path

	csWriter, err := checksum.NewChecksumWriter(algs)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot create checksum writer for '%s'", newpath)
	}

	mainFS, err := NewFSFile(baseFS, newpath, noCompression, logger, append(writers, csWriter)...)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot create zip file FS '%s'", newpath)
	}

	return &fsFileChecksums{
		fsFile:   mainFS,
		csWriter: csWriter,
		csAlgs:   algs,
	}, nil
}

type fsFileChecksums struct {
	*fsFile
	csAlgs   []checksum.DigestAlgorithm
	csWriter *checksum.ChecksumWriter
	zipFS    io.Closer
}

func (zfsrw *fsFileChecksums) String() string {
	return fmt.Sprintf("fsFileChecksums(%v/%s)", zfsrw.baseFS, zfsrw.path)
}

func (zfsrw *fsFileChecksums) Close() error {
	var errs = []error{}

	if err := zfsrw.fsFile.Close(); err != nil {
		errs = append(errs, err)
	}
	if err := zfsrw.csWriter.Close(); err != nil {
		errs = append(errs, err)
	}
	if zfsrw.zipFS != nil {
		if err := zfsrw.zipFS.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if zfsrw.HasChanged() {
		checksums, err := zfsrw.csWriter.GetChecksums()
		if err != nil {
			errs = append(errs, err)
		}
		if len(errs) == 0 {
			for alg, cs := range checksums {
				sideCar := fmt.Sprintf("%s.%s", zfsrw.path, strings.ToLower(string(alg)))
				if _, err := writefs.WriteFile(zfsrw.baseFS, sideCar, []byte(fmt.Sprintf("%s *%s", cs, zfsrw.path))); err != nil {
					errs = append(errs, errors.Wrapf(err, "cannot write sidecar file '%s'", sideCar))
				}
			}
		}
	}

	return errors.WithStack(errors.Combine(errs...))
}
