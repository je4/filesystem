package zipfsrw

import (
	"bufio"
	"emperror.dev/errors"
	"fmt"
	"github.com/je4/filesystem/v2/pkg/writefs"
	"github.com/je4/filesystem/v2/pkg/zipfs"
	"github.com/je4/utils/v2/pkg/checksum"
	"io/fs"
	"strings"
)

// NewZipFSRW creates a new ReadWriteFS
// If the file does not exist, it will be created on the first write operation.
// If the file exists, it will be opened and read.
// Changes will be written to an additional file and then renamed to the original file.
func NewFSFileChecksums(baseFS fs.FS, path string, noCompression bool, algs []checksum.DigestAlgorithm) (writefs.ReadWriteFS, error) {
	newpath := path

	var zfs zipfs.OpenRawZipFS

	if xfs, err := zipfs.NewFSFile(baseFS, path); err != nil {
		if errors.Cause(err) != fs.ErrNotExist {
			return nil, errors.Wrapf(err, "cannot open zip file '%s'", path)
		}
	} else {
		zfs = xfs
		newpath = path + ".tmp"
	}

	// create new file
	fp, err := writefs.Create(baseFS, newpath)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot create zip file '%s'", newpath)
	}

	// add a buffer to the file
	newFPBuffer := bufio.NewWriterSize(fp, 1024*1024)

	csWriter, err := checksum.NewChecksumWriter(algs, newFPBuffer)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot create checksum writer for '%s'", newpath)
	}

	zipFSRWBase, err := NewFS(newFPBuffer, zfs, noCompression)
	if err != nil {
		return nil, errors.Wrap(err, "cannot create zipFSRW")
	}

	return &fsFileChecksums{
		fsFile: &fsFile{
			zipFSRW:     zipFSRWBase,
			name:        path,
			tmpName:     newpath,
			baseFS:      baseFS,
			zipFP:       fp,
			zipFPBuffer: newFPBuffer,
		},
		csWriter: csWriter,
		csAlgs:   algs,
	}, nil
}

type fsFileChecksums struct {
	*fsFile
	csAlgs   []checksum.DigestAlgorithm
	csWriter *checksum.ChecksumWriter
}

func (zfsrw *fsFileChecksums) String() string {
	return fmt.Sprintf("fsFileChecksums(%v/%s)", zfsrw.baseFS, zfsrw.name)
}

func (zfsrw *fsFileChecksums) Close() error {
	var errs = []error{}

	if err := zfsrw.fsFile.Close(); err != nil {
		errs = append(errs, err)
	}
	if err := zfsrw.csWriter.Close(); err != nil {
		errs = append(errs, err)
	}
	if zfsrw.HasChanged() {
		checksums, err := zfsrw.csWriter.GetChecksums()
		if err != nil {
			errs = append(errs, err)
		}
		if len(errs) == 0 {
			for alg, cs := range checksums {
				sideCar := fmt.Sprintf("%s.%s", zfsrw.name, strings.ToLower(string(alg)))
				wfp, err := writefs.Create(zfsrw.baseFS, sideCar)
				if err != nil {
					errs = append(errs, errors.Wrapf(err, "cannot create sidecar file '%s'", sideCar))
				}
				if _, err := wfp.Write([]byte(fmt.Sprintf("%s *%s", cs, zfsrw.name))); err != nil {
					errs = append(errs, errors.Wrapf(err, "cannot write to sidecar file '%s'", sideCar))
				}
				if err := wfp.Close(); err != nil {
					errs = append(errs, errors.Wrapf(err, "cannot close sidecar file '%s'", sideCar))
				}
			}
		}
	}

	return errors.WithStack(errors.Combine(errs...))
}
