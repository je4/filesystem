package zipfsrw

import (
	"bufio"
	"bytes"
	"emperror.dev/errors"
	"encoding/json"
	"fmt"
	"github.com/google/tink/go/core/registry"
	"github.com/google/tink/go/keyset"
	"github.com/je4/filesystem/v2/pkg/writefs"
	"github.com/je4/utils/v2/pkg/checksum"
	"github.com/je4/utils/v2/pkg/encrypt"
	"io"
	"io/fs"
	"strings"
)

// NewFSFileEncryptedChecksums creates a new ReadWriteFS
// If the file does not exist, it will be created on the first write operation.
// If the file exists, it will be opened and read.
// Changes will be written to an additional file and then renamed to the original file.
func NewFSFileEncryptedChecksums(baseFS fs.FS, path string, noCompression bool, algs []checksum.DigestAlgorithm, keyUri string) (writefs.ReadWriteFS, error) {
	// create encrypted file
	encFP, err := writefs.Create(baseFS, path+".aes")
	if err != nil {
		return nil, errors.Wrapf(err, "cannot create zip file '%s'", path)
	}

	// add a buffer to the file
	newEncFPBuffer := bufio.NewWriterSize(encFP, 1024*1024)

	csEncWriter, err := checksum.NewChecksumWriter(algs, newEncFPBuffer)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot create checksum writer for '%s'", path+".aes")
	}

	encWriter, err := encrypt.NewEncryptWriterAESGCM(csEncWriter, []byte(path), nil)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot create encrypt writer for '%s'", path+".aes")
	}

	handle := encWriter.GetKeysetHandle()

	zipFS, err := NewFSFileChecksums(baseFS, path, noCompression, algs, encWriter)
	if err != nil {
		return nil, errors.Wrap(err, "cannot create zipFS")
	}

	return &fsFileEncryptedChecksums{
		fsFileChecksums: zipFS,
		aad:             []byte(path),
		handle:          handle,
		encWriter:       encWriter,
		keyURI:          keyUri,
		csEncWriter:     csEncWriter,
		csEncBuffer:     newEncFPBuffer,
	}, nil
}

type fsFileEncryptedChecksums struct {
	*fsFileChecksums
	aad         []byte
	handle      *keyset.Handle
	encWriter   io.Closer
	keyURI      string
	csEncWriter *checksum.ChecksumWriter
	csEncBuffer *bufio.Writer
}

func (zfsrw *fsFileEncryptedChecksums) String() string {
	return fmt.Sprintf("fsFileEncryptedChecksums(%v/%s)", zfsrw.baseFS, zfsrw.path)
}

func (zfsrw *fsFileEncryptedChecksums) Close() error {
	var errs = []error{}

	if err := zfsrw.fsFileChecksums.Close(); err != nil {
		errs = append(errs, err)
	}
	if err := zfsrw.csEncBuffer.Flush(); err != nil {
		errs = append(errs, err)
	}
	if err := zfsrw.encWriter.Close(); err != nil {
		errs = append(errs, err)
	}
	if err := zfsrw.csEncWriter.Close(); err != nil {
		errs = append(errs, err)
	}

	if len(errs) == 0 {
		client, err := registry.GetKMSClient(zfsrw.keyURI)
		if err != nil {
			errs = append(errs, errors.Wrapf(err, "cannot get KMS client for '%s'", zfsrw.keyURI))
		}
		aead, err := client.GetAEAD(zfsrw.keyURI)
		if err != nil {
			errs = append(errs, errors.Wrapf(err, "cannot get AEAD for entry '%s'", zfsrw.keyURI))
		}

		keyFileName := zfsrw.path + ".aes.key.json"
		keyBuf := bytes.NewBuffer(nil)
		wr := keyset.NewBinaryWriter(keyBuf)

		if err := zfsrw.handle.Write(wr, aead); err != nil {
			errs = append(errs, errors.Wrapf(err, "cannot write %s", keyFileName))
		}
		ts := encrypt.KeyStruct{
			EncryptedKey: keyBuf.Bytes(),
			Aad:          zfsrw.aad,
		}
		jsonBytes, err := json.Marshal(ts)
		if err != nil {
			errs = append(errs, errors.Wrapf(err, "cannot marshal %s", keyFileName))
		} else {
			if err := writefs.WriteFile(zfsrw.baseFS, keyFileName, jsonBytes); err != nil {
				errs = append(errs, errors.Wrapf(err, "cannot write %s", keyFileName))
			}
		}

		checksums, err := zfsrw.csEncWriter.GetChecksums()
		if err != nil {
			errs = append(errs, err)
		}
		if len(errs) == 0 {
			for alg, cs := range checksums {
				sideCar := fmt.Sprintf("%s.aes.%s", zfsrw.path, strings.ToLower(string(alg)))
				wfp, err := writefs.Create(zfsrw.baseFS, sideCar)
				if err != nil {
					errs = append(errs, errors.Wrapf(err, "cannot create sidecar file '%s'", sideCar))
				}
				if _, err := wfp.Write([]byte(fmt.Sprintf("%s *%s.aes", cs, zfsrw.path))); err != nil {
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
