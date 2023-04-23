package writefs

import (
	"emperror.dev/errors"
	"github.com/je4/utils/v2/pkg/checksum"
	"io"
)

func NewSidecarChecksumWriter(w io.Writer, sidecars map[checksum.DigestAlgorithm]io.WriteCloser) (io.WriteCloser, error) {
	digests := []checksum.DigestAlgorithm{}
	for da, _ := range sidecars {
		digests = append(digests, da)
	}
	csw, err := checksum.NewChecksumWriter(digests, w)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	sw := &sidecarChecksumWriter{
		ChecksumWriter: csw,
		sidecars:       make(map[checksum.DigestAlgorithm]io.WriteCloser),
	}
	return sw, nil
}

type sidecarChecksumWriter struct {
	*checksum.ChecksumWriter
	sidecars map[checksum.DigestAlgorithm]io.WriteCloser
}

func (s *sidecarChecksumWriter) Close() error {
	digests, err := s.ChecksumWriter.GetChecksums()
	if err != nil {
		return errors.Wrap(err, "cannot get checksums")
	}
	var errs = []error{}
	for da, d := range digests {
		if sidecar, ok := s.sidecars[da]; ok {
			_, err := sidecar.Write([]byte(d))
			if err != nil {
				return errors.Wrapf(err, "cannot write to sidecar %v", da)
			}
			if err := sidecar.Close(); err != nil {
				errs = append(errs, errors.Wrapf(err, "cannot close sidecar %v", da))
			}
		}
	}
	return errors.Combine(errs...)
}

var (
	_ io.WriteCloser = (*sidecarChecksumWriter)(nil)
)
