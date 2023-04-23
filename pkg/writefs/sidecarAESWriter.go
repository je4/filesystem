package writefs

import (
	"io"
)

func NewSidecarAESWriter(w io.Writer, aesWriter io.WriteCloser) (io.WriteCloser, error) {
	return nil, nil
}

type sidecarAESWriter struct {
	io.WriteCloser
}

func (s *sidecarAESWriter) Close() error {
	return s.WriteCloser.Close()
}
