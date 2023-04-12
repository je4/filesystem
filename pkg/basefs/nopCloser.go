package basefs

import (
	"errors"
	"io"
)

// NopReadCloser returns a ReadCloser with a no-op Close method wrapping
func NewNopReadCloser(r io.Reader) io.ReadCloser {
	return &nopReadCloser{r}
}

type nopReadCloser struct {
	io.Reader
}

func (*nopReadCloser) Close() error {
	return nil
}

func (nrc *nopReadCloser) ForceClose() error {
	rc, ok := nrc.Reader.(io.ReadCloser)
	if !ok {
		return errors.New("reader is not a ReadCloser")
	}
	return rc.Close()
}

func NewNopWriteCloser(w io.Writer) io.WriteCloser {
	return &nopWriteCloser{w}
}

type nopWriteCloser struct {
	io.Writer
}

func (*nopWriteCloser) Close() error {
	return nil
}

func (nwc *nopWriteCloser) ForceClose() error {
	wc, ok := nwc.Writer.(io.WriteCloser)
	if !ok {
		return errors.New("writer is not a WriteCloser")
	}
	return wc.Close()
}

var (
	_ io.ReadCloser  = (*nopReadCloser)(nil)
	_ io.WriteCloser = (*nopWriteCloser)(nil)
)
