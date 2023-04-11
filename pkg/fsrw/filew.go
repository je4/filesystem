package fsrw

import "io"

type FileW interface {
	io.WriteCloser
}

type FileWriterAt interface {
	FileW
	io.WriterAt
}

type FileWSeeker interface {
	FileW
	io.Seeker
}
