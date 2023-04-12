package writefs

import "io"

type FileWrite interface {
	io.WriteCloser
}

type FileWriterAt interface {
	FileWrite
	io.WriterAt
}

type FileWriteSeeker interface {
	FileWrite
	io.Seeker
}
