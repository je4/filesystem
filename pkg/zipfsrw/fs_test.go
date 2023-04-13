package zipfsrw

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"github.com/je4/filesystem/v2/pkg/osfsrw"
	"os"
	"path/filepath"
	"testing"
)

func tempFileName(prefix, suffix string) string {
	randBytes := make([]byte, 16)
	rand.Read(randBytes)
	return prefix + hex.EncodeToString(randBytes) + suffix
}

func TestWriteUpdate(t *testing.T) {
	// create a new zip file

	baseFS := osfsrw.NewOSFSRW(os.TempDir())
	zipFileName := tempFileName("zipfsrwtest_", ".zip")
	// create a new zip file system
	zipFS, err := NewZipFSRW(baseFS, zipFileName)
	if err != nil {
		t.Fatal(err)
	}
	// add some files
	for _, letter := range []rune("abcdefghijklmnopqrstuvw") {
		fname := filepath.ToSlash(filepath.Join(string(letter), "content.txt"))
		fmt.Println("   create file", fname)
		fp, err := zipFS.Create(fname)
		if err != nil {
			t.Fatalf("cannot create file '%s': %v", fname, err)
		}
		for i := 0; i < 1000; i++ {
			_, err = fp.Write([]byte(string(letter)))
			if err != nil {
				t.Fatalf("cannot write to file '%s': %v", fname, err)
			}
		}
		fp.Close()
	}
	// close the zip file system
	if err := zipFS.Close(); err != nil {
		t.Fatal(err)
	}
	fmt.Println("zip file", filepath.Join(os.TempDir(), zipFileName), "created")

	// open the zip file system again
	zipFS, err = NewZipFSRW(baseFS, zipFileName)
	if err != nil {
		t.Fatal(err)
	}
	// add some files
	for _, letter := range []rune("abcdefghijklmnopqrstuvw") {
		fname := filepath.ToSlash(filepath.Join(string(letter), "content.txt"))
		fmt.Println("   check file", fname)
		fp, err := zipFS.Open(fname)
		if err != nil {
			t.Fatalf("cannot open file '%s': %v", fname, err)
		}
		buf := make([]byte, 1000)
		n, err := fp.Read(buf)
		if err != nil && err.Error() != "EOF" {
			t.Fatalf("cannot read from file '%s': %v", fname, err)
		}
		if n != 1000 {
			t.Fatalf("cannot read from file '%s': %v", fname, err)
		}
		for i := 0; i < 1000; i++ {
			if buf[i] != byte(letter) {
				t.Fatalf("invalid content in '%s': %v", fname, err)
			}
		}
		fp.Close()
	}
	// close the zip file system
	if err := zipFS.Close(); err != nil {
		t.Fatal(err)
	}
}
