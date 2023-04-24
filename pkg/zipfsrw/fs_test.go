package zipfsrw

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"github.com/je4/filesystem/v2/pkg/osfsrw"
	"github.com/je4/filesystem/v2/pkg/writefs"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func tempFileName(prefix, suffix string) string {
	randBytes := make([]byte, 16)
	rand.Read(randBytes)
	return prefix + hex.EncodeToString(randBytes) + suffix
}

var baseFS fs.FS       // base file system
var zipFileName string // name of the zip file

func TestMain(m *testing.M) {
	var err error
	baseFS, err = osfsrw.NewFS(os.TempDir())
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	zipFileName = tempFileName("zipfsrwtest_", ".zip")
	os.Exit(m.Run())
}

func TestZipFSRW(t *testing.T) {
	t.Cleanup(func() {
		if err := writefs.Remove(baseFS, zipFileName); err != nil {
			t.Error(err)
		}
	})

	t.Run("create", testZipFSRW_Create)
	t.Run("read", testZipFSRW_Read)
	t.Run("update", testZipFSRW_Update)
	t.Run("readUpdated", testZipFSRW_ReadUpdate)
}

func testZipFSRW_Create(t *testing.T) {
	// create a new zip file

	// create a new zip file system
	zipFS, err := NewFSFile(baseFS, zipFileName)
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
	if err := writefs.Close(zipFS); err != nil {
		t.Fatal(err)
	}
	fmt.Println("zip file", filepath.Join(os.TempDir(), zipFileName), "created")
}

func testZipFSRW_Read(t *testing.T) {
	// open the zip file system again
	zipFS, err := NewFSFile(baseFS, zipFileName)
	if err != nil {
		t.Fatal(err)
	}
	// read the written files
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
	if err := writefs.Close(zipFS); err != nil {
		t.Fatal(err)
	}
}

func testZipFSRW_Update(t *testing.T) {
	// create a new zip file

	// create a new zip file system
	zipFS, err := NewFSFile(baseFS, zipFileName)
	if err != nil {
		t.Fatal(err)
	}
	// add some files
	for _, letter := range []rune("vwxyz") {
		fname := filepath.ToSlash(filepath.Join(string(letter), "content.txt"))
		fmt.Println("   create file", fname)
		fp, err := zipFS.Create(fname)
		if err != nil {
			t.Fatalf("cannot create file '%s': %v", fname, err)
		}
		tlstr := strings.ToUpper(string(letter))
		tl := []byte(tlstr)
		for i := 0; i < 1000; i++ {
			_, err = fp.Write(tl)
			if err != nil {
				t.Fatalf("cannot write to file '%s': %v", fname, err)
			}
		}
		fp.Close()
	}
	// close the zip file system
	if err := writefs.Close(zipFS); err != nil {
		t.Fatal(err)
	}
	fmt.Println("zip file", filepath.Join(os.TempDir(), zipFileName), "updated")
}

func testZipFSRW_ReadUpdate(t *testing.T) {
	// open the zip file system again
	zipFS, err := NewFSFile(baseFS, zipFileName)
	if err != nil {
		t.Fatal(err)
	}
	// read the written files
	t.Run("read", func(t *testing.T) {
		for _, letter := range []rune("vwxyz") {
			t.Run(string(letter), func(t *testing.T) {
				var l = letter
				t.Parallel()
				fname := filepath.ToSlash(filepath.Join(string(l), "content.txt"))
				t.Logf("   check file %s", fname)
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
				tlstr := strings.ToUpper(string(l))
				tl := []byte(tlstr)[0]
				for i := 0; i < 1000; i++ {
					if buf[i] != tl {
						t.Fatalf("invalid content in '%s': %v", fname, err)
					}
					if i%100 == 0 {
						time.Sleep(100 * time.Millisecond)
						// t.Logf("      %v bytes checked", i)
					}
				}
				if err := fp.Close(); err != nil {
					t.Error(err)
				}
				t.Logf("   check file %s done", fname)
			})
		}
	})
	// close the zip file system
	if err := writefs.Close(zipFS); err != nil {
		t.Fatal(err)
	}
}
