package zipfsrw

import (
	"crypto/rand"
	"crypto/sha512"
	"encoding/hex"
	"fmt"
	"github.com/je4/filesystem/v3/pkg/osfsrw"
	"github.com/je4/filesystem/v3/pkg/writefs"
	"github.com/je4/filesystem/v3/pkg/zipfs"
	"github.com/je4/utils/v2/pkg/checksum"
	"github.com/rs/zerolog"
	"io"
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
var zipFileName string // path of the zip file

func TestMain(m *testing.M) {
	var err error
	_logger := zerolog.New(os.Stderr).With().Timestamp().Logger()
	logger := &_logger
	baseFS, err = osfsrw.NewFS(os.TempDir(), logger)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	zipFileName = tempFileName("zipfsrwtest_", ".zip")
	os.Exit(m.Run())
}

func TestZipFSRW(t *testing.T) {
	t.Cleanup(func() {
		if err := writefs.Remove(baseFS, zipFileName+".sha512"); err != nil {
			t.Error(err)
		}
		if err := writefs.Remove(baseFS, zipFileName); err != nil {
			t.Error(err)
		}
	})

	t.Run("create", testZipFSRW_Create)
	t.Run("read", testZipFSRW_Read)
	t.Run("update", testZipFSRW_Update)
	t.Run("checksum", testZipFSRW_Checksum)
}

func testZipFSRW_Checksum(t *testing.T) {
	data, err := fs.ReadFile(baseFS, zipFileName+".sha512")
	if err != nil {
		t.Errorf("cannot read %s/%s: %v", baseFS, zipFileName+".sha512", err)
		return
	}
	parts := strings.SplitN(string(data), " ", 2)
	if len(parts) < 1 {
		t.Errorf("cannot parse %s/%s: %v", baseFS, zipFileName+".sha512", err)
		return
	}
	xsha512 := strings.ToLower(parts[0])
	fp, err := baseFS.Open(zipFileName)
	if err != nil {
		t.Errorf("cannot open %s/%s: %v", baseFS, zipFileName, err)
		return
	}
	defer fp.Close()
	hash := sha512.New()
	if _, err := io.Copy(hash, fp); err != nil {
		t.Errorf("cannot read %s/%s: %v", baseFS, zipFileName, err)
		return
	}
	newsha512 := strings.ToLower(hex.EncodeToString(hash.Sum(nil)))

	if xsha512 != newsha512 {
		t.Errorf("checksum mismatch: %s != %s", xsha512, newsha512)
		return
	}
}

func testZipFSRW_Create(t *testing.T) {
	// create a new zip file

	// create a new zip file system
	zipFS, err := NewFSFileChecksums(baseFS, zipFileName, false, []checksum.DigestAlgorithm{checksum.DigestSHA512})
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
	_logger := zerolog.New(os.Stderr).With().Timestamp().Logger()
	logger := &_logger
	zipFS, err := zipfs.NewFSFile(baseFS, zipFileName, logger)
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
	zipFS, err := NewFSFileChecksums(baseFS, zipFileName, false, []checksum.DigestAlgorithm{checksum.DigestSHA512})
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
	zipFS, err := NewFSFile(baseFS, zipFileName, false)
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
