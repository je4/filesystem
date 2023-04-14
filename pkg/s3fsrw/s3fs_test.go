package s3fsrw

import (
	"encoding/json"
	"github.com/je4/filesystem/v2/pkg/writefs"
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
	"io/fs"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

func TestS3FS(t *testing.T) {
	backend := s3mem.New()
	faker := gofakes3.New(backend)
	ts := httptest.NewServer(faker.Server())
	t.Cleanup(func() {
		ts.Close()
	})
	cred, err := os.ReadFile("./pkg/s3fsrw/credentials.json")
	if err != nil {
		t.Fatal(err)
	}
	var credentials = map[string]string{}
	if err := json.Unmarshal(cred, &credentials); err != nil {
		t.Fatal(err)
	}
	s3fs, err := NewS3FS(
		strings.TrimPrefix(credentials["url"], "https://"),
		// ts.URL[7:],
		credentials["accessKey"],
		credentials["secretKey"],
		"fstest",
		"us-east-1",
		true,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Run("create & read", func(t *testing.T) {
		fp, err := writefs.Create(s3fs, "fstest/test.txt")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := fp.Write([]byte("test")); err != nil {
			t.Fatal(err)
		}
		if err := fp.Close(); err != nil {
			t.Fatal(err)
		}
		data, err := fs.ReadFile(s3fs, "fstest/test.txt")
		if err != nil {
			t.Fatal(err)
		}
		if string(data) != "test" {
			t.Fatal("wrong data")
		}
	})
	t.Run("walkdir", func(t *testing.T) {
		fs.WalkDir(s3fs, "", func(path string, entry fs.DirEntry, err error) error {
			if entry == nil {
				return nil
			}
			if !entry.IsDir() {
				fi, err := entry.Info()
				if err != nil {
					t.Fatal(err)
				}
				t.Logf("     [f] %s [%v]", path, fi.Size())
			} else {
				t.Logf("     [d] %s", path)
			}
			return nil
		})
	})
}
