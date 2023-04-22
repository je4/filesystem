package s3fsrw

import (
	"context"
	"fmt"
	"github.com/je4/filesystem/v2/pkg/writefs"
	"github.com/minio/madmin-go/v2"
	mclient "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	minio "github.com/minio/minio/cmd"
	"github.com/pkg/errors"
	"io/fs"
	"net"
	"os"
	"testing"
	"time"
)

func StartEmbedded() (string, func() error, error) {
	l, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		return "", nil, errors.Wrap(err, "while creating listener")
	}

	addr := l.Addr().String()
	err = l.Close()
	if err != nil {
		return "", nil, errors.Wrap(err, "while closing listener")
	}

	accessKeyID := "minioadmin"
	secretAccessKey := "minioadmin"

	madm, err := madmin.New(addr, accessKeyID, secretAccessKey, false)
	if err != nil {
		return "", nil, errors.Wrap(err, "while creating madimin")
	}

	td, err := os.MkdirTemp("", "")
	if err != nil {
		return "", nil, errors.Wrap(err, "while creating temp dir")
	}

	go minio.Main([]string{"minio", "server", "--quiet", "--address", addr, td})
	time.Sleep(2 * time.Second)

	mc, err := mclient.New(addr, &mclient.Options{
		Creds:  credentials.NewStaticV4(accessKeyID, secretAccessKey, ""),
		Secure: false,
	})

	err = mc.MakeBucket(context.Background(), "test", mclient.MakeBucketOptions{})
	if err != nil {
		return "", nil, errors.Wrap(err, "while creating bucket")
	}

	return addr, func() error {
		err := madm.ServiceStop(context.Background())
		if err != nil {
			return errors.Wrap(err, "while stopping service")
		}

		err = os.RemoveAll(td)
		if err != nil {
			return errors.Wrap(err, "while deleting temp dir")
		}

		return nil
	}, nil

}

func TestMain(m *testing.M) {
	addr, stop, err := StartEmbedded()
	if err != nil {
		panic(err)
	}

	os.Setenv("MINIO_URL", addr)
	os.Setenv("MINIO_ACCESS_KEY", "minioadmin")
	os.Setenv("MINIO_SECRET_KEY", "minioadmin")

	code := m.Run()

	err = stop()
	if err != nil {
		panic(err)
	}

	os.Exit(code)
}

func TestS3FS(t *testing.T) {
	minioAccessKey := os.Getenv("MINIO_ACCESS_KEY")
	minioSecretKey := os.Getenv("MINIO_SECRET_KEY")
	minioURL := os.Getenv("MINIO_URL")

	/*
		cred, err := os.ReadFile("./pkg/s3fsrw/credentials.json")
		if err != nil {
			t.Fatal(err)
		}
		var credentials = map[string]string{}
		if err := json.Unmarshal(cred, &credentials); err != nil {
			t.Fatal(err)
		}
	*/
	s3fs, err := NewS3FS(
		minioURL,
		// ts.URL[7:],
		minioAccessKey,
		minioSecretKey,
		"test",
		"",
		false,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Run("create & read", func(t *testing.T) {
		for i := 0; i < 10; i++ {
			testx := fmt.Sprintf("test%d", i)
			fp, err := writefs.Create(s3fs, "test/"+testx+".txt")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := fp.Write([]byte(testx)); err != nil {
				t.Fatal(err)
			}
			if err := fp.Close(); err != nil {
				t.Fatal(err)
			}
		}
		for i := 0; i < 10; i++ {
			testx := fmt.Sprintf("test%d", i)
			data, err := fs.ReadFile(s3fs, "test/"+testx+".txt")
			if err != nil {
				t.Fatal(err)
			}
			if string(data) != testx {
				t.Fatal("wrong data")
			}
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
