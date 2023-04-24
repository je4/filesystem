package zipfsrw

import (
	"github.com/je4/filesystem/v2/pkg/osfsrw"
	"github.com/je4/filesystem/v2/pkg/writefs"
	"os"
	"path/filepath"
	"testing"
)

var testTmpFile = "file://" + filepath.ToSlash(filepath.Join(os.TempDir(), tempFileName("zipfsrwfactorytest_", ".zip")))
var factory *writefs.Factory

func TestZipFSRWFactory(t *testing.T) {
	var err error
	factory, err = writefs.NewFactory()
	if err != nil {
		t.Fatal(err)
	}
	if err := factory.Register(osfsrw.CreateFS, "^file://", writefs.MediumFS); err != nil {
		t.Fatal(err)
	}
	if err := factory.Register(CreateFS, "\\.zip$", writefs.HighFS); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
	})

	t.Run("create", testZipFSRWFactory_create)
}

func testZipFSRWFactory_create(t *testing.T) {
	fs, err := factory.Get(testTmpFile)
	if err != nil {
		t.Fatal(err)
	}
	if fs == nil {
		t.Fatal("fs is nil")
	}
}
