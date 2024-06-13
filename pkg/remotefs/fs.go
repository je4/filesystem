package remotefs

import (
	"crypto/tls"
	"emperror.dev/errors"
	"encoding/json"
	"fmt"
	"github.com/je4/filesystem/v3/pkg/writefs"
	"github.com/je4/utils/v2/pkg/zLogger"
	"io"
	"io/fs"
	"net/http"
	"path/filepath"
)

func NewFS(tlsConfig *tls.Config, addr string, dir, vfs string, logger zLogger.ZLogger) (*remoteFSRW, error) {
	_logger := logger.With().Str("class", "remoteFSRW").Logger()
	logger = &_logger

	return &remoteFSRW{
		client: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: tlsConfig,
			},
		},
		addr:   addr,
		dir:    dir,
		vfs:    vfs,
		logger: logger,
	}, nil
}

type remoteFSRW struct {
	logger zLogger.ZLogger
	client *http.Client
	addr   string
	vfs    string
	dir    string
}

func (d *remoteFSRW) Fullpath(name string) (string, error) {
	return fmt.Sprintf("vfs://%s/%s", d.vfs, filepath.ToSlash(filepath.Join(d.dir, name))), nil
}

func (d *remoteFSRW) String() string {
	return "remoteFSRW(" + d.vfs + ")"
}

func (d *remoteFSRW) Sub(dir string) (fs.FS, error) {
	return &remoteFSRW{
		client: d.client,
		addr:   d.addr,
		dir:    filepath.Join(d.dir, dir),
		vfs:    d.vfs,
		logger: d.logger,
	}, nil
}

func (d *remoteFSRW) Remove(path string) error {
	req, err := http.NewRequest(http.MethodDelete, fmt.Sprintf("%s/%s/%s", d.addr, d.vfs, path), nil)
	if err != nil {
		return errors.Wrapf(err, "cannot create delete request for '%s/%s/%s'", d.addr, d.vfs, path)
	}
	resp, err := d.client.Do(req)
	if err != nil {
		return errors.Wrapf(err, "cannot delete '%s/%s/%s'", d.addr, d.vfs, path)
	}
	if resp.StatusCode != http.StatusOK {
		return errors.Errorf("cannot delete '%s/%s/%s': %d", d.addr, d.vfs, path, resp.StatusCode)
	}
	return nil
}

func (d *remoteFSRW) Rename(oldPath, newPath string) error {
	return errors.Errorf("rename not supported for remoteFSRW")
}

func (d *remoteFSRW) Open(name string) (fs.File, error) {
	url := fmt.Sprintf("%s/%s/%s?stat", d.addr, d.vfs, name)
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot create stat request for '%s'", url)
	}
	resp, err := d.client.Do(req)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot stat '%s'", url)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, errors.Errorf("cannot stat '%s': %d", url, resp.StatusCode)
	}
	return &file{
		d:    d,
		name: name,
		rc:   resp.Body,
	}, nil
}

func (d *remoteFSRW) Stat(name string) (fs.FileInfo, error) {
	url := fmt.Sprintf("%s/%s/%s?stat", d.addr, d.vfs, name)
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot create stat request for '%s'", url)
	}
	resp, err := d.client.Do(req)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot stat '%s'", url)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, errors.Errorf("cannot stat '%s': %d", url, resp.StatusCode)
	}
	fi := &fileInfo{}
	if err := json.NewDecoder(resp.Body).Decode(fi); err != nil {
		return nil, errors.Wrapf(err, "cannot decode stat '%s'", url)
	}
	return fi, nil
}

func (d *remoteFSRW) Create(path string) (writefs.FileWrite, error) {
	url := fmt.Sprintf("%s/%s/%s", d.addr, d.vfs, path)
	pr, pw := io.Pipe()
	req, err := http.NewRequest(http.MethodPut, url, pr)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot create create request for '%s'", url)
	}
	done := make(chan error)
	go func() {
		resp, err := d.client.Do(req)
		if err != nil {
			done <- errors.Wrapf(err, "cannot create '%s'", url)
		}
		if resp.StatusCode != http.StatusOK {
			done <- errors.Errorf("cannot create '%s': %d", url, resp.StatusCode)
		}
		done <- nil
	}()
	result := &fileWrite{
		d:    d,
		name: path,
		wc:   pw,
		done: done,
	}
	return result, nil
}

func (d *remoteFSRW) ReadFile(name string) ([]byte, error) {
	fp, err := d.Open(name)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot open '%s'", name)
	}
	defer fp.Close()
	return io.ReadAll(fp)
}

var (
	_ writefs.CreateFS    = &remoteFSRW{}
	_ writefs.ReadWriteFS = &remoteFSRW{}
	//_ writefs.MkDirFS     = &remoteFSRW{}
	_ writefs.RenameFS   = &remoteFSRW{}
	_ writefs.RemoveFS   = &remoteFSRW{}
	_ writefs.FullpathFS = &remoteFSRW{}
	//_ fs.ReadDirFS        = &remoteFSRW{}
	_ fs.ReadFileFS = &remoteFSRW{}
	_ fs.StatFS     = &remoteFSRW{}
	_ fs.SubFS      = &remoteFSRW{}
)
