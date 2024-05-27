package s3fsrw

import (
	"bytes"
	"context"
	"crypto/tls"
	"emperror.dev/errors"
	"fmt"
	"github.com/je4/filesystem/v3/pkg/writefs"
	"github.com/je4/utils/v2/pkg/zLogger"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"golang.org/x/exp/slices"
	"io"
	"io/fs"
	"net/http"
)

func NewFS(endpoint, accessKeyID, secretAccessKey, region string, useSSL, debug bool, tlsConfig *tls.Config, logger zLogger.ZWrapper) (*s3FSRW, error) {
	var err error
	fs := &s3FSRW{
		client: nil,
		//		bucket:   bucket,
		region:   region,
		endpoint: endpoint,
		logger:   logger,
	}

	var tr http.RoundTripper = &http.Transport{TLSClientConfig: tlsConfig}
	if debug {
		tr = NewDebuggingRoundTripper(
			&http.Transport{
				TLSClientConfig: tlsConfig,
			},
			logger,
			JustURL,
			URLTiming,
			// CurlCommand,
			RequestHeaders,
			ResponseStatus,
			// ResponseHeaders,
		)
	}
	fs.client, err = minio.New(endpoint, &minio.Options{
		Creds:     credentials.NewStaticV4(accessKeyID, secretAccessKey, ""),
		Secure:    useSSL,
		Region:    region,
		Transport: tr,
	})
	if err != nil {
		return nil, errors.Wrap(err, "cannot create s3 client instance")
	}
	return fs, nil
}

type s3FSRW struct {
	client   *minio.Client
	region   string
	endpoint string
	logger   zLogger.ZWrapper
}

// MkDir does nothing
func (s3FS *s3FSRW) MkDir(path string) error {
	bucket, bucketPath := extractBucket(path)
	if bucketPath != "" {
		return errors.Wrapf(fs.ErrInvalid, "cannot create bucket with subfolders '%s'", path)
	}
	return errors.Wrapf(s3FS.client.MakeBucket(context.Background(), bucket, minio.MakeBucketOptions{Region: s3FS.region}), "cannot create bucket '%s'", bucket)
}

func (s3FS *s3FSRW) Open(path string) (fs.File, error) {
	bucket, bucketPath := extractBucket(path)
	if s3FS.logger != nil {
		s3FS.logger.Debugf("%s - Open(%s)", s3FS.String(), path)
	}
	ctx := context.Background()
	object, err := s3FS.client.GetObject(ctx, bucket, bucketPath, minio.GetObjectOptions{})
	if err != nil {
		return nil, errors.Wrapf(err, "cannot open '%s/%s/%s'", s3FS.client.EndpointURL(), bucket, path)
	}
	objectInfo, err := object.Stat()
	if err != nil {
		object.Close()
		if s3FS.IsNotExist(err) {
			return nil, fs.ErrNotExist
		}
		return nil, errors.Wrapf(err, "cannot stat '%s'", path)
	}
	if objectInfo.Err != nil {
		object.Close()
		return nil, errors.Wrapf(objectInfo.Err, "error in objectInfo of '%s'", path)
	}
	return NewROFile(object, path, s3FS.logger), nil
}

func (s3FS *s3FSRW) ReadFile(path string) ([]byte, error) {
	if s3FS.logger != nil {
		s3FS.logger.Debugf("%s - ReadFile(%s)", s3FS.String(), path)
	}
	fp, err := s3FS.Open(path)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot open '%s'", path)
	}
	defer fp.Close()
	data := bytes.NewBuffer(nil)
	if _, err := io.Copy(data, fp); err != nil {
		return nil, errors.Wrapf(err, "cannot read '%s'", path)
	}
	return data.Bytes(), nil
}

func (s3FS *s3FSRW) ReadDir(path string) ([]fs.DirEntry, error) {
	bucket, bucketPath := extractBucket(path)
	if s3FS.logger != nil {
		s3FS.logger.Debugf("%s - ReadDir(%s)", s3FS.String(), path)
	}
	if bucket == "" {
		bucketInfo, err := s3FS.client.ListBuckets(context.Background())
		if err != nil {
			return nil, errors.Wrapf(err, "cannot list buckets")
		}
		result := []fs.DirEntry{}
		for _, bi := range bucketInfo {
			result = append(result, writefs.NewDirEntry(writefs.NewFileInfoDir(bi.Name)))
		}
		return result, nil
	}
	ctx := context.Background()
	result := []fs.DirEntry{}

	for objectInfo := range s3FS.client.ListObjects(ctx, bucket, minio.ListObjectsOptions{Prefix: bucketPath}) {
		if objectInfo.Err != nil {
			return nil, errors.Wrapf(objectInfo.Err, "cannot read '%s'", path)
		}
		oiHelper := objectInfo
		result = append(result, writefs.NewDirEntry(NewFileInfo(&oiHelper)))
	}
	return result, nil
}

func (s3FS *s3FSRW) Create(path string) (writefs.FileWrite, error) {
	bucket, bucketPath := extractBucket(path)
	if s3FS.logger != nil {
		s3FS.logger.Debugf("%s - Create(%s)", s3FS.String(), path)
	}
	ctx := context.Background()
	wc := NewWriteCloser(path, s3FS.logger)
	go func() {
		ui, err := s3FS.client.PutObject(ctx, bucket, bucketPath, wc.GetReader(), -1, minio.PutObjectOptions{})
		uierr := NewUploadInfo(&ui, err)
		wc.c <- uierr
		if err != nil {
			wc.Close()
		}
	}()
	return wc, nil
}

func (s3FS *s3FSRW) Remove(path string) error {
	bucket, bucketPath := extractBucket(path)
	if s3FS.logger != nil {
		s3FS.logger.Debugf("%s - Delete(%s)", s3FS.String(), path)
	}
	ctx := context.Background()
	if err := s3FS.client.RemoveObject(ctx, bucket, bucketPath, minio.RemoveObjectOptions{}); err != nil {
		if s3FS.IsNotExist(err) {
			return fs.ErrNotExist
		}
		return errors.Wrapf(err, "cannot remove '%s'", path)
	}
	return nil
}

func (s3FS *s3FSRW) Sub(subfolder string) (fs.FS, error) {
	return writefs.NewSubFS(s3FS, subfolder), nil
}

func (s3FS *s3FSRW) String() string {
	return s3FS.endpoint
}

func (s3FS *s3FSRW) Rename(src, dest string) error {
	if s3FS.logger != nil {
		s3FS.logger.Debugf("%s - Rename(%s, %s)", s3FS.String(), src, dest)
	}
	_, err := s3FS.Stat(dest)
	if err != nil {
		if !s3FS.IsNotExist(err) {
			return errors.Wrapf(err, "cannot stat '%s'", dest)
		}
	} else {
		if err := s3FS.Remove(dest); err != nil {
			return errors.Wrapf(err, "cannot delete '%s'", dest)
		}
	}
	// now, dest should not exist...

	srcFP, err := s3FS.Open(src)
	if err != nil {
		return errors.Wrapf(err, "cannot open '%s'", src)
	}
	defer srcFP.Close()
	destFP, err := s3FS.Create(dest)
	if err != nil {
		return errors.Wrapf(err, "cannot create '%s'", dest)
	}
	defer destFP.Close()
	if _, err := io.Copy(destFP, srcFP); err != nil {
		return errors.Wrapf(err, "cannot copy '%s' --> '%s'", src, dest)
	}
	return nil
}

var notFoundStatus = []int{
	http.StatusNotFound,
	// http.StatusForbidden,
	// http.StatusConflict,
	// http.StatusPreconditionFailed,
}

func (s3FS *s3FSRW) IsNotExist(err error) bool {
	errResp, ok := err.(minio.ErrorResponse)
	if !ok {
		return false
	}
	return slices.Contains(notFoundStatus, errResp.StatusCode)
}

func (s3FS *s3FSRW) WalkDir(path string, fn fs.WalkDirFunc) error {
	var err error
	bucket, bucketPath := extractBucket(path)
	if s3FS.logger != nil {
		s3FS.logger.Debugf("%s - WalkDir(%s)", s3FS.String(), path)
	}
	var bucketEntries []fs.DirEntry
	if bucket == "" {
		bucketEntries, err = s3FS.ReadDir("")
		if err != nil {
			return errors.Wrapf(err, "cannot list buckets")
		}
	} else {
		bucketEntries = []fs.DirEntry{writefs.NewDirEntry(writefs.NewFileInfoDir(bucket))}
	}
	for _, bucketEntry := range bucketEntries {
		ctx := context.Background()
		for objectInfo := range s3FS.client.ListObjects(ctx, bucketEntry.Name(), minio.ListObjectsOptions{
			Prefix:    bucketPath,
			Recursive: true,
		}) {
			if err := fn(objectInfo.Key, writefs.NewDirEntry(NewFileInfo(&objectInfo)), nil); err != nil {
				return errors.Wrapf(err, "error in '%s'", objectInfo.Key)
			}
		}

	}
	return nil
}

func (s3FS *s3FSRW) Stat(path string) (fs.FileInfo, error) {
	bucket, bucketPath := extractBucket(path)
	if bucket == "" {
		return writefs.NewFileInfoDir(path), nil
	}
	ctx := context.Background()
	objectInfo, err := s3FS.client.StatObject(ctx, bucket, bucketPath, minio.StatObjectOptions{})
	if err != nil {
		if s3FS.IsNotExist(err) {
			if s3FS.hasContent(path) {
				return writefs.NewFileInfoDir(path), nil
			} else {
				return nil, fs.ErrNotExist
			}
		}
		return nil, errors.Wrapf(err, "cannot stat '%s'", path)
	}
	return &fileInfo{&objectInfo}, nil
}

func (s3FS *s3FSRW) hasContent(prefix string) bool {
	bucket, bucketPath := extractBucket(prefix)
	s3FS.logger.Debugf("%s - hasContent(%s)", s3FS.String(), prefix)
	ctx, cancel := context.WithCancel(context.Background())
	chanObjectInfo := s3FS.client.ListObjects(ctx, bucket, minio.ListObjectsOptions{Prefix: bucketPath})
	objectInfo, ok := <-chanObjectInfo
	if ok {
		if objectInfo.Err != nil {
			cancel()
			return true
		}
	}
	cancel()
	return ok
}

func (s3FS *s3FSRW) HasContent() bool {
	return s3FS.hasContent("")
}

var (
	_ writefs.ReadWriteFS = &s3FSRW{}
	_ writefs.MkDirFS     = &s3FSRW{}
	_ writefs.RenameFS    = &s3FSRW{}
	_ writefs.RemoveFS    = &s3FSRW{}
	_ fs.ReadDirFS        = &s3FSRW{}
	_ fs.ReadFileFS       = &s3FSRW{}
	_ fs.StatFS           = &s3FSRW{}
	_ fs.SubFS            = &s3FSRW{}
	_ fmt.Stringer        = &s3FSRW{}
)
