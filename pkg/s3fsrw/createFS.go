package s3fsrw

import (
	"fmt"
	"github.com/je4/filesystem/v2/pkg/writefs"
	"github.com/op/go-logging"
	"io/fs"
	"regexp"
)

type s3Access struct {
	accessKey string
	secretKey string
	url       string
	useSSL    bool
}

var ARNRegexStr = `^arn:(?P<partition>[^:]*):s3:(?P<region>[^:]*):(?P<namespace>[^:]*):[^:]*`

func NewCreateFSFunc(access map[string]*s3Access, regexpString string, logger *logging.Logger) writefs.CreateFSFunc {
	urnRegexp := regexp.MustCompile(regexpString)

	return func(f *writefs.Factory, path string) (fs.FS, error) {
		urnMatch := urnRegexp.FindStringSubmatch(path)
		result := make(map[string]string)
		for i, name := range urnRegexp.SubexpNames() {
			if i != 0 && name != "" {
				result[name] = urnMatch[i]
			}
		}
		partition, _ := result["partition"]
		acc, ok := access[partition]
		if !ok {
			return nil, fmt.Errorf("partition %s not supported", partition)
		}
		region, _ := result["region"]
		if namespace, ok := result["namespace"]; ok && namespace != "" {
			return nil, fmt.Errorf("namespace %s not supported", namespace)
		}
		subPath, _ := result["relativeid"]

		s3fs, err := NewS3FS(
			acc.url,
			acc.accessKey,
			acc.secretKey,
			region,
			acc.useSSL,
			logger,
		)
		if err != nil {
			return nil, err
		}
		if subPath != "" {
			return s3fs.Sub(subPath)
		}
		return s3fs, nil
	}
}
