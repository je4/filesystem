package s3fsrw

import (
	"crypto/tls"
	"fmt"
	"github.com/je4/filesystem/v3/pkg/writefs"
	"github.com/je4/utils/v2/pkg/zLogger"
	"io/fs"
	"regexp"
)

type S3Access struct {
	AccessKey string
	SecretKey string
	URL       string
	UseSSL    bool
}

var ARNRegexStr = `^arn:(?P<partition>[^:]*):s3:(?P<region>[^:]*):(?P<namespace>[^:]*):(?P<subpath>[^:]*)`

func NewCreateFSFunc(access map[string]*S3Access, regexpString string, debug bool, tlsConfig *tls.Config, logger zLogger.ZLogger) writefs.CreateFSFunc {
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
		subPath, _ := result["subpath"]

		//		_logger := logger.With().Str("class", "s3FSRW").Logger()
		s3fs, err := NewFS(
			acc.URL,
			acc.AccessKey,
			acc.SecretKey,
			region,
			acc.UseSSL,
			debug,
			tlsConfig,
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
