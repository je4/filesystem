package remotefs

import (
	"context"
	"crypto/tls"
	"emperror.dev/errors"
	"fmt"
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
	"github.com/je4/filesystem/v3/pkg/writefs"
	"github.com/je4/utils/v2/pkg/zLogger"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"
)

func NewMainController(addr, extAddr string, tlsConfig *tls.Config, jwtAlgs []string, jwtKeys map[string]string, vfs fs.FS, logger zLogger.ZLogger) (*mainController, error) {
	u, err := url.Parse(extAddr)
	if err != nil {
		return nil, errors.Wrapf(err, "invalid external address '%s'", extAddr)
	}
	subpath := "/" + strings.Trim(u.Path, "/")

	gin.SetMode(gin.DebugMode)
	router := gin.Default()

	_logger := logger.With().Str("httpService", "mainController").Logger()
	c := &mainController{
		addr:    addr,
		extAddr: extAddr,
		jwtAlgs: jwtAlgs,
		jwtKeys: jwtKeys,
		router:  router,
		subpath: subpath,
		logger:  &_logger,
		vfs:     vfs,
	}
	if err := c.Init(tlsConfig); err != nil {
		return nil, errors.Wrap(err, "cannot initialize rest controller")
	}
	return c, nil
}

type mainController struct {
	server  http.Server
	router  *gin.Engine
	addr    string
	subpath string
	logger  zLogger.ZLogger
	vfs     fs.FS
	jwtAlgs []string
	extAddr string
	jwtKeys map[string]string
}

func (ctrl *mainController) Init(tlsConfig *tls.Config) error {
	if len(ctrl.jwtAlgs) == 0 {
		ctrl.router.Use(ctrl.checkAccessMTLS)
	} else {
		ctrl.router.Use(ctrl.checkAccessJWT, cors.Default())
	}
	ctrl.router.Use(cors.Default())

	ctrl.router.GET("/:vfs/*path", ctrl.read)
	ctrl.router.PUT("/:vfs/*path", ctrl.create)
	ctrl.router.DELETE("/:vfs/*path", ctrl.delete)

	ctrl.server = http.Server{
		Addr:      ctrl.addr,
		Handler:   ctrl.router,
		TLSConfig: tlsConfig,
	}

	return nil
}

func (ctrl *mainController) Start(wg *sync.WaitGroup) {
	go func() {
		wg.Add(1)
		defer wg.Done() // let main know we are done cleaning up

		if ctrl.server.TLSConfig == nil {
			fmt.Printf("starting server at http://%s\n", ctrl.addr)
			if err := ctrl.server.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
				// unexpected error. port in use?
				fmt.Errorf("server on '%s' ended: %v", ctrl.addr, err)
			}
		} else {
			fmt.Printf("starting server at https://%s\n", ctrl.addr)
			if err := ctrl.server.ListenAndServeTLS("", ""); !errors.Is(err, http.ErrServerClosed) {
				// unexpected error. port in use?
				fmt.Errorf("server on '%s' ended: %v", ctrl.addr, err)
			}
		}
		// always returns error. ErrServerClosed on graceful close
	}()
}

func (ctrl *mainController) Stop() {
	ctrl.server.Shutdown(context.Background())
}

func (ctrl *mainController) GracefulStop() {
	ctrl.server.Shutdown(context.Background())
}

var isUrlRegexp = regexp.MustCompile(`^[a-z]+://`)

var pathRegexp = regexp.MustCompile(`"/?(.+?)/(.+?)/(.+)?(/(.+?))?$`)

func (ctrl *mainController) checkAccessMTLS(c *gin.Context) {
	vfs := c.Param("vfs")
	vfsUrl := fmt.Sprintf("vfs://%s", vfs)
	for _, cert := range c.Request.TLS.PeerCertificates {
		for _, u := range cert.URIs {
			if u.String() == vfsUrl {
				return
			}
		}
	}
	c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
		"error": fmt.Sprintf("no access to vfs '%s'", vfs),
	})
	return
}

func (ctrl *mainController) checkAccessJWT(c *gin.Context) {
	token := c.Query("token")
	if token == "" {
		authHeader := c.GetHeader("Authorization")
		if !strings.HasPrefix(authHeader, "Bearer ") {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"error": "no token",
			})
			return
		}
		token = strings.TrimPrefix(authHeader, "Bearer ")
	}
	vfs := c.Param("vfs")
	jwtToken, err := jwt.ParseWithClaims(token, &jwt.RegisteredClaims{}, func(token *jwt.Token) (interface{}, error) {
		jwtKey, ok := ctrl.jwtKeys[vfs]
		if !ok {
			return nil, fmt.Errorf("no jwt key for vfs '%s'", vfs)
		}
		tokenAlg := token.Method.Alg()
		for _, alg := range ctrl.jwtAlgs {
			if tokenAlg == alg {
				return []byte(jwtKey), nil
			}
		}
		return nil, fmt.Errorf("alg: %v not supported", tokenAlg)
	})
	if err != nil {
		c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
			"error": fmt.Sprintf("cannot parse jwt token '%s': %v", token, err),
		})
		return
	}
	if !jwtToken.Valid {
		c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
			"error": fmt.Sprintf("invalid jwt token '%s'", token),
		})
		return
	}
	subject, err := jwtToken.Claims.GetSubject()
	if err != nil {
		c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
			"error": fmt.Sprintf("cannot get subject from jwt token '%s': %v", token, err),
		})
		return
	}
	if subject != "vfs."+vfs {
		c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
			"error": fmt.Sprintf("invalid subject '%s' in jwt token '%s'", subject, token),
		})
		return
	}
	return
}
func (ctrl *mainController) read(c *gin.Context) {
	vfs := c.Param("vfs")
	path := strings.Trim(c.Param("path"), "/")
	_, stat := c.GetQuery("stat")

	vfsPath := fmt.Sprintf("vfs://%s/%s", vfs, path)
	ctrl.logger.Debug().Str("vfsPath", vfsPath).Msg("read")
	if stat {
		info, err := fs.Stat(ctrl.vfs, vfsPath)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusNotFound, gin.H{
				"error": fmt.Sprintf("cannot stat '%s': %v", vfsPath, err),
			})
			return
		}
		c.JSON(http.StatusOK, fileInfo{
			Name_:    info.Name(),
			Size_:    info.Size(),
			Mode_:    info.Mode(),
			ModTime_: info.ModTime().Format(time.RFC3339),
			IsDir_:   info.IsDir(),
		})
		return
	}
	//c.Header("Content-Type", mime)
	c.FileFromFS(vfsPath, http.FS(ctrl.vfs))
	return
}

func (ctrl *mainController) create(c *gin.Context) {
	vfs := c.Param("vfs")
	path := strings.Trim(c.Param("path"), "/")

	vfsPath := fmt.Sprintf("vfs://%s/%s", vfs, path)
	ctrl.logger.Debug().Str("vfsPath", vfsPath).Msg("create")
	_, err := fs.Stat(ctrl.vfs, vfsPath)
	if !errors.Is(err, fs.ErrNotExist) {
		ctrl.logger.Error().Err(err).Msgf("'%s' already exists", vfsPath)
		c.AbortWithStatusJSON(http.StatusConflict, gin.H{
			"error": fmt.Sprintf("'%s' already exists", vfsPath),
		})
		return
	}
	fp, err := writefs.Create(ctrl.vfs, vfsPath)
	if err != nil {
		ctrl.logger.Error().Err(err).Msgf("cannot create '%s'", vfsPath)
		c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{
			"error": fmt.Sprintf("cannot create '%s': %v", vfsPath, err),
		})
		return
	}

	written, err := io.Copy(fp, c.Request.Body)
	if err != nil {
		errs := []error{err}
		if err := writefs.Remove(ctrl.vfs, vfsPath); err != nil {
			errs = append(errs, err)
		}
		ctrl.logger.Error().Err(errors.Combine(errs...)).Msgf("cannot write '%s'", vfsPath)
		c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{
			"error": fmt.Sprintf("cannot write '%s': %v", vfsPath, errors.Combine(errs...)),
		})
		fp.Close()
		return
	}

	if err := fp.Close(); err != nil {
		ctrl.logger.Error().Err(err).Msgf("cannot close '%s'", vfsPath)
		c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{
			"error": fmt.Sprintf("cannot close '%s': %v", vfsPath, err),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"path":    vfsPath,
		"written": written,
	})

}

func (ctrl *mainController) delete(c *gin.Context) {
	vfs := c.Param("vfs")
	path := strings.Trim(c.Param("path"), "/")

	vfsPath := fmt.Sprintf("vfs://%s/%s", vfs, path)
	ctrl.logger.Debug().Str("vfsPath", vfsPath).Msg("delete")
	if err := writefs.Remove(ctrl.vfs, vfsPath); err != nil {
		ctrl.logger.Error().Err(err).Msgf("cannot remove '%s'", vfsPath)
		c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{
			"error": fmt.Sprintf("cannot remove '%s': %v", vfsPath, err),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"path":    vfsPath,
		"removed": "ok",
	})

}
