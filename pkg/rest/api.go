package rest

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"emperror.dev/errors"
	"github.com/gin-gonic/gin"
	"github.com/je4/utils/v2/pkg/zLogger"
	"github.com/je4/wsbroker/pkg/rest/docs"
	"go.ub.unibas.ch/cloud/swaggerui"
)

const BASEPATH = "/api/v1"

//	@title			WSBroker API
//	@version		1.0
//	@description	WSBroker API
//	@termsOfService	http://swagger.io/terms/

//	@servers.url			/api/v1
//	@servers.description	WSBroker API base path

//	@contact.name	Jürgen Enge
//	@contact.url	https://ub.unibas.ch
//	@contact.email	juergen.enge@unibas.ch

//	@license.name	Apache 2.0
//	@license.url	http://www.apache.org/licenses/LICENSE-2.0.html

//	@securityDefinitions.apikey	BearerAuth
//	@in							header
//	@name						Authorization

func NewController(addr, extAddr string,
	tlsConfig *tls.Config,
	bearer string,
	logger zLogger.ZLogger,
) (*controller, error) {
	u, err := url.Parse(extAddr)
	if err != nil {
		return nil, errors.Wrapf(err, "invalid external address '%s'", extAddr)
	}
	subpath := "/" + strings.Trim(u.Path, "/")

	// programmatically set swagger info
	docs.SwaggerInfoWSBroker.Host = strings.TrimRight(fmt.Sprintf("%s:%s", u.Hostname(), u.Port()), " :")
	docs.SwaggerInfoWSBroker.BasePath = "/" + strings.Trim(subpath+BASEPATH, "/")
	if u.Scheme != "" {
		docs.SwaggerInfoWSBroker.Schemes = []string{u.Scheme}
	} else {
		docs.SwaggerInfoWSBroker.Schemes = []string{"http", "https"}
	}

	router := gin.Default()

	c := &controller{
		addr:    addr,
		router:  router,
		subpath: subpath,
		logger:  logger,
		bearer:  bearer,
	}
	if err := c.Init(tlsConfig); err != nil {
		return nil, errors.Wrap(err, "cannot initialize rest controller")
	}
	return c, nil
}

type controller struct {
	server  http.Server
	router  *gin.Engine
	addr    string
	subpath string
	logger  zLogger.ZLogger
	bearer  string
}

func (ctrl *controller) Init(tlsConfig *tls.Config) error {
	v1 := ctrl.router.Group(BASEPATH)

	v1.GET("/ping", ctrl.ping)

	if len(ctrl.bearer) > 0 {
		v1.Use(func(c *gin.Context) {
			authHeader := c.Request.Header.Get("Authorization")
			if !strings.HasPrefix(authHeader, "Bearer ") {
				c.AbortWithStatusJSON(http.StatusUnauthorized, HTTPResultMessage{
					Code:    http.StatusUnauthorized,
					Message: "no bearer token found",
				})
				return
			}
			token := strings.TrimPrefix(authHeader, "Bearer ")
			if token != ctrl.bearer {
				c.AbortWithStatusJSON(http.StatusUnauthorized, HTTPResultMessage{
					Code:    http.StatusUnauthorized,
					Message: "invalid bearer token",
				})
				return
			}
		})
	}

	ctrl.router.GET("/swagger/*any", swaggerui.CustomHandler(
		swaggerui.WithTitle("WSBroker API"),
		swaggerui.WithDocFunc(func() string {
			doc := docs.SwaggerInfoWSBroker.ReadDoc()
			if docs.SwaggerInfoWSBroker.BasePath != "" {
				var spec map[string]any
				if err := json.Unmarshal([]byte(doc), &spec); err == nil {
					spec["servers"] = []map[string]any{
						{
							"url":         docs.SwaggerInfoWSBroker.BasePath,
							"description": "WSBroker API base path",
						},
					}
					if b, err := json.Marshal(spec); err == nil {
						return string(b)
					}
				}
			}
			return doc
		}),
	))

	ctrl.server = http.Server{
		Addr:      ctrl.addr,
		Handler:   ctrl.router,
		TLSConfig: tlsConfig,
	}

	return nil
}

func (ctrl *controller) Start(wg *sync.WaitGroup) {
	go func() {
		wg.Add(1)
		defer wg.Done() // let main know we are done cleaning up

		if ctrl.server.TLSConfig == nil {
			fmt.Printf("starting server at http://%s\n", ctrl.addr)
			if err := ctrl.server.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
				fmt.Printf("server on '%s' ended: %v\n", ctrl.addr, err)
			}
		} else {
			fmt.Printf("starting server at https://%s\n", ctrl.addr)
			if err := ctrl.server.ListenAndServeTLS("", ""); !errors.Is(err, http.ErrServerClosed) {
				fmt.Printf("server on '%s' ended: %v\n", ctrl.addr, err)
			}
		}
	}()
}

func (ctrl *controller) GracefulStop() {
	ctrl.server.Shutdown(context.Background())
}

// ping godoc
//
//	@Summary		does pong
//	@ID				get-ping
//	@Description	for testing if server is running
//	@Tags			wsbroker
//	@Produce		plain
//	@Success		200		{string}	string
//	@Router			/ping [get]
func (ctrl *controller) ping(c *gin.Context) {
	c.String(http.StatusOK, "pong")
}
