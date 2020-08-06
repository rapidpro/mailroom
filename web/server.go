package web

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/nyaruka/goflow/utils/jsonx"
	"github.com/nyaruka/mailroom/config"

	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/olivere/elastic"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const (
	// OrgIDKey is our context key for org id
	OrgIDKey = "org_id"

	// UserIDKey is our context key for user id
	UserIDKey = "user_id"

	// MaxRequestBytes is the max body size our web server will accept
	MaxRequestBytes int64 = 1048576 * 32 // 32MB
)

type JSONHandler func(ctx context.Context, s *Server, r *http.Request) (interface{}, int, error)
type Handler func(ctx context.Context, s *Server, r *http.Request, w http.ResponseWriter) error

type jsonRoute struct {
	method  string
	pattern string
	handler JSONHandler
}

var jsonRoutes = make([]*jsonRoute, 0)

type route struct {
	method  string
	pattern string
	handler Handler
}

var routes = make([]*route, 0)

func RegisterJSONRoute(method string, pattern string, handler JSONHandler) {
	jsonRoutes = append(jsonRoutes, &jsonRoute{method, pattern, handler})
}

func RegisterRoute(method string, pattern string, handler Handler) {
	routes = append(routes, &route{method, pattern, handler})
}

// NewServer creates a new web server, it will need to be started after being created
func NewServer(ctx context.Context, config *config.Config, db *sqlx.DB, rp *redis.Pool, s3Client s3iface.S3API, elasticClient *elastic.Client, wg *sync.WaitGroup) *Server {
	s := &Server{
		CTX:           ctx,
		RP:            rp,
		DB:            db,
		S3Client:      s3Client,
		ElasticClient: elasticClient,
		Config:        config,

		wg: wg,
	}

	router := chi.NewRouter()

	//  set up our middlewares
	router.Use(middleware.DefaultCompress)
	router.Use(middleware.RequestID)
	router.Use(middleware.RealIP)
	router.Use(panicRecovery)
	router.Use(middleware.Timeout(60 * time.Second))
	router.Use(requestLogger)

	// wire up our main pages
	router.NotFound(s.WrapJSONHandler(handle404))
	router.MethodNotAllowed(s.WrapJSONHandler(handle405))
	router.Get("/", s.WrapJSONHandler(handleIndex))
	router.Get("/mr/", s.WrapJSONHandler(handleIndex))

	// add any registered json routes
	for _, route := range jsonRoutes {
		router.Method(route.method, route.pattern, s.WrapJSONHandler(route.handler))
	}

	// and any normal routes
	for _, route := range routes {
		router.Method(route.method, route.pattern, s.WrapHandler(route.handler))
	}

	// configure our http server
	s.httpServer = &http.Server{
		Addr:         fmt.Sprintf("%s:%d", config.Address, config.Port),
		Handler:      router,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
	}

	return s
}

// WrapJSONHandler wraps a simple JSONHandler
func (s *Server) WrapJSONHandler(handler JSONHandler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-type", "application/json")

		value, status, err := handler(r.Context(), s, r)

		// handler errored (a hard error)
		if err != nil {
			value = NewErrorResponse(err)
		} else {
			// handler returned an error to use as a the response
			asError, isError := value.(error)
			if isError {
				value = NewErrorResponse(asError)
			}
		}

		serialized, serr := jsonx.MarshalPretty(value)
		if serr != nil {
			logrus.WithError(err).WithField("http_request", r).Error("error serializing handler response")
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(`{"error": "error serializing handler response"}`))
			return
		}

		if err != nil {
			logrus.WithError(err).WithField("http_request", r).Error("error handling request")
			w.WriteHeader(http.StatusInternalServerError)
			w.Write(serialized)
			return
		}

		w.WriteHeader(status)
		w.Write(serialized)
	}
}

// WrapHandler wraps a simple Handler, taking care of passing down server and handling errors
func (s *Server) WrapHandler(handler Handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		err := handler(r.Context(), s, r, w)
		if err == nil {
			return
		}

		logrus.WithError(err).WithField("http_request", r).Error("error handling request")
		w.WriteHeader(http.StatusInternalServerError)
		serialized, _ := json.Marshal(NewErrorResponse(err))
		w.Write(serialized)
		return
	}
}

// Start starts our web server, listening for new requests
func (s *Server) Start() {
	// start serving HTTP
	go func() {
		s.wg.Add(1)
		defer s.wg.Done()

		err := s.httpServer.ListenAndServe()
		if err != nil && err != http.ErrServerClosed {
			logrus.WithFields(logrus.Fields{
				"comp":  "server",
				"state": "stopping",
				"err":   err,
			}).Error()
		}
	}()

	logrus.WithField("address", s.Config.Address).WithField("port", s.Config.Port).Info("server started")
}

// Stop stops our web server
func (s *Server) Stop() {
	// shut down our HTTP server
	if err := s.httpServer.Shutdown(context.Background()); err != nil {
		logrus.WithField("state", "stopping").WithError(err).Error("error shutting down server")
	}
}

func handleIndex(ctx context.Context, s *Server, r *http.Request) (interface{}, int, error) {
	response := map[string]string{
		"url":       fmt.Sprintf("%s", r.URL),
		"component": "mailroom",
		"version":   s.Config.Version,
	}
	return response, http.StatusOK, nil
}

func handle404(ctx context.Context, s *Server, r *http.Request) (interface{}, int, error) {
	return errors.Errorf("not found: %s", r.URL.String()), http.StatusNotFound, nil
}

func handle405(ctx context.Context, s *Server, r *http.Request) (interface{}, int, error) {
	return errors.Errorf("illegal method: %s", r.Method), http.StatusMethodNotAllowed, nil
}

type Server struct {
	CTX           context.Context
	RP            *redis.Pool
	DB            *sqlx.DB
	S3Client      s3iface.S3API
	Config        *config.Config
	ElasticClient *elastic.Client

	wg *sync.WaitGroup

	httpServer *http.Server
}
