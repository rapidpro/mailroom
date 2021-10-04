package web

import (
	"compress/flate"
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/nyaruka/gocommon/jsonx"
	"github.com/nyaruka/mailroom/runtime"

	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
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

type JSONHandler func(ctx context.Context, rt *runtime.Runtime, r *http.Request) (interface{}, int, error)
type Handler func(ctx context.Context, rt *runtime.Runtime, r *http.Request, w http.ResponseWriter) error

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
func NewServer(ctx context.Context, rt *runtime.Runtime, wg *sync.WaitGroup) *Server {
	s := &Server{ctx: ctx, rt: rt, wg: wg}

	router := chi.NewRouter()

	//  set up our middlewares
	router.Use(middleware.Compress(flate.DefaultCompression))
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
		Addr:         fmt.Sprintf("%s:%d", rt.Config.Address, rt.Config.Port),
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

		value, status, err := handler(r.Context(), s.rt, r)

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
		err := handler(r.Context(), s.rt, r, w)
		if err == nil {
			return
		}

		logrus.WithError(err).WithField("http_request", r).Error("error handling request")
		w.WriteHeader(http.StatusInternalServerError)
		serialized := jsonx.MustMarshal(NewErrorResponse(err))
		w.Write(serialized)
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

	logrus.WithField("address", s.rt.Config.Address).WithField("port", s.rt.Config.Port).Info("server started")
}

// Stop stops our web server
func (s *Server) Stop() {
	// shut down our HTTP server
	if err := s.httpServer.Shutdown(context.Background()); err != nil {
		logrus.WithField("state", "stopping").WithError(err).Error("error shutting down server")
	}
}

func handleIndex(ctx context.Context, rt *runtime.Runtime, r *http.Request) (interface{}, int, error) {
	response := map[string]string{
		"url":       fmt.Sprintf("%s", r.URL),
		"component": "mailroom",
		"version":   rt.Config.Version,
	}
	return response, http.StatusOK, nil
}

func handle404(ctx context.Context, rt *runtime.Runtime, r *http.Request) (interface{}, int, error) {
	return errors.Errorf("not found: %s", r.URL.String()), http.StatusNotFound, nil
}

func handle405(ctx context.Context, rt *runtime.Runtime, r *http.Request) (interface{}, int, error) {
	return errors.Errorf("illegal method: %s", r.Method), http.StatusMethodNotAllowed, nil
}

type Server struct {
	ctx context.Context
	rt  *runtime.Runtime

	wg *sync.WaitGroup

	httpServer *http.Server
}
