package web

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/nyaruka/mailroom/config"
	"github.com/nyaruka/mailroom/models"
	"github.com/olivere/elastic"

	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const (
	// OrgIDKey is our context key for org id
	OrgIDKey = "org_id"

	// UserIDKey is our context key for user id
	UserIDKey = "user_id"

	// MaxRequestBytes is the max body size our web server will accept
	MaxRequestBytes int64 = 1048576
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

func RequireUserToken(handler JSONHandler) JSONHandler {
	return func(ctx context.Context, s *Server, r *http.Request) (interface{}, int, error) {
		token := r.Header.Get("authorization")
		if !strings.HasPrefix(token, "Token ") {
			return errors.New("missing authorization header"), http.StatusUnauthorized, nil
		}

		// pull out the actual token
		token = token[6:]

		// try to look it up
		rows, err := s.DB.QueryContext(s.CTX, `
		SELECT 
			user_id, 
			org_id
		FROM
			api_apitoken t
			JOIN orgs_org o ON t.org_id = o.id
			JOIN auth_group g ON t.role_id = g.id
			JOIN auth_user u ON t.user_id = u.id
		WHERE
			key = $1 AND
			g.name IN ('Administrators', 'Editors', 'Surveyors') AND
			t.is_active = TRUE AND
			o.is_active = TRUE AND
			u.is_active = TRUE
		`, token)
		if err != nil {
			return errors.Wrapf(err, "error looking up authorization header"), http.StatusUnauthorized, nil
		}
		defer rows.Close()

		if !rows.Next() {
			return errors.Errorf("invalid authorization header"), http.StatusUnauthorized, nil
		}

		var userID int64
		var orgID models.OrgID
		err = rows.Scan(&userID, &orgID)
		if err != nil {
			return nil, 0, errors.Wrapf(err, "error scanning auth row")
		}

		// we are authenticated set our user id ang org id on our context and call our sub handler
		ctx = context.WithValue(ctx, UserIDKey, userID)
		ctx = context.WithValue(ctx, OrgIDKey, orgID)
		return handler(ctx, s, r)
	}
}

// RequireAuthToken wraps a handler to require that our request to have our global authorization header
func RequireAuthToken(handler JSONHandler) JSONHandler {
	return func(ctx context.Context, s *Server, r *http.Request) (interface{}, int, error) {
		auth := r.Header.Get("authorization")
		if s.Config.AuthToken != "" && fmt.Sprintf("Token %s", s.Config.AuthToken) != auth {
			return fmt.Errorf("invalid or missing authorization header, denying"), http.StatusUnauthorized, nil
		}

		// we are authenticated, call our chain
		return handler(ctx, s, r)
	}
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

		serialized, serr := json.MarshalIndent(value, "", "  ")
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

// ErrorResponse is the type for our error responses, it just contains a single error field
type ErrorResponse struct {
	Error string `json:"error"`
}

// NewErrorResponse creates a new error response from the passed in errro
func NewErrorResponse(err error) *ErrorResponse {
	return &ErrorResponse{err.Error()}
}
