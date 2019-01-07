package web

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	"github.com/nyaruka/mailroom/config"
	"github.com/nyaruka/mailroom/models"
	"github.com/sirupsen/logrus"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
)

const (
	orgIDKey  = "org_id"
	userIDKey = "user_id"
)

// NewServer creates a new web server, it will need to be started after being created
func NewServer(ctx context.Context, db *sqlx.DB, rp *redis.Pool, config *config.Config, wg *sync.WaitGroup) *Server {
	s := &Server{
		ctx: ctx,
		rp:  rp,
		db:  db,
		wg:  wg,

		config: config,
	}

	//  set up our middlewares
	router := chi.NewRouter()
	router.Use(middleware.DefaultCompress)
	router.Use(middleware.StripSlashes)
	router.Use(middleware.RequestID)
	router.Use(middleware.RealIP)
	router.Use(panicRecovery)
	router.Use(middleware.Timeout(30 * time.Second))
	router.Use(requestLogger)

	// wire up our main pages
	router.NotFound(s.wrapJSONHandler(s.handle404))
	router.MethodNotAllowed(s.wrapJSONHandler(s.handle405))
	router.Get("/", s.wrapJSONHandler(s.handleIndex))
	router.Get("/mr", s.wrapJSONHandler(s.handleIndex))

	router.Post("/mr/flow/migrate", s.wrapJSONHandler(s.requireAuthToken(s.handleMigrate)))
	router.Post("/mr/sim/start", s.wrapJSONHandler(s.requireAuthToken(s.handleStart)))
	router.Post("/mr/sim/resume", s.wrapJSONHandler(s.requireAuthToken(s.handleResume)))

	router.Post("/mr/surveyor/submit", s.wrapJSONHandler(s.requireUserToken(s.handleSurveyorSubmit)))

	router.Post("/mr/ivr/start", s.wrapJSONHandler(s.handleIVRStart))

	// configure our http server
	s.httpServer = &http.Server{
		Addr:         fmt.Sprintf("%s:%d", config.Address, config.Port),
		Handler:      router,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
	}

	return s
}

type JSONHandler func(ctx context.Context, r *http.Request) (interface{}, int, error)

func (s *Server) requireUserToken(handler JSONHandler) JSONHandler {
	return func(ctx context.Context, r *http.Request) (interface{}, int, error) {
		token := r.Header.Get("authorization")
		if !strings.HasPrefix("Token ", token) {
			return nil, http.StatusUnauthorized, errors.New("missing authorization header")
		}

		// pull out the actual token
		token = token[6:]

		// try to look it up
		rows, err := s.db.QueryContext(s.ctx, `
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
			g.name IN ("Administrators", "Editors", "Surveyors") AND
			is_active = TRUE AND
			o.is_active = TRUE AND
			u.is_active = TRUE
		`)

		if err != nil {
			return nil, http.StatusUnauthorized, errors.New("invalid authorization header")
		}

		var userID int32
		var orgID models.OrgID
		err = rows.Scan(&userID, orgID)
		if err != nil {
			return nil, http.StatusServiceUnavailable, errors.Wrapf(err, "error scanning auth row")
		}

		// we are authenticated set our user id ang org id on our context and call our sub handler
		ctx = context.WithValue(ctx, userIDKey, userID)
		ctx = context.WithValue(ctx, orgIDKey, orgID)
		return handler(ctx, r)
	}
}

// requireAuthToken wraps a handler to require that our request to have our global authorization header
func (s *Server) requireAuthToken(handler JSONHandler) JSONHandler {
	return func(ctx context.Context, r *http.Request) (interface{}, int, error) {
		auth := r.Header.Get("authorization")
		if s.config.AuthToken != "" && fmt.Sprintf("Token %s", s.config.AuthToken) != auth {
			return nil, http.StatusUnauthorized, fmt.Errorf("invalid or missing authorization header, denying")
		}

		// we are authenticated, call our chain
		return handler(ctx, r)
	}
}

// wrapJSONHandler wraps a simple JSONHandler
func (s *Server) wrapJSONHandler(handler JSONHandler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-type", "application/json")

		value, status, err := handler(r.Context(), r)
		if err != nil {
			value = map[string]string{
				"error": err.Error(),
			}
		}

		serialized, serr := json.Marshal(value)
		if serr != nil {
			logrus.WithError(err).Error("error serializing handler response")
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(`{"error": "error serializing handler response"}`))
			return
		}

		if err != nil {
			logrus.WithError(err).Error("client error")
			w.WriteHeader(status)
			w.Write(serialized)
			return
		}

		w.WriteHeader(http.StatusOK)
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

	logrus.WithField("address", s.config.Address).WithField("port", s.config.Port).Info("server started")
}

// Stop stops our web server
func (s *Server) Stop() {
	// shut down our HTTP server
	if err := s.httpServer.Shutdown(context.Background()); err != nil {
		logrus.WithField("state", "stopping").WithError(err).Error("error shutting down server")
	}
}

type Server struct {
	ctx context.Context
	rp  *redis.Pool
	db  *sqlx.DB
	wg  *sync.WaitGroup

	config *config.Config

	httpServer *http.Server
}
