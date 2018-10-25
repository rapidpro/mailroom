package web

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	"github.com/nyaruka/mailroom/config"
	"github.com/sirupsen/logrus"

	"github.com/gomodule/redigo/redis"
	"github.com/jmoiron/sqlx"
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
	router.Post("/mr/flow/migrate", s.wrapJSONHandler(s.handleMigrate))
	router.Post("/mr/sim/start", s.wrapJSONHandler(s.handleStart))
	router.Post("/mr/sim/resume", s.wrapJSONHandler(s.handleResume))

	// configure our http server
	s.httpServer = &http.Server{
		Addr:         fmt.Sprintf("%s:%d", config.Address, config.Port),
		Handler:      router,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
	}

	return s
}

type JSONHandler func(r *http.Request) (interface{}, int, error)

func (s *Server) wrapJSONHandler(handler JSONHandler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-type", "application/json")

		auth := r.Header.Get("authorization")
		if s.config.AuthToken != "" && s.config.AuthToken != fmt.Sprintf("Token %s", auth) {
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte(`{"error": "missing bearer token"}`))
			return
		}

		value, status, err := handler(r)
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
