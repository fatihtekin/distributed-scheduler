package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"distributed-scheduler/internal/db"
	"distributed-scheduler/internal/models"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
)

type Server struct {
	db                 db.DBClient
	router             *mux.Router
	server             *http.Server
	jobTimeOutDuration time.Duration
}

type ServerConfig struct {
	port               int
	jobTimeOutDuration time.Duration
}

func NewServerConfig(port int, jobTimeOutDuration time.Duration) ServerConfig {
	return ServerConfig{
		port:               port,
		jobTimeOutDuration: jobTimeOutDuration,
	}
}

func NewServer(database db.DBClient, config ServerConfig) *Server {
	s := &Server{
		db:                 database,
		router:             mux.NewRouter(),
		jobTimeOutDuration: config.jobTimeOutDuration,
	}

	s.routes()
	s.server = &http.Server{
		Addr:         fmt.Sprintf(":%d", config.port),
		Handler:      s.router,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
	}

	return s
}

func (s *Server) routes() {
	s.router.HandleFunc("/jobs", s.createJob).Methods("POST")
	s.router.HandleFunc("/jobs/{id}", s.getJob).Methods("GET")
	s.router.HandleFunc("/next-job", s.getNextJob).Methods("GET")
	s.router.HandleFunc("/jobs/{id}/complete", s.completeJob).Methods("POST")
	s.router.HandleFunc("/health", s.health).Methods("GET")
}

func (s *Server) Start() error {
	log.Printf("API server listening on %s", s.server.Addr)
	return s.server.ListenAndServe()
}

func (s *Server) Shutdown() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := s.server.Shutdown(ctx); err != nil {
		log.Printf("Failed to shutdown API server: %v", err)
		return
	}
	log.Print("Server shutdown complete")
}

func (s *Server) createJob(w http.ResponseWriter, r *http.Request) {
	var req models.CreateJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	if req.Type == "" {
		respondError(w, http.StatusBadRequest, "Job type is required")
		return
	}

	if req.Priority < 1 || req.Priority > 10 {
		req.Priority = 5
	}

	job := &models.Job{
		ID:         uuid.New().String(),
		Type:       req.Type,
		Payload:    req.Payload,
		Priority:   req.Priority,
		Status:     models.StatusPending,
		RetryCount: 0,
		CreatedAt:  time.Now(),
	}

	ctx := r.Context()
	if err := s.db.CreateJob(ctx, job); err != nil {
		log.Printf("Failed to create job: %v", err)
		respondError(w, http.StatusInternalServerError, "Failed to create job")
		return
	}

	respondJSON(w, http.StatusCreated, job)
}

func (s *Server) getJob(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	ctx := r.Context()
	job, err := s.db.GetJob(ctx, id)
	if err != nil {
		respondError(w, http.StatusNotFound, "Job not found")
		return
	}

	respondJSON(w, http.StatusOK, job)
}

func (s *Server) getNextJob(w http.ResponseWriter, r *http.Request) {
	workerID := r.Header.Get("X-Worker-ID")

	if workerID == "" {
		// Safety check: Reject the request if the worker doesn't identify itself
		log.Print("Received /next-job request with missing X-Worker-ID header")
		respondError(w, http.StatusBadRequest, "Worker identification required (Missing X-Worker-ID header)")
		return
	}

	log.Printf("Worker %s is requesting the next job...", workerID)

	ctx := r.Context()
	job, err := s.db.GetNextJob(ctx, workerID, s.jobTimeOutDuration)
	if err != nil {
		log.Printf("Failed to get next job: %v", err)
		respondError(w, http.StatusInternalServerError, "Failed to get next job")
		return
	}

	if job == nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	respondJSON(w, http.StatusOK, job)
}

func (s *Server) completeJob(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	var req models.CompleteJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	ctx := r.Context()
	if err := s.db.CompleteJob(ctx, id, req.Status); err != nil {
		log.Printf("Failed to complete job: %v", err)
		respondError(w, http.StatusInternalServerError, "Failed to complete job")
		return
	}

	respondJSON(w, http.StatusOK, map[string]string{"message": "Job completed"})
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	respondJSON(w, http.StatusOK, map[string]string{"status": "healthy"})
}

func respondJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		http.Error(w, fmt.Sprintf("failed to encode JSON: %v", err), http.StatusInternalServerError)
	}
}

func respondError(w http.ResponseWriter, status int, message string) {
	respondJSON(w, status, map[string]string{"error": message})
}
