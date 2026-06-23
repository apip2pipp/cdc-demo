package handler

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"cdc-demo/consumer/model"
	"cdc-demo/consumer/store"
)

// APIHandler holds shared dependencies for REST handlers.
type APIHandler struct {
	DB *sql.DB
}

// ListAuditLogs handles GET /api/audit-logs
// Supports query params: action, table, from, to, limit, offset
func (h *APIHandler) ListAuditLogs(w http.ResponseWriter, r *http.Request) {
	f := model.AuditLogFilter{
		Action:    strings.ToUpper(r.URL.Query().Get("action")),
		TableName: r.URL.Query().Get("table"),
	}

	f.Limit = parseIntParam(r, "limit", 100)
	f.Offset = parseIntParam(r, "offset", 0)

	if s := r.URL.Query().Get("from"); s != "" {
		if t, err := time.Parse("2006-01-02", s); err == nil {
			f.From = &t
		}
	}
	if s := r.URL.Query().Get("to"); s != "" {
		if t, err := time.Parse("2006-01-02", s); err == nil {
			// Include the full end day.
			end := t.Add(24*time.Hour - time.Nanosecond)
			f.To = &end
		}
	}

	logs, total, err := store.ListAuditLogs(h.DB, f)
	if err != nil {
		jsonError(w, "failed to query logs: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Return empty array instead of null when no results.
	if logs == nil {
		logs = []*model.AuditLog{}
	}

	jsonOK(w, model.ListResponse{
		Data:   logs,
		Total:  total,
		Limit:  f.Limit,
		Offset: f.Offset,
	})
}

// GetAuditLog handles GET /api/audit-logs/{id}
func (h *APIHandler) GetAuditLog(w http.ResponseWriter, r *http.Request) {
	// Extract the trailing ID from the path without external router dependency.
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	rawID := parts[len(parts)-1]

	id, err := strconv.ParseInt(rawID, 10, 64)
	if err != nil {
		jsonError(w, "invalid id", http.StatusBadRequest)
		return
	}

	entry, err := store.GetAuditLog(h.DB, id)
	if err != nil {
		jsonError(w, "failed to query log", http.StatusInternalServerError)
		return
	}
	if entry == nil {
		jsonError(w, "not found", http.StatusNotFound)
		return
	}

	jsonOK(w, entry)
}

// GetStats handles GET /api/stats
func (h *APIHandler) GetStats(w http.ResponseWriter, r *http.Request) {
	stats, err := store.GetStats(h.DB)
	if err != nil {
		jsonError(w, "failed to query stats", http.StatusInternalServerError)
		return
	}
	jsonOK(w, stats)
}

// ---- helpers ----------------------------------------------------------------

func jsonOK(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func jsonError(w http.ResponseWriter, msg string, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": msg}) //nolint:errcheck
}

func parseIntParam(r *http.Request, key string, defaultVal int) int {
	s := r.URL.Query().Get(key)
	if s == "" {
		return defaultVal
	}
	v, err := strconv.Atoi(s)
	if err != nil || v < 0 {
		return defaultVal
	}
	return v
}
