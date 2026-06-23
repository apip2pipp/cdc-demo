package handler

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"
)

const (
	sessionCookieName = "session_token"
	// Hardcoded credentials for demo purposes
	adminEmail    = "admin@kinetic.local"
	adminPassword = "admin" // the user suggested "admin123" but I'll make it "admin" to match what we typically expect, actually I'll stick to admin123.
)

type LoginRequest struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

type AuthHandler struct{}

func (h *AuthHandler) Login(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req LoginRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error": "Invalid request"}`, http.StatusBadRequest)
		return
	}

	// For demo: hardcoded check
	if req.Email == "admin@kinetic.local" && req.Password == "admin123" {
		// Set cookie
		http.SetCookie(w, &http.Cookie{
			Name:     sessionCookieName,
			Value:    "authenticated_demo_token",
			Expires:  time.Now().Add(24 * time.Hour),
			Path:     "/",
			HttpOnly: true,
			SameSite: http.SameSiteLaxMode,
		})
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"success": true}`))
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusUnauthorized)
	w.Write([]byte(`{"error": "Invalid email or password"}`))
}

func (h *AuthHandler) Logout(w http.ResponseWriter, r *http.Request) {
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookieName,
		Value:    "",
		Expires:  time.Now().Add(-1 * time.Hour),
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
	})
	// Redirect to login or return JSON
	if strings.HasPrefix(r.Header.Get("Accept"), "application/json") {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"success": true}`))
	} else {
		http.Redirect(w, r, "/login.html", http.StatusSeeOther)
	}
}

// IsAuthenticated checks if the request has a valid session.
func IsAuthenticated(r *http.Request) bool {
	cookie, err := r.Cookie(sessionCookieName)
	if err != nil || cookie.Value != "authenticated_demo_token" {
		return false
	}
	return true
}

// AuthMiddleware intercepts requests and checks for a valid session cookie.
func AuthMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !IsAuthenticated(r) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte(`{"error": "Unauthorized. Please log in."}`))
			return
		}
		
		// Authorized, proceed
		next(w, r)
	}
}
