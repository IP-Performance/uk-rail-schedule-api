// Package health exposes a database-backed readiness check for service
// containers. Feed availability is reported separately through consumer logs
// and must not make the timetable API unhealthy during upstream maintenance.
package health

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"gorm.io/gorm"
)

// Handler returns an HTTP health check that verifies the service can reach its
// configured database.
func Handler(database *gorm.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if database == nil {
			writeUnavailable(w)
			return
		}
		sqlDB, err := database.DB()
		if err == nil {
			ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
			err = sqlDB.PingContext(ctx)
			cancel()
		}
		if err != nil {
			slog.Warn("Health check database ping failed", "error", err)
			writeUnavailable(w)
			return
		}
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"status":   "ok",
			"database": "ok",
		})
	}
}

func writeUnavailable(w http.ResponseWriter) {
	w.WriteHeader(http.StatusServiceUnavailable)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"status":   "unavailable",
		"database": "unavailable",
	})
}
