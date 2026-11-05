package api

import (
	"context"
	"net/http"
	"strings"
	"time"

	"uk-rail-schedule-api/internal/schedule"
	"uk-rail-schedule-api/internal/store"
	internalsync "uk-rail-schedule-api/internal/sync"
	"uk-rail-schedule-api/internal/telemetry"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/render"
)

// ScheduleAPIResponse is the JSON envelope returned by the schedules endpoint.
type ScheduleAPIResponse struct {
	Headcode  string              `json:"headcode,omitempty"`
	Tiploc    string              `json:"tiploc,omitempty"`
	TrainUID  string              `json:"trainuid,omitempty"`
	Date      string              `json:"date"`
	Schedules []schedule.Schedule `json:"schedules"`
}

type MovementAPIResponse struct {
	Date            string                    `json:"date"`
	MatchConfidence string                    `json:"match_confidence,omitempty"`
	Movement        *TrustMovementAPIResponse `json:"movement"`
}

type TrustMovementAPIResponse struct {
	Headcode        string     `json:"headcode,omitempty"`
	TrainUID        string     `json:"train_uid,omitempty"`
	ServiceDate     string     `json:"service_date"`
	EventType       string     `json:"event_type"`
	ActualTime      *time.Time `json:"actual_time,omitempty"`
	PlannedTime     *time.Time `json:"planned_time,omitempty"`
	LocationStanox  string     `json:"location_stanox,omitempty"`
	Platform        string     `json:"platform,omitempty"`
	VariationStatus string     `json:"variation_status,omitempty"`
}

// ErrResponse is a renderable error for chi/render.
type ErrResponse struct {
	Err            error  `json:"-"`
	HTTPStatusCode int    `json:"-"`
	StatusText     string `json:"status"`
	AppCode        int64  `json:"code,omitempty"`
	ErrorText      string `json:"error,omitempty"`
}

func (e *ErrResponse) Render(w http.ResponseWriter, r *http.Request) error {
	render.Status(r, e.HTTPStatusCode)
	return nil
}

var ErrNotFound = &ErrResponse{HTTPStatusCode: 404, StatusText: "Resource not found."}
var ErrUnprocessable = &ErrResponse{HTTPStatusCode: 422, StatusText: "Unprocessable entity."}

// Handler holds dependencies for the JSON API handlers.
type Handler struct {
	Store            *store.Store
	ScheduleFeedFile string
	DataDir          string
}

func (h *Handler) SchedulesCtx(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		headcode := r.URL.Query().Get("headcode")
		tiploc := r.URL.Query().Get("tiploc")
		trainUID := r.URL.Query().Get("trainuid")

		date := time.Now().Format("2006-01-02")
		if r.URL.Query().Has("date") {
			date = r.URL.Query().Get("date")
		}

		toc := "any"
		if r.URL.Query().Has("toc") {
			toc = r.URL.Query().Get("toc")
		}

		schedules, err := h.Store.GetSchedules(headcode, trainUID, date, toc, tiploc, r.URL.Query().Get("hide_passed") == "true")
		if err != nil {
			telemetry.RecordError(r.Context(), "db")
			http.Error(w, err.Error(), 500)
			return
		}
		if len(schedules) == 0 {
			http.Error(w, http.StatusText(404), 404)
			return
		}

		resp := ScheduleAPIResponse{
			Headcode:  headcode,
			Tiploc:    tiploc,
			TrainUID:  trainUID,
			Date:      date,
			Schedules: schedules,
		}
		ctx := context.WithValue(r.Context(), "schedules", resp)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// resolveIdentifier maps the named identifier query parameters to the internal
// identifierType/identifier pair used by the store. Precedence: headcode →
// tiploc → trainuid. Returns ("headcode", "") if none are set.
func resolveIdentifier(headcode, tiploc, trainuid string) (identifierType, identifier string) {
	switch {
	case headcode != "":
		return "headcode", headcode
	case tiploc != "":
		return "tiploc", tiploc
	case trainuid != "":
		return "trainuid", trainuid
	default:
		return "headcode", ""
	}
}

// LegacySchedulesCtx is middleware for the legacy /schedules/{identifierType}/{identifier} route.
// It maps path params to the store query params and stores the result in context under "schedules".
//
// Supported identifierType values:
//   - "tiploc"   — filters by TIPLOC code (path identifier used as the TIPLOC filter)
//   - "uid"      — filters by CIF Train UID; optional "location" query param further filters by TIPLOC
//   - "headcode" — filters by signalling headcode
func (h *Handler) LegacySchedulesCtx(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		identifierType := chi.URLParam(r, "identifierType")
		identifier := chi.URLParam(r, "identifier")

		date := time.Now().Format("2006-01-02")
		if r.URL.Query().Has("date") {
			date = r.URL.Query().Get("date")
		}

		toc := "any"
		if r.URL.Query().Has("toc") {
			toc = r.URL.Query().Get("toc")
		}

		location := r.URL.Query().Get("location")

		var headcode, trainUID, tiplocId string
		switch identifierType {
		case "tiploc":
			tiplocId = identifier
		case "uid":
			trainUID = identifier
			tiplocId = location
		case "headcode":
			headcode = identifier
			tiplocId = location
		default:
			http.Error(w, "unknown identifier type", http.StatusBadRequest)
			return
		}

		schedules, err := h.Store.GetSchedules(headcode, trainUID, date, toc, tiplocId, false)
		if err != nil {
			telemetry.RecordError(r.Context(), "db")
			http.Error(w, err.Error(), 500)
			return
		}
		if len(schedules) == 0 {
			http.Error(w, http.StatusText(404), 404)
			return
		}

		resp := ScheduleAPIResponse{
			Headcode:  headcode,
			Tiploc:    tiplocId,
			TrainUID:  trainUID,
			Date:      date,
			Schedules: schedules,
		}
		ctx := context.WithValue(r.Context(), "schedules", resp)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (h *Handler) StatusCtx(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		status, err := h.Store.GetStatus()
		if err != nil {
			telemetry.RecordError(r.Context(), "db")
			http.Error(w, err.Error(), 500)
			return
		}
		ctx := context.WithValue(r.Context(), "status", status)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (h *Handler) GetSchedules(w http.ResponseWriter, r *http.Request) {
	schedules, ok := r.Context().Value("schedules").(ScheduleAPIResponse)
	if !ok {
		render.Render(w, r, ErrUnprocessable)
		return
	}
	render.JSON(w, r, schedules)
}

func (h *Handler) GetStatus(w http.ResponseWriter, r *http.Request) {
	status, ok := r.Context().Value("status").(store.APIStatus)
	if !ok {
		render.Render(w, r, ErrUnprocessable)
		return
	}
	render.JSON(w, r, status)
}

// GetMovement returns the latest stored ScotRail TRUST movement for a schedule
// UID or an unambiguous headcode on the requested service date.
func (h *Handler) GetMovement(w http.ResponseWriter, r *http.Request) {
	identifierType := chi.URLParam(r, "identifierType")
	identifier := chi.URLParam(r, "identifier")
	date := r.URL.Query().Get("date")
	if date == "" {
		date = time.Now().In(time.Local).Format("2006-01-02")
	}
	if _, err := time.Parse("2006-01-02", date); err != nil {
		http.Error(w, "date must be YYYY-MM-DD", http.StatusBadRequest)
		return
	}
	tiploc := strings.ToUpper(strings.TrimSpace(r.URL.Query().Get("location")))
	if len(tiploc) > 16 {
		http.Error(w, "location must be 16 characters or fewer", http.StatusBadRequest)
		return
	}
	eventTypes := strings.ToUpper(strings.TrimSpace(r.URL.Query().Get("events")))
	if len(eventTypes) > 40 {
		http.Error(w, "events must be 40 characters or fewer", http.StatusBadRequest)
		return
	}
	for _, eventType := range strings.Split(eventTypes, ",") {
		switch strings.TrimSpace(eventType) {
		case "", "DEPARTURE", "ARRIVAL", "PASS":
		default:
			http.Error(w, "events may contain only DEPARTURE, ARRIVAL, and PASS", http.StatusBadRequest)
			return
		}
	}
	movement, confidence, err := h.Store.GetLatestTrustMovement(identifierType, identifier, date, tiploc, eventTypes)
	if err != nil {
		telemetry.RecordError(r.Context(), "db")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	response := MovementAPIResponse{Date: date, MatchConfidence: confidence}
	if movement != nil {
		response.Movement = &TrustMovementAPIResponse{
			Headcode: movement.Headcode, TrainUID: movement.TrainUID,
			ServiceDate: movement.ServiceDate, EventType: movement.EventType,
			ActualTime: movement.ActualTimestamp, PlannedTime: movement.PlannedTimestamp,
			LocationStanox: movement.LocationStanox, Platform: movement.Platform,
			VariationStatus: movement.VariationStatus,
		}
	}
	render.JSON(w, r, response)
}

func (h *Handler) RunRefresh(w http.ResponseWriter, r *http.Request) {
	if internalsync.IsRefreshingDatabase() {
		w.WriteHeader(409)
		render.JSON(w, r, "Database already being refreshed. Please try again later")
		return
	}
	go internalsync.RefreshSchedules(h.ScheduleFeedFile, h.Store.DB, h.DataDir, false)
	w.WriteHeader(201)
	render.JSON(w, r, "Refreshing")
}
