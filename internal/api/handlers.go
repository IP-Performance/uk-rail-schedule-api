package api

import (
	"context"
	"net/http"
	"regexp"
	"strconv"
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

type MovementRouteAPIResponse struct {
	Date            string                     `json:"date"`
	MatchConfidence string                     `json:"match_confidence,omitempty"`
	Movements       []TrustMovementAPIResponse `json:"movements"`
}

type TrustMovementAPIResponse struct {
	Headcode        string     `json:"headcode,omitempty"`
	TrainUID        string     `json:"train_uid,omitempty"`
	ServiceDate     string     `json:"service_date"`
	EventType       string     `json:"event_type"`
	ActualTime      *time.Time `json:"actual_time,omitempty"`
	PlannedTime     *time.Time `json:"planned_time,omitempty"`
	LocationStanox  string     `json:"location_stanox,omitempty"`
	LocationTIPLOC  string     `json:"location_tiploc,omitempty"`
	Platform        string     `json:"platform,omitempty"`
	VariationStatus string     `json:"variation_status,omitempty"`
	ForecastTime    *time.Time `json:"forecast_time,omitempty"`
	Source          string     `json:"source,omitempty"`
}

type TDPositionAPIResponse struct {
	Headcode   string    `json:"headcode"`
	AreaID     string    `json:"area_id"`
	Berth      string    `json:"berth"`
	Stanox     string    `json:"stanox,omitempty"`
	TIPLOC     string    `json:"tiploc,omitempty"`
	Location   string    `json:"location,omitempty"`
	EventTime  time.Time `json:"event_time"`
	AgeSeconds int64     `json:"age_seconds"`
}

type DarwinTimingAPIResponse struct {
	TrainUID       string     `json:"train_uid"`
	ServiceDate    string     `json:"service_date"`
	LocationTIPLOC string     `json:"location_tiploc"`
	EventType      string     `json:"event_type"`
	ActualTime     *time.Time `json:"actual_time,omitempty"`
	ForecastTime   *time.Time `json:"forecast_time,omitempty"`
	Source         string     `json:"source"`
}

// ErrResponse is a renderable error for chi/render.
type ErrResponse struct {
	Err            error  `json:"-"`
	HTTPStatusCode int    `json:"-"`
	StatusText     string `json:"status"`
	AppCode        int64  `json:"code,omitempty"`
	ErrorText      string `json:"error,omitempty"`
}

// Render handles the render behavior.
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

// SchedulesCtx handles the schedules ctx behavior.
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
// resolveIdentifier handles the resolve identifier behavior.
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
// LegacySchedulesCtx handles the legacy schedules ctx behavior.
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

// StatusCtx handles the status ctx behavior.
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

// GetSchedules retrieves the get schedules behavior.
func (h *Handler) GetSchedules(w http.ResponseWriter, r *http.Request) {
	schedules, ok := r.Context().Value("schedules").(ScheduleAPIResponse)
	if !ok {
		render.Render(w, r, ErrUnprocessable)
		return
	}
	render.JSON(w, r, schedules)
}

// GetStatus retrieves the get status behavior.
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
	if identifierType == "trainuid" {
		darwin, err := h.darwinRouteMovements(identifier, date)
		if err != nil {
			telemetry.RecordError(r.Context(), "db")
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		for _, item := range darwin {
			if item.LocationTIPLOC != tiploc || !apiEventAllowed(item.EventType, eventTypes) {
				continue
			}
			if response.Movement == nil || darwinTimingNewer(item, *response.Movement) {
				response.Movement = &item
				response.MatchConfidence = "schedule_uid"
			}
		}
	}
	render.JSON(w, r, response)
}

// GetMovementRoute returns all retained TRUST reports for one service so a
// client can place them against the service's ordered timetable locations.
func (h *Handler) GetMovementRoute(w http.ResponseWriter, r *http.Request) {
	identifierType := chi.URLParam(r, "identifierType")
	identifier := strings.TrimSpace(chi.URLParam(r, "identifier"))
	if identifierType != "trainuid" && identifierType != "headcode" {
		http.Error(w, "identifierType must be trainuid or headcode", http.StatusBadRequest)
		return
	}
	if identifier == "" || len(identifier) > 32 {
		http.Error(w, "identifier must contain 1 to 32 characters", http.StatusBadRequest)
		return
	}
	if identifierType == "headcode" && !regexp.MustCompile(`(?i)^[A-Z0-9]{4}$`).MatchString(identifier) {
		http.Error(w, "headcode must contain four letters or digits", http.StatusBadRequest)
		return
	}
	if identifierType == "trainuid" && !regexp.MustCompile(`(?i)^[A-Z0-9]{1,16}$`).MatchString(identifier) {
		http.Error(w, "trainuid must contain only letters and digits", http.StatusBadRequest)
		return
	}
	location, err := time.LoadLocation("Europe/London")
	if err != nil {
		http.Error(w, "could not load service timezone", http.StatusInternalServerError)
		return
	}
	date := r.URL.Query().Get("date")
	if date == "" {
		date = time.Now().In(location).Format("2006-01-02")
	}
	if _, err := time.Parse("2006-01-02", date); err != nil {
		http.Error(w, "date must be YYYY-MM-DD", http.StatusBadRequest)
		return
	}

	movements, confidence, err := h.Store.GetTrustMovementRoute(identifierType, identifier, date)
	if err != nil {
		telemetry.RecordError(r.Context(), "db")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	response := MovementRouteAPIResponse{
		Date: date, MatchConfidence: confidence,
		Movements: make([]TrustMovementAPIResponse, 0, len(movements)),
	}
	for _, movement := range movements {
		response.Movements = append(response.Movements, TrustMovementAPIResponse{
			Headcode: movement.Headcode, TrainUID: movement.TrainUID,
			ServiceDate: movement.ServiceDate, EventType: movement.EventType,
			ActualTime: movement.ActualTimestamp, PlannedTime: movement.PlannedTimestamp,
			LocationStanox: movement.LocationStanox, Platform: movement.Platform,
			VariationStatus: movement.VariationStatus,
		})
	}
	if identifierType == "trainuid" {
		darwin, err := h.darwinRouteMovements(identifier, date)
		if err != nil {
			telemetry.RecordError(r.Context(), "db")
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		response.Movements = append(response.Movements, darwin...)
	}
	render.JSON(w, r, response)
}

// darwinRouteMovements handles the darwin route movements behavior.
func (h *Handler) darwinRouteMovements(trainUID, serviceDate string) ([]TrustMovementAPIResponse, error) {
	timings, err := h.Store.GetDarwinTimings(trainUID, serviceDate)
	if err != nil {
		return nil, err
	}
	schedules, err := h.Store.GetSchedules("", trainUID, serviceDate, "any", "", false)
	if err != nil {
		return nil, err
	}
	result := make([]TrustMovementAPIResponse, 0, len(timings))
	for _, timing := range timings {
		eventType := strings.ToUpper(timing.EventType)
		var planned *time.Time
		for _, timetable := range schedules {
			for _, stop := range timetable.ScheduleLocation {
				if !strings.EqualFold(stop.TiplocCode, timing.Tiploc) {
					continue
				}
				var clock string
				switch eventType {
				case "ARR":
					clock = firstScheduleClock(stop.PublicArrival, stop.Arrival)
				case "DEP":
					clock = firstScheduleClock(stop.PublicDeparture, stop.Departure)
				case "PASS":
					clock = stop.Pass
				}
				planned = scheduleDateTime(serviceDate, clock)
				if planned != nil {
					break
				}
			}
			if planned != nil {
				break
			}
		}
		name := map[string]string{"ARR": "ARRIVAL", "DEP": "DEPARTURE", "PASS": "PASS"}[eventType]
		if name == "" {
			continue
		}
		result = append(result, TrustMovementAPIResponse{TrainUID: timing.TrainUID, ServiceDate: timing.ServiceDate,
			EventType: name, ActualTime: timing.ActualTime, PlannedTime: planned, ForecastTime: timing.ForecastTime,
			LocationTIPLOC: timing.Tiploc, Source: "DARWIN"})
	}
	return result, nil
}

// firstScheduleClock handles the first schedule clock behavior.
func firstScheduleClock(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

// scheduleDateTime handles the schedule date time behavior.
func scheduleDateTime(serviceDate, scheduleClock string) *time.Time {
	digits := regexp.MustCompile(`[^0-9]`).ReplaceAllString(scheduleClock, "")
	if len(digits) < 4 {
		return nil
	}
	hour, err := strconv.Atoi(digits[:2])
	if err != nil || hour > 47 {
		return nil
	}
	minute, err := strconv.Atoi(digits[2:4])
	if err != nil || minute > 59 {
		return nil
	}
	zone, err := time.LoadLocation("Europe/London")
	if err != nil {
		zone = time.UTC
	}
	date, err := time.ParseInLocation("2006-01-02", serviceDate, zone)
	if err != nil {
		return nil
	}
	planned := date.Add(time.Duration(hour)*time.Hour + time.Duration(minute)*time.Minute).UTC()
	return &planned
}

// apiEventAllowed handles the api event allowed behavior.
func apiEventAllowed(eventType, requested string) bool {
	if requested == "" {
		return true
	}
	for _, part := range strings.Split(requested, ",") {
		if strings.EqualFold(strings.TrimSpace(part), eventType) {
			return true
		}
	}
	return false
}

// darwinTimingNewer handles the darwin timing newer behavior.
func darwinTimingNewer(candidate, current TrustMovementAPIResponse) bool {
	candidateTime := candidate.ActualTime
	if candidateTime == nil {
		candidateTime = candidate.ForecastTime
	}
	currentTime := current.ActualTime
	if currentTime == nil {
		currentTime = current.ForecastTime
	}
	if candidateTime == nil {
		return false
	}
	return currentTime == nil || candidateTime.After(*currentTime)
}

// GetTDPositions returns the latest stored berth positions for a train
// description. TD positions are signalling locations, not GPS coordinates.
func (h *Handler) GetTDPositions(w http.ResponseWriter, r *http.Request) {
	headcode := strings.ToUpper(strings.TrimSpace(chi.URLParam(r, "headcode")))
	if !regexp.MustCompile(`^[A-Z0-9]{4}$`).MatchString(headcode) {
		http.Error(w, "headcode must contain four letters or digits", http.StatusBadRequest)
		return
	}
	positions, err := h.Store.GetTDPositions(headcode)
	if err != nil {
		telemetry.RecordError(r.Context(), "db")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	now := time.Now().UTC()
	response := make([]TDPositionAPIResponse, 0, len(positions))
	for _, position := range positions {
		age := int64(now.Sub(position.EventAt).Seconds())
		if age < 0 {
			age = 0
		}
		response = append(response, TDPositionAPIResponse{
			Headcode: position.Headcode, AreaID: position.AreaID,
			Berth: position.Berth, EventTime: position.EventAt,
			AgeSeconds: age,
			Stanox:     position.Stanox,
		})
		if position.Stanox != "" {
			var place schedule.Tiploc
			if err := h.Store.DB.Where("stanox = ?", position.Stanox).Order("tiploc_code").First(&place).Error; err == nil {
				response[len(response)-1].TIPLOC = place.TiplocCode
				response[len(response)-1].Location = place.TpsDescription
				if response[len(response)-1].Location == "" {
					response[len(response)-1].Location = place.Description
				}
			}
		}
	}
	render.JSON(w, r, map[string]interface{}{
		"headcode":  headcode,
		"positions": response,
	})
}

// GetDarwinRoute returns Darwin actual and forecast timings for a UID/date.
func (h *Handler) GetDarwinRoute(w http.ResponseWriter, r *http.Request) {
	trainUID := strings.ToUpper(strings.TrimSpace(chi.URLParam(r, "trainuid")))
	if !regexp.MustCompile(`^[A-Z0-9]{1,16}$`).MatchString(trainUID) {
		http.Error(w, "trainuid must contain only letters and digits", http.StatusBadRequest)
		return
	}
	loc, err := time.LoadLocation("Europe/London")
	if err != nil {
		http.Error(w, "could not load service timezone", http.StatusInternalServerError)
		return
	}
	date := r.URL.Query().Get("date")
	if date == "" {
		date = time.Now().In(loc).Format("2006-01-02")
	}
	if _, err := time.Parse("2006-01-02", date); err != nil {
		http.Error(w, "date must be YYYY-MM-DD", http.StatusBadRequest)
		return
	}
	timings, err := h.Store.GetDarwinTimings(trainUID, date)
	if err != nil {
		telemetry.RecordError(r.Context(), "db")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	response := make([]DarwinTimingAPIResponse, 0, len(timings))
	for _, timing := range timings {
		response = append(response, DarwinTimingAPIResponse{TrainUID: timing.TrainUID, ServiceDate: timing.ServiceDate,
			LocationTIPLOC: timing.Tiploc, EventType: strings.ToUpper(timing.EventType), ActualTime: timing.ActualTime,
			ForecastTime: timing.ForecastTime, Source: "DARWIN"})
	}
	render.JSON(w, r, map[string]interface{}{"date": date, "train_uid": trainUID, "timings": response})
}

// RunRefresh runs the run refresh behavior.
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
