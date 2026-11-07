package store

import (
	"errors"
	"fmt"
	"strings"

	"uk-rail-schedule-api/internal/schedule"

	"gorm.io/gorm"
)

// GetLatestTrustMovement returns the latest ScotRail movement for a schedule
// UID, or for an unambiguous headcode/service-date combination. When a TIPLOC
// and event types are supplied, the event must also match that timetable stop.
func (s *Store) GetLatestTrustMovement(identifierType, identifier, serviceDate, tiplocCode, eventTypes string) (*schedule.TrustMovement, string, error) {
	if s.DB == nil {
		return nil, "", errors.New("db is nil")
	}
	query := s.DB.Model(&schedule.TrustMovement{}).Where("toc_id = ? AND service_date = ?", "60", serviceDate)
	if tiplocCode != "" {
		var tiploc schedule.Tiploc
		if err := s.DB.Where("tiploc_code = ?", tiplocCode).First(&tiploc).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return nil, "", nil
			}
			return nil, "", fmt.Errorf("lookup TRUST station TIPLOC: %w", err)
		}
		if strings.TrimSpace(tiploc.Stanox) == "" {
			return nil, "", nil
		}
		query = query.Where("location_stanox = ?", strings.TrimSpace(tiploc.Stanox))
	}
	if eventTypes != "" {
		types := make([]string, 0, 3)
		for _, eventType := range strings.Split(eventTypes, ",") {
			if eventType = strings.ToUpper(strings.TrimSpace(eventType)); eventType != "" {
				types = append(types, eventType)
			}
		}
		if len(types) == 0 {
			return nil, "", nil
		}
		query = query.Where("UPPER(event_type) IN ?", types)
	}
	confidence := "schedule_uid"
	switch identifierType {
	case "trainuid":
		query = query.Where("train_uid = ?", identifier)
	case "headcode":
		var trainCount int64
		base := s.DB.Model(&schedule.TrustMovement{}).Where("toc_id = ? AND service_date = ? AND headcode = ?", "60", serviceDate, identifier)
		if err := base.Distinct("train_id").Count(&trainCount).Error; err != nil {
			return nil, "", fmt.Errorf("count TRUST headcode candidates: %w", err)
		}
		if trainCount == 0 {
			return nil, "", nil
		}
		if trainCount > 1 {
			return nil, "ambiguous_headcode", nil
		}
		query = query.Where("headcode = ?", identifier)
		confidence = "unique_headcode"
	default:
		return nil, "", fmt.Errorf("unsupported TRUST movement identifier type %q", identifierType)
	}

	var movement schedule.TrustMovement
	err := query.Order("actual_timestamp DESC").Order("id DESC").First(&movement).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, "", nil
	}
	if err != nil {
		return nil, "", fmt.Errorf("query latest TRUST movement: %w", err)
	}
	return &movement, confidence, nil
}

// GetTrustMovementRoute returns the retained movement reports for one train
// service, ordered by actual time. Headcode and UID lookups are rejected when
// they map to multiple train activations on the same service date.
func (s *Store) GetTrustMovementRoute(identifierType, identifier, serviceDate string) ([]schedule.TrustMovement, string, error) {
	if s.DB == nil {
		return nil, "", errors.New("db is nil")
	}
	buildQuery := func() *gorm.DB {
		query := s.DB.Model(&schedule.TrustMovement{}).
			Where("toc_id = ? AND service_date = ?", "60", serviceDate)
		switch identifierType {
		case "trainuid":
			return query.Where("train_uid = ?", identifier)
		case "headcode":
			return query.Where("headcode = ?", identifier)
		default:
			return query
		}
	}
	confidence := "schedule_uid"
	switch identifierType {
	case "trainuid":
	case "headcode":
		confidence = "unique_headcode"
	default:
		return nil, "", fmt.Errorf("unsupported movement identifier type %q", identifierType)
	}

	var trainCount int64
	if err := buildQuery().Distinct("train_id").Count(&trainCount).Error; err != nil {
		return nil, "", fmt.Errorf("count TRUST route candidates: %w", err)
	}
	if trainCount > 1 {
		if identifierType == "trainuid" {
			return []schedule.TrustMovement{}, "ambiguous_trainuid", nil
		}
		return []schedule.TrustMovement{}, "ambiguous_headcode", nil
	}
	if trainCount == 0 {
		return []schedule.TrustMovement{}, confidence, nil
	}

	var movements []schedule.TrustMovement
	err := buildQuery().Where("actual_timestamp IS NOT NULL").
		Order("actual_timestamp ASC").Order("id ASC").Find(&movements).Error
	if err != nil {
		return nil, "", fmt.Errorf("query TRUST route movements: %w", err)
	}
	return movements, confidence, nil
}

// GetTDPositions returns the current berth occupancy records for a headcode.
// A headcode may temporarily appear in more than one area during a handover.
func (s *Store) GetTDPositions(headcode string) ([]schedule.TDPosition, error) {
	var positions []schedule.TDPosition
	if s.DB == nil {
		return positions, errors.New("db is nil")
	}
	err := s.DB.Where("headcode = ?", strings.ToUpper(strings.TrimSpace(headcode))).
		Order("event_at DESC").Order("area_id").Order("berth").Find(&positions).Error
	if err != nil {
		return nil, fmt.Errorf("query TD positions: %w", err)
	}
	return positions, nil
}
