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
