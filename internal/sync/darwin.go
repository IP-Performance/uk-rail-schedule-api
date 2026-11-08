package sync

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"
	"uk-rail-schedule-api/internal/config"
	"uk-rail-schedule-api/internal/schedule"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
	"gorm.io/gorm"
)

// ListenForDarwin consumes RDM's JSON-encoded Darwin Push Port and keeps only
// the latest actual/forecast arrival, departure, and pass timing per stop.
func ListenForDarwin(ctx context.Context, database *gorm.DB, settings config.DarwinConsumerConfig) {
	dialer := &kafka.Dialer{Timeout: 15 * time.Second, DualStack: true,
		TLS:           &tls.Config{MinVersion: tls.VersionTLS12},
		SASLMechanism: plain.Mechanism{Username: settings.Username, Password: settings.Password}}
	backoff := time.Second
	retentionDays := 7
	if days, err := strconv.Atoi(os.Getenv("TRUST_MOVEMENT_RETENTION_DAYS")); err == nil && days > 0 {
		retentionDays = days
	}
	lastPrune := time.Now()
	for ctx.Err() == nil {
		reader := kafka.NewReader(kafka.ReaderConfig{Brokers: settings.Brokers, Topic: settings.Topic,
			GroupID: settings.GroupID, Dialer: dialer, MinBytes: 1,
			MaxBytes: 10 * 1024 * 1024, MaxWait: time.Second, StartOffset: kafka.LastOffset})
		slog.Info("Starting Darwin Kafka consumer", "topic", settings.Topic)
		for ctx.Err() == nil {
			message, err := reader.FetchMessage(ctx)
			if err != nil {
				if ctx.Err() == nil {
					slog.Warn("Darwin Kafka read failed; reconnecting", "error", err)
				}
				break
			}
			if err := persistDarwinFrame(database, message.Value); err != nil {
				slog.Error("Could not persist Darwin message; reconnecting without committing offset", "error", err)
				break
			}
			if err := reader.CommitMessages(ctx, message); err != nil {
				slog.Warn("Could not commit Darwin Kafka offset; reconnecting", "error", err)
				break
			}
			if time.Since(lastPrune) >= 10*time.Minute {
				cutoff := time.Now().UTC().Add(-time.Duration(retentionDays) * 24 * time.Hour)
				result := database.Where("updated_at < ?", cutoff).Delete(&schedule.DarwinTiming{})
				if result.Error != nil {
					slog.Warn("Could not prune expired Darwin timings", "error", result.Error)
				} else if result.RowsAffected > 0 {
					slog.Info("Pruned expired Darwin timings", "rows", result.RowsAffected, "retention_days", retentionDays)
				}
				lastPrune = time.Now()
			}
		}
		_ = reader.Close()
		if ctx.Err() != nil {
			return
		}
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		backoff = min(backoff*2, time.Minute)
	}
}

func persistDarwinFrame(database *gorm.DB, payload []byte) error {
	root, err := decodeDarwinJSON(payload)
	if err != nil {
		return err
	}
	updates := make([]schedule.DarwinTiming, 0)
	collectDarwinTS(root, &updates)
	if len(updates) == 0 {
		return nil
	}
	return database.Transaction(func(tx *gorm.DB) error {
		scotrailUIDs := make(map[string]bool)
		for _, item := range updates {
			uidDate := item.TrainUID + ":" + item.ServiceDate
			isScotrail, checked := scotrailUIDs[uidDate]
			if !checked {
				var err error
				isScotrail, err = isScotrailSchedule(tx, item.TrainUID, item.ServiceDate)
				if err != nil {
					return err
				}
				scotrailUIDs[uidDate] = isScotrail
			}
			if !isScotrail {
				continue
			}
			var existing schedule.DarwinTiming
			err := tx.Where("train_uid = ? AND service_date = ? AND tiploc = ? AND event_type = ?",
				item.TrainUID, item.ServiceDate, item.Tiploc, item.EventType).First(&existing).Error
			if err == gorm.ErrRecordNotFound {
				if err := tx.Create(&item).Error; err != nil {
					return fmt.Errorf("insert Darwin stop timing: %w", err)
				}
				continue
			}
			if err != nil {
				return fmt.Errorf("find Darwin stop timing: %w", err)
			}
			existing.RID = item.RID
			if item.ActualSet {
				existing.ActualTime = item.ActualTime
			}
			if item.ActualRemoved {
				existing.ActualTime = nil
			}
			if item.ForecastSet {
				existing.ForecastTime = item.ForecastTime
			}
			existing.UpdatedAt = item.UpdatedAt
			if err := tx.Save(&existing).Error; err != nil {
				return fmt.Errorf("update Darwin stop timing: %w", err)
			}
		}
		return nil
	})
}

func isScotrailSchedule(tx *gorm.DB, trainUID, serviceDate string) (bool, error) {
	date, err := time.Parse("2006-01-02", serviceDate)
	if err != nil {
		return false, nil
	}
	weekday := int(date.Weekday())
	if weekday == 0 {
		weekday = 7
	}
	start := date.Unix()
	var count int64
	err = tx.Model(&schedule.Schedule{}).
		Where("cif_train_uid = ? AND atoc_code = ?", trainUID, "SR").
		Where("schedule_start_date_ts <= ? AND schedule_end_date_ts >= ?", start+86399, start).
		Where("substr(schedule_days_runs, ?, 1) = ?", weekday, "1").
		Count(&count).Error
	if err != nil {
		return false, fmt.Errorf("check Darwin ScotRail timetable UID: %w", err)
	}
	return count > 0, nil
}

func decodeDarwinJSON(payload []byte) (any, error) {
	var root any
	if err := json.Unmarshal(payload, &root); err != nil {
		return nil, fmt.Errorf("decode Darwin Kafka envelope: %w", err)
	}
	// RDM's reference client wraps the native JSON message in a `bytes` string.
	for depth := 0; depth < 3; depth++ {
		m, ok := root.(map[string]any)
		if !ok {
			break
		}
		wrapped, ok := m["bytes"].(string)
		if !ok {
			break
		}
		if err := json.Unmarshal([]byte(wrapped), &root); err != nil {
			return nil, fmt.Errorf("decode Darwin bytes payload: %w", err)
		}
	}
	return root, nil
}

func collectDarwinTS(value any, updates *[]schedule.DarwinTiming) {
	switch node := value.(type) {
	case map[string]any:
		for key, child := range node {
			if strings.EqualFold(key, "TS") {
				collectOneDarwinTS(child, updates)
				continue
			}
			collectDarwinTS(child, updates)
		}
	case []any:
		for _, child := range node {
			collectDarwinTS(child, updates)
		}
	}
}

func collectOneDarwinTS(value any, updates *[]schedule.DarwinTiming) {
	root, ok := value.(map[string]any)
	if !ok {
		return
	}
	uid := strings.ToUpper(darwinString(root, "uid"))
	serviceDate := darwinString(root, "ssd")
	if uid == "" || len(serviceDate) != 10 {
		return
	}
	base := schedule.DarwinTiming{TrainUID: uid, ServiceDate: serviceDate, RID: darwinString(root, "rid"), UpdatedAt: time.Now().UTC()}
	walkDarwinLocations(root, base, updates)
}

func walkDarwinLocations(value any, base schedule.DarwinTiming, updates *[]schedule.DarwinTiming) {
	switch node := value.(type) {
	case map[string]any:
		if tpl := strings.ToUpper(darwinString(node, "tpl")); tpl != "" {
			for _, event := range []string{"arr", "dep", "pass"} {
				timing, ok := darwinMap(node, event)
				if !ok {
					continue
				}
				actualRaw := darwinString(timing, "at")
				forecastRaw := firstNonEmptyDarwin(darwinString(timing, "et"), darwinString(timing, "wet"))
				actual := darwinTime(actualRaw, base.ServiceDate)
				forecast := darwinTime(forecastRaw, base.ServiceDate)
				actualSet := darwinHasKey(timing, "at") || darwinHasKey(timing, "atRemoved")
				forecastSet := darwinHasKey(timing, "et") || darwinHasKey(timing, "wet")
				if !actualSet && !forecastSet {
					continue
				}
				item := base
				item.Tiploc = tpl
				item.EventType = strings.ToUpper(event)
				item.ActualTime = actual
				item.ForecastTime = forecast
				item.ActualSet = actualSet
				item.ForecastSet = forecastSet
				if removed, _ := timing["atRemoved"].(bool); removed {
					item.ActualRemoved = true
				}
				*updates = append(*updates, item)
			}
		}
		for key, child := range node {
			if !strings.EqualFold(key, "TS") {
				walkDarwinLocations(child, base, updates)
			}
		}
	case []any:
		for _, child := range node {
			walkDarwinLocations(child, base, updates)
		}
	}
}

func darwinHasKey(m map[string]any, key string) bool {
	for candidate := range m {
		if strings.EqualFold(candidate, key) {
			return true
		}
	}
	return false
}

func darwinMap(m map[string]any, key string) (map[string]any, bool) {
	for candidate, value := range m {
		if strings.EqualFold(candidate, key) {
			result, ok := value.(map[string]any)
			return result, ok
		}
	}
	return nil, false
}

func darwinString(m map[string]any, key string) string {
	for candidate, value := range m {
		if strings.EqualFold(candidate, key) {
			if text, ok := value.(string); ok {
				return strings.TrimSpace(text)
			}
			if value != nil {
				return strings.TrimSpace(fmt.Sprint(value))
			}
		}
	}
	return ""
}

func firstNonEmptyDarwin(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func darwinTime(value, serviceDate string) *time.Time {
	if value == "" {
		return nil
	}
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02T15:04:05", "2006-01-02 15:04:05"} {
		if parsed, err := time.Parse(layout, value); err == nil {
			parsed = parsed.UTC()
			return &parsed
		}
	}
	loc, err := time.LoadLocation("Europe/London")
	if err != nil {
		loc = time.UTC
	}
	date, err := time.ParseInLocation("2006-01-02", serviceDate, loc)
	if err != nil {
		return nil
	}
	clock := strings.ReplaceAll(value, ":", "")
	if len(clock) < 4 {
		return nil
	}
	var hour, minute int
	if _, err := fmt.Sscanf(clock[:4], "%02d%02d", &hour, &minute); err != nil || hour > 47 || minute > 59 {
		return nil
	}
	parsed := date.Add(time.Duration(hour)*time.Hour + time.Duration(minute)*time.Minute).UTC()
	return &parsed
}
