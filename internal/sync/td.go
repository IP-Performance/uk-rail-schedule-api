package sync

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"
	"uk-rail-schedule-api/internal/config"
	"uk-rail-schedule-api/internal/schedule"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type tdEnvelope struct {
	CA *tdMessage `json:"CA_MSG"`
	CB *tdMessage `json:"CB_MSG"`
	CC *tdMessage `json:"CC_MSG"`
}

type tdMessage struct {
	Time   json.RawMessage `json:"time"`
	AreaID string          `json:"area_id"`
	Type   string          `json:"msg_type"`
	From   string          `json:"from"`
	To     string          `json:"to"`
	Descr  string          `json:"descr"`
}

type tdEvent struct {
	MessageType string
	Message     tdMessage
}

// ListenForTDPositions consumes Train Describer C-class messages from the
// configured Kafka topic and keeps only current berth occupancy state.
func ListenForTDPositions(ctx context.Context, database *gorm.DB, settings config.TDConsumerConfig) {
	dialer := &kafka.Dialer{
		Timeout:       15 * time.Second,
		DualStack:     true,
		TLS:           &tls.Config{MinVersion: tls.VersionTLS12},
		SASLMechanism: plain.Mechanism{Username: settings.Username, Password: settings.Password},
	}
	backoff := time.Second
	for ctx.Err() == nil {
		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers: settings.Brokers, Topic: settings.Topic, GroupID: settings.GroupID,
			Dialer: dialer, MinBytes: 1, MaxBytes: 10 * 1024 * 1024,
			MaxWait: time.Second, StartOffset: kafka.LastOffset,
		})
		slog.Info("Starting Train Describer Kafka consumer", "topic", settings.Topic)
		processed := 0
		for ctx.Err() == nil {
			message, err := reader.FetchMessage(ctx)
			if err != nil {
				if ctx.Err() == nil {
					slog.Warn("Train Describer Kafka read failed; reconnecting", "error", err)
				}
				break
			}
			if err := persistTDFrame(database, message.Value); err != nil {
				slog.Error("Could not persist Train Describer message; reconnecting without committing offset", "error", err)
				break
			}
			if err := reader.CommitMessages(ctx, message); err != nil {
				slog.Warn("Could not commit Train Describer Kafka offset; reconnecting", "error", err)
				break
			}
			processed++
			if processed == 1 || processed%10000 == 0 {
				slog.Info("Train Describer messages consumed", "count", processed)
			}
		}
		if err := reader.Close(); err != nil && ctx.Err() == nil {
			slog.Warn("Could not close Train Describer Kafka reader", "error", err)
		}
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

func persistTDFrame(database *gorm.DB, payload []byte) error {
	events, err := decodeTDFrame(payload)
	if err != nil {
		return err
	}
	if len(events) == 0 {
		return nil
	}
	return database.Transaction(func(tx *gorm.DB) error {
		for _, event := range events {
			if err := applyTDEvent(tx, event); err != nil {
				return err
			}
		}
		return nil
	})
}

func decodeTDFrame(payload []byte) ([]tdEvent, error) {
	trimmed := strings.TrimSpace(string(payload))
	if trimmed == "" || trimmed == "null" {
		return nil, nil
	}
	items := []json.RawMessage{payload}
	if strings.HasPrefix(trimmed, "[") {
		if err := json.Unmarshal(payload, &items); err != nil {
			return nil, fmt.Errorf("decode Train Describer message batch: %w", err)
		}
	}
	events := make([]tdEvent, 0, len(items))
	for _, item := range items {
		var envelope tdEnvelope
		if err := json.Unmarshal(item, &envelope); err != nil {
			return nil, fmt.Errorf("decode Train Describer event: %w", err)
		}
		switch {
		case envelope.CA != nil:
			events = append(events, tdEvent{MessageType: "CA", Message: *envelope.CA})
		case envelope.CB != nil:
			events = append(events, tdEvent{MessageType: "CB", Message: *envelope.CB})
		case envelope.CC != nil:
			events = append(events, tdEvent{MessageType: "CC", Message: *envelope.CC})
		}
	}
	return events, nil
}

func applyTDEvent(tx *gorm.DB, event tdEvent) error {
	message := event.Message
	areaID := strings.ToUpper(strings.TrimSpace(message.AreaID))
	headcode := strings.ToUpper(strings.TrimSpace(message.Descr))
	if areaID == "" || !validTDHeadcode(headcode) {
		return nil
	}
	eventAt := parseTDTime(message.Time)
	if eventAt.IsZero() {
		eventAt = time.Now().UTC()
	}
	switch event.MessageType {
	case "CA":
		from := strings.ToUpper(strings.TrimSpace(message.From))
		if from != "" {
			if err := tx.Where("area_id = ? AND berth = ?", areaID, from).
				Delete(&schedule.TDPosition{}).Error; err != nil {
				return fmt.Errorf("clear stepped-from berth: %w", err)
			}
		}
		return upsertTDPosition(tx, areaID, strings.ToUpper(strings.TrimSpace(message.To)), headcode, eventAt)
	case "CB":
		from := strings.ToUpper(strings.TrimSpace(message.From))
		if from == "" {
			return nil
		}
		return tx.Where("area_id = ? AND berth = ? AND headcode = ?", areaID, from, headcode).
			Delete(&schedule.TDPosition{}).Error
	case "CC":
		return upsertTDPosition(tx, areaID, strings.ToUpper(strings.TrimSpace(message.To)), headcode, eventAt)
	default:
		return nil
	}
}

func upsertTDPosition(tx *gorm.DB, areaID, berth, headcode string, eventAt time.Time) error {
	if areaID == "" || berth == "" {
		return nil
	}
	position := schedule.TDPosition{
		AreaID: areaID, Berth: berth, Headcode: headcode,
		EventAt: eventAt.UTC(), UpdatedAt: time.Now().UTC(),
	}
	// A headcode should occupy one berth per TD area. Remove any older state in
	// the area before recording its new berth, including state left by a missed
	// cancel or step message.
	if err := tx.Where("area_id = ? AND headcode = ?", areaID, headcode).
		Delete(&schedule.TDPosition{}).Error; err != nil {
		return fmt.Errorf("clear previous TD berth for headcode: %w", err)
	}
	return tx.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "area_id"}, {Name: "berth"}},
		DoUpdates: clause.AssignmentColumns([]string{"headcode", "event_at", "updated_at"}),
	}).Create(&position).Error
}

func parseTDTime(raw json.RawMessage) time.Time {
	value := strings.TrimSpace(string(raw))
	value = strings.Trim(value, `"`)
	milliseconds, err := strconv.ParseInt(value, 10, 64)
	if err != nil || milliseconds <= 0 {
		return time.Time{}
	}
	return time.UnixMilli(milliseconds).UTC()
}

func validTDHeadcode(value string) bool {
	if len(value) != 4 {
		return false
	}
	for _, char := range value {
		if !((char >= 'A' && char <= 'Z') || (char >= '0' && char <= '9')) {
			return false
		}
	}
	return true
}
