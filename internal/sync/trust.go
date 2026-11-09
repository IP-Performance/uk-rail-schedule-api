package sync

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"
	"uk-rail-schedule-api/internal/schedule"

	gostomp "github.com/go-stomp/stomp/v3"
	"gorm.io/gorm"
)

const scotrailTrustTocID = "60"

var trustLondonLocation = func() *time.Location {
	loc, err := time.LoadLocation("Europe/London")
	if err != nil {
		return time.UTC
	}
	return loc
}()

type trustWireMessage struct {
	Header struct {
		MessageType string `json:"msg_type"`
	} `json:"header"`
	Body struct {
		TrainID            string `json:"train_id"`
		CurrentTrainID     string `json:"current_train_id"`
		RevisedTrainID     string `json:"revised_train_id"`
		TOCID              string `json:"toc_id"`
		DivisionCode       string `json:"division_code"`
		TrainUID           string `json:"train_uid"`
		TrainServiceCode   string `json:"train_service_code"`
		TPOriginTimestamp  string `json:"tp_origin_timestamp"`
		OriginDepTimestamp string `json:"origin_dep_timestamp"`
		EventType          string `json:"event_type"`
		ActualTimestamp    string `json:"actual_timestamp"`
		PlannedTimestamp   string `json:"planned_timestamp"`
		EventTimestamp     string `json:"event_timestamp"`
		LocationStanox     string `json:"loc_stanox"`
		ReportingStanox    string `json:"reporting_stanox"`
		Platform           string `json:"platform"`
		VariationStatus    string `json:"variation_status"`
		TrainTerminated    string `json:"train_terminated"`
	} `json:"body"`
}

// ListenForTrustMovements receives the national TRUST topic but persists only
// ScotRail activations and movement events. It shares the schedule API's DB.
func ListenForTrustMovements(database *gorm.DB, stompURL, login, password string) {
	retentionDays := 7
	if value := os.Getenv("TRUST_MOVEMENT_RETENTION_DAYS"); value != "" {
		if parsed, err := strconv.Atoi(value); err == nil && parsed > 0 {
			retentionDays = parsed
		}
	}
	retention := time.Duration(retentionDays) * 24 * time.Hour
	backoff := time.Second
	const maxBackoff = time.Minute
	lastPrune := time.Now()

	for {
		conn, err := gostomp.Dial("tcp", stompURL,
			gostomp.ConnOpt.HeartBeat(10*time.Minute, 10*time.Minute),
			gostomp.ConnOpt.Login(login, password),
			gostomp.ConnOpt.Header("client-id", "uk-rail-schedule-api-trust-scotrail-v1"),
		)
		if err != nil {
			slog.Warn("TRUST STOMP connection failed; retrying", "retry_after", backoff, "error", err)
			time.Sleep(backoff)
			backoff = min(backoff*2, maxBackoff)
			continue
		}

		sub, err := conn.Subscribe("/topic/TRAIN_MVT_ALL_TOC", gostomp.AckClientIndividual,
			gostomp.SubscribeOpt.Header("activemq.subscriptionName", "uk-rail-schedule-api-trust-scotrail-v1"),
		)
		if err != nil {
			slog.Error("TRUST STOMP subscription failed", "error", err)
			_ = conn.Disconnect()
			time.Sleep(backoff)
			backoff = min(backoff*2, maxBackoff)
			continue
		}
		slog.Info("Subscribed to Network Rail TRUST movements", "topic", "TRAIN_MVT_ALL_TOC", "stored_toc_id", scotrailTrustTocID)
		backoff = time.Second

		for msg := range sub.C {
			if msg == nil || msg.Err != nil || len(msg.Body) == 0 {
				if msg != nil && msg.Err != nil {
					slog.Warn("TRUST subscription ended", "error", msg.Err)
				}
				break
			}
			if err := persistTrustFrame(database, msg.Body); err != nil {
				slog.Error("Could not persist TRUST frame; reconnecting without acknowledgement", "error", err)
				break
			}
			if err := conn.Ack(msg); err != nil {
				slog.Error("Could not acknowledge persisted TRUST frame", "error", err)
				break
			}
			if time.Since(lastPrune) >= 10*time.Minute {
				cutoff := time.Now().UTC().Add(-retention)
				result := database.Where("captured_at < ?", cutoff).Delete(&schedule.TrustMovement{})
				if result.Error != nil {
					slog.Warn("Could not prune expired TRUST movement rows", "error", result.Error)
				} else if result.RowsAffected > 0 {
					slog.Info("Pruned expired TRUST movements", "rows", result.RowsAffected, "retention_days", retentionDays)
				}
				if err := database.Where("updated_at < ?", cutoff).Delete(&schedule.TrustRun{}).Error; err != nil {
					slog.Warn("Could not prune expired TRUST run mappings", "error", err)
				}
				lastPrune = time.Now()
			}
		}
		_ = sub.Unsubscribe()
		_ = conn.Disconnect()
		time.Sleep(backoff)
		backoff = min(backoff*2, maxBackoff)
	}
}

// persistTrustFrame handles the persist trust frame behavior.
func persistTrustFrame(database *gorm.DB, payload []byte) error {
	var messages []trustWireMessage
	trimmed := strings.TrimSpace(string(payload))
	if trimmed == "" || trimmed == "[]" {
		return nil
	}
	if strings.HasPrefix(trimmed, "[") {
		if err := json.Unmarshal(payload, &messages); err != nil {
			return fmt.Errorf("decode TRUST batch: %w", err)
		}
	} else {
		var message trustWireMessage
		if err := json.Unmarshal(payload, &message); err != nil {
			return fmt.Errorf("decode TRUST message: %w", err)
		}
		messages = []trustWireMessage{message}
	}

	return database.Transaction(func(tx *gorm.DB) error {
		for _, message := range messages {
			if err := persistTrustMessage(tx, message); err != nil {
				return err
			}
		}
		return nil
	})
}

// persistTrustMessage handles the persist trust message behavior.
func persistTrustMessage(tx *gorm.DB, message trustWireMessage) error {
	body := message.Body
	messageType := strings.TrimSpace(message.Header.MessageType)
	trainID := strings.TrimSpace(body.TrainID)
	currentTrainID := strings.TrimSpace(body.CurrentTrainID)
	revisedTrainID := strings.TrimSpace(body.RevisedTrainID)

	if messageType == "0001" {
		if strings.TrimSpace(body.TOCID) != scotrailTrustTocID || trainID == "" {
			return nil
		}
		run := schedule.TrustRun{
			TrainID: trainID, TocID: scotrailTrustTocID,
			Headcode: trustHeadcode(trainID), TrainUID: strings.TrimSpace(body.TrainUID),
			ServiceDate:      trustServiceDate(body.TPOriginTimestamp, nil, body.OriginDepTimestamp),
			TrainServiceCode: strings.TrimSpace(body.TrainServiceCode), UpdatedAt: time.Now().UTC(),
		}
		return tx.Save(&run).Error
	}

	ids := trustIDs(trainID, currentTrainID)
	tocID := strings.TrimSpace(body.TOCID)
	if tocID != "" && tocID != scotrailTrustTocID {
		return nil
	}
	run, err := lookupTrustRun(tx, ids)
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return err
	}
	knownScotrailRun := err == nil && run.TocID == scotrailTrustTocID
	if tocID != scotrailTrustTocID && !knownScotrailRun {
		return nil
	}
	if !knownScotrailRun {
		run = schedule.TrustRun{TrainID: trainID, TocID: scotrailTrustTocID, Headcode: trustHeadcode(trainID)}
	}

	if messageType == "0007" && revisedTrainID != "" && run.TrainUID != "" {
		alias := run
		alias.TrainID = revisedTrainID
		alias.Headcode = trustHeadcode(revisedTrainID)
		alias.UpdatedAt = time.Now().UTC()
		if err := tx.Save(&alias).Error; err != nil {
			return err
		}
	}
	if messageType != "0003" {
		return nil
	}

	primaryID := trainID
	if primaryID == "" {
		primaryID = currentTrainID
	}
	if primaryID == "" {
		return nil
	}
	actual := firstTrustTimestamp(body.ActualTimestamp, body.EventTimestamp)
	planned := firstTrustTimestamp(body.PlannedTimestamp, body.OriginDepTimestamp)
	serviceDate := trustServiceDate(body.TPOriginTimestamp, &run, body.ActualTimestamp)
	location := strings.TrimSpace(body.LocationStanox)
	if location == "00000" {
		location = ""
	}
	if location == "" {
		location = strings.TrimSpace(body.ReportingStanox)
		if location == "00000" {
			location = ""
		}
	}
	eventKeyInput := strings.Join([]string{
		messageType, primaryID, strings.TrimSpace(body.EventType), body.ActualTimestamp,
		body.PlannedTimestamp, location, strings.TrimSpace(body.Platform),
	}, "\x00")
	hash := sha256.Sum256([]byte(eventKeyInput))
	row := schedule.TrustMovement{
		EventKey: hex.EncodeToString(hash[:]), CapturedAt: time.Now().UTC(),
		MessageType: messageType, TrainID: primaryID, TocID: scotrailTrustTocID,
		Headcode: run.Headcode, TrainUID: run.TrainUID, ServiceDate: serviceDate,
		EventType: strings.TrimSpace(body.EventType), ActualTimestamp: actual,
		PlannedTimestamp: planned, LocationStanox: location,
		Platform: strings.TrimSpace(body.Platform), VariationStatus: strings.TrimSpace(body.VariationStatus),
		TrainTerminated: strings.TrimSpace(body.TrainTerminated),
	}
	return tx.Where("event_key = ?", row.EventKey).FirstOrCreate(&row).Error
}

// firstTrustTimestamp handles the first trust timestamp behavior.
func firstTrustTimestamp(values ...string) *time.Time {
	for _, value := range values {
		if timestamp := trustTimestamp(value, true); timestamp != nil {
			return timestamp
		}
	}
	return nil
}

// lookupTrustRun handles the lookup trust run behavior.
func lookupTrustRun(tx *gorm.DB, ids []string) (schedule.TrustRun, error) {
	var run schedule.TrustRun
	if len(ids) == 0 {
		return run, gorm.ErrRecordNotFound
	}
	err := tx.Where("train_id IN ?", ids).Order("updated_at DESC").First(&run).Error
	return run, err
}

// trustIDs handles the trust ids behavior.
func trustIDs(ids ...string) []string {
	seen := make(map[string]bool)
	result := make([]string, 0, len(ids))
	for _, id := range ids {
		if id != "" && !seen[id] {
			seen[id] = true
			result = append(result, id)
		}
	}
	return result
}

// trustHeadcode handles the trust headcode behavior.
func trustHeadcode(trainID string) string {
	if len(trainID) < 6 {
		return ""
	}
	return trainID[2:6]
}

// trustServiceDate handles the trust service date behavior.
func trustServiceDate(explicit string, run *schedule.TrustRun, timestamp string) string {
	if len(strings.TrimSpace(explicit)) >= 10 {
		return strings.TrimSpace(explicit)[:10]
	}
	if run != nil && run.ServiceDate != "" {
		return run.ServiceDate
	}
	if stamp := trustTimestamp(timestamp, true); stamp != nil {
		return stamp.In(trustLondonLocation).Format("2006-01-02")
	}
	return ""
}

// trustTimestamp handles the trust timestamp behavior.
func trustTimestamp(raw string, daylightSavingCorrection bool) *time.Time {
	value := strings.TrimSpace(raw)
	if value == "" {
		return nil
	}
	ms, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return nil
	}
	stamp := time.UnixMilli(ms).In(trustLondonLocation)
	if daylightSavingCorrection && stamp.IsDST() {
		stamp = stamp.Add(-time.Hour)
	}
	stamp = stamp.UTC()
	return &stamp
}
