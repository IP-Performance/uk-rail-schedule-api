package sync

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"
	"uk-rail-schedule-api/internal/schedule"

	"gorm.io/gorm"
)

const smartFileURL = "https://publicdatafeeds.networkrail.co.uk/ntrod/SupportingFileAuthenticate?type=SMART"

type smartWireRow struct {
	Area      string `json:"TD"`
	FromBerth string `json:"FROMBERTH"`
	ToBerth   string `json:"TOBERTH"`
	Event     string `json:"EVENT"`
	Stanox    string `json:"STANOX"`
}

type smartWireSnapshot struct {
	BerthData []smartWireRow `json:"BERTHDATA"`
}

// RefreshSMARTReference downloads Network Rail's small, periodically updated
// berth-to-STANOX reference snapshot. It uses the existing Network Rail feed
// credentials and refreshes once per day.
func RefreshSMARTReference(ctx context.Context, database *gorm.DB, username, password string) {
	client := &http.Client{Timeout: 2 * time.Minute}
	for ctx.Err() == nil {
		interval := 24 * time.Hour
		if err := refreshSMARTOnce(ctx, client, database, username, password); err != nil {
			slog.Warn("SMART reference refresh failed", "error", err)
			interval = 15 * time.Minute
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func refreshSMARTOnce(ctx context.Context, client *http.Client, database *gorm.DB, username, password string) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, smartFileURL, nil)
	if err != nil {
		return err
	}
	request.SetBasicAuth(username, password)
	response, err := client.Do(request)
	if err != nil {
		return fmt.Errorf("download SMART snapshot: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("download SMART snapshot returned HTTP %d", response.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, 512<<20))
	if err != nil {
		return fmt.Errorf("read SMART snapshot: %w", err)
	}
	if len(data) >= 2 && data[0] == 0x1f && data[1] == 0x8b {
		reader, err := gzip.NewReader(bytes.NewReader(data))
		if err != nil {
			return fmt.Errorf("open compressed SMART snapshot: %w", err)
		}
		decompressed, readErr := io.ReadAll(io.LimitReader(reader, 512<<20))
		closeErr := reader.Close()
		if readErr != nil {
			return fmt.Errorf("decompress SMART snapshot: %w", readErr)
		}
		if closeErr != nil {
			return fmt.Errorf("close compressed SMART snapshot: %w", closeErr)
		}
		data = decompressed
	}
	var snapshot smartWireSnapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return fmt.Errorf("decode SMART snapshot: %w", err)
	}
	rows := snapshot.BerthData
	steps := make([]schedule.SMARTBerthStep, 0, len(rows))
	seen := make(map[string]struct{}, len(rows))
	for _, row := range rows {
		area, from, to, event, stanox := strings.ToUpper(strings.TrimSpace(row.Area)), strings.ToUpper(strings.TrimSpace(row.FromBerth)), strings.ToUpper(strings.TrimSpace(row.ToBerth)), strings.ToUpper(strings.TrimSpace(row.Event)), strings.TrimSpace(row.Stanox)
		if area == "" || to == "" || stanox == "" {
			continue
		}
		key := strings.Join([]string{area, from, to, event, stanox}, "\x00")
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		steps = append(steps, schedule.SMARTBerthStep{Area: area, FromBerth: from, ToBerth: to, Event: event, Stanox: stanox})
	}
	if len(steps) == 0 {
		return fmt.Errorf("SMART snapshot contained no usable berth transitions")
	}
	err = database.Transaction(func(tx *gorm.DB) error {
		if err := tx.Session(&gorm.Session{AllowGlobalUpdate: true}).Delete(&schedule.SMARTBerthStep{}).Error; err != nil {
			return err
		}
		for start := 0; start < len(steps); start += 500 {
			end := start + 500
			if end > len(steps) {
				end = len(steps)
			}
			if err := tx.CreateInBatches(steps[start:end], 500).Error; err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("replace SMART reference rows: %w", err)
	}
	slog.Info("Loaded SMART berth reference", "transitions", len(steps))
	return nil
}
