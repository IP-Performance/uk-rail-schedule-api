package sync

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"
	"uk-rail-schedule-api/internal/schedule"
	"uk-rail-schedule-api/internal/telemetry"

	"gorm.io/gorm"
)

var (
	mu                 sync.Mutex
	refreshingDatabase bool
)

// IsRefreshingDatabase checks the is refreshing database behavior.
func IsRefreshingDatabase() bool {
	mu.Lock()
	defer mu.Unlock()
	return refreshingDatabase
}

// startRefreshingDatabase handles the start refreshing database behavior.
func startRefreshingDatabase() {
	mu.Lock()
	defer mu.Unlock()
	slog.Info("start refreshing database")
	refreshingDatabase = true
}

// endRefreshingDatabase handles the end refreshing database behavior.
func endRefreshingDatabase(db *gorm.DB, deleteExpired bool) {
	if db != nil && deleteExpired {
		slog.Debug("Deleting expired schedules")
		currentTimestamp := time.Now().Unix()
		db.Delete(&schedule.Schedule{}, "schedule_end_date_ts < ?", currentTimestamp)
	} else {
		slog.Debug("Not deleting expired schedules from database")
	}

	mu.Lock()
	defer mu.Unlock()
	slog.Info("end refreshing database")
	refreshingDatabase = false
}

// SetRefreshingDatabase forces the refresh state — useful for testing and operational resets.
func SetRefreshingDatabase(v bool) {
	mu.Lock()
	defer mu.Unlock()
	refreshingDatabase = v
}

// RefreshSchedules loads the schedule feed file into the database.
// It also replays any VSTP files in the data directory that are newer than the timetable.
func RefreshSchedules(filename string, db *gorm.DB, dataDir string, deleteExpired bool) {
	if IsRefreshingDatabase() {
		slog.Info("Not going to load - schedule feed is already loading in another process")
		return
	}

	// We set the refreshing state here because the feed file is large and takes a while to load, we won't also try to load it again in another process.
	startRefreshingDatabase()
	defer endRefreshingDatabase(db, deleteExpired)

	file, err := os.Open(filename)
	if err != nil {
		slog.Error("Error opening schedule feed file. Cannot load.", "error", err)
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	// The first line must be the timetable metadata record
	scanner.Scan()
	line := scanner.Text()

	var scheduleFeedRecord schedule.ScheduleFeedRecord
	if err := json.Unmarshal([]byte(line), &scheduleFeedRecord); err != nil {
		slog.Error("Error unmarshaling JSON:", "error", err)
		return
	}

	if !scheduleFeedRecord.IsMetadata() {
		slog.Error("First record in feed file is not metadata, cannot continue loading feed file", "record", scheduleFeedRecord)
		return
	}

	// Check if the timetable in the feed file is older than the latest timetable in the database, if it is then we shouldn't load it as it would be out of date.
	var laterTimetable schedule.Timetable
	if err := db.Where("timestamp >= ?", scheduleFeedRecord.Timetable.Timestamp).First(&laterTimetable).Error; err == nil {
		slog.Info("The schedule feed file is older than the timetable in the database, so it won't be loaded.", "timetable", laterTimetable)
		return
	}

	publishedAt := time.Unix(int64(scheduleFeedRecord.Timetable.Timestamp), 0)

	var schedules []schedule.Schedule
	var tiplocs []schedule.Tiploc
	var scheduleCount, tiplocCount int64

	for scanner.Scan() {
		var record schedule.ScheduleFeedRecord
		line := scanner.Text()

		if err := json.Unmarshal([]byte(line), &record); err != nil {
			slog.Error("Error unmarshaling scheduleFeedRecord JSON", "error", err)
			continue
		}

		// We check if the record is a schedule or a tiploc and insert it into the database in batches of 10 to improve performance.
		if record.IsSchedule() {
			sch := record.JSONScheduleV1.ToSchedule(publishedAt)
			sch.AugmentSchedule()
			scheduleCount++
			var existingSchedule schedule.Schedule
			lookupErr := db.Where("combined_id = ?", sch.CombinedID).First(&existingSchedule).Error
			if lookupErr == nil {
				// Save matching schedules one at a time. GORM's slice Save path
				// did not reliably retain the selected IDs here, so daily full
				// feeds accumulated duplicate timetable rows.
				sch.ID = existingSchedule.ID
				// Keep the database-managed creation timestamp from the existing
				// row. A zero Go time here is rejected by MySQL strict mode and
				// would abort the full timetable refresh.
				if err := db.Omit("ScheduleLocation", "CreatedAt").Save(&sch).Error; err != nil {
					slog.Error("Failed to update existing schedule", "error", err, "combined_id", sch.CombinedID)
					return
				}
			} else if errors.Is(lookupErr, gorm.ErrRecordNotFound) {
				schedules = append(schedules, sch)
			} else {
				slog.Error("Failed to look up existing schedule", "error", lookupErr, "combined_id", sch.CombinedID)
				return
			}

			if len(schedules) == 10 {
				if err := db.Create(&schedules).Error; err != nil {
					slog.Error("Failed to insert schedule batch", "error", err)
					return
				}
				schedules = nil
			}
		}

		if record.IsTiploc() {
			tiplocs = append(tiplocs, record.Tiploc)
			tiplocCount++
			if len(tiplocs) == 10 {
				db.Save(&tiplocs)
				tiplocs = nil
			}
		}
	}

	if len(schedules) > 0 {
		if err := db.Create(&schedules).Error; err != nil {
			slog.Error("Failed to insert final schedule batch", "error", err)
			return
		}
	}
	if len(tiplocs) > 0 {
		db.Save(&tiplocs)
	}

	db.Create(&scheduleFeedRecord.Timetable)

	telemetry.RecordFeedRefreshCompleted(context.Background(), scheduleCount, tiplocCount)

	// Replay any VSTP files in the data directory so we can recover from a database deletion
	files, err := os.ReadDir(dataDir)
	if err != nil {
		slog.Error("Failed to read data directory to find vstp files", "error", err)
		return
	}
	for _, f := range files {
		if !f.IsDir() && path.Ext(f.Name()) == ".json" && strings.HasPrefix(f.Name(), "vstp-") {
			fp := path.Join(dataDir, f.Name())
			if err := insertVSTP(fp, db); err != nil {
				slog.Error("Failed to insert vstp file during refresh", "error", err, "filename", fp)
			}
		}
	}
}

// insertVSTP reads a VSTP message from a file, parses it, and inserts the schedule into the database.
func insertVSTP(filename string, db *gorm.DB) error {
	var vstpMsg schedule.VSTPStompMsg

	vstp, err := os.ReadFile(filename)
	if err != nil {
		slog.Error("Failed to read vstp message from file", "error", err, "filename", filename)
		return err
	}

	if err = json.Unmarshal(vstp, &vstpMsg); err != nil {
		slog.Error("Error decoding STOMP message json", "error", err)
		return err
	}

	parsedTimestamp, err := strconv.ParseInt(vstpMsg.VSTPCIFMsgV1.Timestamp, 10, 64)
	if err != nil {
		slog.Error("Error parsing VSTP timestamp", "error", err, "timestamp", vstpMsg.VSTPCIFMsgV1.Timestamp)
		return err
	}

	sch := vstpMsg.VSTPCIFMsgV1.VSTPSchedule.ToSchedule(time.Unix(parsedTimestamp/1000, 0))
	sch.AugmentSchedule()
	slog.Debug("Inserting schedule into db from file", "filename", filename)
	db.Create(&sch)
	return nil
}
