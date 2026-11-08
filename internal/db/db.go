package db

import (
	"log/slog"
	"os"
	"uk-rail-schedule-api/internal/schedule"

	"gorm.io/driver/mysql"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// Open opens (or creates) the SQLite database at the given path and returns a GORM handle.
func Open(databaseFilename string) (*gorm.DB, error) {
	if _, err := os.Stat(databaseFilename); os.IsNotExist(err) {
		slog.Info("Database doesn't exist - creating", "databaseFilename", databaseFilename)
	}

	return open(sqlite.Open(databaseFilename))
}

// OpenMySQL connects to the configured MySQL database using the go-sql-driver DSN.
func OpenMySQL(dsn string) (*gorm.DB, error) {
	if dsn == "" {
		return nil, gorm.ErrInvalidDB
	}
	return open(mysql.Open(dsn))
}

// OpenConfigured selects MySQL when a DSN is provided and SQLite otherwise.
func OpenConfigured(sqliteFilename, mysqlDSN string) (*gorm.DB, error) {
	if mysqlDSN != "" {
		return OpenMySQL(mysqlDSN)
	}
	return Open(sqliteFilename)
}

func open(dialector gorm.Dialector) (*gorm.DB, error) {
	database, err := gorm.Open(dialector, &gorm.Config{
		// Schedule and TIPLOC records are reloaded independently from Network
		// Rail feeds. Keep their ORM associations available for Preload while
		// avoiding backend-specific foreign-key constraints during migration.
		DisableForeignKeyConstraintWhenMigrating: true,
		// Feed filtering intentionally probes absent train IDs. GORM's default
		// warning level logs every expected record-not-found result on the
		// national TRUST topic, so only report actual database errors.
		Logger: logger.Default.LogMode(logger.Error),
	})
	if err != nil {
		return nil, err
	}
	// Earlier SMART imports used a unique key that incorrectly rejected valid
	// transitions shared by multiple STANOX codes. Remove that legacy key so
	// the ambiguity check can decide whether a mapping is safe.
	if database.Migrator().HasIndex(&schedule.SMARTBerthStep{}, "smart_step_key") {
		if err := database.Migrator().DropIndex(&schedule.SMARTBerthStep{}, "smart_step_key"); err != nil {
			return nil, err
		}
	}

	if err := database.AutoMigrate(
		&schedule.Tiploc{},
		&schedule.Schedule{},
		&schedule.ScheduleLocation{},
		&schedule.Timetable{},
		&schedule.TrustRun{},
		&schedule.TrustMovement{},
		&schedule.TDPosition{},
		&schedule.DarwinTiming{},
		&schedule.SMARTBerthStep{},
	); err != nil {
		return nil, err
	}

	return database, nil
}
