// syncd is a long-running daemon that keeps the schedule database up-to-date.
// It loads the initial schedule feed and listens for VSTP, TRUST, and TD updates.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"uk-rail-schedule-api/internal/config"
	"uk-rail-schedule-api/internal/db"
	internalsync "uk-rail-schedule-api/internal/sync"
	"uk-rail-schedule-api/internal/telemetry"

	"github.com/joho/godotenv"
)

// version is set at build time via -ldflags "-X main.version=<value>".
var version = "dev"

// main handles the main behavior.
func main() {
	_ = godotenv.Load()

	logger := setupLogger()
	slog.SetDefault(logger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	shutdownTelemetry := telemetry.Setup(ctx, "uk-rail-schedule-api-syncd", version)
	defer func() {
		if err := shutdownTelemetry(ctx); err != nil {
			slog.Error("Failed to shut down telemetry", "error", err)
		}
	}()

	database, err := db.OpenConfigured(config.GetDatabaseFilename(), config.GetDatabaseDSN())
	if err != nil {
		slog.Error("Failed to open database", "error", err)
		os.Exit(1)
	}

	telemetry.RegisterSyncdObservables(database, config.GetDatabaseFilename())

	slog.Info("Starting schedule sync daemon", "version", version)

	// Initial load of schedule feed
	go internalsync.RefreshSchedules(
		config.GetScheduleFeedFilename(),
		database,
		config.GetDataDir(),
		config.ShouldDeleteExpiredSchedulesAfterRefresh(),
	)

	connErr, stompURL, login, password := config.GetStompConnectionDetails()
	if connErr != nil {
		slog.Warn("STOMP credentials not configured - VSTP feed will not be consumed", "error", connErr)
	} else {
		go internalsync.ListenForVSTP(database, stompURL, login, password, config.GetDataDir())
		go internalsync.ListenForTrustMovements(database, stompURL, login, password)
		go internalsync.RefreshSMARTReference(ctx, database, login, password)
	}
	darwinConfig, darwinEnabled, darwinErr := config.GetDarwinConsumerConfig()
	if darwinErr != nil {
		slog.Error("Darwin consumer is not configured correctly", "error", darwinErr)
	} else if darwinEnabled {
		go internalsync.ListenForDarwin(ctx, database, darwinConfig)
	} else {
		slog.Info("Darwin Kafka consumer is disabled")
	}
	tdConfig, tdEnabled, tdErr := config.GetTDConsumerConfig()
	if tdErr != nil {
		slog.Error("Train Describer consumer is not configured correctly", "error", tdErr)
	} else if tdEnabled {
		go internalsync.ListenForTDPositions(ctx, database, tdConfig)
	} else {
		slog.Info("Train Describer Kafka consumer is disabled")
	}

	// Block until a termination signal is received
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	cancel()
	slog.Info("Shutting down syncd")
}

// setupLogger configures the logger to write to a file if LOG_FILENAME is set, or to stderr otherwise.
func setupLogger() *slog.Logger {
	var output *os.File
	if logFile := os.Getenv("LOG_FILENAME"); logFile != "" {
		f, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			panic("Error opening log file: " + err.Error())
		}
		output = f
	} else {
		output = os.Stderr
	}
	return slog.New(slog.NewTextHandler(output, &slog.HandlerOptions{
		Level:     slog.LevelDebug,
		AddSource: true,
	}))
}
