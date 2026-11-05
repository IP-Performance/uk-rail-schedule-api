package schedule

import "time"

// TrustRun links a TRUST train identity to the schedule activated for it.
type TrustRun struct {
	TrainID          string `gorm:"primaryKey;size:10"`
	TocID            string `gorm:"size:4;index"`
	Headcode         string `gorm:"size:4;index"`
	TrainUID         string `gorm:"size:16;index"`
	ServiceDate      string `gorm:"size:10;index"`
	TrainServiceCode string `gorm:"size:16"`
	UpdatedAt        time.Time
}

// TrustMovement stores only normalized ScotRail movement fields, not raw feed frames.
type TrustMovement struct {
	ID               uint64     `gorm:"primaryKey"`
	EventKey         string     `gorm:"size:64;uniqueIndex"`
	CapturedAt       time.Time  `gorm:"index"`
	MessageType      string     `gorm:"size:4"`
	TrainID          string     `gorm:"size:10;index"`
	TocID            string     `gorm:"size:4"`
	Headcode         string     `gorm:"size:4;index"`
	TrainUID         string     `gorm:"size:16;index"`
	ServiceDate      string     `gorm:"size:10;index"`
	EventType        string     `gorm:"size:16"`
	ActualTimestamp  *time.Time `gorm:"index"`
	PlannedTimestamp *time.Time
	LocationStanox   string `gorm:"size:16"`
	Platform         string `gorm:"size:8"`
	VariationStatus  string `gorm:"size:16"`
	TrainTerminated  string `gorm:"size:8"`
}
