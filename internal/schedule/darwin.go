package schedule

import "time"

// DarwinTiming retains the latest Darwin timing for one service location.
// Only normalized fields used by timetable overlays are stored.
type DarwinTiming struct {
	ID            uint64     `gorm:"primaryKey"`
	TrainUID      string     `gorm:"size:16;uniqueIndex:darwin_timing_key"`
	ServiceDate   string     `gorm:"size:10;uniqueIndex:darwin_timing_key;index"`
	Tiploc        string     `gorm:"column:tiploc;size:16;uniqueIndex:darwin_timing_key"`
	EventType     string     `gorm:"size:8;uniqueIndex:darwin_timing_key"`
	RID           string     `gorm:"size:32;index"`
	ActualTime    *time.Time `gorm:"index"`
	ForecastTime  *time.Time
	UpdatedAt     time.Time
	ActualSet     bool `gorm:"-"`
	ForecastSet   bool `gorm:"-"`
	ActualRemoved bool `gorm:"-"`
}
