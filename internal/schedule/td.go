package schedule

import "time"

// TDPosition is the latest known train description occupying one Train
// Describer berth. It stores current state only; raw TD events are not kept.
type TDPosition struct {
	AreaID    string    `gorm:"primaryKey;size:8" json:"area_id"`
	Berth     string    `gorm:"primaryKey;size:8" json:"berth"`
	Headcode  string    `gorm:"index;size:8" json:"headcode"`
	Stanox    string    `gorm:"size:16" json:"stanox,omitempty"`
	EventAt   time.Time `gorm:"index" json:"event_at"`
	UpdatedAt time.Time `json:"updated_at"`
}
