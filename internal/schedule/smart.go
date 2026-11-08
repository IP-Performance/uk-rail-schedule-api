package schedule

// SMARTBerthStep maps a Train Describer berth transition to its STANOX event.
type SMARTBerthStep struct {
	ID        uint64 `gorm:"primaryKey"`
	Area      string `gorm:"size:8;index:smart_step_lookup"`
	FromBerth string `gorm:"size:8;index:smart_step_lookup"`
	ToBerth   string `gorm:"size:8;index:smart_step_lookup"`
	Event     string `gorm:"size:4"`
	Stanox    string `gorm:"size:16;index"`
}
