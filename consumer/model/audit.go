package model

import "time"

// AuditLog represents a single CDC event stored in the audit database.
type AuditLog struct {
	ID         int64     `json:"id"`
	EventTime  time.Time `json:"event_time"`
	TableName  string    `json:"table_name"`
	Action     string    `json:"action"`
	RecordID   string    `json:"record_id,omitempty"`
	BeforeData *string   `json:"before_data"`
	AfterData  *string   `json:"after_data"`
	RawPayload string    `json:"raw_payload,omitempty"`
}

// AuditLogFilter holds query parameters for listing audit logs.
type AuditLogFilter struct {
	Action    string
	TableName string
	From      *time.Time
	To        *time.Time
	Limit     int
	Offset    int
}

// AuditStats holds aggregated counts per action type.
type AuditStats struct {
	Total     int64 `json:"total"`
	Inserts   int64 `json:"inserts"`
	Updates   int64 `json:"updates"`
	Deletes   int64 `json:"deletes"`
	Snapshots int64 `json:"snapshots"`
}

// ListResponse is the paginated response envelope for audit log queries.
type ListResponse struct {
	Data   []*AuditLog `json:"data"`
	Total  int64       `json:"total"`
	Limit  int         `json:"limit"`
	Offset int         `json:"offset"`
}
