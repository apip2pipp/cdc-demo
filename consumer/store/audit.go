package store

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"cdc-demo/consumer/model"
	_ "modernc.org/sqlite"
)

const schema = `
CREATE TABLE IF NOT EXISTS audit_logs (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    event_time  TEXT NOT NULL,
    table_name  TEXT NOT NULL,
    action      TEXT NOT NULL,
    record_id   TEXT,
    before_data TEXT,
    after_data  TEXT,
    raw_payload TEXT
);
CREATE INDEX IF NOT EXISTS idx_audit_action     ON audit_logs(action);
CREATE INDEX IF NOT EXISTS idx_audit_event_time ON audit_logs(event_time);
CREATE INDEX IF NOT EXISTS idx_audit_table_name ON audit_logs(table_name);
`

// InitDB opens (or creates) the SQLite database and applies the schema.
func InitDB(path string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}
	db.SetMaxOpenConns(1) // SQLite is single-writer
	if _, err := db.Exec(schema); err != nil {
		return nil, fmt.Errorf("apply schema: %w", err)
	}
	return db, nil
}

// SaveAuditLog inserts a new audit log entry and returns the new ID.
func SaveAuditLog(db *sql.DB, l *model.AuditLog) (int64, error) {
	res, err := db.Exec(`
		INSERT INTO audit_logs
			(event_time, table_name, action, record_id, before_data, after_data, raw_payload)
		VALUES (?, ?, ?, ?, ?, ?, ?)`,
		l.EventTime.UTC().Format(time.RFC3339Nano),
		l.TableName,
		l.Action,
		nullStr(l.RecordID),
		l.BeforeData,
		l.AfterData,
		nullStr(l.RawPayload),
	)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

// ListAuditLogs returns a filtered, paginated list of audit logs and the total count.
func ListAuditLogs(db *sql.DB, f model.AuditLogFilter) ([]*model.AuditLog, int64, error) {
	where, args := buildWhere(f)

	var total int64
	if err := db.QueryRow("SELECT COUNT(*) FROM audit_logs"+where, args...).Scan(&total); err != nil {
		return nil, 0, err
	}

	limit := f.Limit
	if limit <= 0 {
		limit = 100
	}

	query := "SELECT id, event_time, table_name, action, record_id, before_data, after_data FROM audit_logs" +
		where + " ORDER BY id DESC LIMIT ? OFFSET ?"
	rows, err := db.Query(query, append(args, limit, f.Offset)...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var logs []*model.AuditLog
	for rows.Next() {
		l := &model.AuditLog{}
		var (
			eventTimeStr string
			recordID     sql.NullString
		)
		if err := rows.Scan(&l.ID, &eventTimeStr, &l.TableName, &l.Action, &recordID, &l.BeforeData, &l.AfterData); err != nil {
			return nil, 0, err
		}
		l.EventTime, _ = time.Parse(time.RFC3339Nano, eventTimeStr)
		if recordID.Valid {
			l.RecordID = recordID.String
		}
		logs = append(logs, l)
	}

	return logs, total, rows.Err()
}

// GetAuditLog returns a single audit log by ID, including the raw payload.
func GetAuditLog(db *sql.DB, id int64) (*model.AuditLog, error) {
	l := &model.AuditLog{}
	var (
		eventTimeStr string
		recordID     sql.NullString
		rawPayload   sql.NullString
	)
	err := db.QueryRow(`
		SELECT id, event_time, table_name, action, record_id, before_data, after_data, raw_payload
		FROM audit_logs WHERE id = ?`, id).Scan(
		&l.ID, &eventTimeStr, &l.TableName, &l.Action,
		&recordID, &l.BeforeData, &l.AfterData, &rawPayload,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	l.EventTime, _ = time.Parse(time.RFC3339Nano, eventTimeStr)
	if recordID.Valid {
		l.RecordID = recordID.String
	}
	if rawPayload.Valid {
		l.RawPayload = rawPayload.String
	}
	return l, nil
}

// GetStats returns aggregated event counts grouped by action.
func GetStats(db *sql.DB) (*model.AuditStats, error) {
	rows, err := db.Query(`SELECT action, COUNT(*) FROM audit_logs GROUP BY action`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	stats := &model.AuditStats{}
	for rows.Next() {
		var action string
		var count int64
		if err := rows.Scan(&action, &count); err != nil {
			return nil, err
		}
		stats.Total += count
		switch action {
		case "INSERT":
			stats.Inserts = count
		case "UPDATE":
			stats.Updates = count
		case "DELETE":
			stats.Deletes = count
		case "SNAPSHOT":
			stats.Snapshots = count
		}
	}
	return stats, rows.Err()
}

func buildWhere(f model.AuditLogFilter) (string, []interface{}) {
	var conditions []string
	var args []interface{}

	if f.Action != "" {
		conditions = append(conditions, "action = ?")
		args = append(args, f.Action)
	}
	if f.TableName != "" {
		conditions = append(conditions, "table_name = ?")
		args = append(args, f.TableName)
	}
	if f.From != nil {
		conditions = append(conditions, "event_time >= ?")
		args = append(args, f.From.UTC().Format(time.RFC3339Nano))
	}
	if f.To != nil {
		conditions = append(conditions, "event_time <= ?")
		args = append(args, f.To.UTC().Format(time.RFC3339Nano))
	}

	if len(conditions) == 0 {
		return "", args
	}
	return " WHERE " + strings.Join(conditions, " AND "), args
}

func nullStr(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}
