package store

import (
	"database/sql"
	"encoding/json"
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
    canonical_payload TEXT,
    hash        TEXT,
    hash_source TEXT,
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
	if err := ensureAuditColumns(db); err != nil {
		return nil, err
	}
	if err := backfillAuditIntegrity(db); err != nil {
		return nil, err
	}
	return db, nil
}

// SaveAuditLog inserts a new audit log entry and returns the new ID.
func SaveAuditLog(db *sql.DB, l *model.AuditLog) (int64, error) {
	res, err := db.Exec(`
		INSERT INTO audit_logs
			(event_time, table_name, action, record_id, before_data, after_data,
			 canonical_payload, hash, hash_source, raw_payload)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		l.EventTime.UTC().Format(time.RFC3339Nano),
		l.TableName,
		l.Action,
		nullStr(l.RecordID),
		l.BeforeData,
		l.AfterData,
		nullStr(l.CanonicalPayload),
		nullStr(l.Hash),
		nullStr(l.HashSource),
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

	query := `SELECT id, event_time, table_name, action, record_id, before_data, after_data,
		canonical_payload, hash, hash_source FROM audit_logs` +
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
			eventTimeStr     string
			recordID         sql.NullString
			canonicalPayload sql.NullString
			hash             sql.NullString
			hashSource       sql.NullString
		)
		if err := rows.Scan(
			&l.ID, &eventTimeStr, &l.TableName, &l.Action, &recordID, &l.BeforeData, &l.AfterData,
			&canonicalPayload, &hash, &hashSource,
		); err != nil {
			return nil, 0, err
		}
		l.EventTime, _ = time.Parse(time.RFC3339Nano, eventTimeStr)
		assignNullable(&l.RecordID, recordID)
		assignNullable(&l.CanonicalPayload, canonicalPayload)
		assignNullable(&l.Hash, hash)
		assignNullable(&l.HashSource, hashSource)
		logs = append(logs, l)
	}

	return logs, total, rows.Err()
}

// GetAuditLog returns a single audit log by ID, including the raw payload.
func GetAuditLog(db *sql.DB, id int64) (*model.AuditLog, error) {
	l := &model.AuditLog{}
	var (
		eventTimeStr     string
		recordID         sql.NullString
		canonicalPayload sql.NullString
		hash             sql.NullString
		hashSource       sql.NullString
		rawPayload       sql.NullString
	)
	err := db.QueryRow(`
		SELECT id, event_time, table_name, action, record_id, before_data, after_data,
		       canonical_payload, hash, hash_source, raw_payload
		FROM audit_logs WHERE id = ?`, id).Scan(
		&l.ID, &eventTimeStr, &l.TableName, &l.Action,
		&recordID, &l.BeforeData, &l.AfterData,
		&canonicalPayload, &hash, &hashSource, &rawPayload,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	l.EventTime, _ = time.Parse(time.RFC3339Nano, eventTimeStr)
	assignNullable(&l.RecordID, recordID)
	assignNullable(&l.CanonicalPayload, canonicalPayload)
	assignNullable(&l.Hash, hash)
	assignNullable(&l.HashSource, hashSource)
	assignNullable(&l.RawPayload, rawPayload)
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

func assignNullable(target *string, value sql.NullString) {
	if value.Valid {
		*target = value.String
	}
}

func ensureAuditColumns(db *sql.DB) error {
	rows, err := db.Query("PRAGMA table_info(audit_logs)")
	if err != nil {
		return fmt.Errorf("inspect audit_logs schema: %w", err)
	}
	defer rows.Close()

	existing := map[string]bool{}
	for rows.Next() {
		var (
			cid        int
			name       string
			columnType string
			notNull    int
			defaultVal sql.NullString
			pk         int
		)
		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultVal, &pk); err != nil {
			return fmt.Errorf("scan audit_logs schema: %w", err)
		}
		existing[name] = true
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read audit_logs schema: %w", err)
	}

	for _, column := range []string{"canonical_payload", "hash", "hash_source"} {
		if existing[column] {
			continue
		}
		if _, err := db.Exec("ALTER TABLE audit_logs ADD COLUMN " + column + " TEXT"); err != nil {
			return fmt.Errorf("add column %s: %w", column, err)
		}
	}

	if _, err := db.Exec("CREATE INDEX IF NOT EXISTS idx_audit_hash ON audit_logs(hash)"); err != nil {
		return fmt.Errorf("create hash index: %w", err)
	}
	return nil
}

func backfillAuditIntegrity(db *sql.DB) error {
	rows, err := db.Query(`
		SELECT id, raw_payload
		FROM audit_logs
		WHERE (hash IS NULL OR hash = '')
		  AND raw_payload IS NOT NULL
		  AND raw_payload <> ''`)
	if err != nil {
		return fmt.Errorf("query audit integrity backfill: %w", err)
	}
	defer rows.Close()

	type update struct {
		id               int64
		canonicalPayload string
		hash             string
		hashSource       string
	}
	var updates []update

	for rows.Next() {
		var (
			id  int64
			raw string
		)
		if err := rows.Scan(&id, &raw); err != nil {
			return fmt.Errorf("scan audit integrity backfill: %w", err)
		}
		canonicalPayload, hash, hashSource := extractIntegrityFromRawPayload(raw)
		if hash == "" && canonicalPayload == "" {
			continue
		}
		updates = append(updates, update{
			id:               id,
			canonicalPayload: canonicalPayload,
			hash:             hash,
			hashSource:       hashSource,
		})
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read audit integrity backfill: %w", err)
	}

	for _, u := range updates {
		if _, err := db.Exec(`
			UPDATE audit_logs
			SET canonical_payload = ?, hash = ?, hash_source = ?
			WHERE id = ?`,
			nullStr(u.canonicalPayload),
			nullStr(u.hash),
			nullStr(u.hashSource),
			u.id,
		); err != nil {
			return fmt.Errorf("update audit integrity backfill id=%d: %w", u.id, err)
		}
	}

	return nil
}

func extractIntegrityFromRawPayload(raw string) (string, string, string) {
	var root map[string]interface{}
	if err := json.Unmarshal([]byte(raw), &root); err != nil {
		return "", "", ""
	}

	payload := root
	if nested, ok := root["payload"].(map[string]interface{}); ok {
		payload = nested
	}

	canonicalPayload := stringFromMap(payload, "canonical_payload")
	hash := stringFromMap(payload, "hash")
	hashSource := ""
	if payload["after"] != nil {
		hashSource = "AFTER"
	} else if payload["before"] != nil {
		hashSource = "BEFORE"
	}

	return canonicalPayload, hash, hashSource
}

func stringFromMap(m map[string]interface{}, key string) string {
	value, ok := m[key].(string)
	if !ok {
		return ""
	}
	return value
}
