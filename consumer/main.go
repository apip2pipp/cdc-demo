package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"cdc-demo/consumer/handler"
	"cdc-demo/consumer/model"
	"cdc-demo/consumer/store"

	"github.com/segmentio/kafka-go"
)

// ---- Debezium envelope types ------------------------------------------------

type debeziumEnvelope struct {
	Payload debeziumPayload `json:"payload"`
}

type debeziumPayload struct {
	Before           json.RawMessage `json:"before"`
	After            json.RawMessage `json:"after"`
	Op               string          `json:"op"`
	Source           *debeziumSource `json:"source"`
	CanonicalPayload string          `json:"canonical_payload"`
	Hash             string          `json:"hash"`
}

type debeziumSource struct {
	DB     string `json:"db"`
	Schema string `json:"schema"`
	Table  string `json:"table"`
}

// ---- Main -------------------------------------------------------------------

func main() {
	var (
		broker   = flag.String("broker", "localhost:9092", "Kafka broker address")
		topics   = flag.String("topics", "cdc_simrs.public.patient,cdc_simrs.public.doctor,cdc_simrs.public.room,cdc_simrs.public.visit,cdc_simrs.public.queue,cdc_simrs.public.medical_record,cdc_simrs.public.diagnosis,cdc_simrs.public.prescription,cdc_simrs.public.laboratory,cdc_simrs.public.billing", "Kafka topics (comma separated)")
		group    = flag.String("group", "simrs-consumer", "Kafka consumer group")
		dbPath   = flag.String("db", "./audit.db", "SQLite database path")
		httpAddr = flag.String("addr", ":8090", "HTTP server address for dashboard")
	)
	flag.Parse()

	// ── Audit database ───────────────────────────────────────────────────────
	db, err := store.InitDB(*dbPath)
	if err != nil {
		log.Fatalf("init db: %v", err)
	}
	defer db.Close()
	log.Printf("audit database ready at %s", *dbPath)

	// ── WebSocket hub ─────────────────────────────────────────────────────────
	hub := handler.NewHub()
	go hub.Run()

	// ── HTTP server (dashboard + API) ─────────────────────────────────────────
	api := &handler.APIHandler{DB: db}
	auth := &handler.AuthHandler{}
	mux := http.NewServeMux()

	// Static files (Protected)
	fs := http.FileServer(http.Dir("./static"))
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/login.html" || strings.HasPrefix(r.URL.Path, "/assets/") {
			fs.ServeHTTP(w, r)
			return
		}
		if !handler.IsAuthenticated(r) {
			http.Redirect(w, r, "/login.html", http.StatusSeeOther)
			return
		}
		fs.ServeHTTP(w, r)
	})

	// Auth API
	mux.HandleFunc("/api/login", auth.Login)
	mux.HandleFunc("/api/logout", auth.Logout)

	// WebSocket endpoint
	mux.HandleFunc("/ws", handler.AuthMiddleware(hub.ServeWS))

	// REST API
	mux.HandleFunc("/api/stats", handler.AuthMiddleware(api.GetStats))
	mux.HandleFunc("/api/audit-logs/", handler.AuthMiddleware(func(w http.ResponseWriter, r *http.Request) {
		// Route /api/audit-logs/  vs  /api/audit-logs/{id}
		path := strings.TrimSuffix(r.URL.Path, "/")
		if path == "/api/audit-logs" {
			api.ListAuditLogs(w, r)
		} else {
			api.GetAuditLog(w, r)
		}
	}))
	// Also handle exact match without trailing slash
	mux.HandleFunc("/api/audit-logs", handler.AuthMiddleware(func(w http.ResponseWriter, r *http.Request) {
		api.ListAuditLogs(w, r)
	}))

	srv := &http.Server{Addr: *httpAddr, Handler: mux}
	go func() {
		log.Printf("dashboard available at http://localhost%s", *httpAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("http server error: %v", err)
		}
	}()

	// ── Kafka consumer ────────────────────────────────────────────────────────
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	topicList := strings.Split(*topics, ",")
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{*broker},
		GroupTopics: topicList,
		GroupID:     *group,
		Dialer:      newKafkaDialer(*broker),
		StartOffset: kafka.FirstOffset,
	})
	defer func() {
		if err := r.Close(); err != nil {
			log.Printf("close reader: %v", err)
		}
	}()

	log.Printf("waiting for events on topics %v from broker %s", topicList, *broker)

	for {
		msg, err := r.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				log.Println("consumer stopped")
				srv.Shutdown(context.Background()) //nolint:errcheck
				return
			}
			log.Printf("read message error: %v", err)
			time.Sleep(2 * time.Second)
			continue
		}

		if len(msg.Value) == 0 || string(msg.Value) == "null" {
			log.Printf("skip tombstone message at offset %d", msg.Offset)
			continue
		}

		if !json.Valid(msg.Value) {
			log.Printf("skip non-json message at offset %d", msg.Offset)
			continue
		}

		payload, ok, err := parseDebeziumPayload(msg.Value)
		if err != nil {
			log.Printf("failed to parse Debezium payload at offset %d: %v", msg.Offset, err)
			continue
		}
		if !ok {
			log.Printf("skip message without Debezium row payload at offset %d", msg.Offset)
			continue
		}

		// Print to terminal (original behaviour preserved).
		printEvent(payload)

		// Build and persist audit log entry.
		entry := buildAuditLog(payload, msg.Value, msg.Topic)
		id, err := store.SaveAuditLog(db, entry)
		if err != nil {
			log.Printf("save audit log: %v", err)
		} else {
			entry.ID = id
			// Broadcast to all connected dashboard clients (without raw payload for bandwidth).
			broadcast := *entry
			broadcast.RawPayload = ""
			hub.Broadcast(broadcast)
			log.Printf("saved audit log id=%d action=%s table=%s", id, entry.Action, entry.TableName)
		}
	}
}

// ---- Kafka helpers ----------------------------------------------------------

func newKafkaDialer(broker string) *kafka.Dialer {
	brokerHost := hostFromAddress(broker)
	netDialer := &net.Dialer{
		Timeout:   10 * time.Second,
		KeepAlive: 30 * time.Second,
	}
	return &kafka.Dialer{
		Timeout: 10 * time.Second,
		DialFunc: func(ctx context.Context, network, address string) (net.Conn, error) {
			host, port, err := net.SplitHostPort(address)
			if err == nil && strings.EqualFold(host, "kafka") {
				address = net.JoinHostPort(brokerHost, port)
			}
			return netDialer.DialContext(ctx, network, address)
		},
	}
}

func hostFromAddress(address string) string {
	host, _, err := net.SplitHostPort(address)
	if err != nil || host == "" {
		return "localhost"
	}
	return host
}

// ---- Debezium parsing -------------------------------------------------------

func parseDebeziumPayload(value []byte) (debeziumPayload, bool, error) {
	// Try wrapped envelope first.
	var env debeziumEnvelope
	if err := json.Unmarshal(value, &env); err != nil {
		return debeziumPayload{}, false, err
	}
	if env.Payload.hasRowData() {
		return env.Payload, true, nil
	}

	// Try flat payload.
	var p debeziumPayload
	if err := json.Unmarshal(value, &p); err != nil {
		return debeziumPayload{}, false, err
	}
	if p.hasRowData() {
		return p, true, nil
	}

	return debeziumPayload{}, false, nil
}

func (p debeziumPayload) hasRowData() bool {
	return p.Op != "" || !isNull(p.Before) || !isNull(p.After)
}

func isNull(b json.RawMessage) bool {
	return b == nil || len(b) == 0 || string(b) == "null"
}

// ---- Event helpers ----------------------------------------------------------

func buildAuditLog(p debeziumPayload, raw []byte, topic string) *model.AuditLog {
	action := operationLabel(p.Op)
	tableName := tableNameFromPayload(p, topic)

	var beforeStr, afterStr *string
	if !isNull(p.Before) {
		s := string(p.Before)
		beforeStr = &s
	}
	if !isNull(p.After) {
		s := string(p.After)
		afterStr = &s
	}

	recordID := extractRecordID(p.After)
	if recordID == "" {
		recordID = extractRecordID(p.Before)
	}

	rawStr := string(raw)
	return &model.AuditLog{
		EventTime:        time.Now().UTC(),
		TableName:        tableName,
		Action:           action,
		RecordID:         recordID,
		BeforeData:       beforeStr,
		AfterData:        afterStr,
		CanonicalPayload: p.CanonicalPayload,
		Hash:             p.Hash,
		HashSource:       hashSourceFromPayload(p),
		RawPayload:       rawStr,
	}
}

func hashSourceFromPayload(p debeziumPayload) string {
	if p.CanonicalPayload == "" && p.Hash == "" {
		return ""
	}
	if !isNull(p.After) {
		return "AFTER"
	}
	if !isNull(p.Before) {
		return "BEFORE"
	}
	return ""
}

func tableNameFromPayload(p debeziumPayload, topic string) string {
	if p.Source != nil && p.Source.Table != "" {
		if p.Source.Schema != "" {
			return p.Source.Schema + "." + p.Source.Table
		}
		return p.Source.Table
	}
	// Fallback: parse from topic name (prefix.schema.table).
	parts := strings.SplitN(topic, ".", 3)
	if len(parts) == 3 {
		return parts[1] + "." + parts[2]
	}
	return topic
}

func extractRecordID(data json.RawMessage) string {
	if isNull(data) {
		return ""
	}
	var m map[string]interface{}
	if err := json.Unmarshal(data, &m); err != nil {
		return ""
	}
	if v, ok := m["id"]; ok {
		return fmt.Sprintf("%v", v)
	}
	return ""
}

func printEvent(p debeziumPayload) {
	op := operationLabel(p.Op)
	current := p.After
	if isNull(current) {
		current = p.Before
	}

	fmt.Println("=================================")
	fmt.Println("EVENT RECEIVED")
	fmt.Printf("Operation : %s\n", op)
	if !isNull(current) {
		var m map[string]interface{}
		if err := json.Unmarshal(current, &m); err == nil {
			for k, v := range m {
				fmt.Printf("%-10s: %v\n", k, v)
			}
		}
	}
	fmt.Println("=================================")
	fmt.Println()
}

func operationLabel(op string) string {
	switch op {
	case "c":
		return "INSERT"
	case "u":
		return "UPDATE"
	case "d":
		return "DELETE"
	case "r":
		return "SNAPSHOT"
	default:
		return strings.ToUpper(op)
	}
}
