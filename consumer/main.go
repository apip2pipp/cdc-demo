package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

type debeziumEnvelope struct {
	Payload debeziumPayload `json:"payload"`
}

type debeziumPayload struct {
	Before *userEvent `json:"before"`
	After  *userEvent `json:"after"`
	Op     string     `json:"op"`
}

type userEvent struct {
	ID        int64  `json:"id"`
	Name      string `json:"name"`
	Email     string `json:"email"`
	CreatedAt int64  `json:"created_at,omitempty"`
}

func main() {
	var (
		broker = flag.String("broker", "localhost:9092", "Kafka broker address")
		topic  = flag.String("topic", "cdc_postgres.public.users", "Kafka topic name")
		group  = flag.String("group", "users-consumer", "Kafka consumer group")
	)
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{*broker},
		Topic:       *topic,
		GroupID:     *group,
		Dialer:      newKafkaDialer(*broker),
		StartOffset: kafka.FirstOffset,
	})
	defer func() {
		if err := r.Close(); err != nil {
			log.Printf("close reader: %v", err)
		}
	}()

	log.Printf("waiting for events on topic %s from broker %s", *topic, *broker)

	for {
		msg, err := r.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				log.Println("consumer stopped")
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
			log.Printf("skip non-json message at offset %d: %s", msg.Offset, string(msg.Value))
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

		printEvent(payload)
	}
}

func newKafkaDialer(broker string) *kafka.Dialer {
	brokerHost := hostFromAddress(broker)
	netDialer := &net.Dialer{
		Timeout:   10 * time.Second,
		KeepAlive: 30 * time.Second,
	}

	return &kafka.Dialer{
		Timeout: 10 * time.Second,
		DialFunc: func(ctx context.Context, network string, address string) (net.Conn, error) {
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

func parseDebeziumPayload(value []byte) (debeziumPayload, bool, error) {
	var envelope debeziumEnvelope
	if err := json.Unmarshal(value, &envelope); err != nil {
		return debeziumPayload{}, false, err
	}
	if envelope.Payload.hasRowData() {
		return envelope.Payload, true, nil
	}

	var payload debeziumPayload
	if err := json.Unmarshal(value, &payload); err != nil {
		return debeziumPayload{}, false, err
	}
	if payload.hasRowData() {
		return payload, true, nil
	}

	return debeziumPayload{}, false, nil
}

func (payload debeziumPayload) hasRowData() bool {
	return payload.Op != "" || payload.Before != nil || payload.After != nil
}

func printEvent(payload debeziumPayload) {
	operation := operationLabel(payload.Op)
	current := payload.After
	if current == nil {
		current = payload.Before
	}

	if current == nil {
		fmt.Println("=================================")
		fmt.Println("EVENT RECEIVED")
		fmt.Printf("Operation : %s\n", operation)
		fmt.Println("No row payload found")
		fmt.Println("=================================")
		fmt.Println()
		return
	}

	fmt.Println("=================================")
	fmt.Println("EVENT RECEIVED")
	fmt.Printf("Operation : %s\n", operation)
	fmt.Printf("ID        : %d\n", current.ID)
	fmt.Printf("Name      : %s\n", current.Name)
	fmt.Printf("Email     : %s\n", current.Email)
	if current.CreatedAt != 0 {
		fmt.Printf("CreatedAt : %d\n", current.CreatedAt)
	}
	fmt.Println("=================================")
	fmt.Println()
}

func operationLabel(op string) string {
	switch op {
	case "c":
		return "CREATE"
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
