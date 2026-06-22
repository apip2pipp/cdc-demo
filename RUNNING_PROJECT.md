docker compose up -d

# cek connector
http://localhost:8083/connectors

# cek status
http://localhost:8083/connectors/users-postgres-connector/status

# jalankan consumer
cd consumer
go run main.go