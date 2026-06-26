docker run --rm -v "${PWD}\smt:/workspace" -w /workspace maven:3.9.9-eclipse-temurin-17 mvn clean test package

docker compose up -d

# daftar connector Debezium dulu kalau belum ada
# jalankan dari folder root cdc-demo
Invoke-RestMethod `
	-Method Post `
	-Uri http://localhost:8083/connectors `
	-ContentType "application/json" `
	-InFile .\connector\postgres-orders.json

# cek connector
Invoke-RestMethod http://localhost:8083/connectors

# cek status
Invoke-RestMethod http://localhost:8083/connectors/orders-postgres-connector/status

# cek custom SMT kebaca oleh Kafka Connect
$plugins = Invoke-RestMethod "http://localhost:8083/connector-plugins?connectorsOnly=false"
$plugins.Where({ $_.class -like "com.afif.cdc.smt*" })

# jalankan consumer + dashboard Go
cd consumer
go run . -topic cdc_postgres.public.orders -group orders-consumer

# buka dashboard Go
http://localhost:8090

# Kafka UI, bukan dashboard Go
http://localhost:8080
