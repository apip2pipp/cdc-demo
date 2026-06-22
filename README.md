# CDC Demo Lokal

Project ini mensimulasikan alur CDC lokal:

PostgreSQL lokal -> WAL -> Debezium -> Kafka -> Golang consumer

## Struktur Folder

- `docker-compose.yml` untuk Kafka, Debezium Connect, dan Kafka UI.
- `sql/orders.sql` untuk tabel `orders` dan publication.
- `connector/postgres-orders.json` untuk payload register connector.
- `consumer/` untuk consumer Golang sederhana.

## Prasyarat

- PostgreSQL lokal sudah aktif di Windows.
- `wal_level = logical`, `max_wal_senders >= 1`, `max_replication_slots >= 1`.
- Database bernama `belajar-postgres` tersedia.
- Docker Desktop berjalan dengan WSL2.
- Golang terpasang jika ingin menjalankan consumer.

## Start Stack Docker

Jalankan dari root project:

```powershell
docker compose up -d
```

Tunggu sampai service berikut siap:

- Kafka di `localhost:9092`
- Debezium Connect di `localhost:8083`
- Kafka UI di `localhost:8080`

## Setup PostgreSQL Lokal

Login ke PostgreSQL lokal lalu jalankan:

```sql
\c "belajar-postgres"
\i 'D:/PROJECT-GITHUB/cdc-demo/sql/orders.sql'
```

File SQL akan membuat role CDC `debezium` dengan password `debezium123`, memberi akses ke database `belajar-postgres`, lalu membuat publication `dbz_orders_publication` untuk tabel `orders`.

## Register Debezium Connector

Setelah Connect siap, register connector:

```powershell
Invoke-RestMethod `
  -Method Post `
  -Uri http://localhost:8083/connectors `
  -ContentType "application/json" `
  -InFile .\connector\postgres-orders.json
```

Status connector bisa dicek dengan:

```powershell
Invoke-RestMethod http://localhost:8083/connectors/orders-postgres-connector/status
```

## Verifikasi Topic Kafka

List topic yang muncul di broker:

```powershell
docker exec kafka sh -lc '/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list'
```

Detail topic Debezium:

```powershell
docker exec kafka sh -lc '/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic cdc_postgres.public.orders'
```

Perkiraan jumlah message pada topic bisa dilihat dari offset terakhir:

```powershell
docker exec kafka sh -lc '/opt/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic cdc_postgres.public.orders --time -1'
```

Untuk demo ini, topic utama yang diharapkan adalah `cdc_postgres.public.orders`.

## Jalankan Consumer Golang

Masuk ke folder consumer lalu jalankan:

```powershell
cd consumer
go mod tidy
go run .
```

Jika topic atau broker ingin diganti:

```powershell
go run . -broker localhost:9092 -topic cdc_postgres.public.orders -group orders-consumer
```

## Test End-to-End

1. Start stack Docker.
2. Jalankan SQL schema `sql/orders.sql`.
3. Register connector dari `connector/postgres-orders.json`.
4. Jalankan consumer Go.
5. Insert data ke tabel `orders`:

```sql
INSERT INTO public.orders (product, qty) VALUES ('Keyboard', 1);

UPDATE public.orders
SET qty = 2
WHERE id = 1;

DELETE FROM public.orders
WHERE id = 1;
```

6. Verifikasi output consumer:

```text
CDC Event:
{ ... payload insert Debezium ... }

CDC Event:
{ ... payload update Debezium ... }

CDC Event:
{ ... payload delete Debezium ... }
```

## Troubleshooting Umum

### 1. Connect tidak bisa connect ke Kafka

Biasanya karena advertised listener salah. Untuk konteks ini, internal container harus pakai `kafka:9092`, sedangkan host Windows pakai `localhost:9092`.

### 2. Connector status FAILED

Periksa:

- password PostgreSQL benar
- `wal_level=logical`
- publication `dbz_orders_publication` sudah ada
- table `public.orders` memang ada
- user punya hak akses ke database dan schema

### 3. Tidak ada event masuk ke Kafka UI

Periksa apakah `INSERT` benar-benar terjadi di database yang sama dengan connector. Pastikan juga topic `cdc_postgres.public.orders` sudah dibuat dan consumer membaca topic yang benar.

### 4. Consumer Go tidak menerima event

Periksa broker address, topic name, dan apakah consumer sudah dijalankan sebelum insert. Jika perlu, gunakan `-broker localhost:9092` dan `-topic cdc_postgres.public.orders`.

### 5. Docker Desktop / WSL2 issue di Windows

Jika container tidak bisa akses PostgreSQL lokal, pastikan connector memakai `host.docker.internal` dan PostgreSQL mengizinkan koneksi dari host Windows.
