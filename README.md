# CDC Audit Trail Dashboard

Real-time audit trail dashboard di atas stack CDC: **PostgreSQL → Debezium → Kafka → Go Consumer → Dashboard Web**.

---

## Prasyarat

- Docker Desktop aktif (WSL2)
- PostgreSQL lokal aktif di port `5432`, database `belajar-postgres`
- `wal_level = logical` di PostgreSQL
- Go 1.22+ terinstall
- Docker image Maven akan dipakai untuk build custom SMT Java

---

## Cara Run dari Awal (First Time Setup)

### Step 1 — Build Custom SMT JAR

Build plugin Kafka Connect SMT:

```powershell
docker run --rm -v "${PWD}\smt:/workspace" -w /workspace maven:3.9.9-eclipse-temurin-17 mvn clean test package
```

Output JAR akan berada di `smt/target/cdc-audit-smt-1.0.0.jar`.

Detail desain SMT ada di `SMT_AUDIT_TRAIL.md`.

---

### Step 2 — Jalankan Docker Stack

Buka terminal di folder `cdc-demo`:

```powershell
docker compose up -d
```

Tunggu sampai 3 container berstatus `Running`:

```
✔ Container kafka      Running
✔ Container kafka-ui   Running
✔ Container debezium   Running
```

---

### Step 3 — Buat Tabel di PostgreSQL *(sekali saja)*

Buka **pgAdmin** → database `belajar-postgres` → Query Tool, lalu jalankan SQL berikut:

```sql
-- Buat tabel orders
CREATE TABLE IF NOT EXISTS public.orders (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    product TEXT NOT NULL,
    qty INTEGER NOT NULL CHECK (qty > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Buat role debezium
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'debezium') THEN
        CREATE ROLE debezium WITH LOGIN REPLICATION PASSWORD 'debezium123';
    ELSE
        ALTER ROLE debezium WITH LOGIN REPLICATION PASSWORD 'debezium123';
    END IF;
END $$;

-- Grant akses
GRANT CONNECT ON DATABASE "belajar-postgres" TO debezium;
GRANT USAGE ON SCHEMA public TO debezium;
GRANT SELECT ON public.orders TO debezium;

-- Set replica identity
ALTER TABLE public.orders REPLICA IDENTITY FULL;

-- Buat publication
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'dbz_orders_publication') THEN
        CREATE PUBLICATION dbz_orders_publication FOR TABLE public.orders;
    END IF;
END $$;
```

---

### Step 4 — Daftarkan Debezium Connector *(sekali saja)*

Pastikan terminal berada di folder root `cdc-demo`:

```powershell
Invoke-RestMethod `
  -Method Post `
  -Uri http://localhost:8083/connectors `
  -ContentType "application/json" `
  -InFile .\connector\postgres-orders.json
```

Cek status connector (tunggu ~5 detik):

```powershell
Start-Sleep -Seconds 5
Invoke-RestMethod http://localhost:8083/connectors/orders-postgres-connector/status
```

Pastikan `connector.state = RUNNING` dan `tasks[0].state = RUNNING`.

---

Connector ini sudah memakai urutan SMT:

```text
CanonicalizeValue -> Sha256HashValue
```

Field tambahan akan muncul di payload Kafka sebagai `canonical_payload` dan `hash`.

---

### Step 5 — Jalankan Consumer + Dashboard

```powershell
cd consumer
go run . -topic cdc_postgres.public.orders -group orders-consumer
```

Output yang diharapkan:

```
audit database ready at ./audit.db
waiting for events on topic cdc_postgres.public.orders from broker localhost:9092
dashboard available at http://localhost:8090
```

---

### Step 6 — Buka Dashboard

```
http://localhost:8090
```

---

### Step 7 — Test CDC dengan Insert Data

Jalankan di pgAdmin → `belajar-postgres` → Query Tool:

```sql
-- INSERT
INSERT INTO public.orders (product, qty) VALUES ('Laptop Gaming', 1);
INSERT INTO public.orders (product, qty) VALUES ('Mechanical Keyboard', 2);

-- UPDATE
UPDATE public.orders SET qty = 5 WHERE product = 'Laptop Gaming';

-- DELETE
DELETE FROM public.orders WHERE product = 'Mechanical Keyboard';
```

Event akan muncul di dashboard **secara realtime** tanpa refresh! 🎉

---

## Run Berikutnya (Setelah Setup Selesai)

Cukup 3 langkah jika JAR SMT belum ada atau source SMT berubah:

```powershell
# 0. Build SMT kalau belum ada / setelah ubah source Java
docker run --rm -v "${PWD}\smt:/workspace" -w /workspace maven:3.9.9-eclipse-temurin-17 mvn clean test package

# 1. Jalankan Docker
docker compose up -d

# 2. Jalankan consumer + dashboard
cd consumer
go run . -topic cdc_postgres.public.orders -group orders-consumer
```

Lalu buka `http://localhost:8090`.

---

## Ports

| Service | URL | Keterangan |
|---|---|---|
| 🟢 **Dashboard** | http://localhost:8090 | Audit Trail Dashboard |
| 📊 Kafka UI | http://localhost:8080 | Monitor Kafka topics |
| 🔗 Debezium | http://localhost:8083 | Connector REST API |
| 🐘 PostgreSQL | localhost:5432 | Database utama |

---

## Troubleshooting

### Port 8090 sudah dipakai
```powershell
Get-NetTCPConnection -LocalPort 8090 | Select-Object -ExpandProperty OwningProcess | ForEach-Object { Stop-Process -Id $_ -Force }
```

### Connector FAILED
```powershell
# Lihat error detail
(Invoke-RestMethod http://localhost:8083/connectors/orders-postgres-connector/status).tasks[0]

# Reset connector
Invoke-RestMethod -Method Delete -Uri http://localhost:8083/connectors/orders-postgres-connector
Start-Sleep -Seconds 3
Invoke-RestMethod -Method Post -Uri http://localhost:8083/connectors -ContentType "application/json" -InFile .\connector\postgres-orders.json
```

> ⚠️ Semua perintah Invoke-RestMethod harus dijalankan dari folder **root `cdc-demo`**, bukan dari subfolder `consumer`.

### Consumer tidak menerima event
Pastikan topic sudah ada:
```powershell
docker exec kafka sh -lc '/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list'
```
Topic `cdc_postgres.public.orders` harus muncul di list.

---

## Struktur Project

```
cdc-demo/
├── docker-compose.yml          — Kafka, Debezium, Kafka UI
├── connector/
│   └── postgres-orders.json   — Debezium connector config
├── smt/
│   ├── pom.xml                — Maven project custom SMT
│   └── src/                   — CanonicalizeValue + Sha256HashValue
├── sql/
│   └── orders.sql             — DDL tabel orders + publication
└── consumer/
    ├── main.go                — Kafka consumer + HTTP server
    ├── model/audit.go         — Struct AuditLog
    ├── store/audit.go         — SQLite CRUD
    ├── handler/
    │   ├── api.go             — REST API endpoints
    │   └── ws.go              — WebSocket hub
    └── static/
        ├── index.html         — Dashboard utama
        ├── detail.html        — Halaman detail event
        └── assets/
            ├── style.css      — Dark glassmorphism theme
            └── app.js         — WebSocket client + UI logic
```
