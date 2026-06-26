# Custom Debezium SMT Audit Trail PoC

Dokumen ini menjelaskan layer baru:

```text
PostgreSQL
  -> Debezium PostgreSQL Connector
  -> Canonicalization SMT
  -> SHA256 Hash SMT
  -> Kafka topic
  -> Go Consumer
```

Hash dibuat di Kafka Connect/Debezium layer, sebelum record ditulis ke Kafka. Consumer Go tidak melakukan hashing.

## Cara Kerja SMT

SMT atau Single Message Transformation adalah hook ringan di Kafka Connect yang berjalan untuk setiap `ConnectRecord`. Untuk Debezium source connector, urutannya kira-kira:

```text
PostgreSQL WAL
  -> Debezium membuat SourceRecord
  -> Kafka Connect menjalankan transforms sesuai urutan config `transforms`
  -> Converter mengubah Connect data menjadi bytes/JSON
  -> Kafka producer menulis record ke topic
```

Artinya jika transform dipasang di Debezium source connector, hasil transform sudah menjadi bagian dari message yang masuk Kafka.

## Debezium Event

Debezium PostgreSQL menghasilkan envelope seperti ini:

```json
{
  "before": null,
  "after": {
    "id": 1,
    "product": "Keyboard",
    "qty": 2
  },
  "source": {
    "db": "belajar-postgres",
    "schema": "public",
    "table": "orders"
  },
  "op": "c"
}
```

Pada runtime SMT, value biasanya berbentuk Kafka Connect `Struct` dengan `Schema`, bukan string JSON mentah. Karena itu custom SMT di project ini mendukung dua bentuk:

- `Struct`, untuk record Debezium normal dengan schema.
- `Map`, untuk mode schemaless.

## Canonicalization SMT

Class:

```text
com.afif.cdc.smt.CanonicalizeValue
```

Desain:

- Membaca field `after`.
- Jika `after` null, fallback ke `before`. Ini penting untuk event `DELETE`.
- Mengubah row payload menjadi JSON stabil.
- Semua key object diurutkan alfabetis.
- Array tetap mengikuti urutan asli.
- Field null tetap ditulis sebagai `null`.
- Menambahkan field top-level baru di envelope: `canonical_payload`.

Contoh row:

```json
{
  "name": "Afif",
  "id": 1
}
```

Menjadi:

```json
{"id":1,"name":"Afif"}
```

## SHA256 Hash SMT

Class:

```text
com.afif.cdc.smt.Sha256HashValue
```

Desain:

- Membaca field `canonical_payload`.
- Menghitung SHA-256 dari string canonical tersebut.
- Menulis hasil hex digest ke field top-level baru: `hash`.

Contoh output di payload Kafka:

```json
{
  "before": null,
  "after": {
    "id": 1,
    "product": "Keyboard",
    "qty": 2
  },
  "op": "c",
  "canonical_payload": "{\"id\":1,\"product\":\"Keyboard\",\"qty\":2}",
  "hash": "..."
}
```

Field `before`, `after`, `source`, dan `op` tidak diubah, supaya consumer lama tetap bisa membaca event Debezium.

## Struktur Project Java

```text
smt/
  pom.xml
  README.md
  src/main/java/com/afif/cdc/smt/
    CanonicalizeValue.java
    Sha256HashValue.java
    CanonicalJson.java
    ConnectRecordUtil.java
  src/main/resources/META-INF/services/
    org.apache.kafka.connect.transforms.Transformation
  src/test/java/com/afif/cdc/smt/
    CanonicalizeValueTest.java
    Sha256HashValueTest.java
```

Dependency utama:

- `org.apache.kafka:connect-api:3.7.0` dengan scope `provided`.
- Versi `3.7.0` dipilih karena image `debezium/connect:2.7.3.Final` di project ini membawa Kafka Connect 3.7.0.

## Build JAR

Host ini belum wajib punya Maven lokal. Build bisa pakai Docker:

```powershell
docker run --rm -v "${PWD}\smt:/workspace" -w /workspace maven:3.9.9-eclipse-temurin-17 mvn clean test package
```

Output JAR:

```text
smt/target/cdc-audit-smt-1.0.0.jar
```

## Install Plugin ke Kafka Connect

`docker-compose.yml` sudah mount folder target ke plugin path Debezium:

```yaml
connect:
  volumes:
    - ./smt/target:/kafka/connect/cdc-audit-smt:ro
```

Setelah JAR dibuild, restart Debezium Connect agar plugin discan ulang:

```powershell
docker compose up -d --force-recreate connect
```

Cek plugin:

```powershell
$plugins = Invoke-RestMethod "http://localhost:8083/connector-plugins?connectorsOnly=false"
$plugins.Where({ $_.class -like "com.afif.cdc.smt*" })
```

Harus muncul:

```text
com.afif.cdc.smt.CanonicalizeValue
com.afif.cdc.smt.Sha256HashValue
```

## Konfigurasi Connector

`connector/postgres-orders.json` sudah mengaktifkan transform berurutan:

```json
"transforms": "canonicalize,hash",

"transforms.canonicalize.type": "com.afif.cdc.smt.CanonicalizeValue",
"transforms.canonicalize.input.field": "after",
"transforms.canonicalize.fallback.field": "before",
"transforms.canonicalize.output.field": "canonical_payload",

"transforms.hash.type": "com.afif.cdc.smt.Sha256HashValue",
"transforms.hash.input.field": "canonical_payload",
"transforms.hash.output.field": "hash"
```

Urutan penting. `hash` harus berjalan setelah `canonicalize`, karena hash membaca `canonical_payload`.

## Recreate Connector

Jika connector lama sudah pernah dibuat, hapus lalu register ulang:

```powershell
Invoke-RestMethod -Method Delete -Uri http://localhost:8083/connectors/orders-postgres-connector
Start-Sleep -Seconds 3
Invoke-RestMethod `
  -Method Post `
  -Uri http://localhost:8083/connectors `
  -ContentType "application/json" `
  -InFile .\connector\postgres-orders.json
```

Cek status:

```powershell
Invoke-RestMethod http://localhost:8083/connectors/orders-postgres-connector/status
```

## Verifikasi

Insert data:

```sql
INSERT INTO public.orders (product, qty) VALUES ('SMT Hash Test', 1);
```

Consume satu message dari Kafka dan cek field:

```powershell
docker exec kafka sh -lc "/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic cdc_postgres.public.orders --from-beginning --max-messages 1"
```

Dengan JSON converter bawaan image ini, output biasanya masih dibungkus:

```json
{
  "schema": { "...": "..." },
  "payload": {
    "after": {
      "id": 13,
      "product": "SMT Hash Test",
      "qty": 1
    },
    "op": "c",
    "canonical_payload": "{\"created_at\":\"...\",\"id\":13,\"product\":\"SMT Hash Test\",\"qty\":1}",
    "hash": "2aa9cf47a0d9c8f78388a5fba8fcd25019da4c7bfe9fdf29f16371c61bda588a"
  }
}
```

## Catatan Produksi

PoC ini cukup untuk membuktikan arsitektur. Untuk produksi, pertimbangkan:

- Gunakan standar canonical JSON formal seperti RFC 8785/JCS jika harus interoperable lintas bahasa.
- Tentukan apakah hash mewakili row state saja, atau event lengkap termasuk `op`, `source`, dan timestamp.
- Simpan `previous_hash` untuk membentuk hash chain audit log.
- Tambahkan signature/HMAC jika hash perlu autentikasi, bukan hanya integritas.
- Hindari menyimpan secret di connector JSON plaintext.
