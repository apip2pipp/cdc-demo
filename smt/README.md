# CDC Audit SMT

Proof-of-concept Kafka Connect Single Message Transformations untuk audit trail:

1. `com.afif.cdc.smt.CanonicalizeValue`
   - Membaca field Debezium `after`.
   - Jika `after` null, fallback ke `before`.
   - Mengubah row payload menjadi canonical JSON dengan key object diurutkan alfabetis.
   - Menambahkan field top-level `canonical_payload`.

2. `com.afif.cdc.smt.Sha256HashValue`
   - Membaca `canonical_payload`.
   - Menghitung SHA-256.
   - Menambahkan field top-level `hash`.

Build:

```powershell
docker run --rm -v "${PWD}\smt:/workspace" -w /workspace maven:3.9.9-eclipse-temurin-17 mvn clean test package
```

Aktivasi connector:

```json
"transforms": "canonicalize,hash",
"transforms.canonicalize.type": "com.afif.cdc.smt.CanonicalizeValue",
"transforms.hash.type": "com.afif.cdc.smt.Sha256HashValue"
```
