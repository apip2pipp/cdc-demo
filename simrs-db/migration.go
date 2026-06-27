package main

import (
	"database/sql"
	"log"
)

var ddl = `
CREATE TABLE IF NOT EXISTS public.patient (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name TEXT NOT NULL,
    dob DATE NOT NULL,
    gender TEXT NOT NULL,
    address TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.doctor (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name TEXT NOT NULL,
    specialization TEXT NOT NULL,
    phone TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.room (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    room_name TEXT NOT NULL,
    type TEXT NOT NULL,
    capacity INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.visit (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    patient_id BIGINT NOT NULL REFERENCES public.patient(id),
    doctor_id BIGINT NOT NULL REFERENCES public.doctor(id),
    visit_date TIMESTAMPTZ NOT NULL DEFAULT now(),
    complaints TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.queue (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    patient_id BIGINT NOT NULL REFERENCES public.patient(id),
    doctor_id BIGINT NOT NULL REFERENCES public.doctor(id),
    queue_number INTEGER NOT NULL,
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.medical_record (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    visit_id BIGINT NOT NULL REFERENCES public.visit(id),
    patient_id BIGINT NOT NULL REFERENCES public.patient(id),
    notes TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.diagnosis (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    medical_record_id BIGINT NOT NULL REFERENCES public.medical_record(id),
    icd10_code TEXT NOT NULL,
    description TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.prescription (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    medical_record_id BIGINT NOT NULL REFERENCES public.medical_record(id),
    medicine_name TEXT NOT NULL,
    dosage TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.laboratory (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    visit_id BIGINT NOT NULL REFERENCES public.visit(id),
    test_name TEXT NOT NULL,
    result TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.billing (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    visit_id BIGINT NOT NULL REFERENCES public.visit(id),
    total_amount DECIMAL(15,2) NOT NULL,
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- REPLICA IDENTITY FULL for Debezium updates/deletes before state
ALTER TABLE public.patient REPLICA IDENTITY FULL;
ALTER TABLE public.doctor REPLICA IDENTITY FULL;
ALTER TABLE public.room REPLICA IDENTITY FULL;
ALTER TABLE public.visit REPLICA IDENTITY FULL;
ALTER TABLE public.queue REPLICA IDENTITY FULL;
ALTER TABLE public.medical_record REPLICA IDENTITY FULL;
ALTER TABLE public.diagnosis REPLICA IDENTITY FULL;
ALTER TABLE public.prescription REPLICA IDENTITY FULL;
ALTER TABLE public.laboratory REPLICA IDENTITY FULL;
ALTER TABLE public.billing REPLICA IDENTITY FULL;

-- DEBEZIUM ROLE
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_roles
        WHERE rolname = 'debezium'
    ) THEN
        CREATE ROLE debezium WITH LOGIN REPLICATION PASSWORD 'debezium123';
    ELSE
        ALTER ROLE debezium WITH LOGIN REPLICATION PASSWORD 'debezium123';
    END IF;
END $$;

GRANT CONNECT ON DATABASE "simrs_db" TO debezium;
GRANT USAGE ON SCHEMA public TO debezium;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium;

-- PUBLICATION
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_publication
        WHERE pubname = 'dbz_simrs_publication'
    ) THEN
        CREATE PUBLICATION dbz_simrs_publication FOR ALL TABLES;
    END IF;
END $$;
`

func RunMigration(db *sql.DB) {
	_, err := db.Exec(ddl)
	if err != nil {
		log.Fatalf("Failed to run DDL: %v", err)
	}
	log.Println("Migration successful.")
}
