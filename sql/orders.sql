CREATE TABLE IF NOT EXISTS public.orders (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    product TEXT NOT NULL,
    qty INTEGER NOT NULL CHECK (qty > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

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

GRANT CONNECT ON DATABASE "belajar-postgres" TO debezium;
GRANT USAGE ON SCHEMA public TO debezium;
GRANT SELECT ON public.orders TO debezium;

ALTER TABLE public.orders REPLICA IDENTITY FULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_publication
        WHERE pubname = 'dbz_orders_publication'
    ) THEN
        CREATE PUBLICATION dbz_orders_publication FOR TABLE public.orders;
    END IF;
END $$;

-- Contoh insert untuk test CDC:
-- INSERT INTO public.orders (product, qty) VALUES ('Keyboard', 1);