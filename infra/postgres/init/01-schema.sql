-- Source of truth: relational model of the product catalog.

CREATE TABLE IF NOT EXISTS products (
    id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    sku        VARCHAR(32)    NOT NULL UNIQUE,
    name       VARCHAR(200)   NOT NULL,
    category   VARCHAR(100)   NOT NULL,
    price      NUMERIC(12, 2) NOT NULL CHECK (price >= 0),
    quantity   INTEGER        NOT NULL DEFAULT 0 CHECK (quantity >= 0),
    created_at TIMESTAMPTZ    NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ    NOT NULL DEFAULT clock_timestamp()
);

COMMENT ON TABLE  products            IS 'Product catalog (source of truth for the SQL -> Kafka -> MongoDB sync).';
COMMENT ON COLUMN products.id         IS 'Stable unique identifier: Kafka message key and MongoDB _id.';
COMMENT ON COLUMN products.updated_at IS 'Last modification time, maintained by trigger; used for change detection.';

-- Supports keyset pagination "WHERE (updated_at, id) > (:ts, :id) ORDER BY updated_at, id".
CREATE INDEX IF NOT EXISTS ix_products_updated_at_id ON products (updated_at, id);

-- updated_at is maintained by the database itself, so every change is detected,
-- including manual UPDATE statements that forget to touch updated_at.
CREATE OR REPLACE FUNCTION products_maintain_timestamps() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        NEW.created_at := clock_timestamp();
        NEW.updated_at := NEW.created_at;
        RETURN NEW;
    END IF;

    NEW.created_at := OLD.created_at;
    IF ROW(NEW.sku, NEW.name, NEW.category, NEW.price, NEW.quantity)
           IS NOT DISTINCT FROM ROW(OLD.sku, OLD.name, OLD.category, OLD.price, OLD.quantity)
       AND NEW.updated_at IS NOT DISTINCT FROM OLD.updated_at THEN
        -- Nothing changed: keep updated_at so no redundant event is produced.
        NEW.updated_at := OLD.updated_at;
    ELSE
        NEW.updated_at := clock_timestamp();
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_products_timestamps ON products;
CREATE TRIGGER trg_products_timestamps
    BEFORE INSERT OR UPDATE ON products
    FOR EACH ROW EXECUTE FUNCTION products_maintain_timestamps();

-- Producer progress: last (updated_at, id) that was acknowledged by Kafka.
CREATE TABLE IF NOT EXISTS sync_checkpoints (
    name            TEXT        PRIMARY KEY,
    last_updated_at TIMESTAMPTZ NOT NULL,
    last_product_id BIGINT      NOT NULL,
    saved_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE sync_checkpoints IS 'Watermark of the producer; survives restarts so changes are not re-read from scratch.';
