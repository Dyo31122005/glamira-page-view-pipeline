-- Danh chieu (dimension) --------------------------------------------------

CREATE TABLE IF NOT EXISTS dim_date (
    date_key     BIGINT PRIMARY KEY,  -- format yyyyMMddHH, vd 2026100815 (granularity theo gio)
    full_date    DATE NOT NULL,
    hour         SMALLINT NOT NULL,
    day          SMALLINT NOT NULL,
    month        SMALLINT NOT NULL,
    year         SMALLINT NOT NULL,
    quarter      SMALLINT NOT NULL,
    day_name     VARCHAR NOT NULL,
    month_name   VARCHAR NOT NULL,
    is_weekend   BOOLEAN NOT NULL,
    created_at   TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dim_location (
    location_key  SERIAL PRIMARY KEY,
    country_code  VARCHAR NOT NULL,  -- vd "GB", tu IP2Location
    country_name  VARCHAR NOT NULL,  -- vd "United Kingdom of Great Britain..."
    region_name   VARCHAR,           -- NULL cho den khi nang cap IP2Location DB5 LITE tro len
    city_name     VARCHAR,           -- NULL cho den khi nang cap IP2Location DB5 LITE tro len
    created_at    TIMESTAMP DEFAULT now(),
    updated_at    TIMESTAMP DEFAULT now()
);

-- natural key la ca 3 cap (khong chi country_code) de san sang tach theo
-- region/city khi co du lieu that; COALESCE vi region/city co the NULL va
-- Postgres coi 2 NULL la khac nhau trong UNIQUE constraint thong thuong.
CREATE UNIQUE INDEX IF NOT EXISTS uq_dim_location_natural_key
    ON dim_location (country_code, COALESCE(region_name, ''), COALESCE(city_name, ''));

CREATE TABLE IF NOT EXISTS dim_product (
    product_key  SERIAL PRIMARY KEY,
    product_id   VARCHAR UNIQUE NOT NULL,  -- natural key tu Kafka
    created_at   TIMESTAMP DEFAULT now(),
    updated_at   TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dim_store (
    store_key   SERIAL PRIMARY KEY,
    store_id    VARCHAR UNIQUE NOT NULL,
    created_at  TIMESTAMP DEFAULT now(),
    updated_at  TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key   SERIAL PRIMARY KEY,
    device_id      VARCHAR UNIQUE NOT NULL,
    email_hashed   VARCHAR,          -- sha256(email), khong luu email tho
    has_email      BOOLEAN NOT NULL DEFAULT FALSE,
    created_at     TIMESTAMP DEFAULT now(),
    updated_at     TIMESTAMP DEFAULT now()
);

-- Su kien (fact) -------------------------------------------------------------

CREATE TABLE IF NOT EXISTS fact_page_view (
    id             VARCHAR PRIMARY KEY,  -- log id tu Kafka, dam bao upsert idempotent
    date_key       BIGINT REFERENCES dim_date (date_key),
    location_key   INT REFERENCES dim_location (location_key),
    product_key    INT REFERENCES dim_product (product_key),
    store_key      INT REFERENCES dim_store (store_key),
    customer_key   INT REFERENCES dim_customer (customer_key),
    browser        VARCHAR,
    os             VARCHAR,
    collection     VARCHAR,
    current_url    VARCHAR,
    referrer_url   VARCHAR,
    local_time     TIMESTAMP,
    ingested_at    TIMESTAMP DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_fact_page_view_date ON fact_page_view (date_key);
CREATE INDEX IF NOT EXISTS idx_fact_page_view_product_date ON fact_page_view (product_key, date_key);
CREATE INDEX IF NOT EXISTS idx_fact_page_view_location_date ON fact_page_view (location_key, date_key);
CREATE INDEX IF NOT EXISTS idx_fact_page_view_store ON fact_page_view (store_key);
