-- schema.sql
--
-- CMS Hospital Quality Data Warehouse
-- Star schema design:
--
--   dim_hospital         → one row per hospital (slowly changing dimension)
--   fact_quality_measures → one row per hospital per quality measure
--
-- To run:
--   python warehouse/create_schema.py
--   (or paste directly into psql)

-- ── Extensions ────────────────────────────────────────────────────────────────

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ── Dimension: Hospital ───────────────────────────────────────────────────────
-- One row per CMS facility. This is the "who" of every quality measure.

DROP TABLE IF EXISTS fact_quality_measures;
DROP TABLE IF EXISTS dim_hospital;

CREATE TABLE dim_hospital (
    facility_id             VARCHAR(10)     PRIMARY KEY,
    facility_name           VARCHAR(255)    NOT NULL,
    address                 VARCHAR(255),
    city                    VARCHAR(100),
    state                   CHAR(2),
    zip_code                CHAR(5),
    county_or_parish        VARCHAR(100),
    phone_number            VARCHAR(20),
    hospital_type           VARCHAR(100),
    hospital_ownership      VARCHAR(100),
    emergency_services      BOOLEAN,
    overall_rating          SMALLINT        CHECK (overall_rating BETWEEN 1 AND 5),
    -- audit columns
    loaded_at               TIMESTAMP       DEFAULT NOW()
);

COMMENT ON TABLE dim_hospital IS
    'One row per CMS hospital facility. Source: Hospital General Information dataset.';

COMMENT ON COLUMN dim_hospital.overall_rating IS
    'CMS overall star rating 1–5. NULL means not yet rated.';

COMMENT ON COLUMN dim_hospital.emergency_services IS
    'Whether the hospital offers emergency services (Yes/No in source → boolean).';

-- ── Fact: Quality Measures ────────────────────────────────────────────────────
-- One row per hospital per quality measure.
-- This is the "what" — actual performance scores.

CREATE TABLE fact_quality_measures (
    id                      SERIAL          PRIMARY KEY,
    facility_id             VARCHAR(10)     NOT NULL REFERENCES dim_hospital(facility_id),
    condition               VARCHAR(100),
    measure_id              VARCHAR(50)     NOT NULL,
    measure_name            VARCHAR(500),
    score                   NUMERIC(8, 2),
    sample                  INTEGER,
    footnote                TEXT,
    start_date              DATE,
    end_date                DATE,
    facility_id_valid       BOOLEAN,
    -- audit columns
    loaded_at               TIMESTAMP       DEFAULT NOW()
);

COMMENT ON TABLE fact_quality_measures IS
    'One row per hospital per quality measure. Source: Timely and Effective Care dataset.';

COMMENT ON COLUMN fact_quality_measures.score IS
    'Numeric performance score. NULL means suppressed or not available.';

COMMENT ON COLUMN fact_quality_measures.sample IS
    'Number of patients in the measure sample. NULL means suppressed (<11) by CMS.';

-- ── Indexes ───────────────────────────────────────────────────────────────────
-- Speed up the most common analytical query patterns

CREATE INDEX idx_fact_facility_id   ON fact_quality_measures (facility_id);
CREATE INDEX idx_fact_measure_id    ON fact_quality_measures (measure_id);
CREATE INDEX idx_fact_condition     ON fact_quality_measures (condition);
CREATE INDEX idx_dim_state          ON dim_hospital (state);
CREATE INDEX idx_dim_rating         ON dim_hospital (overall_rating);