-- Création de l'utilisateur et de la base de données métier RFM
-- Ce script est exécuté automatiquement par PostgreSQL au premier démarrage

-- 1. Utilisateur métier + base de données dédiée
CREATE USER rfm_user WITH PASSWORD 'rfm_pass';
CREATE DATABASE rfm_db OWNER rfm_user;
GRANT ALL PRIVILEGES ON DATABASE rfm_db TO rfm_user;

-- 2. Bascule sur la base métier pour créer les schémas et tables
\connect rfm_db

-- 3. Schémas (pattern lakehouse : raw → clean → analytics)
CREATE SCHEMA IF NOT EXISTS raw       AUTHORIZATION rfm_user;
CREATE SCHEMA IF NOT EXISTS clean     AUTHORIZATION rfm_user;
CREATE SCHEMA IF NOT EXISTS analytics AUTHORIZATION rfm_user;

-- ────────────────────────────────────────────────────────────────────
-- 4. raw.online_retail
-- ────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS raw.online_retail (
    invoice       TEXT,
    stock_code    TEXT,
    description   TEXT,
    quantity      INTEGER,
    invoice_date  TIMESTAMP,
    price         NUMERIC(10,3),
    customer_id   INTEGER,                       -- NULL autorisé : ~22,77 % du dataset
    country       TEXT
);
ALTER TABLE raw.online_retail OWNER TO rfm_user;

-- ────────────────────────────────────────────────────────────────────
-- 5. clean.sales — ventes "propres" pour le calcul RFM (~798 000 lignes)
-- ────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS clean.sales (
    invoice_id    TEXT          NOT NULL,
    stock_code    TEXT          NOT NULL,
    description   TEXT,
    quantity      INTEGER       NOT NULL,
    invoice_date  TIMESTAMP     NOT NULL,
    unit_price    NUMERIC(10,3) NOT NULL,
    customer_id   INTEGER       NOT NULL,
    country       TEXT,
    line_amount   NUMERIC(12,2) GENERATED ALWAYS AS (quantity * unit_price) STORED
);
ALTER TABLE clean.sales OWNER TO rfm_user;
CREATE INDEX IF NOT EXISTS idx_sales_customer ON clean.sales(customer_id);
CREATE INDEX IF NOT EXISTS idx_sales_date     ON clean.sales(invoice_date);

-- ────────────────────────────────────────────────────────────────────
-- 6. Tables d'audit
--    Mêmes colonnes que raw.online_retail — buckets disjoints des ventes.
-- ────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS clean.stock_movements (
    invoice       TEXT,
    stock_code    TEXT,
    description   TEXT,
    quantity      INTEGER,
    invoice_date  TIMESTAMP,
    price         NUMERIC(10,3),
    customer_id   INTEGER,
    country       TEXT
);
ALTER TABLE clean.stock_movements OWNER TO rfm_user;

CREATE TABLE IF NOT EXISTS clean.cancellations (
    invoice       TEXT,
    stock_code    TEXT,
    description   TEXT,
    quantity      INTEGER,
    invoice_date  TIMESTAMP,
    price         NUMERIC(10,3),
    customer_id   INTEGER,
    country       TEXT
);
ALTER TABLE clean.cancellations OWNER TO rfm_user;

CREATE TABLE IF NOT EXISTS clean.non_product_lines (
    invoice       TEXT,
    stock_code    TEXT,
    description   TEXT,
    quantity      INTEGER,
    invoice_date  TIMESTAMP,
    price         NUMERIC(10,3),
    customer_id   INTEGER,
    country       TEXT
);
ALTER TABLE clean.non_product_lines OWNER TO rfm_user;

-- ────────────────────────────────────────────────────────────────────
-- 7. analytics.customer_rfm — table cible du dashboard
-- ────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS analytics.customer_rfm (
    customer_id   INTEGER      PRIMARY KEY,
    recency       INTEGER      NOT NULL,
    frequency     INTEGER      NOT NULL,
    monetary      NUMERIC(12,2) NOT NULL,
    r_score       INTEGER      NOT NULL,
    f_score       INTEGER      NOT NULL,
    m_score       INTEGER      NOT NULL,
    rfm_segment   TEXT         NOT NULL,
    computed_at   TIMESTAMP    DEFAULT now()
);
ALTER TABLE analytics.customer_rfm OWNER TO rfm_user;
CREATE INDEX IF NOT EXISTS idx_rfm_segment ON analytics.customer_rfm(rfm_segment);

-- ────────────────────────────────────────────────────────────────────
-- 8. analytics.customer_rfm_history — snapshot mensuel 
--    Une ligne par (snapshot_date, customer_id) : recalcul RFM rétrospectif
--    à chaque fin de mois en utilisant uniquement les ventes ≤ snapshot_date.
--    Alimente les pages Movements et Cohorts du dashboard v2.
--    Volumétrie attendue : ~24 mois × ~5 850 clients ≈ ~140 000 lignes.
-- ────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS analytics.customer_rfm_history (
    snapshot_date DATE          NOT NULL,
    customer_id   INTEGER       NOT NULL,
    recency       INTEGER       NOT NULL,
    frequency     INTEGER       NOT NULL,
    monetary      NUMERIC(12,2) NOT NULL,
    r_score       INTEGER       NOT NULL,
    f_score       INTEGER       NOT NULL,
    m_score       INTEGER       NOT NULL,
    rfm_segment   TEXT          NOT NULL,
    macro_segment TEXT          NOT NULL,
    PRIMARY KEY (snapshot_date, customer_id)
);
ALTER TABLE analytics.customer_rfm_history OWNER TO rfm_user;
CREATE INDEX IF NOT EXISTS idx_rfm_hist_snapshot ON analytics.customer_rfm_history(snapshot_date);
CREATE INDEX IF NOT EXISTS idx_rfm_hist_macro    ON analytics.customer_rfm_history(macro_segment);
