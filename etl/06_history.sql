-- ════════════════════════════════════════════════════════════════════
-- etl/06_history.sql — Snapshot mensuel RFM 
--
-- Calcule, pour chaque dernier jour de mois entre la 1ère et la
-- dernière facture du dataset, le R/F/M de chaque client en
-- utilisant **uniquement** les ventes ≤ snapshot_date. Reapplique
-- ensuite NTILE(5) puis le CASE WHEN des 11 segments + macros.
--
-- Résultat : analytics.customer_rfm_history (~24 × ~5 850 ≈ 140 K lignes)
--
-- il alimente les pages Movements (% par macro × mois) et Cohorts (KPIs par mois
-- d'acquisition) du dashboard  — visualisations IMPOSSIBLES à
-- produire avec le snapshot unique de analytics.customer_rfm.
--
-- Pré-requis :
--   - clean.sales rempli 
--   - analytics.customer_rfm rempli 
--
-- Idempotent : CREATE IF NOT EXISTS + TRUNCATE + INSERT.
--
-- Performance attendue : ~10-30 s sur Online Retail II. La jointure
-- months × clean.sales fait ~24 × 800K opérations mais Postgres optimise
-- bien grâce aux index sur clean.sales(invoice_date) et (customer_id).
-- ════════════════════════════════════════════════════════════════════

-- ────────────────────────────────────────────────────────────────────
-- 0. DDL idempotente (au cas où le volume Postgres ne contient pas
--    encore la table — utile si init-db.sql n'a pas été rejoué).
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

CREATE INDEX IF NOT EXISTS idx_rfm_hist_snapshot ON analytics.customer_rfm_history(snapshot_date);
CREATE INDEX IF NOT EXISTS idx_rfm_hist_macro    ON analytics.customer_rfm_history(macro_segment);

-- ────────────────────────────────────────────────────────────────────
-- 1. Reset (idempotence)
-- ────────────────────────────────────────────────────────────────────
TRUNCATE analytics.customer_rfm_history;

-- ────────────────────────────────────────────────────────────────────
-- 2. Alimentation : 1 INSERT massif via CTE
-- ────────────────────────────────────────────────────────────────────
WITH months AS (
    -- Génère 1 ligne par fin de mois entre la 1ère et la dernière facture.
    -- end_of_month = 1er jour du mois suivant - 1 jour
    SELECT generate_series(
        (date_trunc('month', (SELECT MIN(invoice_date) FROM clean.sales))::date + INTERVAL '1 month' - INTERVAL '1 day')::date,
        (date_trunc('month', (SELECT MAX(invoice_date) FROM clean.sales))::date + INTERVAL '1 month' - INTERVAL '1 day')::date,
        INTERVAL '1 month'
    )::date AS snapshot_date
),

snapshot_raw AS (
    -- Pour chaque (mois, client) : recency / frequency / monetary calculés
    -- en utilisant uniquement les ventes ≤ snapshot_date.
    -- ⚠️ produit ~24 × 5852 ≈ 140 K lignes après agrégation.
    SELECT
        m.snapshot_date,
        s.customer_id,
        ((m.snapshot_date + 1) - MAX(s.invoice_date)::date)  AS recency,
        COUNT(DISTINCT s.invoice_id)                          AS frequency,
        ROUND(SUM(s.line_amount)::numeric, 2)                 AS monetary
    FROM months m
    JOIN clean.sales s
      ON s.invoice_date::date <= m.snapshot_date
    GROUP BY m.snapshot_date, s.customer_id
),

scored AS (
    -- NTILE(5) recalculé indépendamment pour chaque snapshot.
    -- customer_id stabilise les ex aequo d'un rerun à l'autre.
    SELECT
        snapshot_date,
        customer_id,
        recency,
        frequency,
        monetary,
        6 - NTILE(5) OVER (PARTITION BY snapshot_date ORDER BY recency, customer_id)   AS r_score,
        NTILE(5) OVER     (PARTITION BY snapshot_date ORDER BY frequency, customer_id) AS f_score,
        NTILE(5) OVER     (PARTITION BY snapshot_date ORDER BY monetary, customer_id)  AS m_score
    FROM snapshot_raw
),

labeled AS (
    -- Applique le même CASE WHEN que 04_segments.sql
    -- (ordre spécifique → général, indispensable pour ne pas absorber les
    -- sous-ensembles New Customers / Promising / Cannot Lose Them)
    SELECT
        snapshot_date,
        customer_id,
        recency,
        frequency,
        monetary,
        r_score,
        f_score,
        m_score,
        CASE
            WHEN r_score >= 4 AND f_score >= 4 AND m_score >= 4 THEN 'Champions'
            WHEN r_score >= 3 AND f_score >= 4                  THEN 'Loyal Customers'
            WHEN r_score = 5  AND f_score = 1                   THEN 'New Customers'
            WHEN r_score >= 4 AND f_score <= 2 AND m_score <= 2 THEN 'Promising'
            WHEN r_score >= 4 AND f_score <= 3                  THEN 'Potential Loyalist'
            WHEN r_score = 3  AND f_score = 3                   THEN 'Need Attention'
            WHEN r_score = 3  AND f_score <= 2                  THEN 'About to Sleep'
            WHEN r_score <= 2 AND f_score >= 4 AND m_score >= 4 THEN 'Cannot Lose Them'
            WHEN r_score <= 2 AND f_score >= 3 AND m_score >= 3 THEN 'At Risk'
            WHEN r_score = 2  AND f_score <= 2                  THEN 'Hibernating'
            ELSE                                                     'Lost'
        END AS rfm_segment
    FROM scored
)

INSERT INTO analytics.customer_rfm_history (
    snapshot_date, customer_id, recency, frequency, monetary,
    r_score, f_score, m_score, rfm_segment, macro_segment
)
SELECT
    snapshot_date,
    customer_id,
    recency,
    frequency,
    monetary,
    r_score,
    f_score,
    m_score,
    rfm_segment,
    -- Mapping vers les 4 macro segments (A.LOYAL / B.PROMISING / C.SLEEP / D.LOST)
    CASE
        WHEN rfm_segment IN ('Champions', 'Loyal Customers', 'Potential Loyalist') THEN 'A.LOYAL'
        WHEN rfm_segment IN ('New Customers', 'Promising', 'Need Attention')        THEN 'B.PROMISING'
        WHEN rfm_segment IN ('About to Sleep', 'At Risk', 'Cannot Lose Them')       THEN 'C.SLEEP'
        WHEN rfm_segment IN ('Hibernating', 'Lost')                                  THEN 'D.LOST'
    END AS macro_segment
FROM labeled;
