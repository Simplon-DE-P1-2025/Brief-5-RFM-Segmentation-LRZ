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
        -- CASE WHEN 11 segments harmonisé avec 04_segments.sql
        -- (ordre spécifique → général, codes RFM concaténés avec cast ::TEXT
        -- car r_score/f_score/m_score sont INTEGER et l'opérateur || n'existe
        -- pas nativement pour integer || integer en PostgreSQL).
        CASE
            -- CHAMPIONS
            WHEN r_score::TEXT || f_score::TEXT || m_score::TEXT IN ('555','554','544','545','454','455','445')
                THEN 'Champions'

            -- LOYAL
            WHEN r_score::TEXT || f_score::TEXT || m_score::TEXT IN ('543','444','435','355','354','345','344','335')
                THEN 'Loyal Customers'

            -- POTENTIAL LOYALIST
            WHEN r_score::TEXT || f_score::TEXT || m_score::TEXT IN (
                '553','551','552','541','542','533','532','531','452','451',
                '442','441','431','453','433','432','423','353','352','351',
                '342','341','333','323'
            ) THEN 'Potential Loyalist'

            -- RECENT CUSTOMERS
            WHEN r_score::TEXT || f_score::TEXT || m_score::TEXT IN ('512','511','422','421','412','411','311')
                THEN 'Recent Customers'

            -- PROMISING
            WHEN r_score::TEXT || f_score::TEXT || m_score::TEXT IN (
                '525','524','523','522','521','515','514','513','425','424',
                '413','414','415','315','314','313'
            ) THEN 'Promising'

            -- NEED ATTENTION
            WHEN r_score::TEXT || f_score::TEXT || m_score::TEXT IN ('535','534','443','434','343','334','325','324')
                THEN 'Need Attention'

            -- ABOUT TO SLEEP
            WHEN r_score::TEXT || f_score::TEXT || m_score::TEXT IN ('331','321','312','221','213','231','241','251')
                THEN 'About to Sleep'

            -- AT RISK
            WHEN r_score::TEXT || f_score::TEXT || m_score::TEXT IN (
                '255','254','245','244','253','252','243','242','235',
                '234','225','224','153','152','145','143','142','135','134',
                '133','125','124'
            ) THEN 'At Risk'

            -- CANNOT LOSE THEM
            WHEN r_score::TEXT || f_score::TEXT || m_score::TEXT IN ('155','154','144','214','215','115','114','113')
                THEN 'Cannot Lose Them'

            -- HIBERNATING (sans '231','241','251' — déjà dans About to Sleep)
            WHEN r_score::TEXT || f_score::TEXT || m_score::TEXT IN (
                '332','322','233','232','223','222','132',
                '123','122','212','211'
            ) THEN 'Hibernating'

            -- LOST
            WHEN r_score::TEXT || f_score::TEXT || m_score::TEXT IN ('111','112','121','131','141','151')
                THEN 'Lost'

            ELSE 'Unclassified'
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
        WHEN rfm_segment IN ('Recent Customers', 'Promising', 'Need Attention')        THEN 'B.PROMISING'
        WHEN rfm_segment IN ('About to Sleep', 'At Risk', 'Cannot Lose Them')       THEN 'C.SLEEP'
        WHEN rfm_segment IN ('Hibernating', 'Lost')                                  THEN 'D.LOST'
    END AS macro_segment
FROM labeled;
