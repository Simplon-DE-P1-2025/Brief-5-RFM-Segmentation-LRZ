-- ════════════════════════════════════════════════════════════════════
-- etl/03_rfm.sql — Calcul RFM via NTILE(5)
--
-- `MAX(invoice_date) + interval '1 day'` comme date de
--   référence, ce qui retourne un TIMESTAMP. La soustraction
--   `timestamp - timestamp` retourne un INTERVAL (et non un INTEGER),
--   ce qui casse le typage attendu pour la colonne `recency INTEGER`.
--
--   Correctif : on cast en DATE et on additionne 1 jour en arithmétique
--   DATE pure (`MAX(invoice_date)::date + 1`). La soustraction
--   `date - date` retourne directement un INTEGER (nombre de jours),
--   ce qui correspond exactement au typage de la colonne.
--
-- Volumétrie attendue : 5 500 → 5 943 clients (5 852 sur Online Retail II).
-- Pré-requis : clean.sales rempli
-- Idempotent : suppose un TRUNCATE préalable de analytics.customer_rfm.
-- ════════════════════════════════════════════════════════════════════

WITH ref AS (
    -- ref_date = jour qui suit la dernière facture du dataset.
    -- On reste en type DATE (date + integer → date) pour pouvoir
    -- soustraire deux dates et obtenir un INTEGER (nombre de jours).
    SELECT MAX(invoice_date)::date + 1 AS ref_date
    FROM clean.sales
),
rfm_raw AS (
    SELECT
        s.customer_id,
        (r.ref_date - MAX(s.invoice_date)::date)        AS recency,
        COUNT(DISTINCT s.invoice_id)                     AS frequency,
        ROUND(SUM(s.line_amount)::numeric, 2)            AS monetary
    FROM clean.sales s, ref r
    GROUP BY s.customer_id, r.ref_date
)
INSERT INTO analytics.customer_rfm
    (customer_id, recency, frequency, monetary, r_score, f_score, m_score, rfm_segment)
SELECT
    customer_id,
    recency,
    frequency,
    monetary,
    -- R inversé : récence basse = meilleur score (5 = très récent, 1 = très ancien)
    -- customer_id sert de tie-breaker stable pour rendre le scoring rejouable.
    6 - NTILE(5) OVER (ORDER BY recency, customer_id)   AS r_score,
    NTILE(5) OVER (ORDER BY frequency, customer_id)     AS f_score,
    NTILE(5) OVER (ORDER BY monetary, customer_id)      AS m_score,
    'TBD'                                             AS rfm_segment   -- assigné à l'étape 04
FROM rfm_raw;
