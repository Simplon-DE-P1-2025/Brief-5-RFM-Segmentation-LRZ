-- ════════════════════════════════════════════════════════════════════
-- etl/05_view_rfm_v.sql — Vues d'enrichissement consommées par le dashboard
--
-- Crée :
--   1. analytics.customer_first_purchase  → date d'acquisition par client
--   2. analytics.customer_rfm_v           → customer_rfm + segment_label
--                                            (préfixé A.→K.) + macro_segment
--                                            (A.LOYAL/B.PROMISING/C.SLEEP/D.LOST)
--
-- Le préfixe alphabétique sur segment_label permet le tri qualitatif
-- automatique (du meilleur segment au pire), qui est celui utilisé par
-- le dashboard de référence JustDataPlease.
--
-- Mapping macro segment validé en session 2026-04-08 :
--   A.LOYAL     = Champions + Loyal Customers + Potential Loyalist
--   B.PROMISING = Recent Customers + Promising + Need Attention
--   C.SLEEP     = About to Sleep + At Risk + Cannot Lose Them
--   D.LOST      = Hibernating + Lost
--
-- Note 2026-04-09 : 'Recent Customers' remplace l'ancien 'New Customers'
-- suite au refactor de 04_segments.sql vers 11 segments par codes RFM.
--
-- Idempotent : CREATE OR REPLACE.
-- ════════════════════════════════════════════════════════════════════

-- ────────────────────────────────────────────────────────────────────
-- 1. customer_first_purchase
--    Vue non matérialisée : sur 802 K lignes avec un index sur
--    clean.sales(customer_id), le GROUP BY reste sub-seconde.
--    À convertir en MATERIALIZED VIEW si on cible plusieurs millions
--    de lignes (au Sprint 5 par exemple).
-- ────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW analytics.customer_first_purchase AS
SELECT
    customer_id,
    MIN(invoice_date)::date                        AS first_purchase_dt,
    date_trunc('month', MIN(invoice_date))::date   AS acquisition_month
FROM clean.sales
GROUP BY customer_id;

COMMENT ON VIEW analytics.customer_first_purchase IS
  'Date de première commande par client (1ère facture). Source : clean.sales. Utilisé par les charts cohorte (Page 3 du dashboard v2).';

-- ────────────────────────────────────────────────────────────────────
-- 2. customer_rfm_v
--    Étend customer_rfm avec :
--      - segment_label : préfixé A.→K. pour tri qualitatif
--      - macro_segment : A.LOYAL / B.PROMISING / C.SLEEP / D.LOST
-- ────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW analytics.customer_rfm_v AS
SELECT
    c.customer_id,
    c.recency,
    c.frequency,
    c.monetary,
    c.r_score,
    c.f_score,
    c.m_score,
    c.rfm_segment,
    c.computed_at,
    CASE c.rfm_segment
        WHEN 'Champions'          THEN 'A.CHAMPIONS'
        WHEN 'Loyal Customers'    THEN 'B.LOYAL'
        WHEN 'Potential Loyalist' THEN 'C.POTENTIAL_LOYALIST'
        WHEN 'Recent Customers'      THEN 'D.RECENT_CUSTOMERS'
        WHEN 'Promising'          THEN 'E.PROMISING'
        WHEN 'Need Attention'     THEN 'F.NEED_ATTENTION'
        WHEN 'About to Sleep'     THEN 'G.ABOUT_TO_SLEEP'
        WHEN 'At Risk'            THEN 'H.AT_RISK'
        WHEN 'Cannot Lose Them'   THEN 'I.CANNOT_LOSE'
        WHEN 'Hibernating'        THEN 'J.HIBERNATING'
        WHEN 'Lost'               THEN 'K.LOST'
    END AS segment_label,
    CASE
        WHEN c.rfm_segment IN ('Champions', 'Loyal Customers', 'Potential Loyalist')
            THEN 'A.LOYAL'
        WHEN c.rfm_segment IN ('Recent Customers', 'Promising', 'Need Attention')
            THEN 'B.PROMISING'
        WHEN c.rfm_segment IN ('About to Sleep', 'At Risk', 'Cannot Lose Them')
            THEN 'C.SLEEP'
        WHEN c.rfm_segment IN ('Hibernating', 'Lost')
            THEN 'D.LOST'
    END AS macro_segment
FROM analytics.customer_rfm c;

COMMENT ON VIEW analytics.customer_rfm_v IS
  'Vue d''affichage du RFM avec préfixes alphabétiques (A.→K.) et macro segments (A.LOYAL/B.PROMISING/C.SLEEP/D.LOST). Source de vérité du dashboard v2.';
