-- ════════════════════════════════════════════════════════════════════
-- etl/04_segments.sql — Assignation des 11 segments RFM via UPDATE CASE
--
--     Champions → Loyal Customers → Potential Loyalist → New Customers
--                                 → Promising → ...
--
--   PROBLÈME : `Potential Loyalist` (r>=4 AND f<=3) est un sur-ensemble de :
--     - `New Customers`  (r=5 AND f=1)
--     - `Promising`      (r>=4 AND f<=2 AND m<=2)
--   Avec l'ordre de base, Potential Loyalist absorbe ces deux sous-ensembles :
--   on obtient seulement 9 segments peuplés sur 11 (50 New Customers et
--   ~364 Promising sont reclassés à tort en Potential Loyalist).
--
--   Idem `Cannot Lose Them` (r<=2 AND f>=4 AND m>=4) doit être testé AVANT
--   `At Risk` (r<=2 AND f>=3 AND m>=3) qui est un sur-ensemble.
--
--   CORRECTIF : ordonner les CASE du **plus spécifique au plus général**.
--
-- Volumétrie attendue (5 852 clients sur Online Retail II) :
--   Champions          1299    Hibernating       620
--   Lost               1029    Potential Loyalist 515
--   Loyal Customers     689    At Risk           450
--   About to Sleep      377    Promising         317
--   Need Attention      265    Cannot Lose Them  241
--   New Customers        50
--
-- Pré-requis : analytics.customer_rfm rempli avec rfm_segment='TBD'.
-- Idempotent : UPDATE rejouable, met à jour toutes les lignes.
-- ════════════════════════════════════════════════════════════════════

UPDATE analytics.customer_rfm SET rfm_segment = CASE
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
END;
