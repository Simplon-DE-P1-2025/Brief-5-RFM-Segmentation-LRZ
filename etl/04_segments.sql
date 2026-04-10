-- ════════════════════════════════════════════════════════════════════
-- etl/04_segments.sql — Assignation des 11 segments RFM
--
-- Délègue la logique métier à analytics.fn_rfm_segment(r, f, m), définie
-- dans etl/00_functions.sql. Auparavant, ce script contenait un CASE WHEN
-- 56 lignes DUPLIQUÉ à l'identique dans etl/06_history.sql, ce qui était
-- une bombe à retardement de maintenance (bug appliqué deux fois ou
-- divergence silencieuse entre snapshot courant et historique).
--
-- Le mapping est basé sur des LISTES ÉNUMÉRÉES de codes RFM (pas des
-- conditions r>=4 AND f<=3 qui se chevaucheraient). L'exclusivité
-- mutuelle des 125 codes possibles est garantie par construction et
-- VÉRIFIABLE via la vue analytics.v_rfm_codes_coverage.
--
-- Volumétrie attendue (5 852 clients sur Online Retail II) :
--   Champions          1299    Hibernating       620
--   Lost               1029    Potential Loyalist 515
--   Loyal Customers     689    At Risk           450
--   About to Sleep      377    Promising         317
--   Need Attention      265    Cannot Lose Them  241
--   Recent Customers     50
--
-- Pré-requis :
--   - analytics.customer_rfm rempli avec rfm_segment='TBD'
--   - analytics.fn_rfm_segment installée (etl/00_functions.sql)
-- Idempotent : UPDATE rejouable, met à jour toutes les lignes.
-- ════════════════════════════════════════════════════════════════════

UPDATE analytics.customer_rfm
SET rfm_segment = analytics.fn_rfm_segment(r_score, f_score, m_score);
