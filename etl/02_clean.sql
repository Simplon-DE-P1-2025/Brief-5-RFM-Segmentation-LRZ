-- ════════════════════════════════════════════════════════════════════
-- etl/02_clean.sql — Cascade de cleaning exclusive (raw → clean.*)
--
-- Source de vérité : Romain/projet_rfm_pipeline.md §6.1
-- La cascade route certaines lignes de raw.online_retail vers EXACTEMENT
-- UN bucket métier grâce à un ordre strictement décroissant en spécificité :
--
--   1. clean.stock_movements    (priorité max — mouvements internes)
--   2. clean.cancellations      (factures C* hors stock_movements)
--   3. clean.non_product_lines  (codes produits techniques POST/DOT/...)
--   4. clean.sales              (ventes "propres" pour le RFM)
--
-- Important : cette cascade n'est PAS exhaustive sur tout le raw.
-- Les transactions anonymes restantes (customer_id NULL), ainsi que
-- certaines anomalies quantity/price hors scope, sont écartées
-- silencieusement par le filtre final de clean.sales.
--
-- Volumétrie attendue (sur Online Retail II, ~1,07 M lignes raw) :
--   stock_movements    ≈   4 382
--   cancellations      ≤  19 494
--   non_product_lines  ≤   6 074
--   sales              ≈ 802 637  (~798 000 attendu, +0,6 %)
--
-- ════════════════════════════════════════════════════════════════════

-- 1. Mouvements de stock internes (priorité max) — ~4 382 lignes
INSERT INTO clean.stock_movements
SELECT * FROM raw.online_retail
WHERE description IS NULL
  AND customer_id IS NULL
  AND price = 0;

-- 2. Annulations — ≤ 19 494 lignes (exclut stock_movements)
INSERT INTO clean.cancellations
SELECT * FROM raw.online_retail
WHERE invoice LIKE 'C%'
  AND NOT (description IS NULL AND customer_id IS NULL AND price = 0);

-- 3. Lignes non-produits — ≤ 6 074 (exclut stock_movements ET cancellations)
INSERT INTO clean.non_product_lines
SELECT * FROM raw.online_retail
WHERE stock_code ~ '^(POST|DOT|M|BANK CHARGES|AMAZONFEE|ADJUST|S|D|C2|CRUK|TEST.*|gift_.*|PADS|DCGS.*)$'
  AND invoice NOT LIKE 'C%'
  AND NOT (description IS NULL AND customer_id IS NULL AND price = 0);

-- 4. Ventes propres pour le RFM — ~798 000 lignes
INSERT INTO clean.sales (invoice_id, stock_code, description, quantity,
                         invoice_date, unit_price, customer_id, country)
SELECT invoice, stock_code, description, quantity,
       invoice_date, price, customer_id, country
FROM raw.online_retail
WHERE customer_id IS NOT NULL
  AND invoice NOT LIKE 'C%'
  AND quantity > 0
  AND price > 0
  AND stock_code !~ '^(POST|DOT|M|BANK CHARGES|AMAZONFEE|ADJUST|S|D|C2|CRUK|TEST.*|gift_.*|PADS|DCGS.*)$';
