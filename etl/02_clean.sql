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
-- Ce script LOG les compteurs après chaque INSERT (RAISE NOTICE) ainsi
-- qu'un compteur global "lignes raw rejetées" pour audit trail. Les
-- messages remontent dans les logs Airflow de la task transform_clean_rfm.
--
-- Note regex (`stock_code ~ '^(POST|DOT|M|...)$'`) : la regex est
-- BIEN ANCRÉE par `^...$`, donc `M` matche EXACTEMENT la chaîne "M" et
-- pas "MEDIUM" ni "MUG". Les wildcards `TEST.*`, `gift_.*`, `DCGS.*`
-- sont intentionnels (familles de codes).
--
-- Volumétrie attendue (sur Online Retail II, ~1,07 M lignes raw) :
--   stock_movements    ≈   4 382
--   cancellations      ≤  19 494
--   non_product_lines  ≤   6 074
--   sales              ≈ 802 637  (~798 000 attendu, +0,6 %)
--
-- ════════════════════════════════════════════════════════════════════

DO $$
DECLARE
    n_raw          BIGINT;
    n_stock        BIGINT;
    n_cancel       BIGINT;
    n_nonprod      BIGINT;
    n_sales        BIGINT;
    n_total_routed BIGINT;
    n_rejected     BIGINT;
BEGIN
    SELECT COUNT(*) INTO n_raw FROM raw.online_retail;
    RAISE NOTICE '[02_clean] raw.online_retail : % lignes en entrée', n_raw;

    -- 1. Mouvements de stock internes (priorité max) — ~4 382 lignes
    INSERT INTO clean.stock_movements
    SELECT * FROM raw.online_retail
    WHERE description IS NULL
      AND customer_id IS NULL
      AND price = 0;
    GET DIAGNOSTICS n_stock = ROW_COUNT;
    RAISE NOTICE '[02_clean] clean.stock_movements    : % lignes', n_stock;

    -- 2. Annulations — ≤ 19 494 lignes (exclut stock_movements)
    INSERT INTO clean.cancellations
    SELECT * FROM raw.online_retail
    WHERE invoice LIKE 'C%'
      AND NOT (description IS NULL AND customer_id IS NULL AND price = 0);
    GET DIAGNOSTICS n_cancel = ROW_COUNT;
    RAISE NOTICE '[02_clean] clean.cancellations      : % lignes', n_cancel;

    -- 3. Lignes non-produits — ≤ 6 074 (exclut stock_movements ET cancellations)
    INSERT INTO clean.non_product_lines
    SELECT * FROM raw.online_retail
    WHERE stock_code ~ '^(POST|DOT|M|BANK CHARGES|AMAZONFEE|ADJUST|S|D|C2|CRUK|TEST.*|gift_.*|PADS|DCGS.*)$'
      AND invoice NOT LIKE 'C%'
      AND NOT (description IS NULL AND customer_id IS NULL AND price = 0);
    GET DIAGNOSTICS n_nonprod = ROW_COUNT;
    RAISE NOTICE '[02_clean] clean.non_product_lines  : % lignes', n_nonprod;

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
    GET DIAGNOSTICS n_sales = ROW_COUNT;
    RAISE NOTICE '[02_clean] clean.sales              : % lignes', n_sales;

    -- Audit trail global : combien de lignes raw n'ont été routées
    -- vers AUCUN bucket (anomalies customer_id NULL hors stock_movements,
    -- quantity ≤ 0, price ≤ 0, etc.)
    n_total_routed := n_stock + n_cancel + n_nonprod + n_sales;
    n_rejected     := n_raw - n_total_routed;
    RAISE NOTICE '[02_clean] -------- TOTAL routé    : % lignes', n_total_routed;
    RAISE NOTICE '[02_clean] -------- REJETÉ (raw \ buckets) : % lignes (% %% du raw)',
                 n_rejected,
                 ROUND((n_rejected::numeric / NULLIF(n_raw, 0)::numeric) * 100, 2);
END
$$;
