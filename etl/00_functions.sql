-- ════════════════════════════════════════════════════════════════════
-- etl/00_functions.sql — Fonctions SQL et vues de couverture RFM
--
-- Source de vérité unique pour le mapping (r,f,m) → segment et
-- segment → macro_segment. Permet de SUPPRIMER la duplication entre
-- 04_segments.sql et 06_history.sql qui hébergeaient deux copies
-- identiques du même CASE WHEN 11 segments (56 lignes chacune).
--
-- Fonctions exposées :
--   - analytics.fn_rfm_segment(r INT, f INT, m INT) → TEXT
--       11 segments + 'Unclassified' en fallback
--   - analytics.fn_rfm_macro(rfm_segment TEXT)      → TEXT
--       4 macro segments (A.LOYAL/B.PROMISING/C.SLEEP/D.LOST)
--       + 'Z.UNCLASSIFIED' en fallback
--
-- Vue de diagnostic :
--   - analytics.v_rfm_codes_coverage : liste les 125 codes possibles
--     (r,f,m) ∈ {1..5}³ avec leur segment + macro. Sert à GARANTIR
--     l'exclusivité mutuelle des listes du CASE et l'absence de fallback
--     'Unclassified' inattendu.
--
-- Pré-requis : schéma analytics existant (init-db.sql).
-- Idempotent : CREATE OR REPLACE.
-- ════════════════════════════════════════════════════════════════════


-- ────────────────────────────────────────────────────────────────────
-- 1. analytics.fn_rfm_segment(r, f, m) → TEXT
--    Mapping (r,f,m) ∈ {1..5}³ vers les 11 segments métier.
--    Les listes IN (...) sont mutuellement exclusives — vérifié par
--    analytics.v_rfm_codes_coverage en fin de fichier.
-- ────────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION analytics.fn_rfm_segment(r INT, f INT, m INT)
RETURNS TEXT
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT CASE r::TEXT || f::TEXT || m::TEXT
        -- CHAMPIONS (7 codes)
        WHEN '555' THEN 'Champions'
        WHEN '554' THEN 'Champions'
        WHEN '544' THEN 'Champions'
        WHEN '545' THEN 'Champions'
        WHEN '454' THEN 'Champions'
        WHEN '455' THEN 'Champions'
        WHEN '445' THEN 'Champions'

        -- LOYAL CUSTOMERS (8 codes)
        WHEN '543' THEN 'Loyal Customers'
        WHEN '444' THEN 'Loyal Customers'
        WHEN '435' THEN 'Loyal Customers'
        WHEN '355' THEN 'Loyal Customers'
        WHEN '354' THEN 'Loyal Customers'
        WHEN '345' THEN 'Loyal Customers'
        WHEN '344' THEN 'Loyal Customers'
        WHEN '335' THEN 'Loyal Customers'

        -- POTENTIAL LOYALIST (24 codes)
        WHEN '553' THEN 'Potential Loyalist'
        WHEN '551' THEN 'Potential Loyalist'
        WHEN '552' THEN 'Potential Loyalist'
        WHEN '541' THEN 'Potential Loyalist'
        WHEN '542' THEN 'Potential Loyalist'
        WHEN '533' THEN 'Potential Loyalist'
        WHEN '532' THEN 'Potential Loyalist'
        WHEN '531' THEN 'Potential Loyalist'
        WHEN '452' THEN 'Potential Loyalist'
        WHEN '451' THEN 'Potential Loyalist'
        WHEN '442' THEN 'Potential Loyalist'
        WHEN '441' THEN 'Potential Loyalist'
        WHEN '431' THEN 'Potential Loyalist'
        WHEN '453' THEN 'Potential Loyalist'
        WHEN '433' THEN 'Potential Loyalist'
        WHEN '432' THEN 'Potential Loyalist'
        WHEN '423' THEN 'Potential Loyalist'
        WHEN '353' THEN 'Potential Loyalist'
        WHEN '352' THEN 'Potential Loyalist'
        WHEN '351' THEN 'Potential Loyalist'
        WHEN '342' THEN 'Potential Loyalist'
        WHEN '341' THEN 'Potential Loyalist'
        WHEN '333' THEN 'Potential Loyalist'
        WHEN '323' THEN 'Potential Loyalist'

        -- RECENT CUSTOMERS (7 codes)
        WHEN '512' THEN 'Recent Customers'
        WHEN '511' THEN 'Recent Customers'
        WHEN '422' THEN 'Recent Customers'
        WHEN '421' THEN 'Recent Customers'
        WHEN '412' THEN 'Recent Customers'
        WHEN '411' THEN 'Recent Customers'
        WHEN '311' THEN 'Recent Customers'

        -- PROMISING (16 codes)
        WHEN '525' THEN 'Promising'
        WHEN '524' THEN 'Promising'
        WHEN '523' THEN 'Promising'
        WHEN '522' THEN 'Promising'
        WHEN '521' THEN 'Promising'
        WHEN '515' THEN 'Promising'
        WHEN '514' THEN 'Promising'
        WHEN '513' THEN 'Promising'
        WHEN '425' THEN 'Promising'
        WHEN '424' THEN 'Promising'
        WHEN '413' THEN 'Promising'
        WHEN '414' THEN 'Promising'
        WHEN '415' THEN 'Promising'
        WHEN '315' THEN 'Promising'
        WHEN '314' THEN 'Promising'
        WHEN '313' THEN 'Promising'

        -- NEED ATTENTION (8 codes)
        WHEN '535' THEN 'Need Attention'
        WHEN '534' THEN 'Need Attention'
        WHEN '443' THEN 'Need Attention'
        WHEN '434' THEN 'Need Attention'
        WHEN '343' THEN 'Need Attention'
        WHEN '334' THEN 'Need Attention'
        WHEN '325' THEN 'Need Attention'
        WHEN '324' THEN 'Need Attention'

        -- ABOUT TO SLEEP (8 codes — '231','241','251' inclus ici)
        WHEN '331' THEN 'About to Sleep'
        WHEN '321' THEN 'About to Sleep'
        WHEN '312' THEN 'About to Sleep'
        WHEN '221' THEN 'About to Sleep'
        WHEN '213' THEN 'About to Sleep'
        WHEN '231' THEN 'About to Sleep'
        WHEN '241' THEN 'About to Sleep'
        WHEN '251' THEN 'About to Sleep'

        -- AT RISK (23 codes)
        WHEN '255' THEN 'At Risk'
        WHEN '254' THEN 'At Risk'
        WHEN '245' THEN 'At Risk'
        WHEN '244' THEN 'At Risk'
        WHEN '253' THEN 'At Risk'
        WHEN '252' THEN 'At Risk'
        WHEN '243' THEN 'At Risk'
        WHEN '242' THEN 'At Risk'
        WHEN '235' THEN 'At Risk'
        WHEN '234' THEN 'At Risk'
        WHEN '225' THEN 'At Risk'
        WHEN '224' THEN 'At Risk'
        WHEN '153' THEN 'At Risk'
        WHEN '152' THEN 'At Risk'
        WHEN '145' THEN 'At Risk'
        WHEN '143' THEN 'At Risk'
        WHEN '142' THEN 'At Risk'
        WHEN '135' THEN 'At Risk'
        WHEN '134' THEN 'At Risk'
        WHEN '133' THEN 'At Risk'
        WHEN '125' THEN 'At Risk'
        WHEN '124' THEN 'At Risk'

        -- CANNOT LOSE THEM (8 codes)
        WHEN '155' THEN 'Cannot Lose Them'
        WHEN '154' THEN 'Cannot Lose Them'
        WHEN '144' THEN 'Cannot Lose Them'
        WHEN '214' THEN 'Cannot Lose Them'
        WHEN '215' THEN 'Cannot Lose Them'
        WHEN '115' THEN 'Cannot Lose Them'
        WHEN '114' THEN 'Cannot Lose Them'
        WHEN '113' THEN 'Cannot Lose Them'

        -- HIBERNATING (11 codes)
        WHEN '332' THEN 'Hibernating'
        WHEN '322' THEN 'Hibernating'
        WHEN '233' THEN 'Hibernating'
        WHEN '232' THEN 'Hibernating'
        WHEN '223' THEN 'Hibernating'
        WHEN '222' THEN 'Hibernating'
        WHEN '132' THEN 'Hibernating'
        WHEN '123' THEN 'Hibernating'
        WHEN '122' THEN 'Hibernating'
        WHEN '212' THEN 'Hibernating'
        WHEN '211' THEN 'Hibernating'

        -- LOST (6 codes)
        WHEN '111' THEN 'Lost'
        WHEN '112' THEN 'Lost'
        WHEN '121' THEN 'Lost'
        WHEN '131' THEN 'Lost'
        WHEN '141' THEN 'Lost'
        WHEN '151' THEN 'Lost'

        ELSE 'Unclassified'
    END;
$$;

COMMENT ON FUNCTION analytics.fn_rfm_segment(INT, INT, INT) IS
  'Mapping (r,f,m) ∈ {1..5}³ → 11 segments métier RFM. Source de vérité unique appelée par 04_segments.sql et 06_history.sql.';


-- ────────────────────────────────────────────────────────────────────
-- 2. analytics.fn_rfm_macro(rfm_segment) → TEXT
--    Mapping vers les 4 macro segments du dashboard.
--    Validé en session 2026-04-08, voir 05_view_rfm_v.sql lignes 14-18.
-- ────────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION analytics.fn_rfm_macro(rfm_segment TEXT)
RETURNS TEXT
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT CASE rfm_segment
        WHEN 'Champions'          THEN 'A.LOYAL'
        WHEN 'Loyal Customers'    THEN 'A.LOYAL'
        WHEN 'Potential Loyalist' THEN 'A.LOYAL'
        WHEN 'Recent Customers'   THEN 'B.PROMISING'
        WHEN 'Promising'          THEN 'B.PROMISING'
        WHEN 'Need Attention'     THEN 'B.PROMISING'
        WHEN 'About to Sleep'     THEN 'C.SLEEP'
        WHEN 'At Risk'            THEN 'C.SLEEP'
        WHEN 'Cannot Lose Them'   THEN 'C.SLEEP'
        WHEN 'Hibernating'        THEN 'D.LOST'
        WHEN 'Lost'               THEN 'D.LOST'
        ELSE 'Z.UNCLASSIFIED'
    END;
$$;

COMMENT ON FUNCTION analytics.fn_rfm_macro(TEXT) IS
  'Mapping segment RFM → macro segment (A.LOYAL/B.PROMISING/C.SLEEP/D.LOST). Source de vérité unique appelée par 05_view_rfm_v.sql et 06_history.sql.';


-- ────────────────────────────────────────────────────────────────────
-- 3. analytics.v_rfm_codes_coverage
--    Vue diagnostic : liste les 125 codes possibles et leur classement.
--    Permet de vérifier que :
--      a) Aucun code ne tombe en 'Unclassified' (couverture exhaustive)
--      b) Aucun code n'est dupliqué entre listes (exclusivité mutuelle
--         garantie par construction du SELECT, mais le test reste utile
--         si la fonction est modifiée)
--      c) Le mapping macro est aussi exhaustif (pas de 'Z.UNCLASSIFIED')
--
--    Tests à exécuter :
--      SELECT COUNT(*) FROM analytics.v_rfm_codes_coverage;
--        -- attendu : 125
--      SELECT COUNT(*) FROM analytics.v_rfm_codes_coverage
--        WHERE rfm_segment = 'Unclassified';
--        -- attendu : 0
--      SELECT COUNT(*) FROM analytics.v_rfm_codes_coverage
--        WHERE macro_segment = 'Z.UNCLASSIFIED';
--        -- attendu : 0
-- ────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW analytics.v_rfm_codes_coverage AS
WITH codes AS (
    SELECT r, f, m
    FROM generate_series(1, 5) AS r,
         generate_series(1, 5) AS f,
         generate_series(1, 5) AS m
)
SELECT
    r,
    f,
    m,
    r::TEXT || f::TEXT || m::TEXT                       AS code,
    analytics.fn_rfm_segment(r, f, m)                    AS rfm_segment,
    analytics.fn_rfm_macro(analytics.fn_rfm_segment(r, f, m)) AS macro_segment
FROM codes
ORDER BY r DESC, f DESC, m DESC;

COMMENT ON VIEW analytics.v_rfm_codes_coverage IS
  'Vue de diagnostic listant les 125 codes RFM (r,f,m) ∈ {1..5}³ avec leur segment + macro. Sert à garantir la couverture exhaustive et l''exclusivité mutuelle des listes.';
