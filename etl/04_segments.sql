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

    -- HIBERNATING
    -- Note : '231','241','251' retirés car déjà dans About to Sleep (CASE
    -- prend la première correspondance → ces codes tombent en About to Sleep).
    WHEN r_score::TEXT || f_score::TEXT || m_score::TEXT IN (
        '332','322','233','232','223','222','132',
        '123','122','212','211'
    ) THEN 'Hibernating'

    -- LOST
    WHEN r_score::TEXT || f_score::TEXT || m_score::TEXT IN ('111','112','121','131','141','151')
        THEN 'Lost'

    ELSE 'Unclassified'
END;
