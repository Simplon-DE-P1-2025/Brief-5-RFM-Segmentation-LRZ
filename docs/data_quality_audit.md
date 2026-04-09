# Audit qualité des données — `online_retail_combined.csv`

> Audit réalisé avec **csvkit 2.2.0** en vue de préparer l'ETL.
> Fichier source : `online_retail_combined.csv` (90 Mo, 1 067 371 lignes, 8 colonnes), obtenu par concaténation des 2 feuilles de `online_retail_II.xlsx` (cf. [`typage_ingestion.md`](./typage_ingestion.md)).
> Période couverte : **2009-12-01 → 2011-12-09**.
>

---

## 1. Vue d'ensemble

### 1.1 Schéma des colonnes

```bash
csvcut -n online_retail_combined.csv
```

| # | Colonne       | Type détecté |
|---|---------------|--------------|
| 1 | Invoice       | Text         |
| 2 | StockCode     | Text         |
| 3 | Description   | Text         |
| 4 | Quantity      | Number       |
| 5 | InvoiceDate   | DateTime     |
| 6 | Price         | Number       |
| 7 | Customer ID   | Number       |
| 8 | Country       | Text         |

### 1.2 Statistiques globales (`csvstat`)

```bash
csvstat online_retail_combined.csv
```

| Colonne     | Non-null  | Uniques | Min / Plus court     | Max / Plus long     | Moyenne     | Médiane | StDev   |
|-------------|-----------|---------|----------------------|---------------------|-------------|---------|---------|
| Invoice     | 1 067 371 | 53 628  | —                    | 7 caractères        | —           | —       | —       |
| StockCode   | 1 067 371 | 5 305   | —                    | 12 caractères       | —           | —       | —       |
| Description | 1 062 989 | 5 699   | —                    | 35 caractères       | —           | —       | —       |
| Quantity    | 1 067 371 | 1 057   | **-80 995**          | **80 995**          | 9.94        | 3       | 172.71  |
| InvoiceDate | 1 067 371 | 47 635  | 2009-12-01 07:45     | 2011-12-09 12:50    | —           | —       | —       |
| Price       | 1 067 371 | 2 807   | **-53 594.36**       | **38 970**          | 4.65        | 2.10    | 123.55  |
| Customer ID | 824 364   | 5 943   | 12 346               | 18 287              | 15 324.64   | 15 255  | 1 697.46|
| Country     | 1 067 371 | 43      | —                    | 20 caractères       | —           | —       | —       |

**Lignes totales** : 1 067 371 — **Factures uniques** : 53 628 — **Clients uniques** : 5 943 — **Pays** : 43.

---

## 2. Valeurs manquantes

```bash
# Une commande par colonne — chaque comptage rend wc -l + 1 (ligne d'entête conservée par csvgrep)
csvgrep -c Invoice       -r '^$' online_retail_combined.csv | wc -l   # 1   (0 manquant)
csvgrep -c StockCode     -r '^$' online_retail_combined.csv | wc -l   # 1   (0 manquant)
csvgrep -c Description   -r '^$' online_retail_combined.csv | wc -l   # 4383
csvgrep -c InvoiceDate   -r '^$' online_retail_combined.csv | wc -l   # 1   (0 manquant)
csvgrep -c "Customer ID" -r '^$' online_retail_combined.csv | wc -l   # 243008
```

| Colonne     | Lignes manquantes | %         |
|-------------|-------------------|-----------|
| Invoice     | 0                 | 0,00 %    |
| StockCode   | 0                 | 0,00 %    |
| Description | **4 382**         | 0,41 %    |
| Quantity    | 0                 | 0,00 %    |
| InvoiceDate | 0                 | 0,00 %    |
| Price       | 0                 | 0,00 %    |
| Customer ID | **243 007**       | **22,77 %** |
| Country     | 0                 | 0,00 %    |

### 2.1 Corrélation Description nulle ↔ Customer ID nul

```bash
csvgrep -c Description -r '^$' online_retail_combined.csv \
  | csvgrep -c "Customer ID" -r '^$' \
  | wc -l   # 4383  → 100 % d'overlap
```

Les **4 382 lignes sans Description sont aussi sans Customer ID**.

### 2.2 Description nulle ↔ StockCode présent (réparable par lookup)

```bash
# Toutes les lignes sans Description ont-elles un StockCode ?
csvgrep -c Description -r '^$' online_retail_combined.csv \
  | csvgrep -c StockCode -r '^$' | wc -l   # 1 → 0 cas où les deux sont nuls

# Combien de StockCodes distincts dans ces lignes ?
csvgrep -c Description -r '^$' online_retail_combined.csv \
  | csvcut -c StockCode | tail -n +2 | sort -u | wc -l   # 2451

# Calcul de l'intersection avec les StockCodes ayant une Description ailleurs
csvgrep -c Description -r '^$'   online_retail_combined.csv | csvcut -c StockCode | tail -n +2 | sort -u > /tmp/stock_null_desc.txt
csvgrep -c Description -r '^.+$' online_retail_combined.csv | csvcut -c StockCode | tail -n +2 | sort -u > /tmp/stock_with_desc.txt

comm -12 /tmp/stock_null_desc.txt /tmp/stock_with_desc.txt | wc -l   # 2096 récupérables
comm -23 /tmp/stock_null_desc.txt /tmp/stock_with_desc.txt | wc -l   # 355  non récupérables

# Combien de LIGNES (et non plus de StockCodes) sont récupérables ?
comm -12 /tmp/stock_null_desc.txt /tmp/stock_with_desc.txt > /tmp/stock_recoverable.txt
csvgrep -c Description -r '^$' online_retail_combined.csv \
  | csvgrep -c StockCode -f /tmp/stock_recoverable.txt | wc -l   # 4020 → 4019 lignes
```

| Niveau    | Total | Récupérables (lookup) | Non récupérables |
|-----------|-------|-----------------------|------------------|
| StockCodes uniques | 2 451 | **2 096 (85,5 %)** | 355 (14,5 %) |
| Lignes             | 4 382 | **4 019 (91,7 %)** | 363 (8,3 %) |

→ **Les 4 382 lignes sans Description ont toutes un StockCode** (0 ligne avec les deux nuls). **91,7 % d'entre elles peuvent être réparées** par un simple lookup `StockCode → Description` sur les autres lignes du fichier. Seules **363 lignes** (355 StockCodes inconnus) restent non récupérables et devront être isolées.

---

## 3. Anomalies numériques

### 3.1 Quantités négatives (retours / annulations)

```bash
csvgrep -c Quantity -r '^-' online_retail_combined.csv | wc -l                                  # 22951 (-1 entête)
csvgrep -c Quantity -r '^-' online_retail_combined.csv | csvgrep -c Invoice -r '^C' | wc -l    # 19495 (-1 entête)
```

| Catégorie                                                  | Lignes  |
|------------------------------------------------------------|---------|
| Quantity < 0                                               | 22 950  |
| ↳ dont liées à une **facture annulée** (Invoice débute par `C`) | 19 494  |
| ↳ **non rattachées** à une facture annulée                 | 3 456   |

→ Les ~3 500 quantités négatives "orphelines" sont probablement des **ajustements de stock** (à investiguer en ETL : faut-il les écarter ou les marquer ?).

### 3.2 Prix anormaux

```bash
csvgrep -c Price -r '^-' online_retail_combined.csv         | wc -l   # 6   (5 lignes)
csvgrep -c Price -r '^0(\.0+)?$' online_retail_combined.csv | wc -l   # 6203 (6202 lignes)
```

| Catégorie     | Lignes  | Remarque                                              |
|---------------|---------|-------------------------------------------------------|
| Price < 0     | 5       | extrêmes : `-53 594.36` (probable correction comptable) |
| Price == 0    | **6 202** | dont 4 382 écritures techniques (cf. §3.4) ; reste ~1 820 vraies anomalies |

#### 3.2.1 Top descriptions pour `Price == 0`

```bash
csvgrep -c Price -r '^0(\.0+)?$' online_retail_combined.csv \
  | csvcut -c Description | tail -n +2 | sort | uniq -c | sort -rn | head -30
```

| Description (regroupée)                                            | Lignes  | Catégorie                       |
|--------------------------------------------------------------------|--------:|---------------------------------|
| _(vide)_                                                           | **4 382** | **Écritures techniques** (cf. §3.4) |
| `check` / `checked`                                                | 170     | Contrôles d'inventaire          |
| `damages` / `damaged` / `Damaged`                                  | 182     | Casse                           |
| `?`                                                                | 92      | Données incertaines             |
| `found` / `Found`                                                  | 37      | Stock retrouvé                  |
| `smashed` / `thrown away` / `Unsaleable, destroyed.`               | 27      | Destruction                     |
| `missing`                                                          | 27      | Stock manquant                  |
| `sold as set on dotcom`                                            | 20      | Bundle e-commerce               |
| `adjustment`                                                       | 16      | Ajustement explicite            |
| `dotcom` / `amazon`                                                | 23      | Markers canal                   |
| **Vrais produits** (`OWL DOORSTOP`, `IVORY KITCHEN SCALES`, `POLYESTER FILLER PAD`…) | **~1 200** | **Échantillons, cadeaux ou erreurs de saisie** |

→ Une fois retirées les **écritures techniques** (4 382 lignes) et les **notes d'opérateur** (~620 lignes), il reste **~1 200 lignes "vrais produits à 0 €"** qui méritent une investigation métier ciblée.

### 3.3 Outliers extrêmes

- **Quantity** : min `-80 995`, max `+80 995` — la valeur miroir suggère une transaction et son annulation symétrique.
- **Price** : min `-53 594.36`, max `38 970` — à inspecter manuellement (cf. lignes `M`, `ADJUST`, `AMAZONFEE`).
- **StDev** très élevée sur Quantity (172.7) et Price (123.6) → distribution lourdement skewed, médianes (3 et 2.10 €) bien plus représentatives que les moyennes.

### 3.4 Profil unifié des écritures techniques

```bash
# Triple équivalence vérifiée
csvgrep -c Description -r '^$' online_retail_combined.csv | csvgrep -c "Customer ID" -r '^$' | wc -l   # 4383
csvgrep -c Description -r '^$' online_retail_combined.csv | csvgrep -c Price -r '^0(\.0+)?$' | wc -l    # 4383
```

Les **4 382 lignes "techniques"** identifiées séparément (Description NULL), (Customer ID NULL parmi celles-là) et (Price = 0) sont **exactement les mêmes lignes**. Il existe donc un **profil unique parfaitement détectable** :

> `Description IS NULL` ⟺ `Customer ID IS NULL` ⟺ `Price = 0`

| Caractéristique         | Valeur                             |
|-------------------------|------------------------------------|
| Volume                  | **4 382 lignes** (0,41 % du fichier) |
| Description             | NULL (100 %)                       |
| Customer ID             | NULL (100 %)                       |
| Price                   | 0 (100 %)                          |
| StockCode               | présent (100 %, 2 451 SKU distincts) |
| Quantity                | présent — souvent négatif (write-off) |

→ **Conséquence ETL majeure** : ces lignes sont des **mouvements de stock internes** (et non des ventes). Elles peuvent être routées d'un seul filtre vers une table dédiée `stock_movements` au lieu d'être traitées séparément par chacun des trois critères.

---

## 4. Factures et codes spéciaux

### 4.1 Factures annulées

```bash
csvgrep -c Invoice -r '^C' online_retail_combined.csv | wc -l   # 19495 (-1 entête)
```

→ **19 494 lignes** appartiennent à des factures annulées (préfixe `C`).

### 4.2 StockCodes non-produits

```bash
csvcut -c StockCode online_retail_combined.csv | tail -n +2 \
  | grep -viE '^[0-9]+[A-Za-z]*$' | sort | uniq -c | sort -rn | head -20
```

| StockCode      | Lignes | Signification probable                  |
|----------------|--------|-----------------------------------------|
| `POST`         | 2 122  | Frais postaux                           |
| `DOT`          | 1 446  | Dotcom postage                          |
| `M`            | 1 421  | Manuel / ajustement                     |
| `C2`           | 282    | Carriage                                |
| `D`            | 177    | Discount                                |
| `S`            | 104    | Samples                                 |
| `BANK CHARGES` | 102    | Frais bancaires                         |
| `ADJUST`       | 67     | Ajustement de stock                     |
| `AMAZONFEE`    | 43     | Frais marketplace Amazon                |
| `CRUK`         | 16     | Don Cancer Research UK                  |
| `gift_0001_*`  | ~61    | Cartes-cadeaux (10 / 20 / 30 £)         |
| `TEST001/2`    | ~15    | Lignes de test                          |
| `DCGS*`, `PADS`, etc. | ~120 | Codes administratifs divers       |

```bash
csvgrep -c StockCode \
  -r '^(POST|DOT|M|BANK CHARGES|AMAZONFEE|ADJUST|S|D|C2|CRUK|TEST.*|gift_.*|PADS|DCGS.*)$' \
  online_retail_combined.csv | wc -l   # 6075 (-1 entête)
```

→ Au total **~6 074 lignes** correspondent à des **transactions non-produit** à isoler dans une table séparée (`fees`, `adjustments`) lors de l'ETL.

### 4.3 Format dominant des SKU : `5 chiffres + 1 lettre`

```bash
csvgrep -c StockCode -r '^[0-9]{5}[A-Za-z]$' online_retail_combined.csv | wc -l                                  # 127521  → 127520 lignes
csvgrep -c StockCode -r '^[0-9]{5}[A-Za-z]$' online_retail_combined.csv | csvcut -c StockCode | tail -n +2 | sort -u | wc -l   # 1647 SKU uniques

# Familles produit (préfixe à 5 chiffres)
csvgrep -c StockCode -r '^[0-9]{5}[A-Za-z]$' online_retail_combined.csv \
  | csvcut -c StockCode | tail -n +2 | sed 's/.$//' | sort -u | wc -l   # 617

# Distribution des lettres-variantes
csvgrep -c StockCode -r '^[0-9]{5}[A-Za-z]$' online_retail_combined.csv \
  | csvcut -c StockCode | tail -n +2 | sed 's/^[0-9]*//' | tr 'a-z' 'A-Z' | sort | uniq -c | sort -rn
```

#### Volumétrie

| Indicateur                                   | Valeur       |
|----------------------------------------------|--------------|
| Lignes concernées                            | **127 520**  |
| **% du fichier total**                       | **11,95 %**  |
| SKU uniques (variants)                       | 1 647        |
| Familles produit (préfixe 5 chiffres)        | 617          |
| Variants par famille (moyenne)               | ~2,7         |

#### Sémantique

Format **« code famille + lettre de variante »** : la partie numérique identifie un produit générique, la lettre désigne une déclinaison (couleur, style, motif). Exemples vérifiés :

| StockCode | Description                          |
|-----------|--------------------------------------|
| `15056N`  | EDWARDIAN PARASOL **NATURAL**        |
| `15056P`  | EDWARDIAN PARASOL **PINK**           |
| `85099B`  | JUMBO BAG **RED** RETROSPOT          |
| `85099C`  | JUMBO BAG BAROQUE **BLACK WHITE**    |
| `85099F`  | JUMBO BAG **STRAWBERRY**             |
| `85123A`  | WHITE HANGING HEART T-LIGHT HOLDER   |

**Le code "base" sans lettre n'existe pas** : `csvgrep -c StockCode -r '^85123$'` et `^85099$` renvoient zéro résultat. Le suffixe lettre fait donc partie intégrante du SKU.

#### Distribution des lettres-variantes

```
B: 36 532   A: 32 464   C: 16 411   D:  9 570   L: 5 461
E:  5 395   F:  4 728   S:  4 326   G:  3 649   P: 2 099
N:  1 642   W:  1 276   M:  1 016   K:    930   H:   644
J:    568   U:    393   R:    193   V:     93   I:    42
T:     38   Z:     19   O:     19   Y:     12
```

24 lettres utilisées (toutes sauf Q et X). `A`/`B` représentent à eux seuls **54 %** des variants — souvent les deux premières couleurs/styles d'une gamme.

#### Implication ETL

Pour modéliser proprement, le SKU `85099B` peut être **éclaté en deux clés** :
- `product_family_id` = `85099` (préfixe numérique)
- `variant_code` = `B` (lettre)

→ Permet les analyses **par famille produit** (toutes couleurs confondues) en plus des analyses par SKU exact.

---

## 5. Doublons

```bash
tail -n +2 online_retail_combined.csv | sort | uniq -d | wc -l   # 32907
```

→ **32 907 lignes distinctes** apparaissent en doublon exact (toutes colonnes identiques). Le volume total de lignes dupliquées est plus élevé (chaque "valeur dupliquée" peut apparaître ≥ 2 fois). À dédupliquer prudemment : deux ventes identiques à la même seconde sont rares mais pas impossibles sur un même panier.

> **Décision projet RFM v1** : **pas de déduplication automatique**. Les doublons exacts sont considérés comme des paniers plausibles (un client peut acheter 2 fois le même article dans la même seconde via une saisie en double). à discuter

---

## 6. Répartition géographique

```bash
csvcut -c Country online_retail_combined.csv | tail -n +2 | sort | uniq -c | sort -rn
```

| Pays              | Lignes   | %      |
|-------------------|----------|--------|
| United Kingdom    | 981 330  | 91,9 % |
| EIRE (Irlande)    | 17 866   | 1,67 % |
| Germany           | 17 624   | 1,65 % |
| France            | 14 330   | 1,34 % |
| Netherlands       | 5 140    | 0,48 % |
| Spain             | 3 811    | 0,36 % |
| Switzerland       | 3 189    | 0,30 % |
| Belgium           | 3 123    | 0,29 % |
| Portugal          | 2 620    | 0,25 % |
| Australia         | 1 913    | 0,18 % |
| _… 33 autres pays_| 16 425   | 1,54 % |

**Points d'attention** :
- `Unspecified` : **756 lignes** sans pays renseigné — à requalifier.
- `European Community` : **61 lignes** — libellé fourre-tout, à remapper.
- `RSA` (= South Africa) : code à uniformiser avec un libellé complet.
- Le marché est massivement britannique (~92 %) : tout modèle/agrégat global sera dominé par l'UK.

---

## 7. Synthèse — préparation de l'ETL

### 7.1 Verdict global

Le fichier est **structurellement sain** (8 colonnes constantes, types homogènes, 0 valeur manquante sur 6 colonnes sur 8) mais présente **trois gros chantiers fonctionnels** :

1. **22,8 % de transactions anonymes** (Customer ID nul) — à conserver mais marquées.
2. **Mélange ventes / ajustements / frais** dans la même table — à séparer pour fiabiliser l'analyse commerciale.
3. **4 382 mouvements de stock internes** identifiables par un profil unique `Description NULL ∧ Customer ID NULL ∧ Price = 0` (cf. §3.4) — à router en bloc vers `stock_movements`.

Aucune ligne n'est à supprimer en aveugle : tout peut être traité par enrichissement, flag ou éclatement en tables dédiées.

### 7.2 Modèle cible proposé

> **Note** : ce modèle est la **cible idéale de l'audit** (avec dimensions products/countries normalisées et conservation des anonymes). La pipeline RFM v1 implémente un sous-ensemble plus restrictif : pas de dim products, pas de dim countries, anonymes écartés. 

```
                       ┌─ products            (1 647 SKU + 617 familles)        
                       │
sales_clean ───────────┤─ customers           (5 943 + bucket "anonymous")      
  ~1 023 000 lignes    │                                                        
                       └─ countries           (43 → libellés normalisés ISO)   

stock_movements        ~4 382   lignes (Description+Customer+Price tous nuls/0)   
cancellations          ~19 494  lignes (Invoice C*)                                             [≤19 494]
non_product_lines      ~6 074   lignes (POST, DOT, M, fees, gift_cards…)                        [≤6 074]
data_issues            ~363     lignes (Description irréparable, Price ≠ 0)                     
```

### 7.3 Chantiers priorisés

#### P1 — bloquants pour l'analyse commerciale

| # | Chantier                                  | Volume    | Action ETL                                                                                           |
|---|-------------------------------------------|-----------|------------------------------------------------------------------------------------------------------|
| 1 | **Mouvements de stock internes**    | **4 382** | **Router en bloc** vers `stock_movements` (filtre unique : `Description IS NULL ∧ Customer IS NULL ∧ Price = 0`) |
| 2 | StockCodes non-produits                   | ~6 074    | **Éclater** vers `non_product_lines` (sous-types : `postage`, `fee`, `adjustment`, `gift_card`)      |
| 3 | Factures annulées (`C*`)                  | 19 494    | **Éclater** vers `cancellations`, ajouter lien vers la facture d'origine quand identifiable          |
| 4 | `Customer ID` manquants        | ~238 625  | **Conserver** dans `sales_clean` avec `customer_id = NULL` + flag `is_anonymous = true`              |

#### P2 — qualité fine et modélisation

> **Décisions projet RFM** : Le RFM est client-centrique et ne nécessite pas l'éclatement des SKU ni la traçabilité fine des ajustements. 

| # | Chantier                              | Volume          | Action ETL                                                                  |
|---|---------------------------------------|-----------------|-----------------------------------------------------------------------------|
| 5 | SKU `5 chiffres + 1 lettre` (variants) | 127 520 (11,95 %) | Éclater en `product_family_id` (617) + `variant_code` (24 lettres) — candidat phase 2 produit |
| 6 | Quantités négatives "orphelines"       | 3 456           | Flag `is_adjustment` (non rattachées à un `C*` ni à un mouvement) — écartées via `quantity > 0` dans le pipeline |
| 7 | Prix ≤ 0 résiduels          | **~1 825** (1 820 à 0 + 5 négatifs) | Flag `is_freebie` ; sous-distinguer notes opérateur (`check`, `damages`…) et "vrais produits à 0 €" (~1 200 lignes — investigation métier) — écartées via `price > 0` dans le pipeline |
| 8 | Doublons exacts                        | ≥ 32 907        | Dédupliquer après validation métier (conserver si InvoiceDate identique au mm:ss et Quantity > 1 plausible) — non dédupliqués  |

#### P3 — finitions

> **Décisions projet RFM** : les chantiers 9 à 12 sont tous `[HORS SCOPE RFM]`. Les prix négatifs et outliers sont écartés silencieusement par les filtres `price > 0 AND quantity > 0`. La normalisation pays est différée à une éventuelle phase 2 (segmentation géographique fine).

| # | Chantier                                  | Volume    | Action ETL                                          |
|---|-------------------------------------------|-----------|-----------------------------------------------------|
|  9 | Prix négatifs                            | 5         | Inspection manuelle (probables corrections compta) — écartées via `price > 0` |
| 10 | Outliers Quantity / Price                | ~qq dizaines | Winsorisation P99 ou exclusion explicite — non traités |
| 11 | Pays `Unspecified` / `European Community`| 817       | Catégorie `unknown` ou remapping pays — `country` reste brut dans `clean.sales` |
| 12 | Libellés `RSA`, `EIRE`                   | —         | Table de correspondance ISO 3166 — phase 2 segmentation géo |

---

## Annexe — Toutes les commandes utilisées

```bash
# 0. Installation
uv tool install csvkit

# 1. Schéma
csvcut -n online_retail_combined.csv

# 2. Stats globales
csvstat online_retail_combined.csv

# 3. Valeurs manquantes (une commande par colonne)
csvgrep -c Invoice       -r '^$' online_retail_combined.csv | wc -l
csvgrep -c StockCode     -r '^$' online_retail_combined.csv | wc -l
csvgrep -c Description   -r '^$' online_retail_combined.csv | wc -l
csvgrep -c InvoiceDate   -r '^$' online_retail_combined.csv | wc -l
csvgrep -c "Customer ID" -r '^$' online_retail_combined.csv | wc -l

# 4. Overlap Description / Customer ID
csvgrep -c Description -r '^$' online_retail_combined.csv \
  | csvgrep -c "Customer ID" -r '^$' | wc -l

# 4 bis. Overlap Description NULL / StockCode → faisabilité du lookup
csvgrep -c Description -r '^$' online_retail_combined.csv \
  | csvgrep -c StockCode -r '^$' | wc -l   # 0 cas avec les deux nuls

# 4 ter. Profil unifié écritures techniques (Description ⟺ Customer ID ⟺ Price = 0)
csvgrep -c Description -r '^$' online_retail_combined.csv | csvgrep -c "Customer ID" -r '^$' | wc -l   # 4383
csvgrep -c Description -r '^$' online_retail_combined.csv | csvgrep -c Price -r '^0(\.0+)?$' | wc -l    # 4383

# 4 quater. Top descriptions pour Price = 0
csvgrep -c Price -r '^0(\.0+)?$' online_retail_combined.csv \
  | csvcut -c Description | tail -n +2 | sort | uniq -c | sort -rn | head -30

csvgrep -c Description -r '^$'   online_retail_combined.csv | csvcut -c StockCode | tail -n +2 | sort -u > /tmp/stock_null_desc.txt
csvgrep -c Description -r '^.+$' online_retail_combined.csv | csvcut -c StockCode | tail -n +2 | sort -u > /tmp/stock_with_desc.txt
comm -12 /tmp/stock_null_desc.txt /tmp/stock_with_desc.txt | wc -l                 # StockCodes récupérables
comm -23 /tmp/stock_null_desc.txt /tmp/stock_with_desc.txt | wc -l                 # StockCodes non récupérables
comm -12 /tmp/stock_null_desc.txt /tmp/stock_with_desc.txt > /tmp/stock_recoverable.txt
csvgrep -c Description -r '^$' online_retail_combined.csv \
  | csvgrep -c StockCode -f /tmp/stock_recoverable.txt | wc -l                     # Lignes récupérables

# 5. Anomalies numériques
csvgrep -c Quantity -r '^-'         online_retail_combined.csv | wc -l
csvgrep -c Quantity -r '^-'         online_retail_combined.csv \
  | csvgrep -c Invoice -r '^C' | wc -l
csvgrep -c Price    -r '^-'         online_retail_combined.csv | wc -l
csvgrep -c Price    -r '^0(\.0+)?$' online_retail_combined.csv | wc -l

# 6. Factures annulées
csvgrep -c Invoice -r '^C' online_retail_combined.csv | wc -l

# 7. StockCodes spéciaux
csvcut -c StockCode online_retail_combined.csv | tail -n +2 \
  | grep -viE '^[0-9]+[A-Za-z]*$' | sort | uniq -c | sort -rn | head -20

csvgrep -c StockCode \
  -r '^(POST|DOT|M|BANK CHARGES|AMAZONFEE|ADJUST|S|D|C2|CRUK|TEST.*|gift_.*|PADS|DCGS.*)$' \
  online_retail_combined.csv | wc -l

# 7 bis. Format dominant : 5 chiffres + 1 lettre (variants produit)
csvgrep -c StockCode -r '^[0-9]{5}[A-Za-z]$' online_retail_combined.csv | wc -l
csvgrep -c StockCode -r '^[0-9]{5}[A-Za-z]$' online_retail_combined.csv \
  | csvcut -c StockCode | tail -n +2 | sort -u | wc -l                             # variants uniques
csvgrep -c StockCode -r '^[0-9]{5}[A-Za-z]$' online_retail_combined.csv \
  | csvcut -c StockCode | tail -n +2 | sed 's/.$//' | sort -u | wc -l              # familles
csvgrep -c StockCode -r '^[0-9]{5}[A-Za-z]$' online_retail_combined.csv \
  | csvcut -c StockCode | tail -n +2 | sed 's/^[0-9]*//' | tr 'a-z' 'A-Z' | sort | uniq -c | sort -rn

# 8. Doublons exacts
tail -n +2 online_retail_combined.csv | sort | uniq -d | wc -l

# 9. Cardinalités
csvcut -c Invoice       online_retail_combined.csv | tail -n +2 | sort -u | wc -l
csvcut -c StockCode     online_retail_combined.csv | tail -n +2 | sort -u | wc -l
csvcut -c "Customer ID" online_retail_combined.csv | tail -n +2 | grep -v '^$' | sort -u | wc -l

# 10. Distribution pays
csvcut -c Country online_retail_combined.csv | tail -n +2 | sort | uniq -c | sort -rn

# 11. Bornes temporelles (rapide grâce à l'ordre ISO)
csvcut -c InvoiceDate online_retail_combined.csv | tail -n +2 | sort | (head -1; tail -1)
```
