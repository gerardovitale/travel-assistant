# Reports Catalog

Every report the system computes, with the query behind it, its granularity, the question it answers and the
endpoints it feeds. Two layers:

- **A. Pre-computed by fuel-ingestor** (`fuel-ingestor/app/aggregator/`). Runs daily at 05:00 UTC via
  `trigger-ingestor.yaml` (ingestor, then aggregator, then commodity job). Output: Parquet in
  `gs://travel-assistant-spain-fuel-prices/aggregates/`.
- **B. Ad-hoc in fuel-dashboard** (`app/data/duckdb_engine.py` + `app/services/*`). Runs over the latest raw
  snapshot (`latest_stations`, public stations only, `sale_type = 'p'`) or over the A-layer Parquets.

## Conventions

- Price filter everywhere: `price IS NOT NULL AND price > 0`.
- `FUEL_PRICE_COLUMNS`: 14 fuel columns (`aggregator/shared.py`).
- Brand reports use `normalize_brand()` (`aggregator/brand_utils.py`): collapses aliases, drops non-brand junk.
- Historical builds use the latest raw file per day.
- Consumers: **v2** = `/insights` (`insights_v2.js`), **legacy** = `/insights/{tab}` (`insights.js`),
  **search** = `/` (`search.js`), **trip** = `/trip` (`trip.js`), **MCP** = `app/mcp_layer/server.py`.
- All endpoints are under `/api/v1`.

---

## A. Ingestor pre-computed aggregates

### A1. `province_daily_stats.parquet` (`pipelines/province_stats.py`)

- **Grain:** date x province x fuel_type. Incremental (dedup on date), unbounded history.
- **Query (pandas):** per fuel, `groupby(province)[fuel].agg(mean, min, max, count)` on rows with price > 0.
  Columns: `avg_price, min_price, max_price, station_count`.
- **Question:** What did fuel cost in each province each day?
- **Feeds:** `/zones/provinces` (B12), forecast fallback (B16), fuel-type report (B19-B22), source of A3.

### A2. `zip_code_daily_stats.parquet` (`pipelines/zip_code_stats.py`)

- **Grain:** date x zip_code x province x fuel_type. Incremental, **365-day retention**.
- **Query:** `groupby([zip_code, province])[fuel].agg(mean, min, max, count)`.
- **Question:** Daily price per postal code (base of every local trend).
- **Feeds:** loaded into the DuckDB table `zip_code_daily_stats` (background refresh), which serves B7-B11
  and B16. MCP: `get_price_trend`, `forecast_price_regime`.

### A3. `day_of_week_stats.parquet` (`pipelines/day_of_week_stats.py`)

- **Grain:** day_of_week (0-6) x fuel_type x province (plus `__national__`). Full rebuild from A1 each run.
- **Query:** province rows: `groupby(dow, fuel, province).agg(sum_price = sum(avg_price),
count_days = nunique(date), min/max_daily_avg)`. National: station-weighted daily average
  `sum(avg * count) / sum(count)` per date, then the same aggregation.
- **Question:** Which weekday is cheapest historically (raw level, not trend-adjusted)?
- **Feeds:** `/historical/day-of-week` (B14), legacy only.

### A4. `brand_daily_stats.parquet` (`pipelines/brand_stats.py`)

- **Grain:** date x brand (normalized) x fuel_type. Only brands with >= `MIN_STATION_COUNT` (10) stations that day.
- **Query:** `groupby(brand)[fuel].agg(mean, min, max, count)` after `normalize_brand(label)`.
- **Question:** Daily price level per brand chain.
- **Feeds:** `/historical/brands` (B15), legacy.

### A5. `daily_ingestion_stats.parquet` (`pipelines/ingestion_stats.py`)

- **Grain:** date (one row per day). Incremental.
- **Query:** `len` and `nunique` of eess_id, label, province, municipality id/name, (province, municipality,
  locality) tuples, locality names, ccaa_id; count of fuel columns with any non-null value.
- **Question:** Did ingestion succeed and how complete was each day?
- **Feeds:** `/quality/inventory` (B23), legacy Calidad tab. MCP: `get_data_quality_inventory`.

### A6. `reports/brand_win_rate.parquet` (`reports/brand_win_rate.py`)

- **Grain:** brand x direction (cheapest | priciest) x geo_level (zip_code | locality | municipality) x
  geo_value x fuel (gasoline_95_e5, diesel_a). Full rebuild over all history. Kept if `appearances >= 30`.
- **Query (DuckDB):**
  ```sql
  base         AS (dt, geo, normalize_brand(label) label, price WHERE price > 0 AND label IS NOT NULL),
  boundary     AS (SELECT dt, geo, MIN|MAX(price) boundary_price FROM base GROUP BY 1, 2),
  joint        AS (boundary JOIN base ON dt, geo; is_at_boundary = boundary_price = price),
  brand_day    AS (SELECT dt, geo, brand, MAX(is_at_boundary::int) hit GROUP BY 1, 2, 3),
  tie_adjusted AS (win_credit = CASE WHEN hit = 1 THEN 1.0 / SUM(hit) OVER (PARTITION BY dt, geo) ELSE 0 END)
  SELECT brand, ..., COUNT(*) appearances, ROUND(AVG(win_credit) * 100, 2) win_rate_pct,
         CASE WHEN >= 384 'high' WHEN >= 100 'medium' ELSE 'low' END confidence_level, MAX(dt) last_updated
  GROUP BY brand, direction, geo_level, geo_value, fuel_type HAVING COUNT(*) >= 30
  ```
- **Question:** How often is brand X the cheapest (or priciest) option in its area on a given day?
- **Feeds:** `/reportes/win-rate` (B24), legacy Reportes. MCP: `get_brand_win_rate`.

### A7. `reports/brand_price_comparison.parquet` (`reports/brand_comparison.py`)

- **Grain:** brand x geo_level x geo_value x fuel (same dimensions as A6, no direction). `appearances >= 10`.
- **Query (DuckDB):**
  ```sql
  brand_daily  AS (SELECT dt, geo, label brand, AVG(price) brand_avg_price GROUP BY dt, geo, label),
  market_daily AS (SELECT dt, geo, AVG(price) market_avg_price GROUP BY dt, geo),  -- includes the brand itself
  joint        AS (brand_daily JOIN market_daily ON dt, geo; is_below_market = brand_avg < market_avg)
  SELECT brand, ..., AVG(brand_avg_price), AVG(market_avg_price),
         AVG((brand_avg - market_avg) / market_avg * 100) price_delta_pct,  -- mean of daily deltas
         AVG(is_below_market) * 100 days_below_market_pct, COUNT(*) appearances, confidence_level, MAX(dt)
  GROUP BY brand, geo_level, geo_value, fuel_type HAVING COUNT(*) >= 10
  ```
- **Question:** How much more or less does brand X charge than its local market, and how often is it below?
- **Feeds:** `/reportes/price-comparison` (B25), `/reportes/coverage` (B26), `/reportes/brands` (B27), all
  legacy. MCP: `get_brand_price_comparison`, `list_report_brands`.

### A8. `reports/brand_scorecard.parquet` (`reports/brand_scorecard.py`)

- **Grain:** province x brand x fuel. **Last 90 days** ending at max(dt). Public stations only, zip_code and
  province required, brands with >= 5 stations nationally on the last day. Every measure is a **sum**, so
  the dashboard can re-aggregate any set of provinces.
- **Query (DuckDB):**
  ```sql
  stations       AS (dt, zip, province, eess_id, brand, coalesce(brand, 'eess:' || eess_id) competitor, price),
  market         AS (SELECT dt, zip, SUM(price) price_sum, MIN(price) min_price, COUNT(*) stations GROUP BY dt, zip),
  competitor_day AS (GROUP BY dt, zip, competitor: AVG/SUM/MIN price, COUNT stations),
  scored         AS (market_price = (m.price_sum - c.price_sum) / (m.stations - c.stations),  -- rest of the zip
                     win_credit = 1/N on ties when c.min_price = m.min_price),
  competition    AS (SELECT province, brand, SUM(stations) station_days,
                     SUM((price - market_price) * stations) price_gap_sum, COUNT(*) zip_days,
                     SUM(price < market_price) below_market_days, SUM(win_credit) win_days
                     WHERE brand IS NOT NULL AND market_stations > stations GROUP BY province, brand),
  footprint      AS (COUNT DISTINCT eess_id on the last day GROUP BY province, brand)
  footprint FULL OUTER JOIN competition
  ```
- **Question:** Against the other stations in the same zip on the same day, how much cheaper or pricier is
  each brand, how often is it below average, and how often is it the cheapest?
- **Feeds:** `/brands/scorecard` (B6), v2 Marcas section.

### A9. `brand_competitiveness.parquet` and `brand_competitiveness_monthly.parquet` (`brand_competitiveness.py`)

- **Grain:** overall = brand x geo_level x geo_value x fuel (>= 30 appearances). Monthly = year_month x brand x
  geo_level x fuel. Hard-coded brands `ballenoil, repsol`; raw `lower(label)` (no `normalize_brand`); no tie split.
- **Query:** same shape as A6 "cheapest", at station level: `is_cheapest = min_price = price`,
  `AVG(is_cheapest) * 100`.
- **Question:** Legacy win rate for two brands, plus monthly trend.
- **Feeds:** **nothing.** No dashboard or MCP consumer; superseded by A6.

### A10. `commodity_prices.parquet` (`ingestor/commodity_price.py`, `commodity_main.py`)

- **Grain:** date x series_id (Brent `DCOILBRENTEU`). Incremental on (date, series_id), 730-day retention.
- **Query:** none (fetch, validate, upsert).
- **Question:** Brent crude price series.
- **Feeds:** `/commodities/trend`, `/commodities/correlation` (B17, B18), legacy Tendencias (flag
  `commodities_enabled`).

---

## B. Dashboard ad-hoc reports

### B-I. Over `latest_stations` (today's snapshot)

| #   | Endpoint(s)                                        | Query                                                                                                                                          | Grain            | Question                         | Consumer                               |
| --- | -------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------- | ---------------- | -------------------------------- | -------------------------------------- |
| B1  | `/stations/cheapest-by-zip`                        | `query_cheapest_by_zip`: `WHERE zip = $1 AND price > 0 [AND label = ANY] ORDER BY price LIMIT n`; `pct_vs_avg` vs B5                           | station          | Cheapest stations in my zip?     | search, MCP `find_cheapest_by_zip`     |
| B2  | `/stations/nearest-by-address`                     | `query_nearest_stations`: Haversine `distance_km`, `ORDER BY dist LIMIT n` (x3 oversample, OSRM road-distance re-rank)                         | station          | Closest stations?                | search                                 |
| B3  | `/stations/cheapest-by-address`                    | `query_stations_within_radius`: bbox prefilter + Haversine <= r (r x 1.3 with OSRM), sort by price                                             | station          | Cheapest within R km?            | search, MCP `find_cheapest_by_address` |
| B4  | `/stations/best-by-address`                        | B3 rows plus `total_cost = price * (tank + 2 * dist * cons / 100)`, `score = 10 * (max - cost) / (max - min)`                                  | station          | Cheapest including the drive?    | search                                 |
| B5  | `/stats/national-avg`, SSR headline on `/insights` | `query_national_avg_stats`: `AVG(price), COUNT(*)` with optional region filter (lru-cached)                                                    | country / region | National average today           | v2 SSR, `pct_vs_avg` everywhere        |
| B28 | `/zones/cheapest`, `/zones/district-zips`          | `query_cheapest_zones`: `GROUP BY zip_code AVG/MIN/COUNT WHERE province = $1 ORDER BY avg`                                                     | zip (today)      | Cheapest zips in a province?     | legacy Zonas                           |
| B29 | `/zones/municipality-zips`                         | `query_cheapest_zones_by_municipality`: as B28 plus `municipality = $2`                                                                        | zip (today)      | Cheapest zips in a municipality? | legacy Zonas                           |
| B30 | `/zones/province-map`, `/zones/province-geojson`   | `query_avg_price_by_province`: `GROUP BY province AVG, COUNT`                                                                                  | province (today) | Province price map               | legacy Zonas                           |
| B31 | `/zones/districts`, `/zones/district-geojson`      | `query_stations_by_province` + point-in-polygon `assign_districts` (Madrid only), mean per district                                            | Madrid district  | Cheapest Madrid district?        | legacy Zonas                           |
| B32 | `/trip/plan`                                       | `query_stations_along_corridor`: per-waypoint bbox range join + Haversine, `MIN(dist)`, `ARG_MIN(wp_idx)`; then `trip_planner.py` optimisation | station on route | Where to refuel on this trip?    | trip                                   |
| -   | `/provinces`, `/labels`, `/zones/municipalities`   | `SELECT DISTINCT province`; `label, COUNT(*) GROUP BY label ORDER BY cnt LIMIT n`; distinct municipality                                       | lookup           | Picker options                   | all pages                              |

### B-II. Over the `zip_code_daily_stats` DuckDB cache (A2)

| #   | Endpoint                       | Query                                                                                                                                                                                                       | Grain              | Question                                        | Consumer                          |
| --- | ------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------ | ----------------------------------------------- | --------------------------------- |
| B7  | `/trends/price?zip_code=`      | `query_cached_zip_code_price_trend`: `WHERE zip, fuel`, window = N days ending at the latest date. Fallback while the cache is cold: raw parquets via `query_price_trends` (`AVG/MIN/MAX GROUP BY date`)    | day x zip          | Price trend in my zip                           | v2, legacy, MCP `get_price_trend` |
| B8  | `/trends/price` (no zip)       | `query_national_price_trend`: `sum(avg * cnt) / sum(cnt)`, `MIN(min)`, `MAX(max)` `GROUP BY date`; window ends at `MAX(date)`; province or region filter                                                    | day x scope        | National / province / region trend              | v2 Tendencia, legacy              |
| B9  | `/trends/group`                | `query_national_group_price_trend`, `query_cached_group_price_trend`: as B8/B7 with `fuel_type = ANY($1)`, `GROUP BY date, fuel`                                                                            | day x fuel variant | How do fuel variants compare (95 E5 vs E10...)? | legacy                            |
| B10 | `/trends/refuel-timing` (days) | `query_day_of_week_deviation`: station-weighted daily price, `price - AVG(price) OVER (week)` on complete Mon-Sun weeks, `AVG(dev) GROUP BY isodow`; 180-day window                                         | weekday x scope    | Which weekday is cheapest, trend removed?       | v2 "Cuando repostar"              |
| B11 | `/historical/volatility`       | `query_volatility_by_zone`: `STDDEV_SAMP(avg_price)`, CV, range, `COUNT` per (zip, province); `HAVING days >= 70% of window AND AVG(stations) >= 3`; optional mainland-only; window ends at the latest date | zip x period       | Most stable / volatile zips?                    | legacy Historico                  |

### B-III. Over other A-layer Parquets (downloaded per request via `gcs_client.download_aggregate`)

| #   | Endpoint                                                   | Source                                                               | Query                                                                                                                                                                                                                                                                    | Grain                       | Question                                    | Consumer                                 |
| --- | ---------------------------------------------------------- | -------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | --------------------------- | ------------------------------------------- | ---------------------------------------- |
| B6  | `/brands/scorecard`                                        | A8                                                                   | `brand_service._scores`: `scope_mask(province, region)`, `SUM` measures per brand; keep `stations >= 10` (region) or `>= 3` (province) and `zip_days >= 30`; `price_gap = price_gap_sum / station_days`; `below_market_pct`, `win_pct` = days / `zip_days`; low-cost tag | brand x scope x fuel        | Which brands beat their local market?       | v2 Marcas                                |
| B12 | `/zones/provinces`                                         | A1                                                                   | `query_province_ranking`: `AVG(avg_price)`, `MIN`, `MAX`, `SUM(station_count)` per province, N days ending at the latest date                                                                                                                                            | province x period           | Province price ranking                      | legacy Zonas                             |
| B14 | `/historical/day-of-week`                                  | A3                                                                   | `sum_price / count_days` for a province or the station-weighted `__national__` row                                                                                                                                                                                       | weekday                     | Cheapest weekday (raw level)?               | legacy Historico                         |
| B15 | `/historical/brands`                                       | A4                                                                   | `query_brand_ranking`: `sum(avg * cnt) / sum(cnt)`, `MIN`, `MAX` per brand, `LIMIT top_n`; plus `query_brand_price_trend` (daily, top brands)                                                                                                                            | brand x period; day x brand | Cheapest brands nationally, and their trend | legacy Historico                         |
| B16 | `/historical/forecast`, `/trends/refuel-timing` (forecast) | A2 (zip), else B8 series (province / region), else A1 when < 60 days | `forecast_service`: tercile regimes (cheap / normal / expensive), 3x3 Markov transition matrix on consecutive days, P(cheaper within 3d / 7d), expected stay, confidence. Needs >= 60 days and >= 30 transitions                                                         | zip / province / region     | Refuel today or wait?                       | v2, legacy, MCP `forecast_price_regime`  |
| B17 | `/commodities/trend`                                       | A10                                                                  | `query_commodity_price_trend`: `WHERE series_id`, N days ending at that series' latest date                                                                                                                                                                              | day                         | Brent price                                 | legacy (flag)                            |
| B18 | `/commodities/correlation`                                 | A10 + B8                                                             | Inner join on date, Pearson `corr(avg_price, brent)`, >= 14 observations                                                                                                                                                                                                 | scalar                      | Does pump price track Brent?                | legacy (flag)                            |
| B19 | `/reportes/fuel-type/cost`                                 | A1 + vehicle catalog                                                 | Station-weighted daily price per energy (95 E5 / diesel A / LPG) per scope; latest common date; `cost_per_100km = consumption * price`                                                                                                                                   | vehicle                     | EUR / 100 km per model                      | legacy (flag `report_fuel_type_enabled`) |
| B20 | `/reportes/fuel-type/breakeven`                            | A1 + catalog                                                         | Latest common day: `price_ratio = p_d / p_g`, `breakeven_ratio = c_g / c_d`, `margin_pct`, winner (+-1% tie band), breakeven diesel price and consumption                                                                                                                | pair x scope                | Is diesel or gasoline cheaper to run now?   | v2 calculator, legacy                    |
| B21 | `/reportes/fuel-type/provinces`                            | A1 + catalog                                                         | Pivot province x date, latest day both fuels reported, B20 metrics per province, sorted by margin                                                                                                                                                                        | pair x province             | Where does diesel win?                      | legacy                                   |
| B22 | `/reportes/fuel-type/history`                              | A1 + catalog                                                         | Daily `price_ratio` vs breakeven, winner per day, flips / crossovers, % days diesel wins                                                                                                                                                                                 | pair x day                  | Has the winner flipped over time?           | legacy                                   |
| -   | `/reportes/fuel-type/vehicles`                             | catalog JSON only                                                    | Options, `breakeven_ratio`, diesel list-price premium                                                                                                                                                                                                                    | pair                        | Picker                                      | v2, legacy                               |
| B23 | `/quality/inventory`                                       | A5 + GCS listing                                                     | Days / months / years with `record_count > 0`, total bytes, latest-day uniques, missing calendar days                                                                                                                                                                    | dataset                     | Is the data complete and fresh?             | legacy Calidad, MCP                      |
| B24 | `/reportes/win-rate`                                       | A6                                                                   | Filter fuel + direction + `geo_level = zip_code` + brands (<= 4, default `settings.report_brands`); per brand appearance-weighted mean of `win_rate_pct`, `SUM(appearances)`, confidence band                                                                            | brand                       | How often is the brand cheapest / priciest? | legacy Reportes, MCP                     |
| B25 | `/reportes/price-comparison`                               | A7                                                                   | `geo_level = zip_code` only; per brand appearance-weighted mean of delta %, days-below %, brand / market average                                                                                                                                                         | brand                       | Brand vs market                             | legacy Reportes, MCP                     |
| B26 | `/reportes/coverage`                                       | A7                                                                   | Per brand `nunique(geo_value)` per geo_level; `total_observations` = `SUM(appearances)` at zip level only                                                                                                                                                                | brand                       | Brand footprint                             | legacy Reportes                          |
| B27 | `/reportes/brands`                                         | A7                                                                   | Brands in >= 10 zip codes (top 50) plus configured defaults, ordered by zip coverage                                                                                                                                                                                     | brand list                  | Picker                                      | legacy Reportes, MCP                     |

---

## C. Issues spotted while cataloguing (follow-ups, not fixed here)

1. **A9 is an orphan.** `brand_competitiveness*.parquet` is computed daily (full raw scan, two blobs) and
   nothing reads it.
2. **Fixed: B24 / B25 / B26 counted the same station-days up to three times.** The services collapsed A6 / A7
   across every `geo_level` (zip, locality, municipality), inflating `appearances` and the confidence band.
   They now use `REPORT_GEO_LEVEL = "zip_code"` (`station_service.py`), the same level the brand picker ranks by.
   Note: brand figures shift (appearances drop, weighted means now reflect zip codes only).
3. **Fixed: window anchors were inconsistent.** B8 and B10 anchored on `MAX(date)`; B7, B9, B11, B12, B15 and
   B17 used `CURRENT_DATE - N`, so a late ingestion silently shortened those windows. All now share
   `_window_ending_at_latest` (`duckdb_engine.py`): the N days ending on the table's latest date (the
   commodity window ends on its own series' latest date). A window is now exactly N days where it used to be
   N + 1 when data was fresh.
4. **Fixed: B14 mainland-only was not station-weighted** (it averaged province means) and nothing called it.
   The dead `exclude_provinces` path was removed; B14 serves one province or the weighted national row.
5. **Two weekday reports disagree by design.** A3 / B14 give the raw level (legacy); B10 gives the
   trend-removed deviation (v2).
