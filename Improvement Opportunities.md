# Fuel Dashboard Improvement Roadmap

## Context

The fuel-dashboard already has 5 feature-rich tabs (Search, Trends, Zones, Trip Planner, Historical Analysis) with 14
fuel types, DuckDB queries, OSRM routing, and Plotly maps. This roadmap adds deeper analytics, smarter recommendations,
trip planner enhancements, and UX improvements across 4 phases ordered by impact and complexity.

---

## Shared Infrastructure (prep work)

| Piece                                 | Used by                           | Where                              |
| ------------------------------------- | --------------------------------- | ---------------------------------- |
| `query_brand_stats()` DuckDB query    | Brand comparison, Price vs avg    | `duckdb_engine.py`                 |
| `brand_daily_stats.parquet` aggregate | Brand trends, Station report card | `aggregator.py`                    |
| `eess_id` in DuckDB SELECT lists      | Station report card               | `duckdb_engine.py` (minor)         |
| CSV export utility                    | Export results                    | `components.py`                    |
| Browser geolocation JS bridge         | Near me button                    | `pages.py` via `ui.run_javascript` |

---

## Phase 1: Quick Wins (S complexity each)

### 1.1 Best Day to Refuel Advice

Surface existing day-of-week patterns as actionable tip on Search tab after results load.

- `pages.py` — add advice card in `_build_search_panel()` after results
- `view_models.py` — new `best_day_advice()` function
- `components.py` — optional `advice_card()` component
- **Reuses**: existing `get_day_of_week_pattern()` service

### 1.2 Price Relative to Average

Show "X% cheaper than zone average" for each station in search results.

- `duckdb_engine.py` — new `query_zone_avg_price()` or inline computation
- `schemas.py` — add `pct_vs_avg: Optional[float]` to `StationResult`
- `station_service.py` — compute percentile in result builders
- `components.py` — add column to `station_results_table()`

### 1.3 Export Results as CSV

Download button after search/zone/trend results.

- `pages.py` — add download button after each results section
- `components.py` — new `csv_download_button(data, filename)` using NiceGUI `ui.download`

### 1.4 Browser Geolocation ("Near Me")

GPS button next to address inputs using `navigator.geolocation`.

- `pages.py` — add button in search and trip panels, wire JS callback
- `components.py` — reusable `geolocation_button(callback)` component
- Requires HTTPS in production

---

## Phase 2: Deeper Analytics (M complexity each)

### 2.1 Brand/Operator Comparison

New subtab ranking fuel station brands by price, with trend charts.

- `aggregator.py` — new `compute_brand_daily_stats()` + GCS upload
- `duckdb_engine.py` — `query_brand_ranking(fuel_type)`, `query_brand_trends()`
- `station_service.py` — new service functions
- `charts.py` — brand trend chart
- `pages.py` — new subtab in Historical Analysis
- `view_models.py` — brand KPI functions

### 2.2 Price Volatility Analysis

Rank zones by price stability (std dev, coefficient of variation).

- `aggregator.py` — extend or add volatility aggregate
- `duckdb_engine.py` — `query_volatility_by_zone(fuel_type)`
- `pages.py` — new subtab in Historical Analysis
- `view_models.py` — volatility KPIs

### 2.3 Savings Calculator

Estimate annual savings from switching stations based on driving habits.

- `pages.py` — input form (km/year, consumption, frequency, prices)
- `view_models.py` — `calculate_savings()` pure function
- `components.py` — savings calculator form
- Benefits from 1.2 (auto-populate price differences)

### 2.4 Multi-Fuel Comparison

Side-by-side diesel vs gasoline for same zone/province.

- `pages.py` — dual fuel selectors in zones or trends tab
- `charts.py` — `build_multi_fuel_chart()` grouped bar chart
- Reuses existing query infrastructure called twice

---

## Phase 3: Trip Planner Enhancements

### 3.1 Multi-Waypoint Trips (L)

Support A -> B -> C with dynamic waypoint inputs.

- `trip_planner.py` — refactor `plan_trip()` to accept `waypoints: List[str]`, chain OSRM segments
- `routing.py` — call `get_full_route()` per segment, concatenate
- `schemas.py` — update `TripPlan` with waypoint info
- `pages.py` — dynamic waypoint input list (add/remove buttons)
- `charts.py` — show waypoint markers on trip map

### 3.2 Round-Trip Planning (M)

Plan fuel for return journey; fuel at destination becomes starting state for return.

- `trip_planner.py` — new `plan_round_trip()` or flag
- `schemas.py` — extend `TripPlan` with return leg
- `pages.py` — round-trip checkbox + return leg rendering
- **Note**: If 3.1 is done first, round-trip = multi-waypoint [A, B, A]

### 3.3 "What If" Fuel Level (S)

Re-run stop selection when slider changes without re-querying stations.

- `pages.py` — `on_value_change` handler caching station data
- `trip_planner.py` — lighter `replan_stops()` using cached stations

---

## Phase 4: Advanced Features (L complexity each)

### 4.1 Station Report Card

Historical pricing of a single station, compared to zone average.

- `aggregator.py` — optional `station_daily_prices.parquet` (start with diesel_a + gasoline_95_e5)
- `duckdb_engine.py` — `query_station_history(eess_id, fuel_type, blob_names)`, `query_station_by_name()`
- `station_service.py` — `get_station_report()`
- `pages.py` — new tab or subtab
- `charts.py` — station history chart with zone average overlay

### 4.2 Favorite Stations

Star button on results, saved favorites list with current prices.

- Persistence: browser localStorage (simplest) or SQLite file
- `pages.py` — favorites panel, star buttons
- `components.py` — `favorite_button()` component
- New: `data/favorites_store.py` persistence layer
- Pairs well with 4.1 (click favorite -> show report card)

### 4.3 Animated Price Heatmap

Province-level map with animation controls showing price evolution over time.

- `duckdb_engine.py` — `query_multi_day_province_prices()` from existing province_daily_stats
- `charts.py` — `build_animated_heatmap()` with Plotly animation frames
- `pages.py` — new panel with play/pause controls
- Keep province-level granularity (postal-code too heavy for animation)

---

## Summary

| #   | Feature               | Phase | Size | New Aggregate?   | New Query?   |
| --- | --------------------- | ----- | ---- | ---------------- | ------------ |
| 1.1 | Best day to refuel    | 1     | S    | No               | No           |
| 1.2 | Price vs average      | 1     | S    | No               | Yes (simple) |
| 1.3 | Export CSV            | 1     | S    | No               | No           |
| 1.4 | Geolocation           | 1     | S    | No               | No           |
| 2.1 | Brand comparison      | 2     | M    | Yes              | Yes          |
| 2.2 | Price volatility      | 2     | M    | Yes (or derived) | Yes          |
| 2.3 | Savings calculator    | 2     | M    | No               | No           |
| 2.4 | Multi-fuel comparison | 2     | M    | No               | No (reuse)   |
| 3.1 | Multi-waypoint trips  | 3     | L    | No               | No (reuse)   |
| 3.2 | Round-trip planning   | 3     | M    | No               | No (reuse)   |
| 3.3 | What-if fuel slider   | 3     | S    | No               | No           |
| 4.1 | Station report card   | 4     | L    | Optional         | Yes          |
| 4.2 | Favorite stations     | 4     | L    | No               | No           |
| 4.3 | Animated heatmap      | 4     | L    | No               | Yes          |

## Sequencing Notes

- Phase 1 features are independent — can be developed in parallel
- Phase 2: Brand comparison (2.1) first — establishes aggregate used by 4.1
- Phase 3: Multi-waypoint (3.1) before round-trip (3.2) — round-trip is a special case
- Prep: Add `eess_id` to DuckDB SELECT lists early (needed by Phase 4)

## Verification

- Each feature: add tests in `tests/` following existing patterns with `fixture.py`
- Run `make fuel-dashboard.test` after each feature
- Run `pre-commit run --all-files` before committing
- For aggregate changes: run `make fuel-ingestor.test` too

## Critical Files

- `fuel-dashboard/app/data/duckdb_engine.py` — touched by 8/14 features
- `fuel-dashboard/app/ui/pages.py` — every feature needs UI changes
- `fuel-dashboard/app/services/station_service.py` — most analytics features
- `fuel-ingestor/app/aggregator.py` — new aggregates for Phase 2+
- `fuel-dashboard/app/services/trip_planner.py` — Phase 3 features
