# Spanish Fuel Price Finder

## Elevator Pitch

**Spanish Fuel Price Finder** is a free, open-source platform that helps drivers and fleet managers across Spain find
the cheapest fuel — not just by price, but by factoring in the real cost of getting there. It ingests daily data from
Spain's official government API covering 12,000+ stations and 14 fuel types, then serves it through an interactive
dashboard with smart search, trip planning, regional maps, and trend analytics. Built on a modern serverless stack, it
turns raw public data into actionable savings.

---

## The Problem

Fuel prices in Spain vary significantly — even stations a few kilometers apart can differ by 10-15 cents per liter.
Drivers typically default to the nearest station or a familiar brand, leaving money on the table every fill-up.

- **No intelligent comparison exists.** Basic price lists don't account for detour costs — the "cheapest" station 20 km
  away may cost more in fuel burned getting there.
- **No trend visibility.** Prices fluctuate by day of week and season, but consumers have no way to see patterns and
  time their purchases.
- **Fleet blind spots.** Logistics companies operating across provinces lack a unified view of regional pricing to
  optimize routes and fuel stops.

---

## What It Delivers Today

### Real-Time Price Transparency

Daily automated ingestion from Spain's Ministry of Industry API. Every station, every fuel type, every morning by 05:00
UTC.

### Smart Station Search

Four search modes that go beyond simple price sorting:

- **By postal code** — instant price list for your area
- **Nearest by address** — geocoded results ranked by distance
- **Cheapest by radius** — filter within a configurable distance
- **Best overall** — a scoring algorithm that ranks stations by _total estimated cost_: fuel price multiplied by tank
  volume, plus the fuel burned on the round-trip detour. This answers the real question: _"Where should I actually fill
  up?"_

### Trip Planner

Plan a route from A to B and get optimized fuel stop recommendations based on:

- Current fuel level and tank size
- Vehicle consumption rate (l/100km)
- Detour tolerance (minutes)
- Multiple strategies: cost-optimized, time-efficient, or minimum stops

### Regional Analytics

- **Province choropleth maps** — see which regions are cheapest at a glance
- **District-level detail** — drill into Madrid neighborhoods
- **Price rankings** — best and worst provinces for each fuel type

### Trend Analysis

- **7 / 30 / 90-day price history** by location and fuel type
- **Day-of-week patterns** — discover that Mondays are consistently cheaper in your province
- **KPIs** — current price vs. period average, percentage change

### Broad Fuel Coverage

14 fuel types tracked: Diesel A/B/Premium, Gasoline 95/98 (E5/E10/Premium), Biodiesel, Bioethanol, CNG, LNG, LPG, and
Hydrogen.

---

## How It Works

```
Spain Gov API ──→ Cloud Run Job ──→ Google Cloud Storage ──→ DuckDB ──→ Interactive Dashboard
   (daily)         (fetch, transform,     (Parquet files)      (in-memory     (FastAPI + NiceGUI)
                    validate, upload)                            analytics)
```

1. **Ingest** — A Cloud Run Job fetches the government's JSON feed daily, maps 30+ Spanish columns to English, validates
   data types, and writes Parquet files to GCS.
2. **Aggregate** — A post-processing step computes province-level daily statistics and running day-of-week averages.
3. **Serve** — A FastAPI application loads the latest snapshot into DuckDB for millisecond analytical queries, geocodes
   addresses via Nominatim, computes road distances via OSRM, and renders an interactive dashboard with NiceGUI.
4. **Deploy** — The entire infrastructure is defined in Terraform and deployed via GitHub Actions with automated
   testing, container security scanning (Trivy), and zero-downtime rollouts.

---

## Tech Stack

| Layer              | Technology                    | Role                                                        |
| ------------------ | ----------------------------- | ----------------------------------------------------------- |
| Ingestion          | Cloud Run Jobs, pandas        | Daily batch pipeline                                        |
| Storage            | Google Cloud Storage, Parquet | Compressed columnar data lake                               |
| Analytics          | DuckDB                        | In-process OLAP engine, sub-second queries on 12K+ stations |
| API                | FastAPI                       | Type-safe REST endpoints with rate limiting                 |
| UI                 | NiceGUI                       | Python-native reactive dashboard, no JavaScript required    |
| Routing            | OSRM                          | Open-source road distance and route geometry                |
| Geocoding          | Nominatim                     | OpenStreetMap-backed address resolution                     |
| Maps               | Plotly + GeoJSON              | Interactive choropleth and station maps                     |
| Infrastructure     | Terraform                     | Declarative, version-controlled GCP resources               |
| CI/CD              | GitHub Actions                | Test, scan, build, deploy on every push                     |
| Package Management | uv                            | Fast, modern Python dependency resolution                   |

---

## Future Expansion

The architecture is designed for growth. Each expansion below builds on existing capabilities with minimal rework.

### Multi-Country Expansion

The EU mandates open fuel price data. Portugal, France, Italy, and Germany publish similar APIs. The pipeline's
column-mapping pattern (`entity.py`) makes adding a new country a configuration task, not a rewrite.

### Fleet Management & B2B Integration

The API-first design (FastAPI with OpenAPI docs) enables direct integration with fleet management platforms. Logistics
companies could query optimal fuel stops programmatically for their entire fleet.

### Price Prediction & ML

Over a year of historical data with day-of-week and province-level aggregations provides a strong foundation for
time-series forecasting. Predict tomorrow's prices and recommend _when_ to fill up, not just _where_.

### Mobile App / PWA

NiceGUI renders responsive HTML that already works on mobile browsers. A Progressive Web App wrapper or thin native
shell would deliver a native-like experience with minimal effort.

### Price Alerts & User Preferences

Notify users when fuel drops below a threshold at their favorite stations or in their postal code. The zone and trend
infrastructure already supports this — it needs a notification layer.

### EV Charging Integration

Extend coverage to electric vehicle charging stations as Spain's charging network grows. The station data model
(location, price, fuel type) maps directly to charging points.

### Savings Tracking

Show users how much they've saved over time by choosing optimized stations vs. their nearest default. Turns a one-time
lookup into an ongoing value proposition.

---

## Why This Project Matters

This isn't a toy or a tutorial — it's a production system processing real government data for a real consumer problem.
It demonstrates:

- **End-to-end data engineering** — from raw API to interactive analytics
- **Modern cloud-native architecture** — serverless, IaC, CI/CD, container security
- **Product thinking** — the "best station" scoring algorithm solves the actual user problem, not just the obvious one
- **Scalable design** — adding countries, fuel types, or delivery channels requires extension, not rebuilding
