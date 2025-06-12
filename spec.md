# Spanish Fuel Price Finder App – Developer Specification

## Overview

This document defines the end-to-end specification for building the MVP of the "Spanish Fuel Price Finder" web and
mobile-friendly app. The app helps users find the best fuel deal nearby based on price, distance, and opening hours. It
does not require user authentication and is designed for fast, minimal interaction.

---

## 1. User Experience

### 1.1. Target Users

* General public in Spain with mobile or desktop internet access
* No login or signup required

### 1.2. Features

* Auto-detect location via GPS (browser/device geolocation)
* Manual location override (text input or map click – future)
* Fuel type selection (full list from dataset)
* Max distance filter (5, 10, 15 km, unlimited)
* Show only currently open stations
* Brand filter/exclusion (`Rótulo` field)
* Vehicle type selector (fuel efficiency presets)
* Two modes:

    * Basic: Sort by fuel price or distance
    * Smart: Calculate net effective cost (price + roundtrip fuel usage)
* "Best pick" summary display
* Get directions button (opens in Google/Apple Maps)
* Manual data refresh button

---

## 2. Architecture

### 2.1. Frontend

* Stack: React + Vite + Tailwind CSS
* Type: Progressive Web App (PWA)
* Hosting: Firebase Hosting, Cloudflare Pages, or GCS + Cloud CDN

### 2.2. Backend

* Stack: Python + FastAPI
* Hosting Options:

    * Cloud Run (self-contained with SQLite)
    * Lightweight VM (GCE, Oracle Free Tier)
* API Routes (examples):

    * `GET /stations?lat=..&lon=..&fuel_type=..&distance=..`
    * `GET /recommendation?lat=..&lon=..&vehicle_type=..&fuel_type=..`

### 2.3. Database

* Format: SQLite (shipped with container or VM volume)
* Source: Daily CSV from GCS updated via existing Cloud Run job
* Transformation: CSV to normalized SQLite tables via scheduled ingestion step

---

## 3. Data Handling

### 3.1. Ingestion

* Use existing Cloud Run job to fetch government fuel price data
* Add post-processing step to:

    * Parse CSV
    * Normalize data
    * Populate SQLite DB with clean, indexed data

### 3.2. API Consumption

* FastAPI reads from SQLite DB
* Applies geospatial filtering (e.g., haversine formula)
* Filters stations by:

    * Max distance
    * Opening hours (current time vs. `Horario` field)
    * Fuel type availability (non-empty string)
    * Brand inclusion/exclusion

### 3.3. Smart Ranking Calculation

* Roundtrip distance = 2 \* station distance
* Vehicle fuel consumption lookup by category (e.g., Sedan: 6.5 L/100km)
* Net cost = (price per liter) + ((distance / 100) \* fuel consumption \* price per liter)

---

## 4. Error Handling Strategy

### Frontend

* Geolocation failure fallback: prompt user to enter location manually
* API errors: display friendly error messages ("Unable to load stations")
* Empty results: display fallback text ("No open stations within range")

### Backend

* Input validation: return 400 for malformed coordinates or fuel types
* Logging: basic console logging for all endpoints (query params, errors)
* Future-proof: optional hooks for metrics or Sentry integration

---

## 5. CI/CD

### GitHub Actions Workflows

* **Backend**: Build and deploy FastAPI to Cloud Run or GCE on push to `main`
* **Frontend**: Build PWA and deploy to Firebase Hosting or Cloudflare Pages on push

---

## 6. Testing Plan

### 6.1. Unit Tests

* Backend logic: filters, geodistance calculation, smart ranking logic
* SQLite ingestion script: ensure schema correctness and edge cases handled

### 6.2. Integration Tests

* End-to-end API tests (FastAPI + SQLite)
* Ensure valid fuel type and distance filtering returns accurate results

### 6.3. Frontend Tests

* Component tests for filter UI, best-pick card, and refresh logic
* Geolocation + manual override workflow tests

### 6.4. Manual Testing (MVP)

* Device compatibility: iPhone, Android, desktop browser
* PWA installation on mobile
* GPS and manual location override flows

---

## 7. Nice to Haves (Future)

* Anonymous usage analytics
* Interactive maps via Leaflet or Google Maps
* Persistent user preferences in local storage
* Optional login for saving preferences across devices
* Clustered station display or comparison tool

---

## 8. Deliverables Summary

* SQLite DB builder script (ETL from GCS CSV)
* FastAPI app with REST endpoints
* React + Vite + Tailwind frontend with PWA support
* GitHub Actions pipelines for full CI/CD
* Deployment instructions for Cloud Run and Firebase/Cloudflare

---

End of spec.
