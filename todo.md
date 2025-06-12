# TODO – Spanish Fuel Price Finder App (MVP Implementation Roadmap)

This todo list breaks down the project into atomic, iterative steps for robust, test-driven, incremental development.
Each prompt builds on the previous, ensuring no orphaned code and safe progress.

---

## 1. Environment & Project Initialization

### 1.1. Monorepo Setup

> Create a new git repository with the following structure:
> - backend/
> - frontend/
> - infra/
> - .gitignore
> - README.md
    > Add a brief description to the README and a license file of your choice.

### 1.2. Backend Python Env

> In the backend/ folder, initialize a Python project using Poetry or pip-tools.
> Add dependencies: fastapi, uvicorn, sqlalchemy, aiosqlite, pytest.
> Create a minimal `main.py` that runs a “Hello world” FastAPI app with a `/health` endpoint returning
`{"status": "ok"}`.
> Write a test for the health endpoint using pytest and httpx.

### 1.3. Frontend React Env

> In the frontend/ folder, initialize a new Vite+React project with TypeScript and Tailwind CSS.
> Add a placeholder App with a heading: “Spanish Fuel Price Finder”.
> Add a test that renders the heading using Vitest or React Testing Library.

---

## 2. Data Ingestion Pipeline

### 2.1. CSV Download

> In backend/data_ingestion.py, write a function that downloads a sample CSV file from a configurable URL or local
> path.
> Log the number of rows in the file.
> Write a unit test that mocks the file download and checks row count logging.

### 2.2. CSV Parsing

> Add a function to parse the CSV and return a list of gas station records as dicts.
> Write unit tests for parsing normal and edge-case rows (missing fields, extra whitespace, etc).

---

## 3. Database Schema Design & Population

### 3.1. SQLite Schema & Migration

> Design an initial SQLite schema for gas stations, including indexes for efficient location and fuel type queries.
> Create a migration script that creates the tables and indexes.

### 3.2. ETL to DB

> Implement ETL logic to take parsed station records and insert them into the SQLite DB.
> Write tests to ensure data is inserted and can be queried as expected.

---

## 4. Backend API Development (FastAPI)

### 4.1. List Endpoint

> Expand FastAPI app to include an endpoint `/stations` that returns a paginated list of all stations (no filtering
> yet).
> Write a test that checks correct pagination.

### 4.2. Add Query Params & Validation

> Add query parameters for latitude, longitude, fuel_type, and max_distance (but don't filter yet).
> Write input validation tests for these parameters.

### 4.3. DB Integration

> Connect the endpoint to query the SQLite DB for stations, return as JSON.
> Test that DB queries return expected structure.

---

## 5. Core Backend Filtering, Sorting, and Ranking Logic

### 5.1. Geospatial Filtering

> Implement geospatial filtering using the haversine formula.
> Write unit tests for the distance calculation.

### 5.2. Distance Filter

> Add logic to filter only stations within max_distance from the query location.
> Test with stations both inside and outside the range.

### 5.3. Opening Hours Filter

> Add filter for opening hours (“only show currently open”).
> Write a utility to parse the `Horario` field and test edge cases (overnight, 24h, etc).

### 5.4. Brand Filter

> Implement brand inclusion/exclusion logic via a query parameter.
> Write tests for filtering by brand.

### 5.5. Sorting Logic

> Add sorting by price and by distance.
> Test the sorting functionality.

### 5.6. Smart Recommendation

> Implement smart ranking: calculate net effective cost using vehicle type presets.
> Expose `/recommendation` endpoint returning the best station for given preferences.
> Test with various user profiles and routes.

---

## 6. API Testing (Unit + Integration)

### 6.1. Integration Tests

> Write integration tests for the full filter pipeline (all params combined).
> Test API error cases (bad input, no stations found, etc).

---

## 7. Frontend PWA Scaffolding

### 7.1. React Main View

> Create a React app with a main view for best-pick summary, filters, and a refresh button.
> Write a smoke test that renders the main view.

### 7.2. PWA Setup

> Add PWA manifest and service worker using Vite PWA plugin.
> Test installability in Chrome.

---

## 8. Frontend-Backend Integration

### 8.1. API Client

> Write a typed API client to fetch `/stations` and `/recommendation`.
> Mock API responses in tests.

### 8.2. Wire Main View

> Wire the main view to call the backend API and display best-pick summary.
> Test UI with mocked and live API.

---

## 9. User Interaction & UI Components

### 9.1. Location Detection

> Implement location detection using browser geolocation API, with fallback to manual input.
> Test with and without permissions.

### 9.2. Filter Controls

> Build filter controls: fuel type dropdown, distance slider, brand selector, and open-only toggle.
> Test that state updates and API calls reflect UI changes.

### 9.3. Directions Button

> Implement “Get Directions” button linking to Google/Apple Maps using station coordinates.
> Test URL correctness.

### 9.4. Refresh Control

> Add refresh button and show last updated time.
> Test manual refresh triggers API fetch.

---

## 10. Error Handling, Logging, and Edge Cases

### 10.1. Frontend Error UI

> Add user-friendly error banners for failed API calls, empty states, and geolocation issues.
> Write tests for error UI.

### 10.2. Backend Logging

> Backend: add logging for all incoming requests, queries, and errors using Python logging module.

---

## 11. CI/CD Setup

### 11.1. GitHub Actions Backend

> Write GitHub Actions workflow for backend: lint, test, build Docker image, deploy to Cloud Run/VM.

### 11.2. GitHub Actions Frontend

> Write workflow for frontend: lint, test, build, deploy to Firebase/Cloudflare Pages.

### 11.3. Deployment Docs

> Document deployment steps for both backend and frontend.

---

## 12. Manual and Automated Testing

### 12.1. Manual QA

> Create a manual testing checklist for all supported device/browser combinations and main flows.

---

**End of todo.md**
