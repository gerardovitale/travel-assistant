# Engineering Conventions

The standard way to plan, build and ship any feature, change or improvement in travel-assistant. Applies to humans
and AI agents alike. `CLAUDE.md` (stack, commands, layout) and `spain-fuel-fetcher/SCHEMA.md` (data contract)
complement this file.

---

## 1. Workflow (trunk-based)

- **One branch: `main`.** No feature branches, no PRs. Small, always-releasable changes.
- **Uncommitted until approved.** Changes land in the working tree of local `main`; commit and push only after
  explicit approval from the owner (Gerardo).
- **One commit per slice** (see §3). Never mix unrelated changes in one commit.
- **Commit messages:** [Conventional Commits](https://www.conventionalcommits.org), scoped by service, imperative mood.

  ```text
  feat(fuel-dashboard): add zone drill-down tab behind insights_zones_enabled
  fix(fuel-ingestor): keep zip codes as strings in province_stats
  refactor(fuel-dashboard): vectorise distance ranking in station_service
  chore(infra): bump google provider to 6.x
  ```

  Types: `feat`, `fix`, `refactor`, `perf`, `test`, `docs`, `chore`, `actions` (CI). Scopes: `fuel-dashboard`,
  `fuel-ingestor`, `spain-fuel-fetcher`, `commodity-fetcher`, `infra`.

- `main` must stay green: a push to `main` triggers `deploy.yaml` (test → build → deploy). Never push red.

## 2. Planning

Every non-trivial task (more than ~1 file or any user-visible change) starts with a short written plan, agreed before
coding.

**Plan template**

```markdown
## Goal

One sentence: the user outcome. "Drivers can compare average prices by zip code on Insights."

## Non-goals

What this explicitly does not cover.

## Slices

1. <slice name> — ingestion: … / API: … / UI: … / tests: … / flag: …
2. …

## Risks & open questions

Data volume, external API limits, Pi CPU/memory, schema changes.
```

**Definition of Ready** — a slice can start when:

- [ ] Goal and acceptance criteria are stated in user terms.
- [ ] Data source exists (or slice 1 creates it).
- [ ] Flag name is chosen (if user-visible).
- [ ] Impact on the Raspberry Pi (CPU, memory, startup time) is considered for anything touching DuckDB/pandas.

## 3. Vertical slices

Deliver features as **thin end-to-end slices**, not horizontal layers. Each slice delivers one user-visible (or
verifiable) piece across every layer it needs, with its own tests, and is reviewed before the next one starts.

```text
spain-fuel-fetcher / fuel-ingestor  →  GCS Parquet  →  app/data  →  app/services  →  app/api | app/main  →  templates
      (fetch, aggregate)                                (DuckDB)     (business logic)   (routes)               (Jinja)
```

**Example — Insights redesign, sliced**

| #   | Slice                  | Ingestion                                | Backend                                | UI                          | Flag                          |
| --- | ---------------------- | ---------------------------------------- | -------------------------------------- | --------------------------- | ----------------------------- |
| 1   | Zone averages tab      | `aggregator/pipelines/zip_code_stats.py` | `duckdb_engine` query + service fn     | `insights.html` "Zonas" tab | `insights_zones_enabled`      |
| 2   | Historical trend chart | reuse daily Parquet                      | historical query + `/api/...` endpoint | chart component             | `insights_historical_enabled` |
| 3   | Brand report           | `reports/brand_comparison.py`            | report service                         | Reportes tab                | `insights_reportes_enabled`   |

**Rules**

- A slice is small enough to review in one sitting (rule of thumb: < ~400 changed lines excluding fixtures/lock).
- Skip layers a slice does not need; never build a layer "for later".
- Each slice leaves `main` releasable: either complete, or hidden behind a flag.
- Refactors that a slice needs go in a **separate preceding commit** (`refactor:`), with no behaviour change.

## 4. Feature flags

Use flags for incremental rollout of anything user-visible or risky. Flags live in
`fuel-dashboard/app/config.py` (pydantic-settings, env prefix `DASHBOARD_`).

```python
# config.py
insights_zones_enabled: bool = False  # Zonas tab on /insights. Flip on once zip_code_stats is backfilled.
```

```python
# main.py: gate the route / context
ctx["insights_zones_enabled"] = settings.insights_zones_enabled
```

```jinja
{# insights.html #}
{% if insights_zones_enabled %} … {% endif %}
```

```bash
DASHBOARD_INSIGHTS_ZONES_ENABLED=true make fuel-dashboard.run
```

- **Naming:** `<area>_<feature>_enabled`. Boolean, with a one-line comment saying what it gates and when to flip it.
- **Default `False`** for new, unverified features; flip the default to `True` in its own commit once verified in
  production.
- **Test both states** (flag on and off) at the route level.
- **Remove the flag** (and the dead branch) once the feature is stable, typically within a couple of releases. Flags
  are not permanent config.
- Disabled routes return **404**, not an empty page (see `styleguide_enabled`).

## 5. Code standards

**Tooling (enforced by pre-commit):** black (120), flake8 (120), reorder-python-imports, prettier (Markdown), gitleaks,
terraform fmt/validate. Type-check with `make typecheck`.

**Python**

- Python 3.13 native typing: `list[str]`, `dict[str, float]`, `X | None`. Type hints on all public functions.
- `snake_case` functions/modules, `PascalCase` classes, `UPPER_SNAKE_CASE` constants.
- Pydantic models for API I/O (`app/api/schemas.py`); dataclasses or plain dicts internally.
- `logger = logging.getLogger(__name__)` per module; no `print`. Log at boundaries (external calls, cache refresh),
  not in hot loops.
- Fail fast on bad input at the boundary (Pydantic / FastAPI validation); don't swallow exceptions. External calls
  (OSRM, geocoding, GCS, gov API) always have a timeout and a defined fallback.
- Config only via `Settings`; never read `os.environ` directly or hard-code URLs/limits.
- No `assert` for runtime validation in app code (it is active on the Pi, but semantically wrong); raise explicit
  errors.

**Layering (fuel-dashboard)**

| Layer     | Path                  | Responsibility                                     | Must not                        |
| --------- | --------------------- | -------------------------------------------------- | ------------------------------- |
| Routes    | `app/api/`, `main.py` | HTTP, validation, rate limits, flag gating         | contain business logic or SQL   |
| Services  | `app/services/`       | business rules (ranking, trip planning, forecasts) | know about HTTP or templates    |
| Data      | `app/data/`           | DuckDB queries, GCS cache, external clients        | contain presentation formatting |
| Templates | `app/web/templates/`  | presentation only                                  | compute business values         |

**Data processing & performance** (the dashboard runs on a Raspberry Pi 5)

- Push filtering/aggregation into **DuckDB SQL**; return only the rows needed.
- In pandas, **vectorise**: no `iterrows()`/row-wise `apply()` on hot paths.

  ```python
  # ❌ df["d"] = df.apply(lambda r: haversine(lat, lon, r.lat, r.lon), axis=1)
  # ✅ df["d"] = haversine_np(lat, lon, df["lat"].to_numpy(), df["lon"].to_numpy())
  ```

- Use a bounding-box prefilter before exact geo distance (see `geo_utils.py`).
- Parameterise SQL (`?` placeholders); never f-string user input into SQL.
- For perf work, measure before and after with a benchmark script and state the numbers in the commit body.

**Shared code:** fetch/validate/transform of the gov API lives only in `spain-fuel-fetcher` (`spain_fuel_api`). Do not
duplicate it in services. `ingest-fuel-prices/` is legacy, don't extend it.

## 6. Data & ingestion

- `spain-fuel-fetcher/SCHEMA.md` is the **data contract**. Any column change updates it in the same commit.
- **Schema changes are additive**: add new columns; don't rename or drop in place. If a rename is unavoidable, add the
  new one, migrate readers, then remove the old one in a later slice (expand → migrate → contract).
- Jobs are **idempotent**: re-running a day overwrites the same GCS object path; it never duplicates data.
- New aggregates go in `fuel-ingestor/app/aggregator/pipelines/`; if they need history, ship a backfill
  (`app/aggregator/backfill.py`) in the same slice, before the UI flag is flipped.
- Validate at ingestion (types, nulls, ranges such as `0 < price < 5 €/l`). Bad data fails the job loudly rather than
  reaching the dashboard.
- Keep Spanish locale handling (comma decimals, `Europe/Madrid` timestamps) inside the fetcher; downstream is
  normalized English columns, UTC-aware where relevant.

## 7. UI & design system

- **Stack:** Jinja2 + Tailwind with the **Material 3 tokens** defined in `base.html`. Use tokens
  (`bg-surface`, `text-on-surface-variant`, `primary`), never raw hex colours or ad-hoc Tailwind palette classes.
- **Reuse components** from `templates/components/ui.html` (`{% import "components/ui.html" as ui %}`). A new pattern
  used twice becomes a macro and is added to `/styleguide` (`DASHBOARD_STYLEGUIDE_ENABLED=true`).
- **Mobile first:** design at 360 px width first; most drivers are on a phone.
- **Accessibility:** semantic HTML, labels on every input, `aria-*` on interactive custom widgets, visible focus,
  WCAG AA contrast in light and dark.
- **Copy is Spanish**, user-facing numbers in Spanish format (`1,459 €/l`, `12,3 km`) via the shared formatters
  (`static/js` format helpers / Jinja filters), not inline formatting.
- JS stays small and progressive: pages must render useful content server-side before JS runs.

## 8. Testing

| Level        | Tool                        | Where                                     | What                                                          |
| ------------ | --------------------------- | ----------------------------------------- | ------------------------------------------------------------- |
| Unit         | pytest                      | `<service>/tests/test_*.py`               | services, transforms, geo, queries (fast, no network)         |
| API / routes | pytest + FastAPI TestClient | `tests/test_api.py`, `test_web_routes.py` | status codes, schemas, flag on/off                            |
| JS unit      | node (`npm run unit:test`)  | `fuel-dashboard/tests/unit/`              | formatters, small helpers                                     |
| E2E UI       | Playwright                  | `fuel-dashboard/tests/ui/*.spec.ts`       | critical user journeys with `DASHBOARD_UI_TEST_MODE` fixtures |

- **Every behaviour change ships with tests; every bug fix starts with a failing regression test.**
- Shared test data in `tests/fixture.py`. **No real network calls** in tests: mock OSRM, geocoding, GCS and the gov API.
- Data transforms (entity mapping, price normalisation, aggregations) get thorough edge-case tests; errors there
  silently corrupt downstream data.
- Test names describe behaviour: `test_returns_cheapest_station_within_radius`, not `test_func1`.
- New UI flows add/extend a Playwright spec and its UI-test fixture (`app/ui_test_support.py`).
- **Docker tests (`make <service>.test`) are the CI source of truth.** `make test-local` is for fast iteration.

## 9. Definition of Done

A slice is done when:

- [ ] Acceptance criteria met; flag default is set deliberately.
- [ ] Tests added/updated; `make done` passes (setup, typecheck, local tests, UI tests, image scan).
- [ ] `pre-commit run --all-files` passes.
- [ ] Self-review with `/review-local` on the uncommitted diff.
- [ ] Docs updated where behaviour/contract changed (`SCHEMA.md`, `CLAUDE.md`, flag comment).
- [ ] UI changes checked at mobile and desktop width, light and dark.
- [ ] Summary shared for approval: what changed, files, how it was validated, screenshots for UI.

## 10. Release

1. Owner approves the uncommitted diff.
2. Commit on `main` (Conventional Commit), push.
3. `deploy.yaml`: detect changes → Docker tests → build & push arm64 image → Trivy scan → Terraform apply.
4. The new dashboard image (Docker Hub) is picked up on the Raspberry Pi by Watchtower (`fuel-dashboard/docker-compose.yml`);
   verify on fuelprecision.com.
5. If the slice was flagged: verify in production, then flip the flag default in a follow-up commit.
6. **Rollback:** turn the flag off (env var, no redeploy of code), or `git revert <sha>` and push. Never rewrite
   `main` history.

Infra changes (`infra/`): run `make backend.plan`, include the plan summary for approval, then apply.
