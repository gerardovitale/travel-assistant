# Repository Guidelines

## Project Structure & Module Organization

Repo has two Python services plus infra:

- `fuel-ingestor/`: batch ingestion job (`app/`) and tests (`tests/`).
- `fuel-dashboard/`: FastAPI + Jinja2 dashboard (`app/api`, `app/services`, `app/web`, `app/data`) and tests (`tests/`).
- `infra/`: Terraform for runtime resources; `infra/backend_support/` for Terraform backend/bootstrap.
- `scripts/`: helper scripts (for example Dockerized test runner).
- `maps/` and `data/`: static geospatial and data assets.

Feature code inside each service's `app/` package. Mirror tests in corresponding `tests/` folder.

## Build, Test, and Development Commands

Docker default for tests and local runs.

- `make test`: run both service test suites.
- `make fuel-ingestor.test` / `make fuel-dashboard.test`: run one suite via `scripts/run-docker-test.sh`.
- `make fuel-ingestor.local`: build and run ingestor locally (writes to `fuel-ingestor/output/`).
- `make fuel-dashboard.run`: build and serve dashboard on `localhost:8080`.
- `make backend.init|plan|apply|destroy`: Terraform backend support lifecycle.
- `pre-commit run --all-files`: run formatting, linting, secret scanning, and Terraform checks.

## Coding Style & Naming Conventions

- Python style: 4-space indent, `snake_case` functions/files, `PascalCase` classes, `UPPER_SNAKE_CASE` constants.
- Pre-commit enforces: `black` (120), `flake8` (120), `reorder-python-imports`.
- Terraform changes must pass `terraform fmt` and `terraform validate` hooks.

## Testing Guidelines

- Framework: `pytest` (in `Dockerfile.test` containers).
- Naming: `test_*.py`; keep test modules near feature area.
- Container invocation: `pytest --durations=5 -vv /app/tests/`.
- No coverage threshold. Add/extend tests for every behavior change and regression fix.

## Commit Guidelines

Full rules in `docs/CONVENTIONS.md`. Trunk-based on `main`: no branches or PRs, changes stay uncommitted until the owner
approves.

- Conventional Commits scoped by service: `feat(fuel-dashboard): …`, `fix(fuel-ingestor): …`, `chore(infra): …`.
- Before asking for approval, share: what changed and why, affected paths, validation run (`make done`,
  `pre-commit run --all-files`), screenshots for UI changes, Terraform plan notes for infra changes.

## Data Source API

- Spain fuel API returns JSON with Spanish field names.
- Column mapping (Spanish → English) in `fuel-ingestor/app/ingestor/entity.py`.
- Schema changes: update entity mapping in ingestor and downstream consumers in `fuel-dashboard/app/services/`.

## Testing Priorities

- Data transform logic (entity mapping, price normalization) needs thorough tests — errors silently corrupt downstream
  data.
- API endpoint tests use fixture data from `tests/fixture.py`, never call external APIs.
- Dashboard UI tests focus on view model logic and service layer, not visual rendering.

## Security & Configuration Tips

- Never commit secrets (`.env`, service-account JSON keys, cloud credentials).
- Run pre-commit before pushing; `gitleaks` included and must stay passing.
