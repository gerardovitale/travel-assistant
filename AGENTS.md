# Repository Guidelines

## Project Structure & Module Organization
This repository contains two Python services plus infrastructure code:
- `fuel-ingestor/`: batch ingestion job (`app/`) and tests (`tests/`).
- `fuel-dashboard/`: FastAPI/NiceGUI dashboard (`app/api`, `app/services`, `app/ui`, `app/data`) and tests (`tests/`).
- `infra/`: Terraform for runtime resources; `infra/backend_support/` for Terraform backend/bootstrap.
- `scripts/`: helper scripts (for example Dockerized test runner).
- `maps/` and `data/`: static geospatial and data assets.

Keep feature code inside each service’s `app/` package and mirror with tests in the corresponding `tests/` folder.

## Build, Test, and Development Commands
Docker is the default execution path for tests and local runs.
- `make test`: run both service test suites.
- `make fuel-ingestor.test` / `make fuel-dashboard.test`: run one suite via `scripts/run-docker-test.sh`.
- `make fuel-ingestor.local`: build and run ingestor locally (writes to `fuel-ingestor/output/`).
- `make fuel-dashboard.run`: build and serve dashboard on `localhost:8080`.
- `make backend.init|plan|apply|destroy`: Terraform backend support lifecycle.
- `pre-commit run --all-files`: run formatting, linting, secret scanning, and Terraform checks.

## Coding Style & Naming Conventions
- Python style: 4-space indentation, `snake_case` for functions/files, `PascalCase` for classes, `UPPER_SNAKE_CASE` for constants.
- Formatting/linting are enforced by pre-commit: `black` (line length 120), `flake8` (max line length 120), and `reorder-python-imports`.
- Terraform changes must pass `terraform fmt` and `terraform validate` hooks.

## Testing Guidelines
- Framework: `pytest` (executed in `Dockerfile.test` containers).
- Naming: test files use `test_*.py`; keep test modules close to the feature area they verify.
- Common invocation in containers: `pytest --durations=5 -vv /app/tests/`.
- No explicit coverage threshold is configured; add/extend tests for every behavior change and regression fix.

## Commit & Pull Request Guidelines
- Follow the project’s existing commit style: `feat:`, `fix:`, `refactor:`, `chore:`, `actions:` + concise imperative summary.
- Keep commits focused by service (`fuel-ingestor`, `fuel-dashboard`, or `infra`) when possible.
- PRs should include:
  - What changed and why.
  - Affected paths/services.
  - Validation steps run (for example `make test`, `pre-commit run --all-files`).
  - Screenshots for dashboard UI changes and Terraform plan notes for infra changes.

## Data Source API
- The Spain fuel prices API returns JSON with Spanish field names.
- Column mapping (Spanish → English) is defined in `fuel-ingestor/app/entity.py`.
- When modifying the data schema, update both the entity mapping in the ingestor and any downstream consumers in `fuel-dashboard/app/services/`.

## Testing Priorities
- Data transformation logic (entity mapping, price normalization) needs thorough testing — errors here silently corrupt downstream data.
- API endpoint tests should use fixture data from `tests/fixture.py`, never call external APIs.
- Dashboard UI tests focus on view model logic and service layer, not visual rendering.

## Security & Configuration Tips
- Never commit secrets (`.env`, service-account JSON keys, cloud credentials).
- Run pre-commit before pushing; `gitleaks` is included and should remain passing.
