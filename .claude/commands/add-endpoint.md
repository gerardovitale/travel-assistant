Add a new API endpoint to the fuel-dashboard service.

Description of the endpoint: $ARGUMENTS

## Instructions

1. **Understand the request**: Parse the endpoint description to determine the path, HTTP method, and what data it should return.

2. **Study existing patterns**: Read these files to understand the conventions:
   - `fuel-dashboard/app/api/` — existing router files for route patterns
   - `fuel-dashboard/app/services/` — existing service functions for business logic patterns
   - `fuel-dashboard/app/main.py` — how routers are registered
   - `fuel-dashboard/tests/` — existing test patterns and fixtures

3. **Generate code** following the established patterns:
   - **Router**: Add route in `fuel-dashboard/app/api/` (new file or extend existing one). Use FastAPI router with Pydantic response models.
   - **Service**: Add business logic in `fuel-dashboard/app/services/`. Keep data access via DuckDB engine patterns from existing services.
   - **Tests**: Add tests in `fuel-dashboard/tests/` using pytest + FastAPI TestClient. Use `tests/fixture.py` for shared test data.
   - **Register router** in `main.py` if a new router file was created.

4. **Validate**: Run `make fuel-dashboard.test-local` to ensure tests pass.

5. **Report**: Summarize what was created (files, endpoint path, test count).
