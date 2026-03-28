Run a full pre-push validation check for this project. Execute the following steps in order and report results for each:

## Step 1: Code Quality
Run `pre-commit run --all-files` and report which hooks passed/failed.

## Step 2: Tests
Detect which services have changes using `git diff --name-only HEAD` and `git diff --name-only --cached`:
- If `fuel-ingestor/` changed, run `make fuel-ingestor.test-local`
- If `fuel-dashboard/` changed, run `make fuel-dashboard.test-local`
- If both or neither changed, run `make test-local`

## Step 3: Infrastructure (conditional)
If any files in `infra/` have changed:
1. Run `cd infra && terraform fmt -check`
2. Run `cd infra && terraform validate`
Report results or skip if no infra changes.

## Summary
At the end, provide a clear summary table:

| Check | Status |
|-------|--------|
| Pre-commit (formatting, linting, secrets) | PASS/FAIL |
| Tests (service name) | PASS/FAIL |
| Terraform validation | PASS/FAIL/SKIPPED |

If everything passes, confirm it's safe to push. If anything fails, list what needs fixing.
