Run tests smartly for this project.

If an argument is provided ("$ARGUMENTS"), treat it as the service name (e.g., `fuel-dashboard` or `fuel-ingestor`) and run `make <service>.test-local` for that service only.

If no argument is provided:

1. Run `git diff --name-only HEAD` and `git diff --name-only --cached` to detect which files have changed.
2. If changes are in `fuel-ingestor/`, run `make fuel-ingestor.test-local`.
3. If changes are in `fuel-dashboard/`, run `make fuel-dashboard.test-local`.
4. If both services have changes, run `make test-local`.
5. If no changes are detected, run `make test-local` (full suite).

After tests complete, report a summary:

- Which service(s) were tested
- Number of tests passed/failed
- Total duration
- If any tests failed, show the failure output and suggest fixes
