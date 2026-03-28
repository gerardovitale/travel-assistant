Review recently changed or newly added code for common security vulnerabilities.

Target: $ARGUMENTS

If no target is specified, auto-detect changed files using `git diff --name-only HEAD` and `git diff --name-only --cached`.

## Vulnerability Checklist

Scan the target code for these categories (at minimum):

### Injection Attacks
- **SQL Injection**: Raw SQL built via string concatenation/f-strings. Check DuckDB queries in `app/data/` and `app/services/`.
- **Command Injection**: Unsanitized input passed to `os.system`, `subprocess`, or `eval`/`exec`.
- **Template Injection**: User input rendered directly in templates without escaping.

### Cross-Site Attacks
- **XSRF / CSRF**: State-changing endpoints (POST/PUT/DELETE) missing CSRF token validation.
- **XSS (Cross-Site Scripting)**: User-supplied data rendered in HTML without sanitization (especially in NiceGUI UI components in `app/ui/`).

### Authentication & Authorization
- **Broken Auth**: Missing or weak authentication on sensitive endpoints.
- **Insecure Direct Object Reference (IDOR)**: User-controlled IDs used to access resources without ownership checks.
- **Hardcoded Secrets**: API keys, passwords, tokens embedded in source code.

### Data Exposure
- **Sensitive Data in Logs**: PII, credentials, or tokens written to logs or error messages.
- **Overly Broad API Responses**: Endpoints returning more data than the client needs.
- **Missing HTTPS / Insecure Transport**: HTTP links or disabled TLS verification.

### Infrastructure & Config
- **Permissive CORS**: Wildcard (`*`) or overly broad CORS origins.
- **Missing Security Headers**: No Content-Security-Policy, X-Frame-Options, Strict-Transport-Security, etc.
- **Dockerfile / Terraform Misconfigurations**: Running as root, overly permissive IAM roles, public buckets.

### Input Validation
- **Buffer Overflow / Resource Exhaustion**: Unbounded input sizes, missing pagination limits, no request body size caps.
- **Path Traversal**: User input used to construct file paths without sanitization.
- **Deserialization Attacks**: Unsafe use of `pickle`, `yaml.load` (without SafeLoader), or similar.

### Dependency & Supply Chain
- **Known Vulnerable Dependencies**: Check `pyproject.toml` / `uv.lock` for obviously outdated or flagged packages (if pip-audit or similar is available, run it).

## Process

1. **Identify scope**: Determine which files to review (from arguments or git diff).
2. **Read each file** and analyze against the checklist above.
3. **Classify findings** by severity:
   - **CRITICAL**: Exploitable now, data loss or unauthorized access likely.
   - **HIGH**: Exploitable with moderate effort, significant impact.
   - **MEDIUM**: Requires specific conditions, limited impact.
   - **LOW**: Defense-in-depth improvement, unlikely to be exploited alone.
4. **Fix CRITICAL and HIGH** issues directly in the code.
5. **Generate a security report** as a Markdown file at `SECURITY_REVIEW.md` in the repo root.

## Report Format (`SECURITY_REVIEW.md`)

Write the report following this structure:

```markdown
# Security Review Report

**Date**: YYYY-MM-DD
**Scope**: [files reviewed]
**Reviewer**: Claude Code (Automated)

## Summary

| Severity | Count |
|----------|-------|
| CRITICAL | N |
| HIGH     | N |
| MEDIUM   | N |
| LOW      | N |

## Findings

### [SEV-001] Title — SEVERITY

**File**: `path/to/file.py:line`
**Category**: e.g., SQL Injection
**Status**: FIXED / FLAGGED

**Description**: What the vulnerability is and how it could be exploited.

**Before** (if fixed):
\```python
# vulnerable code
\```

**After** (if fixed):
\```python
# remediated code
\```

**Recommendation** (if flagged only): What should be done.

---

(repeat for each finding)

## Passed Checks

List categories from the checklist that were reviewed and found clean.
```

## Final Steps

6. Present the findings summary to the user.
7. If any CRITICAL or HIGH issues were fixed, explain each fix and the concern it addresses.
8. If MEDIUM/LOW issues were only flagged, briefly describe what the user should consider.
