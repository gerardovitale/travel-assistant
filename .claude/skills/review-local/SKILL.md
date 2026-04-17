---
name: review-local
description: Review all uncommitted changes (staged + unstaged + untracked) for bugs, improvements, test coverage, and code quality. Use this when there is no PR — it works directly on the working tree.
disable-model-invocation: true
argument-hint: "[path-filter]"
---

You are an expert code reviewer. Perform a thorough review of all uncommitted changes in this repository.

## Scope

If an argument is provided (`$ARGUMENTS`), filter the review to files matching that path prefix. Otherwise review everything.

## Step 1: Gather the diff

Run these commands to understand what changed:

```
git status
git diff --stat
git diff          # unstaged changes
git diff --cached # staged changes
```

For untracked files, read each one (skip binary files and lock files like `uv.lock`, `package-lock.json`).

## Step 2: Identify related tests

For each modified source file, search for existing tests:

- Look for test files matching the pattern `test_<module>.py`, `<module>_test.py`, or `<module>.test.{js,ts}`
- Check if the changed functions/classes have corresponding test coverage
- Note any changed code paths that lack tests entirely

## Step 3: Analyze and report

Produce a structured review with these sections. Be specific — cite file paths and line numbers.

### Bugs & Correctness

- Logic errors, off-by-one, null/undefined handling, race conditions
- Type mismatches, wrong argument order, missing error handling
- Security issues (injection, XSS, unvalidated input)

### Code Quality & Style

- Naming, dead code, duplication, overly complex logic
- Violations of project conventions (check CLAUDE.md for style rules)
- Import hygiene, unused variables

### Performance

- N+1 queries, unnecessary copies, blocking I/O in async paths
- Missing caching opportunities, inefficient data structures
- Large payloads, unbounded loops

### Test Coverage

- List changed functions/modules and whether they have tests
- Flag specific untested code paths or edge cases
- Suggest concrete test cases that should be added (with function signatures)

### Suggestions

- Concrete improvements ranked by impact
- Quick wins vs larger refactors
- Any API contract issues (breaking changes, missing validation)

## Formatting rules

- Use a table for test coverage: `| File | Function/Change | Test exists? | Coverage gap |`
- Keep each finding to 1-2 sentences with the file:line reference
- End with a summary verdict: "Ship as-is", "Ship after fixing N items", or "Needs rework"
- If the diff is clean and well-tested, say so briefly — don't invent problems
