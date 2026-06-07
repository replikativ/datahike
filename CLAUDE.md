# Datahike — notes for Claude

## CHANGELOG

`CHANGELOG.md` is the curated, user-facing change feed — not a git-log mirror. It highlights new features, stability transitions, breaking changes, deprecations, and notable fixes. Internal refactors, CI tweaks, dep bumps, and test-only changes are intentionally omitted.

**When a PR has a user-visible behavior change, add a CHANGELOG entry** under the current version's section:
- **Features** — new APIs or capabilities (typically marked *Experimental*).
- **Notable fixes** — bug fixes that change observed behavior, especially correctness or compliance gaps.
- **Status changes** — when an API contract solidifies (e.g., graduating from *Experimental*).

Each entry follows the existing style: bold lead-in — em-dash — concise description, ending with `(0.x.NNNN, [#NNN])`. Leave `[#TODO]` for the PR number if the PR isn't open yet; fill it in at PR-open time.

PRs that are purely internal (refactor, CI, dep bump, test-only) do not need a CHANGELOG entry.
