# ADR-005: Unified Logging with org.replikativ/logging

## Context

The replikativ ecosystem consists of approximately 13 repositories. Over time, three different
logging libraries ended up in use across these repos: Timbre (used by datahike, konserve,
replikativ, mercurius), Telemere (used by kabel), and Trove (used by proximum). This caused
dependency conflicts when multiple replikativ libraries were composed in a single application
and led to inconsistent logging patterns with no shared convention for structured logging.

Additionally, there was no standard requiring structured log identifiers. Log calls across
the ecosystem were free-form, making it difficult to filter and analyze log output in
production. A unified approach with mandatory structured IDs was needed.

## Options

### Option A: Keep the status quo

Each repository continues to choose its own logging library independently.

#### Pro

- No migration effort required
- Repositories remain fully independent in their logging choices

#### Contra

- Dependency conflicts when composing multiple replikativ libraries (e.g. Timbre and Telemere
  pulling in conflicting versions of shared dependencies)
- Inconsistent logging API across the ecosystem
- No structured logging standard; no way to enforce mandatory log identifiers
- Users must learn and configure multiple logging libraries depending on which replikativ
  libraries they use

### Option B: Standardize on org.replikativ/logging wrapping taoensso/trove

Introduce a single shared logging library (`org.replikativ/logging`) that wraps `taoensso/trove`
and enforce its use across all replikativ repositories.

#### Pro

- Single logging dependency across the entire ecosystem
- Consistent structured logging API with mandatory namespaced keyword IDs on every log call
- Trove's backend flexibility allows users to bring their own logging implementation: SLF4J
  (Logback, Log4j2), console, Telemere, or Timbre adapters
- Mandatory IDs (`:project/component` pattern) enable structured filtering and analysis
- Eliminates dependency conflicts between logging libraries

#### Contra

- Breaking change for all downstream users who configured Timbre directly
- Migration effort required across all ~13 repositories
- Contributors must learn the new logging conventions

## Status

**ACCEPTED**

## Decision

Adopt `org.replikativ/logging` based on `taoensso/trove` as the unified logging library for
the entire replikativ ecosystem. All repositories have been migrated. Every log call requires
a namespaced keyword ID following the `:project/component` pattern as its first argument.

## Consequences

- There is a single logging dependency (`org.replikativ/logging`) across all replikativ
  repositories, eliminating cross-library dependency conflicts.
- All log calls follow a consistent structured API with mandatory keyword IDs, enabling
  reliable filtering and analysis of log output.
- Users can bring their own logging backend (SLF4J, Timbre, Telemere, console) via trove's
  backend flexibility. No logging implementation is forced on downstream applications.
- All repositories must update their requires from `taoensso.timbre`, `taoensso.telemere`,
  or `taoensso.trove` to `replikativ.logging`.
- This is a breaking change. Existing Timbre configuration code (e.g. `timbre/merge-config!`,
  custom appenders) will need updating. Users who want to preserve Timbre functionality can
  add Timbre to their classpath as a trove backend.
- The `raise` macro moves from `datahike.tools/raise` to `replikativ.logging/raise` and now
  passes `&form` coordinates to trove for correct source location reporting (CLJ-865
  workaround).
