# Test and build scratch

On platforms exposing the full process invocation, ordinary `bb test`, `bb kaocha`,
and build tasks automatically run with owned scratch. On Windows, use the explicit
launcher (native CI should use this on every platform):

```sh
bb --config bb/scratch.edn -m tools.scratch run -- bb ni-cli
```

Put the complete original invocation after `run --`, including any VM flags,
custom `--config`, runner options and task arguments. This bypasses OS command
inspection without reconstructing or dropping options. Bare task invocation
fails with this instruction when complete process information is unavailable;
it does not fall back to unmanaged scratch. No Python is required.

The default root is `.scratch` under the current checkout. Override it with
`DATAHIKE_SCRATCH_ROOT=/disk/path`. CircleCI uses the same checkout-local
default. Choose a disk-backed checkout/root: this code does not change mounts
or enforce a disk quota. Keep the root outside `target`, which builds delete.

The task initializer enters the same Babashka command once as a child with:

- `TMPDIR`, `TMP`, and `TEMP` pointing to `<root>/run-<unique>/tmp`.
- `JAVA_TOOL_OPTIONS` extended with `-Djava.io.tmpdir` pointing there.
- An internal `DATAHIKE_SCRATCH_RUN` marker so nested tasks share the run.

This happens before task dependencies execute. A real child environment also
covers direct `ProcessBuilder` launches by tools.build, beyond Babashka's
process helpers. The Babashka process sets its own `java.io.tmpdir` as well.
Build artifacts retain their usual paths. Dependency resolution needed to load
Babashka's own task configuration happens before the initializer.

The Unix-domain nREPL fixture uses a short relative `.scratch/nrepl-<unique>`
path under the checkout because socket path limits also apply when the configured
scratch root is long. It removes that directory in `finally`, including startup
failures.

Cleanup has two layers:

1. The migrated secondary-index tests use `datahike.test.scratch/fixture` with
   `use-fixtures :each`. Each test gets its own subdirectory, deleted in
   `finally`. Existing test bodies release connections/indexes before the outer
   fixture removes files. Deletion errors fail the test instead of being hidden.
   The shared `with-db` helper also cleans up if the body or setup transaction
   throws. These fixtures work with direct JVM test invocation too.
2. Babashka removes the entire run directory after the command exits, including
   failed commands. Shutdown hooks stop remaining child processes and wait for
   them before removing files. Files needed by the JVM/native libraries can
   therefore remain until process exit without accumulating between runs.

Every 30 seconds the owner reports logical file bytes and entry counts. These
are observations, not quotas or peak measurements. This output also resets
CircleCI's no-output timer; use job time limits for hung commands.

For direct commands that are not bb tasks, or scratch inspection without loading
project dependencies:

```sh
bb --config bb/scratch.edn -m tools.scratch run -- clojure -M:test -m kaocha.runner
bb --config bb/scratch.edn -m tools.scratch status
bb --config bb/scratch.edn -m tools.scratch clean
```

Startup and `clean` reclaim unlocked runs marked finished, including interrupted
removal. Active locks, unknown metadata, and incomplete crash records are
retained. SIGKILL or a host crash cannot execute `finally` or shutdown hooks;
an unlocked dead parent does not establish that its children have stopped.
Incomplete runs require checking for remaining processes before manual removal.
The janitor never scans arbitrary `/tmp` paths or existing worktrees.

Place long-lived worktrees separately, for example `.internal/my-fix`.
Never put a worktree inside a run directory: run directories are deleted.

Run the Babashka lifecycle regression tests with:

```sh
bb --config bb/scratch.edn -m tools.scratch-test
```
