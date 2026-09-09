# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with
code in this repository.

## Project Overview

QuestDB is an open-source time-series database written primarily in zero-GC Java
with native C/C++ libraries for performance-critical operations. It features
column-oriented storage, SIMD-accelerated vector execution, and specialized
time-series SQL extensions.

## Coding guidelines

Java class members are grouped by kind (static vs. instance) and visibility.

Use the modern Java 17 features:

- enhanced switch
- multiline string literal
- pattern variables in instanceof checks

However, the java-questdb-client module targets Java 11. When writing code in
the client module, use only legacy Java features.

Whenever dealing with column data, results of expressions, SQL statements, etc.,
always consider what the behavior should be when something is NULL. Be careful
to distinguish NULL as a sentinel value for "not initialized yet" vs. an actual
NULL value.

When choosing a name for a boolean variable, field or method, always use the
is... or has... prefix, as appropriate.

**Log messages must use strictly ASCII characters.** QuestDB's log
infrastructure does not reliably render non-ASCII (e.g., em dashes, curly
quotes, Unicode symbols). Use only plain ASCII punctuation in all `LOG.info()`,
`LOG.error()`, etc. calls.

Use `ObjList<T>` instead of `T[]` object arrays. `ObjList` is QuestDB's
standard resizable list and integrates with `Misc.freeObjList()` /
`Misc.freeObjListIfCloseable()` for resource cleanup.

### Tests

- write all tests using assertMemoryLeak(). This isn't needed for narrow unit
  tests that doesn't allocate native memory.
- resource leaks are a pain point in QuestDB. Always think carefully about all
  possible code paths, especially error paths, and write tests that ensure
  correct resource cleanup on each path.
- assert query results with the fluent `assertQuery(query)` builder
  (`AbstractCairoTest.assertQuery(...)`): `assertQuery(sql).returns(expected)`.
  Chain factory-property assertions as the query warrants — `.timestamp(...)`,
  `.expectSize()`, `.noRandomAccess()`, `.sizeMayVary()`, `.ddl(...)`,
  `.mutateWith(...)`, `.withEngine(...)`, `.withContext(...)`. For execution
  plans use `.assertsPlan(...)` / `.assertsPlanContaining(...)` or fold the plan
  into a data assertion with `.withPlan(...)` / `.withPlanContaining(...)`.
- the old `assertSql(...)` / `TestUtils.assertSql(...)` query-result helpers have
  been REMOVED — `.returns(...)` runs a strictly stronger battery (a second
  cursor pass, a `calculateSize()` cross-check, a variable-column check, and the
  factory-property assertions) that catches bugs the old single-pass print/compare
  silently missed. (`TestServerMain.assertSql(sql, expected)` is a separate
  live-`ServerMain` convenience wrapper and is unrelated.)
- **never use `.returnsOnce(...)` unless the query's projection is genuinely
  non-deterministic across a re-read** — an unseeded `rnd_*` function, or
  time-varying output such as `now()`/`sysdate()`/`systimestamp()`. `returnsOnce`
  deliberately skips the second cursor pass and every check listed above, so for
  any deterministic query it leaves real bugs untested. Default to `.returns(...)`;
  reach for `.returnsOnce(...)` only with a stated reason that the output cannot be
  stable across two reads.
- **Never assert peer behaviour with a hand-built frame or a fake socket.** Server
  tests must drive the real `java-questdb-client` for anything that asserts how a
  client reacts to server output — close codes, NACK classification, ack watermark
  advance, reconnect eligibility. The client is already on the test classpath (53 of
  91 QWP tests import it). A hand-forged frame encodes the contract you *assume* the
  client implements, so it passes even when the pinned client disagrees; that is how
  the ROLE_CHANGE (4001) close shipped against a client that classified it as a
  poison strike. If a test names a client class in a comment to justify an assertion,
  it must import that class instead.
- Fake sockets (`MockRawSocket`, `TestableContext`) remain valid for *transport fault
  injection only* — partial sends, `PeerIsSlowToWriteException` resume paths, and
  malformed frames a correct client would never emit. They must never encode what a
  correct client would send in response to the server.
- **Any change to a wire constant or wire format requires a test that exercises the
  pinned submodule client.** A green OSS build does not otherwise prove the pinned
  client agrees.
- use execute() to run non-queries (DDL)
- prefer UPPERCASE for SQL keywords (CREATE TABLE, INSERT, SELECT ... AS ... FROM,
  etc.), but mixing cases is acceptable since SQL is case-insensitive
- use a single INSERT statement to insert multiple rows
- use multiline strings for longer statements (multiple INSERT rows, complex
  queries), as well as to assert multiline query results
- use underscore to separate thousands in numbers with 5 digits or more

### QuestDB's SQL dialect

- QuestDB supports multidimensional arrays (e.g., `DOUBLE[]`, `DOUBLE[][]`).
  Dimensionality is encoded in the column type itself, so `DOUBLE[]` and
  `DOUBLE[][]` are distinct column types.
- QuestDB supports the expr::TYPE syntax for casts. Always prefer it to
  CAST(expr, type)
- QuestDB supports underscores as thousands separator: 1_000_000. Always use
  them in numbers of 5 digits or more, and always have that in mind when writing
  implementation code. `Numbers.parseInt()` / `parseLong()` already support
  underscore separators.
- QuestDB does not support DELETE. Rows can only be soft-deleted through
  application logic rules, such as a "deleted BOOLEAN" column.
- QuestDB does support ALTER TABLE DROP PARTITION to mass-delete data.

### Error Position Convention

`SqlException.$(position, msg)` — the position should point at the specific
offending character, not the start of the expression.

## Git & PR Conventions

- **PRs are squash-merged. Commit history on a PR branch is throwaway** — only
  the squashed commit message that lands on `master` is preserved. Do not
  spend effort tidying the branch's history: no soft resets to "commit all at
  once", no rewording prior commits, no force pushes to clean up. Adding a
  fix-up commit on top is always fine. The squash flow folds the lot at merge
  time anyway.
- **Bundle related fixes into one PR; do NOT propose splitting by logic.** This
  is a high-throughput shop (~20 PRs/day) and CI is the bottleneck, not review:
  a full run takes ~40 min and can go red on an unrelated flake. Splitting one
  branch into N "logically clean" PRs multiplies that cost — N× CI time, N×
  flake exposure, PRs failing on each other's flakes, and N× the babysitting to
  shepherd them all to merge. The squash-merge collapses everything into one
  tidy `master` commit anyway, so multiple fixes on a branch cost nothing at
  merge time. Default to adding the change to the PR/branch already in flight,
  especially when the fixes share a CI lineage (one change is what makes the
  other's CI go green). When a branch carries more than one fix, give each its
  own clearly-labeled section in the PR body instead of opening another PR. Only
  split if the user explicitly asks, or if the changes must merge/revert
  independently.
- **When asked to "send a change to PR #N" / update PR metadata:** push the
  commit(s) to that PR's branch (rebase onto the remote head first if it moved),
  then update the PR title/body to cover everything the branch now contains.
  Re-running CI on that branch is the validation; do not open a new PR.
- **Do not create worktrees or `pr-*` checkout branches when reviewing or
  iterating on a PR.** All work belongs on `vi_api`. Even when a PR exists on a
  separate branch (e.g. `pr-7128`), the canonical state to review and modify is
  whatever is currently merged into `vi_api` — follow-up fixes routinely land
  there directly, so `pr-*` branches lag and reviewing them in isolation gives
  a misleading picture. If a `gh pr` command needs to fetch a PR's diff, fetch
  the diff only (`gh pr diff`); do not check the branch out.

## Investigating failures

- **Never dismiss a failure as "pre-existing", "flaky", "unrelated", or "a
  known issue" without actually proving it.** That label is a hypothesis,
  not a conclusion. Treat any red test, red CI job, or surprising log line
  as a live bug to investigate until the evidence — git log, reproduction
  on master, a real timing constraint, an upstream report — forces a
  different conclusion. Only after that proof can the issue be set aside,
  and the proof itself should be reported back so it can be verified.
- **`java-questdb-client/` is a separate git repo** (a git submodule). Always
  `cd` into it and commit there independently. Never commit it from the parent
  repo as a submodule pointer update without also committing inside it first.
- PR titles must follow Conventional Commits format: `type(scope): description`
  (e.g., `fix(sql): fix ...`, `feat(core): add ...`). The description part is
  copied to release notes, so it must read well on its own — repeat the verb
  (e.g., `fix(sql): fix ...` not `fix(sql): DECIMAL comparison ...`).
- PR title descriptions must speak to the end-user about the positive impact,
  not about internal implementation details.
- PR descriptions must use a level-headed, analytical tone. Present both
  positive and negative effects of the PR with equal weight — don't cherry-pick
  good results, don't sell it, don't use superlatives or bold emphasis on
  numbers. Point out regressions and tradeoffs as prominently as improvements.
- PRs that fix a GitHub issue must reference it with `Fixes #NNN` at the top of
  the PR body.
- Commit titles do NOT use Conventional Commits prefixes. Keep them short (up to
  50 chars) and descriptive in plain English.
- When committing, always include a full long-form description in the commit
  message body (not just the title). Lines in the description can be longer than
  in the commit title: up to 72 characters.
- In PR test plans, use plain bullet points (`-`), not check marks or
  checkboxes.
- Always add GitHub labels consistent with the PR title (e.g., a `perf(sql):` PR
  should get "SQL" and "Performance" labels).
- Common PR labels: `Bug`, `CI`, `Compatibility`, `Core`, `Documentation`,
  `Enhancement`, `Flaky Test`, `ILP`, `Materialized View`, `New feature`,
  `Performance`, `Postgres Wire`, `REST API`, `SQL`, `Security`, `UI`, `WAL`,
  `Windows`, `regression`, `rust`, `storage`.
- Use active voice in commit messages, PR descriptions, and code comments. Name
  the acting subject — a class, method, caller, or component — instead of
  writing "is/are + past participle" constructions.
  - Good: "`determineExportMode()` inspects the compiled factory"
  - Avoid: "The export mode is determined by inspecting the compiled factory"
  - Good: "`setUp()` pre-computes per-column metadata into flat arrays"
  - Avoid: "Per-column metadata are pre-computed into flat arrays at setup time"
  - Good: "The ring queue passes the factory to the exporter"
  - Avoid: "The factory is passed through the ring queue to the exporter"
  - Good: "The materializer converts computed SYMBOL columns to STRING"
  - Avoid: "Symbol columns that are computed are converted to STRING"

## Build Commands

### Prerequisites

- Java 11+ (64-bit)
- Maven 3
- `JAVA_HOME` environment variable set

### Building

```bash
# Build JAR without tests (fastest)
mvn clean package -DskipTests -P local-client

# Build with web console
mvn clean package -DskipTests -P build-web-console

# Build with web console and native binaries
mvn clean package -DskipTests -P build-web-console,build-binaries
```

When you build just the core module with `mvn -pl core`, it will fall use a
pre-built java-questdb-client module, installed in the local Maven cache. It may
be stale and result in build errors. Fix this issue with:

```bash
cd java-questdb-client && mvn install -DskipTests && cd -
```

This should install a fresh version into the Maven cache.

**Do NOT add `clean` to that command.** The client no longer commits its native
libraries — `libquestdb` is built from source into
`core/target/classes/io/questdb/client/bin-local/`, which `clean` deletes and a
plain `mvn install` does not regenerate. The resulting jar is missing its native,
and `mvn -pl core test` then fails with hundreds of
`NoClassDefFoundError: Could not initialize class io.questdb.client.std.Os`
across unrelated suites — a failure that looks nothing like its cause.

If the native is already missing (or you changed the client's C sources), rebuild
it before installing. Requires `cmake` and `nasm`:

```bash
cd java-questdb-client/core
export MACOSX_DEPLOYMENT_TARGET=13.0   # macOS only
cmake -B cmake-build-release -DCMAKE_BUILD_TYPE=Release
cmake --build cmake-build-release --config Release
cd .. && mvn install -DskipTests && cd -
```

Do not copy natives out of an older cached jar to repair this. The client's C
sources change between versions, so an older `libquestdb` links against a
different symbol set and fails with `UnsatisfiedLinkError` instead — a different
wall of errors that also does not name its cause.

### Running Tests

Do not run multiple `mvn test` commands in parallel — each invocation triggers a
full build and they interfere with each other. Run test commands sequentially.

```bash
# Run all tests
mvn test

# Run a specific test class
mvn -Dtest=ClassNameTest test

# Run a specific test method
mvn -Dtest=ClassNameTest#methodName test
```

### Running QuestDB

```bash
# After building with web console:
mkdir <root_directory>
java --add-exports=java.base/jdk.internal.vm=io.questdb -p core/target/questdb-<version>-SNAPSHOT.jar -m io.questdb/io.questdb.ServerMain -d <root_directory>
# Web console at http://localhost:9000
```

### Building and Validating Rust Code

The Rust crate lives in `core/rust/qdbr/`. Before considering any Rust task
complete, run all four checks from that directory:

```bash
cd core/rust/qdbr
cargo fmt        # Fix formatting
cargo check --all-targets  # Compile all targets including tests
cargo clippy --all-targets  # Lint — zero warnings required
cargo test --lib  # Run unit tests
```

All four must pass with zero errors and zero warnings.

After writing or modifying tests, check coverage with:

```bash
cargo llvm-cov --lib --text -- <module_name>
```

For every uncovered line, either write a test that reaches it or prove it is
unreachable and mark it with `expect()` / `debug_assert!`.

A panic in Rust code called via JNI aborts the entire JVM with no recovery.
Never use `unwrap()` or `expect()` on data derived from file contents or
external input. Use `Result` / `Option` with proper error propagation (`?`)
instead.

### Building Native C/C++ Libraries

```bash
cd core
cmake -B build/release -DCMAKE_BUILD_TYPE=Release
cmake --build build/release --config Release
# Artifacts go to core/src/main/resources/io/questdb/bin/
```

## Architecture

### Module Structure

- **core/** - Main database engine (all production Java code)
- **benchmarks/** - JMH micro-benchmarks
- **compat/** - Compatibility tests
- **utils/** - Build utilities
- **examples/** - Usage examples
- **win64svc/** - Windows service wrapper
- **java-questdb-client** - Java client for data ingestion (legacy ILP and
  QuestDB's QWP)

### Core Package Layout (`core/src/main/java/io/questdb/`)

- **cairo/** - Storage engine: table readers/writers, columnar storage, WAL,
  transactions, partitioning, indexing
- **griffin/** - SQL engine: parser, compiler, optimizer, code generator,
  execution
- **cutlass/** - Network protocols:
    - `pgwire/` - PostgreSQL wire protocol
    - `http/` - REST API and web console
    - `line/` - InfluxDB Line Protocol (ILP)
    - `text/` - CSV import
- **std/** - Custom collections and utilities (zero-allocation data structures)
- **mp/** - Message passing and worker queues
- **jit/** - JIT compilation for filters
- **log/** - Logging infrastructure
- **tasks/** - Background job definitions

### Key Design Principles

1. **Zero-GC on data paths**: No allocations during query execution or data
   ingestion. Use object pools and pre-allocated buffers.

2. **No third-party Java dependencies**: Algorithms are implemented from first
   principles for tight integration and performance.

3. **Native code for performance**: SIMD operations, memory management, and
   platform-specific optimizations in C/C++ via JNI.

4. **Column-oriented storage**: Data stored by column for compression and
   vectorized operations.

### Entry Points

- `ServerMain.java` - Main server entry point
- `CairoEngine.java` - Storage engine core
- `SqlCompiler.java` / `SqlCompilerImpl.java` - SQL compilation
- `TableWriter.java` / `TableReader.java` - Table I/O
