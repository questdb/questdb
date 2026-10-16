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

## Testing

Write the tests while developing the change; do not leave test cases for review
to discover. A change is done when it meets every rule below that applies to it.
The `fix-pr` skill enforces these rules item by item; work done without it
follows the same rules.

Before writing code, list the behavior the change adds or alters and go through
the dimensions below: each one that applies needs tests, and each one you skip
needs a one-line reason. Write the failing tests first.

Expected results come from an independent source: the specification, plain
Java or `BigDecimal` arithmetic, PostgreSQL or DuckDB behavior, documented
QuestDB semantics, or a reference query. Never run the code under test and
paste its output into the expectation.

### What exhaustive means

1. **Values.** Test functions, operators, arithmetic and casts with every value
   class of every argument type: NULL, zero, negative, positive, the smallest
   and largest values and one step inside each, overflow, and the type's special
   values. Pass each value as a constant, a column and a bind variable; constant
   folding and runtime evaluation must agree (`AbstractFunctionFactoryTest` can
   call a function either way). QuestDB specifics:
   - INT and LONG store NULL as their smallest value (`Numbers.INT_NULL`,
     `Numbers.LONG_NULL`): the smallest real value is MIN+1, and MIN must
     behave as NULL.
   - DOUBLE and FLOAT store NULL as NaN; test +/-Infinity, -0.0 and the
     smallest positive value separately.
   - DECIMAL: maximum precision and scale, rounding at the last digit, overflow.
   - TIMESTAMP and DATE: before 1970, far future, microsecond vs nanosecond
     precision, leap days, DST transitions in time-zone functions.
   - Strings: empty vs NULL, multi-byte and 4-byte UTF-8, invalid UTF-8, very
     long values.
2. **Types.** Test every column type the feature accepts, not only the common
   ones: SYMBOL, VARCHAR and STRING, CHAR, UUID, IPv4, LONG256, GEOHASH, each
   DECIMAL width, arrays. JIT filter bugs 7546-7549 (UUID, IPv4, CHAR, SYMBOL
   compared with a number) went unnoticed because nothing exercised those
   types.
3. **Lines, branches and conditions.** Run the tests written for the change
   with coverage (`ci/diff_coverage.py`, below), read the lines, branches and
   conditions that never ran, and add tests that reach them; repeat until
   everything runs. Coverage that only an unrelated test provides does not
   count. A line that cannot run is proven unreachable and guarded with an
   assertion.
4. **Every test can fail.** Write each test before the code it covers and see
   it fail. Assert the exact result the code produces - the rows, the value,
   the error message and position, the resulting state - never only that
   something ran or did not throw. A test added to reach an uncovered line
   asserts what that line does, so that breaking the line fails the test.
5. **Failure paths and misuse.** Test what a careless or hostile user does:
   - input: wrong type, out of range, empty, huge, malformed;
   - sequence: operate on a dropped, renamed, altered or truncated table; reuse
     a compiled query after TRUNCATE, ALTER, new data or new bind values;
     repeat the operation; cancel it at each stage; disconnect mid-stream;
   - environment: failing file operations (`TestFilesFacadeImpl` overrides),
     the per-query memory limit (`*MemoryTrackerTest`), fault injection in the
     query fuzzer (`-Dquestdb.fuzz.faults`).

   After each failure, assert the error, that nothing leaked, and that the
   table or connection still works.

   Hostile input must not crash or hang the server or leak its resources. Fix
   such a failure in the change that introduces it. Report the rest -
   pre-existing ones and those a review lets through - only as issues in
   `questdb/questdb-enterprise` with the label `crash`, never in a public
   repository, issue or comment: they show how to take a server down. When
   `gh repo view questdb/questdb-enterprise` fails, as it does for OSS
   contributors, show such issues in the console instead.
6. **Parsing and error messages.** Test every input the parser rejects with
   `assertException(sql, position, message)`: the position points at the
   offending character and the message tells the user what was expected.
   Cover missing, extra and wrong tokens, unterminated strings and brackets,
   keywords used as names (quoted and unquoted), letter case, comments and deep
   nesting. SQL that is stored and parsed again (views, materialized views)
   needs a round-trip test.
7. **Execution modes.** The same SQL takes different physical paths. Test each
   one the change can reach: WAL and non-WAL tables (`WalMode`), out-of-order
   and in-order writes, JIT on and off, parallel and serial execution, native
   and parquet partitions, indexed and non-indexed symbols, partitioned and
   non-partitioned tables, columns added after data exists.
8. **SQL contexts.** Test a new or changed SQL feature in every context that
   applies: as a subquery and as a CTE; on both sides of each JOIN type; under
   GROUP BY, SAMPLE BY, LATEST ON and window functions; with ORDER BY and LIMIT,
   including negative LIMIT; with WHERE on indexed and non-indexed symbols;
   with bind variables; inside UNION, EXCEPT and INTERSECT; in a materialized
   view definition and refresh; under EXPLAIN; and as a PostgreSQL wire
   prepared statement executed twice. Write one deterministic test per context;
   combinations of three or more come from the fuzzer.
9. **Data shape and scale.** Empty table and empty partition, a single row, an
   all-NULL column, partition and page-frame boundaries, more data than one
   page frame or row group, and enough distinct keys to make hash maps grow.
   When a configuration property bounds the behavior, test at the server
   default and at its limits, not only at the test default.
10. **Independent oracle.** Check results against something other than the
    code under test:
    - QuestDB-only SQL (SAMPLE BY, LATEST ON, ASOF, LT, SPLICE, HORIZON and
      WINDOW JOIN): compare with an equivalent plain-SQL query through
      `assertSqlCursors(reference, actual)`, or with a simple model written in
      the test.
    - SQL that DuckDB or PostgreSQL also implements: add or extend a
      sqllogictest file for the feature under
      `core/src/test/resources/sqllogictest/test/` (run by `SqlTest`,
      `ParquetTest` and `DedupTest`), and note in the file every place where
      QuestDB deliberately differs. Grow these files feature by feature; do not
      import whole suites.
11. **Fuzzing.** Where a fuzz harness covers the area (storage and WAL in
    `cairo/fuzz` with `FuzzRunner`, queries in `griffin/fuzz/QueryFuzzTest`,
    parsers, protocols), teach its generator the new feature and pin the new
    shapes in a generator-coverage test such as `FilterShapeCoverageTest`. A
    fuzzer finds bugs only in what it can generate. Before opening the PR, run
    it locally with a large budget (`-Dquestdb.fuzz.queries=N` for the query
    fuzzer); CI keeps its small budget.
12. **Concurrency.** Test shared state the change touches under concurrent
    use, forcing the interleaving with latches, barriers or test hooks (never
    `Thread.sleep()`), and add a concurrent fuzz test when the state is complex.
13. **Persistence and compatibility.** Test changes to on-disk state across
    restart and reopen, crash recovery, and upgrade from data written by the
    previous version, including migrations. Test changes to replication or wire
    formats against the real peer (see "Writing tests").
14. **Performance.** A change made for speed, or one on a data path, carries a
    deterministic test of its performance property: the plan shape
    (`assertsPlan`), no allocation per row, or a count of operations, IO calls
    or map resizes. Elapsed time is not a test; measure speed with a JMH
    benchmark in `benchmarks/` and report the numbers in the PR.

### Test integrity

- Never weaken an existing test to make a change pass: do not shrink its data,
  relax its expected values, delete cases, or switch `.returns(...)` to
  `.returnsOnce(...)`. If an expected value was wrong, the commit that changes
  it says why.
- Random tests seed `Rnd` and print the seeds, so every failure reproduces.
- Tests are deterministic and fit CI's time budget; long runs sit behind a
  budget property such as `questdb.fuzz.queries`.

### Measuring coverage locally

`ci/diff_coverage.py` runs the named tests with coverage and lists the changed
lines, branches and conditions they leave unexecuted: Java through JaCoCo, Rust
through LLVM coverage, including Rust code that Java calls through JNI. Work
the way CI's coverage report and IntelliJ's coverage runner allow: run the
relevant tests once, read what never ran, write tests for it, then measure
only the new tests with `--append`. It adds their coverage to the previous
run's and compiles only the changed files, so a large change never re-runs its
whole test set to check a few new tests.

```bash
# Changes since the merge-base with origin/master, including uncommitted ones
python3 ci/diff_coverage.py --tests 'FooTest,BarTest#testBaz*'
# Add the coverage of newly written tests to the previous run
python3 ci/diff_coverage.py --append --tests FooGapTest
# Re-read the last run: one line per file, or the lines of some files
python3 ci/diff_coverage.py --no-run --summary
python3 ci/diff_coverage.py --no-run --paths core/src/main/java/io/questdb/griffin/Foo.java
# Only the uncommitted changes
python3 ci/diff_coverage.py --base HEAD --tests FooTest
# Rust only, with the unit tests of the changed crates
python3 ci/diff_coverage.py --lang rust --tests none --rust-tests my_module
```

Name the tests written for the change: coverage that only an unrelated test
provides does not count. After changing production code, run without
`--append` again; the script warns when earlier coverage may be stale. CI's
coverage report (50% of changed lines, lines only) is a floor, not the target. The first run needs java-questdb-client
installed (see "Building") and, for Rust, the `llvm-tools` component of the
qdbr toolchain; the script prints the command when it is missing.

### Writing tests

- keep tests plain and maintainable. Drive the code through SQL, public APIs,
  configuration, and the existing test helpers and base classes; reuse their
  fixtures, and prefer a parameterized or table-driven test to copies of the
  same test. Never use reflection (`setAccessible`, `getDeclaredField`,
  `Field.set`) to reach or change private state, and never mock internal
  collaborators to assert call sequences. If a path cannot be reached through
  a supported surface, add a small, documented test hook (an overridable
  method or a configuration property) or prove the path unreachable.
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

## Design records

Before reporting a review finding against a feature, check `docs/` for its
design records: a behavior they list as accepted is a project decision.
Record a newly accepted limitation there, not in the public docs.

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
  own bullet in the PR body instead of opening another PR. Only split if the
  user explicitly asks, or if the changes must merge/revert independently.
- **When asked to "send a change to PR #N" / update PR metadata:** push the
  commit(s) to that PR's branch (rebase onto the remote head first if it moved),
  then bring the PR title and body up to date with the branch's net effects.
  Re-running CI on that branch is the validation; do not open a new PR.
- PR titles follow Conventional Commits: `type(scope): description`. The
  description goes to the release notes and must read well on its own, so
  repeat the verb (`fix(sql): fix ...`, not `fix(sql): DECIMAL comparison ...`).
- `feat`, `fix`, `perf` and `docs` titles speak to the user: what they gain or
  no longer suffer, in their terms (SQL, settings, symptoms), with no class
  names or internal mechanisms.
  - Good: `fix(sql): fix wrong results when comparing IPv4 columns in filters`
  - Avoid: `fix(sql): use unsigned comparison for IPv4 in JIT filters`
- `chore`, `ci`, `test`, `build`, `refactor` and `style` titles may be
  technical, but must make the reason for the change obvious. A change users
  can see never takes one of these types. `revert` titles follow the rules of
  the type they revert.
  - Good: `chore(build): speed up builds by recompiling only changed files`
  - Avoid: `chore(build): add incremental compilation to core`
- PR bodies are short bullets, not prose:
  - One bullet per net effect, at most 15 words. State what users gain or no
    longer suffer (for a technical type, developers), not what changed in the
    code. Put `Fixes #NNN` on the bullet for that issue.
    - Good: `IPv4 filters with < or > no longer return wrong rows. Fixes #NNN`
    - Avoid: `Use unsigned comparison for IPv4 in the JIT backend`
  - Measure effects against the base branch: fixing a bug the PR itself
    introduced is not an effect.
  - Give each regression, tradeoff and breaking change its own bullet, as
    plainly as the gains. Don't sell: no superlatives, no bold numbers.
  - A change made for speed, or one on a data path, adds its JMH results as a
    table: case, before, after, with the slower cases included.
  - Link tandem PRs on one line.
  - Nothing else: no test plan, coverage numbers, fuzz seeds, file walkthrough
    or design narrative. The code and its tests carry those.
- Commit titles do NOT use Conventional Commits prefixes. Keep them short (up to
  50 chars) and descriptive in plain English.
- When committing, always include a full long-form description in the commit
  message body (not just the title). Lines in the description can be longer than
  in the commit title: up to 72 characters.
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

Do not run multiple `mvn test` commands in parallel — each invocation builds the
module and they interfere with each other. Run test commands sequentially.

```bash
# Run all tests
mvn test

# Run a specific test class
mvn -Dtest=ClassNameTest test

# Run a specific test method
mvn -Dtest=ClassNameTest#methodName test
```

For repeated runs of core tests, compile through `ci/incremental_compile.py`:

```bash
python3 ci/incremental_compile.py -- test -Dtest=ClassNameTest
```

Maven's compiler plugin recompiles all of core's ~6,000 sources (about 40
seconds) whenever any one of them changed. The script compiles the changed
sources, then reads the class files to see whether anything other classes
compile against changed: signatures, supertypes, non-private members, constant
values. If not, it is done: an edit to a method body or a private member
compiles in about two seconds. If so, it recompiles only the sources that use
what changed, found in the class files and, for constants and imports, in the
sources: a new public method on a leaf class compiles in about six seconds, on
a widely used class such as `TableUtils` in about 25. When the dependents
exceed about half of the code, it falls back to Maven's full rebuild. Add
`--verbose` to see what compiles and why.

Every source file must compile to a class of its own name; `SourceFileNameTest`
checks it. A file whose only top-level type has another name, or an empty file,
looks changed on every build and makes Maven recompile the whole module.

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

Java tests load the committed `libquestdbr` binary, or a dev build that an
earlier run left in `core/target/classes/io/questdb/rust/`, not your current
Rust source. To run them against changed Rust code, build the library with the
`build-rust-library` profile, e.g.
`mvn -f core/pom.xml test -P build-rust-library -Dtest=FooTest`.

After writing or modifying tests, check coverage with
`python3 ci/diff_coverage.py --lang rust` (see "Measuring coverage locally"; add
`--tests` with the Java tests that call the code through JNI). For every
uncovered line or branch, either write a test that reaches it or prove it is
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
