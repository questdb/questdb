# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with
code in this repository.

## Project Overview

QuestDB is an open-source time-series database written primarily in zero-GC Java
with native C/C++ libraries for performance-critical operations. It features
column-oriented storage, SIMD-accelerated vector execution, and specialized
time-series SQL extensions.

## Coding guidelines

Java class members are grouped by kind (static vs. instance) and visibility, and
sorted alphabetically. When adding new methods or fields, insert them in the
correct alphabetical position among existing members of the same kind. Don't
insert comments as "section headings" because methods won't stay together after
auto-sorting.

Use modern Java features:

- enhanced switch
- multiline string literal
- pattern variables in instanceof checks

Whenever dealing with column data, results of expressions, SQL statements, etc.,
always consider what the behavior should be when something is NULL. Be careful
to distinguish NULL as a sentinel value for "not initialized yet" vs. an actual
NULL value.

### Tests

- write all tests using assertMemoryLeak()
- use assertQueryNoLeakCheck() to assert the results of queries
- use execute() to run non-queries (DDL)
- use UPPERCASE for SQL keywords (CREATE TABLE, INSERT, SELECT ... AS ... FROM,
  etc.)
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
  message body (not just the title).
- In PR test plans, use plain bullet points (`-`), not check marks or
  checkboxes.
- Always add GitHub labels consistent with the PR title (e.g., a `perf(sql):` PR
  should get "SQL" and "Performance" labels).
- Common PR labels: `Bug`, `CI`, `Compatibility`, `Core`, `Documentation`,
  `Enhancement`, `Flaky Test`, `ILP`, `Materialized View`, `New feature`,
  `Performance`, `Postgres Wire`, `REST API`, `SQL`, `Security`, `UI`, `WAL`,
  `Windows`, `regression`, `rust`, `storage`.

## Writing Style

- Prefer active voice over passive voice in commit messages, PR descriptions,
  and comments.
  - Good: "The owner thread waits for the latch"
  - Avoid: "The latch is waited on by the owner thread"

## Build Commands

### Prerequisites

- Java 11+ (64-bit)
- Maven 3
- `JAVA_HOME` environment variable set

### Building

```bash
# Build JAR without tests (fastest)
mvn clean package -DskipTests

# Build with web console
mvn clean package -DskipTests -P build-web-console

# Build with web console and native binaries
mvn clean package -DskipTests -P build-web-console,build-binaries
```

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
java -p core/target/questdb-<version>-SNAPSHOT.jar -m io.questdb/io.questdb.ServerMain -d <root_directory>
# Web console at http://localhost:9000
```

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
