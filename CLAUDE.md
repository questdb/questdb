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

When choosing a name for a boolean variable, field or method, always use the
is... or has... prefix, as appropriate.

Use `ObjList<T>` instead of `T[]` object arrays. `ObjList` is QuestDB's
standard resizable list and integrates with `Misc.freeObjList()` /
`Misc.freeObjListIfCloseable()` for resource cleanup.

### Tests

- write all tests using assertMemoryLeak(). This isn't needed for narrow unit
  tests that doesn't allocate native memory.
- resource leaks are a pain point in QuestDB. Always think carefully about all
  possible code paths, especially error paths, and write tests that ensure
  correct resource cleanup on each path.
- use assertQueryNoLeakCheck() to assert the results of queries. This method
  asserts factory properties (supportsRandomAccess, expectSize, expectedTimestamp)
  in addition to data correctness. Storage tests (typically in the cairo test
  package) that only verify data persistence should use assertSql() instead,
  because the factory properties are irrelevant for data-correctness checks and
  can cause false failures.
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

<!-- GSD:project-start source:PROJECT.md -->
## Project

**Harden Percentile/Quantile Functions**

Hardening pass on QuestDB's percentile_disc, percentile_cont, quantile_disc, and quantile_cont aggregate and window functions (PR #6680, `nw_percentile` branch). The functions exist and pass basic tests but have correctness bugs, performance pathologies, resource leaks, and test gaps identified by code review.

**Core Value:** Every percentile/quantile query must return correct results on all inputs — including edge cases like null groups, all-equal values, large partitions, and invalid percentile arguments — without crashing, leaking memory, or producing silent wrong answers.

### Constraints

- **Zero-GC:** No Java heap allocations on data paths (query execution, aggregation)
- **No third-party deps:** All algorithms implemented from first principles
- **Backward compat:** Function signatures and SQL syntax must not change
- **Test conventions:** assertMemoryLeak(), assertQueryNoLeakCheck(), UPPERCASE SQL keywords
- **Pre-compiled Rust:** sqllogictest runner uses pre-compiled native binaries; Rust source changes require rebuild workflow
<!-- GSD:project-end -->

<!-- GSD:stack-start source:codebase/STACK.md -->
## Technology Stack

## Languages
- Java 17 - Core database engine and server, zero-GC design
- Rust (nightly) - Performance-critical operations, Parquet codec, JNI bindings
- C/C++ - SIMD operations, platform-specific I/O, vectorized aggregations
- Java ILP Client - Client library for Java applications
## Runtime
- Java Runtime Environment (JRE) 17+ (64-bit)
- Rust nightly toolchain for library compilation
- C/C++ compiler support for Linux, macOS, Windows, FreeBSD
- Maven 3.0+ - Java dependency and build management
- Cargo - Rust package management and compilation
## Frameworks
- No third-party Java frameworks - QuestDB implements everything from first principles
- Cairo Engine (`io.questdb.cairo.*`) - Column-oriented storage, transactions, partitioning
- Griffin SQL Engine (`io.questdb.griffin.*`) - SQL parsing, compilation, optimization, JIT
- PostgreSQL Wire Protocol (`io.questdb.cutlass.pgwire.*`) - PG-compatible connections
- InfluxDB Line Protocol (`io.questdb.cutlass.line.*`) - ILP TCP and UDP ingestion
- HTTP (`io.questdb.cutlass.http.*`) - REST API and web console
- Cutlass Network Layer (`io.questdb.cutlass.*`) - Unified network protocol handling
- JUnit 4.13.2 - Test framework
- SQLLogicTest (Rust) - SQL dialect compatibility testing
- Maven plugins:
- CMake 3.5+ - C/C++ native library compilation
- jlink - Custom JRE runtime generation (Java 17 modules)
## Key Dependencies
- `junit:junit` 4.13.2 - Unit testing
- `org.postgresql:postgresql` 42.7.7 - PostgreSQL JDBC for test compatibility
- `org.questdb:questdb-client` 1.0.1 - ILP client for test scenarios
- `jni` 0.21.1 - Java Native Interface for Rust-Java interop (core, qdbr, sqllogictest)
- `parquet2` 0.17.2 - Parquet file format implementation (local crate)
- `rayon` 1.10.0 - Data parallelism in Rust
- `serde`/`serde_json` 1.0.210/1.0.145 - Serialization framework
- `tokio` 1.38.0 - Async runtime for SQLLogicTest engines
- `rapidhash` 4.4.1 - Fast hashing for columnar operations
- `parquet-format-safe` 0.2.4 - Safe Parquet metadata handling
- `parquet2` vendored with compression codecs (snap, brotli, streaming-decompression)
- `arrow` 56.2.0 - Arrow data format support (dev dependencies)
- Platform-specific system APIs:
- CPU instruction detection and SIMD support via VCL (Vectorized Class Library)
## Configuration
- Property-based configuration via `DefaultServerConfiguration.java` and factory providers
- Configuration properties loaded into memory configuration classes
- JVM arguments for GC control: `-XX:+UseParallelGC`, `-ea` (assertions enabled)
- Platform-specific profiles: `platform-linux-x86-64`, `platform-osx-aarch64`, `platform-linux-aarch64`, `platform-windows-x86-64`, `platform-freebsd-x86-64`
- Feature profiles: `build-web-console`, `build-binaries`, `build-rust-library`, `qdbr-release`, `qdbr-coverage`
- Rust compilation flags configurable via `qdbr.rustflags` (default: `-D warnings`)
- Web console packaging: v1.2.0 downloaded from npmjs registry
## Platform Requirements
- Java 11+ (build requires Java 17+)
- Maven 3.0+
- Rust nightly toolchain
- CMake 3.5+
- C/C++ compiler (gcc/clang on Unix, MSVC on Windows)
- JAVA_HOME environment variable required for JNI headers
- Java 17 JRE or higher
- Linux x86-64, Linux aarch64, macOS x86-64, macOS aarch64, Windows x86-64, or FreeBSD x86-64
- No external JVM dependencies - zero-GC production deployments possible
- Native libraries (libquestdbr.so/dylib/dll) bundled in JAR or provided via source control
<!-- GSD:stack-end -->

<!-- GSD:conventions-start source:CONVENTIONS.md -->
## Conventions

## Naming Patterns
- GroupBy functions: `[FunctionName]GroupByFunction.java` (e.g., `SumDoubleGroupByFunction.java`)
- GroupBy factories: `[FunctionName]GroupByFunctionFactory.java` (e.g., `MaxDoubleGroupByFunctionFactory.java`)
- Window function factories: `[FunctionName]WindowFunctionFactory.java` (e.g., `VarDoubleWindowFunctionFactory.java`)
- Test files: `[TopicName]Test.java` (e.g., `InsertNullTest.java`, `GroupByFunctionTest.java`)
- Use camelCase: `computeFirst()`, `computeNext()`, `isThreadSafe()`
- Boolean methods use `is` or `has` prefix: `isConstant()`, `hasTimestamp()`, `isThreadSafe()`
- Getter methods: `getDouble()`, `getLong()`, `getValueIndex()`
- Setter methods: `setEmpty()`, `setNull()`, `setDouble()`
- Local variables: camelCase (e.g., `histogram`, `histogramIndex`, `percentile`)
- Instance fields: camelCase (e.g., `valueIndex`, `precision`, `arg`)
- Static constants: UPPER_SNAKE_CASE (e.g., `ARRAY_MASK`, `CONST_MASK`, `TYPE_MASK`)
- NULL sentinel values: `Numbers.LONG_NULL` for longs, `Double.NaN` for doubles
- Classes: PascalCase (e.g., `ApproxPercentileDoubleGroupByFunction`)
- Interfaces: PascalCase (e.g., `GroupByFunction`, `UnaryFunction`)
- Type variables: Single uppercase letter (e.g., `T`, `D`)
## Code Style
- IntelliJ IDEA code style is configured in `.idea/codeStyles/Project.xml`
- No auto-formatting tool (Prettier, spotless) enforced; relies on IDE configuration
- No explicit linting configuration found; code follows Java conventions
- Java class members grouped by kind (static vs. instance) and visibility
- Within each group, sorted alphabetically by name
- Do NOT use comments as "section headings" — methods must stay together after auto-sorting
- Order: Static public constants → Static protected → Static package-private → Static private → Instance public → Instance protected → Instance package-private → Instance private
## Import Organization
- No path aliases observed; full package paths used throughout
## Error Handling
- Use `SqlException` with position information for SQL parsing/compilation errors
- Position passed to `SqlException.$()` should point at specific offending character, not expression start
- Example: `throw SqlException.$(funcPosition, "percentile must be between 0.0 and 1.0");`
- For validation failures, use `CairoException` for Cairo-level errors
- Doubles: Use `Double.NaN` as NULL sentinel, check with `Double.isNaN(value)` and `Numbers.isFinite(value)`
- Longs: Use `Numbers.LONG_NULL` as NULL sentinel, check with `Numbers.isNull(value)`
- Bytes/shorts/booleans: Cannot be NULL in QuestDB; always have default values (0 for numbers, false for booleans)
- Always consider behavior for NULL values; distinguish between NULL as "not initialized yet" vs. actual NULL column value
## Logging
- Log class: `io.questdb.log.Log` from `LogFactory.getLog()`
- Example: `protected static final Log LOG = LogFactory.getLog(AbstractCairoTest.class);`
## Comments
- Use comments sparingly
- Document complex algorithms or non-obvious intent
- Avoid comments that restate code
- Used selectively for public APIs
- Example from `Decimal64LoaderFunctionFactory.java`: Detailed JavaDoc for factory methods
## Function Design
- `computeFirst()`, `computeNext()`, `merge()` are typically 10-20 lines
- Factory methods often 10 lines or less
- Constructor parameters typically match class name pattern conventions
- Method parameters use type prefix when ambiguous (e.g., `mapValue`, `record`, `rowId`)
- GroupBy functions implement interface methods with specific return types (void for compute, double/long/int for getters)
- Use Optional sparingly; prefer null or sentinel values
## Module Design
- GroupBy function implementation classes extend specific base types: `DoubleFunction`, `LongFunction`, etc.
- Implement interfaces: `GroupByFunction`, `UnaryFunction`, `BinaryFunction` as appropriate
- Factories implement `FunctionFactory` interface
- Not observed in codebase; individual function classes are imported directly
## Collections and Memory
- Use `ObjList<T>` instead of `T[]` for object arrays
- ObjList integrates with `Misc.freeObjList()` / `Misc.freeObjListIfCloseable()` for resource cleanup
- Examples from codebase: `ObjList<DoubleHistogram> histograms = new ObjList<>()`
- Always call `Misc.free()` or `Misc.freeObjList()` for resources
- Used in `close()` methods and error handling paths
- Example from `ApproxPercentileDoubleGroupByFunction` (if it had close): Would call `Misc.free()` on histogram list
## Modern Java Features
- Use modern switch expressions instead of switch statements
- Example: `return switch (type) { case 'D' -> DOUBLE; ... }`
- Use triple-quoted strings for SQL and long text
- Example from tests: `"""SELECT ... FROM ... WHERE ..."""`
- Use pattern matching in instanceof checks
- Example: `if (func instanceof UnaryFunction unaryFunc) { unaryFunc.getArg(); }`
## SQL Dialect Patterns
- Prefer `expr::TYPE` syntax for casts over `CAST(expr, TYPE)`
- Use underscore separators in numbers: `1_000_000`
- Use UPPERCASE for SQL keywords in test assertions: `CREATE TABLE`, `INSERT`, `SELECT ... FROM`
- Use multiline strings for complex SQL statements
- Single INSERT statement for multiple rows
## Function Factory Pattern
- `D` = DOUBLE, `L` = LONG, `I` = INT, `S` = STRING, etc.
- Uppercase (e.g., `D`) = non-constant parameter
- Lowercase (e.g., `d`) = constant parameter
- `[]` suffix = array type
- `"max(D)"` - takes one double parameter
- `"approx_percentile(DD)"` - takes two doubles
- `"variance(D)"` - takes one double
<!-- GSD:conventions-end -->

<!-- GSD:architecture-start source:ARCHITECTURE.md -->
## Architecture

## Pattern Overview
- Bytecode code generation for query execution plans (JIT compilation via `SqlCodeGenerator`)
- Columnar storage with memory-mapped files for efficient SIMD operations
- Function factory registry pattern with dynamic class loading (`FunctionFactoryScanner`)
- Group-by aggregation via mutable in-memory maps with native buffer backing
- Record cursor abstraction for lazy row materialization
- Window function support with cached context propagation
## Layers
- Purpose: Parse, compile, and optimize SQL statements to executable query plans
- Location: `core/src/main/java/io/questdb/griffin/`
- Contains: Expression parser, function registry, SQL compiler, code generator
- Depends on: Cairo (storage layer), Standard utilities
- Used by: Network protocols (HTTP, PGWire), application code via `SqlExecutionContext`
- Purpose: Manage columnar table storage, transactions, WAL, partitioning, record cursors
- Location: `core/src/main/java/io/questdb/cairo/`
- Contains: Table readers/writers, memory management, indexing, symbol tables, array types
- Depends on: Standard utilities, native memory via JNI
- Used by: Griffin (SQL layer), direct API consumers
- Purpose: Execute generated query plans with record cursors, aggregation, joins
- Location: `core/src/main/java/io/questdb/griffin/engine/`
- Contains: Operators, groupby processors, join logic, order-by, functions
- Depends on: Cairo, function factories
- Used by: SQL compiler to build query execution chains
- Purpose: High-performance vector operations (SIMD), memory operations
- Location: `core/src/main/c/`
- Contains: SIMD kernels, hash functions, array operations
- Depends on: No Java dependencies
- Used by: Cairo and standard utilities via JNI
## Data Flow
- **Heap Memory:** Expression nodes, metadata, function factories (short-lived)
- **Native Memory (Direct Memory):** 
- **Query Context:** Shared via `SqlExecutionContext` which holds:
## Key Abstractions
- Purpose: Row-like interface with column accessor methods (`getByte()`, `getLong()`, `getDouble()`, etc.)
- Examples: `Record`, `VirtualRecord`, `RecordChainRecord`
- Pattern: Query results always expose via `Record` interface, not raw arrays
- Purpose: Lazy iterator over record stream; materializes one record at a time
- Examples: Table reader cursors, filtered cursors, grouped cursors
- Pattern: Implements `hasNext()` / `next()` with state kept in cursor object
- Purpose: Creates cursor instances; contains plan configuration
- Examples: `TableReaderRecordCursorFactory`, `GroupByRecordCursorFactory`
- Pattern: Factory pre-compiles metadata and configuration; creates lightweight cursor instances
- Purpose: Expression evaluation (scalar or aggregate)
- Examples: `DoubleColumn` (column reference), `DoubleConstant`, `AddDoubleDoubleFunction`
- Pattern: Single `getType()` and one or more typed getter (`getInt()`, `getDouble()`, etc.)
- Purpose: Aggregate function for GROUP BY operations
- Examples: `SumDoubleGroupByFunction`, `CountGroupByFunction`, `FirstLongGroupByFunction`
- Pattern: Maintains state in `MapValue`; accumulates via `computeFirst()`, `computeNext()`, or batched `computeBatch()`
- Purpose: Factory that creates `Function` instances from argument list
- Examples: `SumDoubleGroupByFunctionFactory`
- Pattern: Signature encodes name and argument types (e.g., `"sum(D)"` for sum of double); factory called during compilation
- Purpose: Copies record values into native memory or other sinks
- Examples: Generated bytecode sinks, `LoopingRecordSink`
- Pattern: Created via `RecordSinkFactory.getInstanceClass()` (bytecode generation) or `getInstance()` (fallback loop)
- Purpose: Stores intermediate record results in a linked list in native memory
- Location: `core/src/main/java/io/questdb/cairo/RecordChain.java`
- Pattern: Implements `RecordCursor` + `RecordSinkSPI`; used for:
- Purpose: Resizable arrays for aggregate state in native memory
- Location: `core/src/main/java/io/questdb/griffin/engine/groupby/GroupByDoubleList.java`, `GroupByLongList.java`
- Pattern: Flyweight wrappers around allocated buffers; header stores capacity and size; data pointer managed by `GroupByAllocator`
- Purpose: Multidimensional array type supporting computed columns and window results
- Location: `core/src/main/java/io/questdb/cairo/arr/DirectArray.java`
- Pattern: Owns native memory; supports shape (e.g., 2D arrays); metadata-driven resize
## Entry Points
- Location: `core/src/main/java/io/questdb/ServerMain.java`
- Triggers: JVM startup
- Responsibilities:
- Location: `core/src/main/java/io/questdb/griffin/SqlCompilerImpl.java`
- Triggers: Query string received from client
- Responsibilities:
- Location: `core/src/main/java/io/questdb/griffin/SqlExecutionContext.java`
- Purpose: Holds query-scoped state (bind variables, execution progress, security)
- Created: Once per query request
- Location: `core/src/main/resources/function_list.txt` (optional priority list)
- Scanned: `FunctionFactoryScanner.scan()` loads all classes implementing `FunctionFactory` from classpath
- Ordering: Applied via reflection-based class discovery; optional priority from `function_list.txt`
## Error Handling
- **SqlException:** Thrown during parsing and compilation; includes position in SQL text
- **CairoException:** Thrown during execution (I/O, table access, validation)
- **UnsupportedOperationException:** Thrown for unimplemented features in operators or functions
- **Circuit Breaker:** Cancels long-running queries via `SqlExecutionCircuitBreaker`
## Cross-Cutting Concerns
- Logger instances created via `LogFactory.getLog(Class)` 
- Advisory logs for initialization (function loading, configuration)
- Errors and warnings for operational issues
- Type checking during compilation (`FunctionParser.resolveFunctionOverload()`)
- Implicit cast attempts (e.g., CHAR to STRING) via `CastFunctionFactory`
- Null handling: NULL propagates through most functions; special null-aware functions exist
- `SecurityContext` passed in `SqlExecutionContext`
- Table access controlled via `TableToken` with security context
- Native allocations via `Vm.getCARWInstance()`, `Unsafe.malloc()`
- Allocations tagged with `MemoryTag` for accounting
- Query-scoped memory freed on cursor close
- Multi-threaded group-by via `GroupByRecordCursorFactory` with worker pools
- Row-level locking handled by `TableWriter`
- Per-column memory-mapped files allow concurrent reads
<!-- GSD:architecture-end -->

<!-- GSD:skills-start source:skills/ -->
## Project Skills

| Skill | Description | Path |
|-------|-------------|------|
| review-pr | Review a GitHub pull request against QuestDB coding standards | `.claude/skills/review-pr/SKILL.md` |
<!-- GSD:skills-end -->

<!-- GSD:workflow-start source:GSD defaults -->
## GSD Workflow Enforcement

Before using Edit, Write, or other file-changing tools, start work through a GSD command so planning artifacts and execution context stay in sync.

Use these entry points:
- `/gsd-quick` for small fixes, doc updates, and ad-hoc tasks
- `/gsd-debug` for investigation and bug fixing
- `/gsd-execute-phase` for planned phase work

Do not make direct repo edits outside a GSD workflow unless the user explicitly asks to bypass it.
<!-- GSD:workflow-end -->

<!-- GSD:profile-start -->
## Developer Profile

> Profile not yet configured. Run `/gsd-profile-user` to generate your developer profile.
> This section is managed by `generate-claude-profile` -- do not edit manually.
<!-- GSD:profile-end -->
