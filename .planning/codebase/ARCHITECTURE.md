# Architecture

**Analysis Date:** 2026-04-13

## Pattern Overview

**Overall:** Layered SQL Query Engine with Columnar Storage and Native Code Integration

QuestDB employs a classic three-tier architecture: SQL compilation layer (Griffin) above columnar storage engine (Cairo), with native C/C++ code for performance-critical vector operations. The system is designed for zero-GC on data paths, meaning allocations must not occur during query execution or data ingestion.

**Key Characteristics:**
- Bytecode code generation for query execution plans (JIT compilation via `SqlCodeGenerator`)
- Columnar storage with memory-mapped files for efficient SIMD operations
- Function factory registry pattern with dynamic class loading (`FunctionFactoryScanner`)
- Group-by aggregation via mutable in-memory maps with native buffer backing
- Record cursor abstraction for lazy row materialization
- Window function support with cached context propagation

## Layers

**SQL Layer (Griffin):**
- Purpose: Parse, compile, and optimize SQL statements to executable query plans
- Location: `core/src/main/java/io/questdb/griffin/`
- Contains: Expression parser, function registry, SQL compiler, code generator
- Depends on: Cairo (storage layer), Standard utilities
- Used by: Network protocols (HTTP, PGWire), application code via `SqlExecutionContext`

**Storage Layer (Cairo):**
- Purpose: Manage columnar table storage, transactions, WAL, partitioning, record cursors
- Location: `core/src/main/java/io/questdb/cairo/`
- Contains: Table readers/writers, memory management, indexing, symbol tables, array types
- Depends on: Standard utilities, native memory via JNI
- Used by: Griffin (SQL layer), direct API consumers

**Execution Engine (Engine):**
- Purpose: Execute generated query plans with record cursors, aggregation, joins
- Location: `core/src/main/java/io/questdb/griffin/engine/`
- Contains: Operators, groupby processors, join logic, order-by, functions
- Depends on: Cairo, function factories
- Used by: SQL compiler to build query execution chains

**Native Layer (C/C++):**
- Purpose: High-performance vector operations (SIMD), memory operations
- Location: `core/src/main/c/`
- Contains: SIMD kernels, hash functions, array operations
- Depends on: No Java dependencies
- Used by: Cairo and standard utilities via JNI

## Data Flow

**SQL Query Execution:**

1. **Parsing** (`ExpressionParser.java`, `SqlParser`)
   - Tokenizes SQL text into expression trees
   - Builds query model with WHERE, GROUP BY, ORDER BY clauses
   - Resolves table references and column names

2. **Compilation** (`SqlCodeGenerator.java`)
   - Converts parsed query model to bytecode using `BytecodeAssembler`
   - Generates bytecode for:
     - Projection expressions
     - Filter predicates
     - Grouping key sink functions
     - Aggregation functions
   - May generate multiple chunks if bytecode exceeds 8KB (limit via `RecordSinkFactory`)

3. **Execution** (Record Cursor Factories)
   - `RecordCursorFactory` implementations create `RecordCursor` instances
   - Cursors lazily materialize rows from table readers
   - Operators chain cursors (filter, project, join, group, order)

4. **Aggregation** (GroupBy Operators)
   - `GroupByRecordCursorFactory` creates cursor with group-by semantics
   - Groups are stored in maps (e.g., `GroupByLongHashSet` for single long keys)
   - Aggregate state held in `MapValue` objects via native memory
   - Functions (e.g., `SumDoubleGroupByFunction`) update aggregate state

5. **Result Materialization**
   - Grouped results converted back to record streams
   - Window functions computed with cached context
   - Results streamed to client via protocol (HTTP REST, PGWire)

**State Management:**

- **Heap Memory:** Expression nodes, metadata, function factories (short-lived)
- **Native Memory (Direct Memory):** 
  - Table data (memory-mapped files)
  - Aggregate state (allocated by `GroupByAllocator` via `malloc`)
  - Record chains for intermediate results (`RecordChain` uses `MemoryCARW`)
  - Array data for computed columns and window buffers

- **Query Context:** Shared via `SqlExecutionContext` which holds:
  - Bind variables
  - Runtime constants
  - Security context
  - Circuit breaker for query cancellation

## Key Abstractions

**Record:**
- Purpose: Row-like interface with column accessor methods (`getByte()`, `getLong()`, `getDouble()`, etc.)
- Examples: `Record`, `VirtualRecord`, `RecordChainRecord`
- Pattern: Query results always expose via `Record` interface, not raw arrays

**RecordCursor:**
- Purpose: Lazy iterator over record stream; materializes one record at a time
- Examples: Table reader cursors, filtered cursors, grouped cursors
- Pattern: Implements `hasNext()` / `next()` with state kept in cursor object

**RecordCursorFactory:**
- Purpose: Creates cursor instances; contains plan configuration
- Examples: `TableReaderRecordCursorFactory`, `GroupByRecordCursorFactory`
- Pattern: Factory pre-compiles metadata and configuration; creates lightweight cursor instances

**Function:**
- Purpose: Expression evaluation (scalar or aggregate)
- Examples: `DoubleColumn` (column reference), `DoubleConstant`, `AddDoubleDoubleFunction`
- Pattern: Single `getType()` and one or more typed getter (`getInt()`, `getDouble()`, etc.)

**GroupByFunction:**
- Purpose: Aggregate function for GROUP BY operations
- Examples: `SumDoubleGroupByFunction`, `CountGroupByFunction`, `FirstLongGroupByFunction`
- Pattern: Maintains state in `MapValue`; accumulates via `computeFirst()`, `computeNext()`, or batched `computeBatch()`

**FunctionFactory:**
- Purpose: Factory that creates `Function` instances from argument list
- Examples: `SumDoubleGroupByFunctionFactory`
- Pattern: Signature encodes name and argument types (e.g., `"sum(D)"` for sum of double); factory called during compilation

**RecordSink:**
- Purpose: Copies record values into native memory or other sinks
- Examples: Generated bytecode sinks, `LoopingRecordSink`
- Pattern: Created via `RecordSinkFactory.getInstanceClass()` (bytecode generation) or `getInstance()` (fallback loop)

**RecordChain:**
- Purpose: Stores intermediate record results in a linked list in native memory
- Location: `core/src/main/java/io/questdb/cairo/RecordChain.java`
- Pattern: Implements `RecordCursor` + `RecordSinkSPI`; used for:
  - Storing grouped intermediate results
  - Window function state
  - Subquery materialization

**GroupByDoubleList / GroupByLongList:**
- Purpose: Resizable arrays for aggregate state in native memory
- Location: `core/src/main/java/io/questdb/griffin/engine/groupby/GroupByDoubleList.java`, `GroupByLongList.java`
- Pattern: Flyweight wrappers around allocated buffers; header stores capacity and size; data pointer managed by `GroupByAllocator`

**DirectArray:**
- Purpose: Multidimensional array type supporting computed columns and window results
- Location: `core/src/main/java/io/questdb/cairo/arr/DirectArray.java`
- Pattern: Owns native memory; supports shape (e.g., 2D arrays); metadata-driven resize

## Entry Points

**Main Server:**
- Location: `core/src/main/java/io/questdb/ServerMain.java`
- Triggers: JVM startup
- Responsibilities:
  - Creates `CairoEngine` from `Bootstrap` configuration
  - Launches HTTP server (`HttpServer`), PGWire server (`PGServer`)
  - Runs background jobs (metadata hydration, view compilation, WAL purge)

**SQL Execution:**
- Location: `core/src/main/java/io/questdb/griffin/SqlCompilerImpl.java`
- Triggers: Query string received from client
- Responsibilities:
  - Calls `ExpressionParser` to parse SQL
  - Builds query plan via `SqlCodeGenerator`
  - Returns compiled `CompiledQuery` with result cursor factory

**Query Execution Context:**
- Location: `core/src/main/java/io/questdb/griffin/SqlExecutionContext.java`
- Purpose: Holds query-scoped state (bind variables, execution progress, security)
- Created: Once per query request

**Function Registration:**
- Location: `core/src/main/resources/function_list.txt` (optional priority list)
- Scanned: `FunctionFactoryScanner.scan()` loads all classes implementing `FunctionFactory` from classpath
- Ordering: Applied via reflection-based class discovery; optional priority from `function_list.txt`

## Error Handling

**Strategy:** Immediate exception propagation with position tracking for SQL errors

**Patterns:**

- **SqlException:** Thrown during parsing and compilation; includes position in SQL text
  - Position points to offending character (not start of expression)
  - Example: `SqlException.$("position", "error message")`

- **CairoException:** Thrown during execution (I/O, table access, validation)
  - Wraps underlying platform errors
  - Includes error tags for categorization

- **UnsupportedOperationException:** Thrown for unimplemented features in operators or functions
  - Example: `computeBatch()` in `GroupByFunction` not overridden

- **Circuit Breaker:** Cancels long-running queries via `SqlExecutionCircuitBreaker`
  - Checked during record cursor iteration
  - Set by timeout or explicit cancellation

## Cross-Cutting Concerns

**Logging:** 
- Logger instances created via `LogFactory.getLog(Class)` 
- Advisory logs for initialization (function loading, configuration)
- Errors and warnings for operational issues

**Validation:**
- Type checking during compilation (`FunctionParser.resolveFunctionOverload()`)
- Implicit cast attempts (e.g., CHAR to STRING) via `CastFunctionFactory`
- Null handling: NULL propagates through most functions; special null-aware functions exist

**Authentication:**
- `SecurityContext` passed in `SqlExecutionContext`
- Table access controlled via `TableToken` with security context

**Memory Management:**
- Native allocations via `Vm.getCARWInstance()`, `Unsafe.malloc()`
- Allocations tagged with `MemoryTag` for accounting
- Query-scoped memory freed on cursor close

**Concurrency:**
- Multi-threaded group-by via `GroupByRecordCursorFactory` with worker pools
- Row-level locking handled by `TableWriter`
- Per-column memory-mapped files allow concurrent reads

---

*Architecture analysis: 2026-04-13*
