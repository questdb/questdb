# Codebase Structure

**Analysis Date:** 2026-04-13

## Directory Layout

```
core/
├── src/main/java/io/questdb/
│   ├── cairo/                    # Columnar storage engine
│   ├── griffin/                  # SQL compiler and execution engine
│   │   ├── engine/               # Query operators and execution
│   │   │   ├── functions/        # Function implementations
│   │   │   ├── groupby/          # Group-by aggregation logic
│   │   │   ├── join/             # Join operators
│   │   │   ├── orderby/          # Sorting operators
│   │   │   ├── table/            # Table scan operators
│   │   │   └── window/           # Window function support
│   │   └── model/                # Query model (AST) classes
│   ├── cutlass/                  # Network protocols
│   ├── std/                      # Standard utilities (no third-party deps)
│   ├── mp/                       # Message passing, worker queues
│   └── log/                      # Logging
├── src/main/resources/
│   └── function_list.txt         # Optional function loading order
└── src/main/c/                   # Native C/C++ code
```

## Directory Purposes

**`cairo/`:**
- Purpose: Columnar table storage, transactions, WAL, partitioning, memory management
- Contains: Table readers/writers, column indexing, symbol tables, partition management
- Key files: `CairoEngine.java`, `TableReader.java`, `TableWriter.java`, `RecordChain.java`

**`cairo/arr/`:**
- Purpose: Multidimensional array support for computed columns
- Contains: `DirectArray`, `ArrayView`, `ArrayTypeDriver`
- Key usage: Window functions, array literals

**`griffin/`:**
- Purpose: SQL parsing, compilation, and function management
- Contains: `ExpressionParser.java`, `SqlCodeGenerator.java`, `FunctionParser.java`
- Key files: `FunctionFactory.java`, `FunctionFactoryCache.java`, `FunctionFactoryScanner.java`

**`griffin/engine/`:**
- Purpose: Query execution operators and runtime evaluation
- Contains: Record cursors, operators, function implementations
- Key files: `RecordComparator.java`, `QueryProgress.java`

**`griffin/engine/functions/`:**
- Purpose: Built-in function implementations (scalar and aggregate)
- Contains: 77+ subdirectories for function categories
- Categories:
  - `columns/` - Column reference functions (`ByteColumn`, `LongColumn`, etc.)
  - `constants/` - Constant value functions (`BooleanConstant`, `Decimal64Constant`, etc.)
  - `cast/` - Type casting functions
  - `bool/`, `eq/`, `neq/` - Logical and comparison operators
  - `array/` - Array operations
  - `math/`, `string/`, `date/`, `geo/`, `finance/` - Domain-specific functions

**`griffin/engine/functions/groupby/`:**
- Purpose: Aggregate functions for GROUP BY operations
- Contains: 400+ files implementing aggregate functions
- Pattern: `*GroupByFunction` + `*GroupByFunctionFactory`
- Examples:
  - `SumDoubleGroupByFunction` / `SumDoubleGroupByFunctionFactory`
  - `CountGroupByFunction` / `CountGroupByFunctionFactory`
  - `ApproxCountDistinctIntGroupByFunction` / Factory
  - `ArgMaxDoubleDoubleGroupByFunction` / Factory (argmax operations)
  - Statistical: `StdDevGroupByFunction`, `VarianceGroupByFunction`, etc.

**`griffin/engine/functions/window/`:**
- Purpose: Window function execution context and factories
- Contains: `WindowFunction.java`, `WindowContext.java`, `CachedWindowRecordCursorFactory.java`

**`griffin/engine/groupby/`:**
- Purpose: Group-by aggregation engine and data structures
- Contains: Group-by maps, allocators, grouping logic
- Key files:
  - `GroupByDoubleList.java` - Resizable double array in native memory
  - `GroupByLongList.java` - Resizable long array in native memory
  - `GroupByLongHashSet.java` - Hash set for single long keys
  - `GroupByCharSequenceLongHashMap.java` - Hash map for string keys
  - `GroupByRecordCursorFactory.java` - Main group-by cursor factory
  - `GroupByAllocator.java` - Native memory allocator for aggregate state

**`griffin/engine/join/`:**
- Purpose: Join operators (INNER, LEFT, CROSS, ASOF)
- Contains: Join cursor factories and state management

**`griffin/engine/table/`:**
- Purpose: Table scan operators with filtering and optimization
- Contains: 140+ cursor factories for different scan patterns

**`griffin/model/`:**
- Purpose: Query abstract syntax tree (AST) and query model classes
- Contains: `QueryModel.java`, `ExpressionNode.java`, `ExecutionModel.java`
- Key files: AST nodes for WHERE, GROUP BY, ORDER BY, joins

## Key File Locations

**Entry Points:**
- `ServerMain.java` - Server initialization and lifecycle
- `SqlCompilerImpl.java` - SQL compilation orchestrator
- `SqlExecutionContext.java` - Query execution state container

**Configuration:**
- `CairoConfiguration.java` - Database configuration interface
- `DynamicPropServerConfiguration.java` - Runtime-configurable settings
- `core/src/main/resources/function_list.txt` - Function loading order

**Core Logic:**
- `ExpressionParser.java` (130KB) - SQL expression parsing
- `SqlCodeGenerator.java` (537KB) - Bytecode generation for query plans
- `FunctionParser.java` (79KB) - Function name resolution and compilation
- `RecordToRowCopierUtils.java` (164KB) - Bytecode generation for record copying
- `LateralJoinRewriter.java` (145KB) - LATERAL JOIN transformation

**Testing:**
- `core/src/test/java/` - JUnit tests co-located with source
- Naming: `*Test.java` for unit tests
- Pattern: `AbstractQueryBuilderTest` base for SQL execution tests

**Special Directories:**
- `core/src/main/c/` - Native C/C++ code compiled to JNI libraries
- `core/rust/` - Rust components (e.g., SQL logic tests)
- `core/target/` - Build artifacts (generated, not committed)
- `.planning/codebase/` - Architecture documentation (this directory)

## Naming Conventions

**Files:**
- `*Factory` - Creates instances of a type (e.g., `GroupByDoubleList` → `GroupByAllocator` allocates buffers)
- `*Cursor` - Implements `RecordCursor` interface
- `*CursorFactory` - Implements `RecordCursorFactory` interface
- `*Function` - Implements `Function` interface
- `*FunctionFactory` - Implements `FunctionFactory` interface
- `*GroupByFunction` - Implements `GroupByFunction` interface
- `Abstract*` - Base class providing partial implementation
- `*Impl` - Concrete implementation

**Directories:**
- All lowercase with slashes (Java convention)
- Plural for collections: `functions/`, `groupby/`, `engine/`
- Prefix with category: `engine/join/`, `engine/table/`

**Java Classes:**
- `GroupByDoubleList` - Specialized list for double values (flyweight pattern)
- `GroupByLongList` - Specialized list for long values (flyweight pattern)
- `GroupByAllocator` - Memory allocator interface
- `FastGroupByAllocator` - Optimized allocator implementation
- `RecordChain` - Linked-list structure for intermediate results in native memory

## Where to Add New Code

**New Scalar Function:**
1. Implementation: `core/src/main/java/io/questdb/griffin/engine/functions/[category]/MyFunction.java`
2. Factory: `core/src/main/java/io/questdb/griffin/engine/functions/[category]/MyFunctionFactory.java`
3. Tests: `core/src/test/java/io/questdb/griffin/engine/functions/[category]/MyFunctionTest.java`
4. Optional: Add to `function_list.txt` if overload priority needed

**New Aggregate (GroupBy) Function:**
1. Implementation: `core/src/main/java/io/questdb/griffin/engine/functions/groupby/MyGroupByFunction.java`
2. Factory: `core/src/main/java/io/questdb/griffin/engine/functions/groupby/MyGroupByFunctionFactory.java`
3. Extends: `GroupByFunction` interface (implement `computeFirst()`, `computeNext()`, `merge()`)
4. State: Use `GroupByDoubleList` or `GroupByLongList` for aggregate values
5. Tests: `core/src/test/java/io/questdb/griffin/engine/functions/groupby/MyGroupByFunctionTest.java`

**New Query Operator:**
1. Cursor: `core/src/main/java/io/questdb/griffin/engine/[category]/MyRecordCursor.java`
2. Factory: `core/src/main/java/io/questdb/griffin/engine/[category]/MyRecordCursorFactory.java`
3. Extends: `RecordCursorFactory` interface
4. Tests: `core/src/test/java/io/questdb/griffin/engine/[category]/MyRecordCursorFactoryTest.java`

**New Table Access Method:**
- Location: `core/src/main/java/io/questdb/griffin/engine/table/[Name]RecordCursorFactory.java`
- Parent: `RecordCursorFactory`
- Purpose: Define specific scan pattern (e.g., `SelectedRecordCursorFactory` for key lookup)

**Utility Functions:**
- Shared helpers: `core/src/main/java/io/questdb/griffin/engine/groupby/GroupByUtils.java`
- Type conversions: `core/src/main/java/io/questdb/cairo/ColumnTypeConverter.java`

**Configuration:**
- Property-driven: `core/src/main/java/io/questdb/cairo/CairoConfiguration.java`
- Add method to interface; implement in `DynamicPropServerConfiguration.java`

## Special Directories

**`core/src/main/resources/`:**
- Purpose: Resources bundled in JAR
- Contains: `function_list.txt` (optional function loading order)
- Generated: `function_list.txt` is auto-generated during build

**`core/src/main/c/`:**
- Purpose: Native C/C++ code compiled to JNI libraries
- Build: Via CMake (see `core/CMAKE_README.md`)
- Artifacts: Compiled to `core/src/main/resources/io/questdb/bin/`

**`core/src/test/java/`:**
- Purpose: Unit and integration tests
- Pattern: Mirror source structure
- Base classes:
  - `AbstractQueryBuilderTest` - SQL execution testing framework
  - `AbstractAllTypesTest` - Tests all column types
  - `AbstractGrindingTest` - Stress testing

**`core/target/`:**
- Generated: Build artifacts (not committed)
- Contains: Compiled JAR, test reports, generated function list

**`.planning/codebase/`:**
- Purpose: Architecture documentation consumed by GSD tools
- Files: `ARCHITECTURE.md`, `STRUCTURE.md`, `CONVENTIONS.md`, `TESTING.md`, `STACK.md`, `INTEGRATIONS.md`, `CONCERNS.md`
- Generated: By GSD mappers; updated as codebase evolves

## Inter-Module Dependencies

**Griffin → Cairo:** 
- Query compilation generates bytecode that calls Cairo classes
- Function implementations interact with `Record`, `RecordCursor`, `TableReader`

**Cairo → Standard Utilities:**
- Allocations via `Unsafe`, `Vect` (SIMD)
- Collections: `ObjList`, hash maps from `std/`

**Griffin Engine → Functions:**
- Operators instantiate functions via factories
- `FunctionParser` resolves function names to factories

**Both → JNI/Native:**
- `Vect` class wraps SIMD kernels
- `Unsafe` wraps memory operations

---

*Structure analysis: 2026-04-13*
