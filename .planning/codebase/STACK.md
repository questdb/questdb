# Technology Stack

**Analysis Date:** 2026-04-09
**Scope:** SAMPLE BY fill on the GROUP BY fast path

## Languages

**Primary:**
- Java 17 (target `javac.target=17` in `core/pom.xml`). Zero-GC on data paths. No third-party runtime dependencies.

**Secondary:**
- C/C++ via JNI for SIMD-vectorized operations and native memory management.
- Rust (`rust/qdbr`) for supplementary native modules.

## Runtime

**Environment:**
- JDK 17+ (64-bit), module system (`-p`, `-m` flags).
- Version: 9.3.5-SNAPSHOT (`pom.xml` line 28).

**Package Manager:**
- Maven 3
- Lockfile: not applicable (Maven resolves from repositories).

## Build

- `mvn clean package -DskipTests` for JAR without tests.
- Native libs: `cmake -B build/release -DCMAKE_BUILD_TYPE=Release && cmake --build build/release --config Release` in `core/`.
- Test: `mvn -Dtest=ClassNameTest test` (sequential only, do not run multiple `mvn test` in parallel).

## Key Internal Libraries Used by the Fill Subsystem

### Custom Collections (`core/src/main/java/io/questdb/std/`)

- `ObjList<T>` (`std/ObjList.java`) -- resizable object list. Use instead of `T[]`. Integrates with `Misc.freeObjList()`.
- `IntList` (`std/IntList.java`) -- resizable int list. Stores per-column fill modes in `SampleByFillRecordCursorFactory`.
- `DirectLongList` (`std/DirectLongList.java`) -- off-heap long list. Used by `FillRangeRecordCursorFactory` to collect present timestamps.
- `Numbers` (`std/Numbers.java`) -- null sentinels (`LONG_NULL`, `INT_NULL`, `IPv4_NULL`) and numeric parsing.

### Bytecode Assembler

- `BytecodeAssembler` (`std/BytecodeAssembler.java`) -- generates JVM bytecode at runtime for `RecordSink` instances.
- `RecordSinkFactory` (`cairo/RecordSinkFactory.java`) -- creates `RecordSink` via bytecode assembly. Used by `generateFill()` to build key-extraction sinks for keyed queries. Method size limit = 8000 bytes to stay within C2 inline threshold.

### RecordSink and RecordSinkSPI

- `RecordSink` (`cairo/RecordSink.java`) -- copies selected columns from a `Record` into a `RecordSinkSPI` writer. Generated per-query by `RecordSinkFactory`.
- `RecordSinkSPI` (`cairo/RecordSinkSPI.java`) -- write side of the sink protocol.

### Map Implementations

- `Map` interface (`cairo/map/Map.java`) -- hash map used by old cursor-path `SampleByFillPrevRecordCursor` for keyed aggregation.
- `MapKey` / `MapValue` -- key/value pairs in the map.

### TimestampSampler Hierarchy

- `TimestampSampler` interface (`griffin/engine/groupby/TimestampSampler.java`) -- defines `nextTimestamp()`, `previousTimestamp()`, `round()`, `setStart()`.
- `SimpleTimestampSampler` (`griffin/engine/groupby/SimpleTimestampSampler.java`) -- fixed-size buckets (seconds, minutes, hours, days, weeks). Arithmetic: `start + n * bucket`.
- `MonthTimestampMicrosSampler` (`griffin/engine/groupby/MonthTimestampMicrosSampler.java`) -- variable-size month buckets (micros precision). Calendar-aware month addition.
- `MonthTimestampNanosSampler` -- same for nanosecond timestamps.
- `WeekTimestampMicrosSampler` / `WeekTimestampNanosSampler` -- week-based samplers.
- `YearTimestampMicrosSampler` / `YearTimestampNanosSampler` -- year-based samplers.
- `TimestampSamplerFactory` (`griffin/engine/groupby/TimestampSamplerFactory.java`) -- parses interval strings (e.g., `'1h'`, `'5m'`, `'1M'`) and delegates to `TimestampDriver.getTimestampSampler()`.

### TimestampDriver

- `TimestampDriver` interface (`cairo/TimestampDriver.java`) -- abstraction for microsecond vs. nanosecond timestamp precision. Provides:
  - `getTimestampSampler(interval, timeUnit, position)` -- factory method for `TimestampSampler`.
  - `getTimestampConstantNull()` -- sentinel null constant.
  - `from(timestamp, timestampType)` -- cross-precision conversion.
  - `getTimestampType()` -- returns `ColumnType.TIMESTAMP_MICRO` or `ColumnType.TIMESTAMP_NANO`.
  - Calendar methods: `addMonths()`, `addDays()`, `floorYYYY()`, etc.
- Accessed via `ColumnType.getTimestampDriver(timestampType)`.

### Function Framework

- `Function` (`cairo/sql/Function.java`) -- evaluated expression. Fill values are parsed into `Function` instances.
- `ConstantFunction` -- marker interface for compile-time constants.
- `NullConstant` -- singleton null function.
- `TimestampFunction` -- base for timestamp-returning functions, parameterized by timestamp type.
- `GroupByFunction` -- aggregate function interface (used by old cursor-path, not by new fill cursor).

### Record and Cursor Abstractions

- `Record` (`cairo/sql/Record.java`) -- column-typed accessor (getInt, getLong, getDouble, getTimestamp, etc.).
- `RecordCursor` (`cairo/sql/RecordCursor.java`) -- iterable stream of records.
- `NoRandomAccessRecordCursor` -- cursor that does not support random access (used by fill cursors).
- `RecordCursorFactory` (`cairo/sql/RecordCursorFactory.java`) -- produces `RecordCursor` instances. Key method: `followedOrderByAdvice()` (default `false`).
- `AbstractRecordCursorFactory` (`cairo/AbstractRecordCursorFactory.java`) -- base class for factories.

## Configuration

**Environment:**
- `.env` file present -- contains environment configuration.
- `CairoConfiguration` governs engine-wide settings (memory tags, page sizes).

**Build:**
- `pom.xml` (root) -- parent POM.
- `core/pom.xml` -- core module, `javac.target=17`.

---

*Stack analysis: 2026-04-09*
