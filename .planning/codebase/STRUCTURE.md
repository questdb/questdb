# Codebase Structure

**Analysis Date:** 2026-04-09

## Directory Layout

```
core/
├── src/main/java/io/questdb/griffin/
│   ├── SqlOptimiser.java              # SAMPLE BY -> GROUP BY rewrite
│   ├── SqlCodeGenerator.java          # Factory construction (generateSampleBy, generateFill)
│   ├── SqlUtil.java                   # Helpers (isTimestampFloorFunction, etc.)
│   ├── model/
│   │   └── QueryModel.java            # AST: sampleBy*, fill* fields
│   └── engine/
│       ├── groupby/                   # All fill cursors + GROUP BY infrastructure
│       │   ├── SampleByFillRecordCursorFactory.java      # [NEW] Unified fast-path fill cursor
│       │   ├── FillRangeRecordCursorFactory.java          # [EXISTING] Two-pass fast-path fill (NULL/value only)
│       │   ├── SampleByFillPrevRecordCursor.java          # Slow-path PREV fill (keyed)
│       │   ├── SampleByFillPrevRecordCursorFactory.java
│       │   ├── SampleByFillPrevNotKeyedRecordCursor.java  # Slow-path PREV fill (non-keyed)
│       │   ├── SampleByFillPrevNotKeyedRecordCursorFactory.java
│       │   ├── SampleByFillNullRecordCursorFactory.java   # Slow-path NULL fill (keyed)
│       │   ├── SampleByFillNullNotKeyedRecordCursorFactory.java
│       │   ├── SampleByFillValueRecordCursor.java         # Slow-path value fill (keyed)
│       │   ├── SampleByFillValueRecordCursorFactory.java
│       │   ├── SampleByFillValueNotKeyedRecordCursor.java # Slow-path value fill (non-keyed)
│       │   ├── SampleByFillValueNotKeyedRecordCursorFactory.java
│       │   ├── SampleByFillNoneRecordCursor.java          # Slow-path no-fill (keyed)
│       │   ├── SampleByFillNoneRecordCursorFactory.java
│       │   ├── SampleByFillNoneNotKeyedRecordCursor.java  # Slow-path no-fill (non-keyed)
│       │   ├── SampleByFillNoneNotKeyedRecordCursorFactory.java
│       │   ├── SampleByInterpolateRecordCursorFactory.java # LINEAR fill
│       │   ├── SampleByFirstLastRecordCursorFactory.java   # Optimized first/last aggregation
│       │   ├── SampleByFillRecord.java                    # Dual-function record (data or fill) for slow path
│       │   ├── AbstractSampleByFillRecordCursor.java      # Base for slow-path fill cursors with SampleByFillRecord
│       │   ├── AbstractSampleByFillRecordCursorFactory.java # Base for slow-path fill factories (owns Map)
│       │   ├── AbstractSampleByRecordCursorFactory.java   # Base for all SAMPLE BY factories
│       │   ├── AbstractSampleByCursor.java                # Base for all SAMPLE BY cursors (timezone, offset handling)
│       │   ├── AbstractNoRecordSampleByCursor.java        # Base for slow-path cursors (groupByFunctions, allocator)
│       │   ├── AbstractVirtualRecordSampleByCursor.java   # Slow-path cursor with VirtualRecord
│       │   ├── TimestampSampler.java                      # Interface: bucket boundary calculation
│       │   ├── TimestampSamplerFactory.java               # Creates TimestampSampler from interval spec
│       │   ├── SimpleTimestampSampler.java                # Fixed-size bucket sampler (seconds, minutes, hours)
│       │   ├── MonthTimestampMicrosSampler.java            # Calendar month sampler (micros)
│       │   ├── MonthTimestampNanosSampler.java             # Calendar month sampler (nanos)
│       │   ├── WeekTimestampMicrosSampler.java             # Calendar week sampler (micros)
│       │   ├── WeekTimestampNanosSampler.java              # Calendar week sampler (nanos)
│       │   ├── YearTimestampMicrosSampler.java             # Calendar year sampler (micros)
│       │   ├── YearTimestampNanosSampler.java              # Calendar year sampler (nanos)
│       │   ├── GroupByRecordCursorFactory.java             # Standard keyed GROUP BY (vectorized)
│       │   ├── GroupByNotKeyedRecordCursorFactory.java     # Non-keyed GROUP BY
│       │   ├── GroupByUtils.java                          # Shared GROUP BY utilities
│       │   ├── GroupByFunctionsUpdater.java               # Interface for updating group-by functions
│       │   ├── GroupByFunctionsUpdaterFactory.java         # Creates updater via bytecode assembly
│       │   └── ...                                        # Maps, allocators, hyperloglog, vect, etc.
│       └── functions/
│           └── date/
│               ├── TimestampFloorFromOffsetUtcFunctionFactory.java  # timestamp_floor_utc() function
│               └── AbstractTimestampFloorFromOffsetFunctionFactory.java
├── src/test/java/io/questdb/test/griffin/
│   ├── SampleBySqlParserTest.java             # Parser tests for SAMPLE BY syntax
│   └── engine/groupby/
│       ├── SampleByTest.java                  # Main SAMPLE BY test suite (18,163 lines)
│       ├── SampleByFillTest.java              # Tests for new unified fill cursor (72 lines, WIP)
│       ├── SampleByConfigTest.java            # Config tests (ALIGN TO CALENDAR default)
│       ├── SampleByNanoTimestampTest.java     # Nanosecond timestamp SAMPLE BY tests
│       └── SampleByNanoTimestampConfigTest.java
```

## Directory Purposes

**`core/src/main/java/io/questdb/griffin/`:**
- Purpose: SQL engine -- parsing, optimization, code generation
- Key files for fill work:
  - `SqlOptimiser.java` (11,555 lines): `rewriteSampleBy()` at line ~7842
  - `SqlCodeGenerator.java` (10,423 lines): `generateSampleBy()` at line ~6776, `generateFill()` at line ~3225, `generateSelectGroupBy()` at line ~7526

**`core/src/main/java/io/questdb/griffin/engine/groupby/`:**
- Purpose: All GROUP BY and SAMPLE BY execution infrastructure
- Contains: Cursor factories, cursor implementations, timestamp samplers, group-by utilities
- Key files: `SampleByFillRecordCursorFactory.java` (719 lines, new), `FillRangeRecordCursorFactory.java` (723 lines)

**`core/src/main/java/io/questdb/griffin/model/`:**
- Purpose: Query model AST nodes
- Key file: `QueryModel.java` -- stores `sampleBy*` fields (parsed) and `fill*` fields (rewritten)

**`core/src/test/java/io/questdb/test/griffin/engine/groupby/`:**
- Purpose: Test suites for GROUP BY and SAMPLE BY
- Key file: `SampleByTest.java` (18,163 lines) -- comprehensive SAMPLE BY test coverage
- Key file: `SampleByFillTest.java` (72 lines) -- new fill cursor tests (work in progress)

## Key File Locations

**Entry Points:**
- `core/src/main/java/io/questdb/griffin/SqlOptimiser.java`: Optimizer rewrite entry (`rewriteSampleBy()`)
- `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java`: Code generator entry (`generateSelectGroupBy()`)

**Configuration:**
- `core/src/main/java/io/questdb/PropertyKey.java`: `CAIRO_SQL_SAMPLEBY_DEFAULT_ALIGNMENT_CALENDAR` key
- `core/src/main/java/io/questdb/cairo/CairoConfiguration.java`: `getSampleByDefaultAlignmentCalendar()`, `isValidateSampleByFillType()`

**Core Fill Logic:**
- `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java`: New unified fill cursor (fast path)
- `core/src/main/java/io/questdb/griffin/engine/groupby/FillRangeRecordCursorFactory.java`: Existing two-pass fill cursor (fast path, NULL/value only)
- `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillPrevRecordCursor.java`: PREV fill cursor (slow path, keyed)
- `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillPrevNotKeyedRecordCursor.java`: PREV fill cursor (slow path, non-keyed)

**Timestamp Bucketing:**
- `core/src/main/java/io/questdb/griffin/engine/groupby/TimestampSampler.java`: Interface
- `core/src/main/java/io/questdb/griffin/engine/groupby/SimpleTimestampSampler.java`: Fixed-interval implementation
- `core/src/main/java/io/questdb/griffin/engine/groupby/TimestampSamplerFactory.java`: Factory

**Testing:**
- `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByTest.java`: Main test suite
- `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java`: New fill cursor tests
- `core/src/test/java/io/questdb/test/griffin/SampleBySqlParserTest.java`: Parser tests

## Naming Conventions

**Files:**
- Factory pattern: `SampleByFill{Mode}{Keyed?}RecordCursorFactory.java`
  - Mode: `Prev`, `Null`, `Value`, `None`, `Interpolate`
  - Keyed: omitted for keyed, `NotKeyed` suffix for non-keyed
  - Examples: `SampleByFillPrevRecordCursorFactory.java` (keyed PREV), `SampleByFillNullNotKeyedRecordCursorFactory.java` (non-keyed NULL)
- Cursor pattern: `SampleByFill{Mode}{Keyed?}RecordCursor.java`
- Abstract bases: `Abstract{purpose}*.java`
- New unified cursor: `SampleByFillRecordCursorFactory.java` (no mode suffix -- handles all modes)

**Directories:**
- Production code: `core/src/main/java/io/questdb/griffin/engine/groupby/`
- Test code: `core/src/test/java/io/questdb/test/griffin/engine/groupby/`

## Where to Add New Code

**New fill mode or fill cursor enhancement:**
- Implementation: `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java` (extend `SampleByFillCursor`)
- Tests: `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java` (new tests) and `SampleByTest.java` (regression tests)

**New optimizer rewrite rule:**
- Implementation: `core/src/main/java/io/questdb/griffin/SqlOptimiser.java`, modify `rewriteSampleBy()` eligibility conditions (line ~7875)

**New code generator path:**
- Implementation: `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java`, modify `generateFill()` (line ~3225) or `generateSampleBy()` (line ~6776)

**New timestamp sampler (new interval unit):**
- Implementation: `core/src/main/java/io/questdb/griffin/engine/groupby/` -- new class implementing `TimestampSampler`
- Factory: `core/src/main/java/io/questdb/griffin/engine/groupby/TimestampSamplerFactory.java`
- Tests: `core/src/test/java/io/questdb/test/griffin/engine/groupby/` -- new test class

**Extending fill for keyed queries on the fast path:**
- Primary cursor: `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java` -- the `isNonKeyed = true` flag and associated `SampleByFillCursor.hasNext()` logic
- Guard removal: `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java` -- `guardAgainstFillWithKeyedGroupBy()` (line ~9880)
- Optimizer gate: `core/src/main/java/io/questdb/griffin/SqlOptimiser.java` -- keyed bail-out at line ~8001

## Special Directories

**`core/src/main/java/io/questdb/griffin/engine/groupby/vect/`:**
- Purpose: Vectorized aggregate function implementations (SIMD-accelerated)
- Generated: No
- Committed: Yes
- Relevance: Used by the parallel GROUP BY path that the fill cursor wraps

**`core/src/main/java/io/questdb/griffin/engine/groupby/hyperloglog/`:**
- Purpose: HyperLogLog implementation for count_distinct
- Generated: No
- Committed: Yes

---

*Structure analysis: 2026-04-09*
