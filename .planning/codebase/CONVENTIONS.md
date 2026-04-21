# Coding Conventions

**Analysis Date:** 2026-04-09

## Zero-GC on Data Paths

All query execution and data ingestion paths must avoid heap allocations. Use object pools, pre-allocated buffers, and primitive arrays.

**Rules:**
- Use `long[]`, `short[]`, `boolean[]` flat arrays for per-column state, not boxed types or `ArrayList`
- Reuse cursor and record objects across `getCursor()` calls (see `SampleByFillCursor` pattern in `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java`)
- Use `IntList` and `ObjList<T>` instead of `java.util.List` or raw arrays for object collections
- Store typed values as raw long bits via `Double.doubleToRawLongBits()` / `Float.floatToRawIntBits()` for uniform primitive storage (see `readColumnAsLongBits()` in `SampleByFillRecordCursorFactory.java` lines 399-410)

**Example — flat primitive array for per-key state:**
```java
private long[] simplePrev;  // simplePrev[col] stores raw bits
private boolean hasSimplePrev;
```

## No Third-Party Java Dependencies

All algorithms and data structures are implemented from first principles. Use QuestDB's `io.questdb.std.*` collections, not `java.util.*` equivalents.

**Use instead:**
- `io.questdb.std.ObjList<T>` instead of `T[]` or `ArrayList<T>`
- `io.questdb.std.IntList` instead of `int[]` or `ArrayList<Integer>`
- `io.questdb.std.Numbers` for null sentinel constants
- `io.questdb.std.Misc` for resource cleanup

## Member Ordering

Sort Java class members by kind (static vs. instance) and visibility, then alphabetically within each group. Do not insert comment section headings between members.

**Order:**
1. Static constants (public, then package-private, then private)
2. Static methods (public, then package-private, then private)
3. Instance fields (public, then package-private, then private)
4. Constructors
5. Instance methods (public, then package-private, then private)
6. Inner classes

## Modern Java Features

**Enhanced switch:**
```java
return switch (type) {
    case ColumnType.DOUBLE -> Double.doubleToRawLongBits(record.getDouble(col));
    case ColumnType.FLOAT -> Float.floatToRawIntBits(record.getFloat(col));
    case ColumnType.INT, ColumnType.IPv4, ColumnType.GEOINT -> record.getInt(col);
    default -> record.getLong(col);
};
```
Source: `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java` lines 400-410

**Multiline strings:**
```java
String expected = """
        sum\tts
        1.0\t2024-01-01T00:00:00.000000Z
        null\t2024-01-01T01:00:00.000000Z
        """;
```

**Pattern variables in instanceof:**
```java
if (e instanceof FlyweightMessageContainer fmc) {
    TestUtils.assertContains(fmc.getFlyweightMessage(), contains);
}
```

## Naming Conventions

**Booleans:** Always use `is...` or `has...` prefix:
```java
private boolean isInitialized;
private boolean isNonKeyed;
private boolean isOpen;
private boolean isGapFilling;
private boolean hasPendingRow;
private boolean hasSimplePrev;
private boolean hasExplicitTo;
private boolean hasPrevFill;
private boolean isMapBuildPending;
private boolean isMapInitialized;
```

**Files:** Class name matches file name. CursorFactory classes end in `RecordCursorFactory`. Cursor classes end in `RecordCursor`.

**Methods:** camelCase. Accessors use `get` prefix. Factory methods use `of()` for initialization.

## ObjList and IntList

Use `ObjList<T>` instead of `T[]` object arrays. `ObjList` integrates with `Misc.freeObjList()` / `Misc.freeObjListIfCloseable()` for resource cleanup.

```java
// Declaration
private final ObjList<Function> constantFills;

// Access
constantFills.getQuick(col).getDouble(null);

// Cleanup in _close()
Misc.freeObjList(constantFills);
```

Use `IntList` for integer arrays:
```java
private final IntList fillModes;

// Access
fillModes.getQuick(col);
```

## Resource Cleanup with Misc

**`Misc.free(T)`** — closes a single `Closeable` and returns `null`:
```java
baseCursor = Misc.free(baseCursor);
```

**`Misc.freeObjList(ObjList<T>)`** — closes all elements in an `ObjList`:
```java
Misc.freeObjList(constantFills);
```

**`Misc.freeObjListIfCloseable(ObjList<T>)`** — closes elements only if they implement `Closeable`.

**Pattern — factory `_close()`:**
```java
@Override
protected void _close() {
    base.close();
    Misc.free(fromFunc);
    Misc.free(toFunc);
    Misc.freeObjList(constantFills);
}
```
Source: `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java` lines 160-165

**Pattern — try/catch cleanup in constructor:**
```java
Map map = null;
try {
    map = MapFactory.createOrderedMap(configuration, keyTypes, valueTypes);
    // ... use map ...
} catch (Throwable th) {
    Misc.free(map);
    close();
    throw th;
}
```
Source: `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillNoneRecordCursorFactory.java` lines 74-104

**Pattern — cursor `close()` with open guard:**
```java
@Override
public void close() {
    if (isOpen) {
        map.close();
        super.close();
        isOpen = false;
    }
}
```
Source: `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillPrevRecordCursor.java` lines 93-99

**Pattern — getCursor error path:**
```java
@Override
public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
    final RecordCursor baseCursor = base.getCursor(executionContext);
    try {
        cursor.of(baseCursor, executionContext);
        return cursor;
    } catch (Throwable th) {
        cursor.close();
        throw th;
    }
}
```
Source: `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java` lines 119-128

## Null Sentinel Values

Use `Numbers.*_NULL` constants, never Java `null` for primitive-like columns:

| Type | Null sentinel | Source |
|------|--------------|--------|
| `int` | `Numbers.INT_NULL` (`Integer.MIN_VALUE`) | `core/src/main/java/io/questdb/std/Numbers.java` |
| `long` | `Numbers.LONG_NULL` (`Long.MIN_VALUE`) | same |
| `double` | `Double.NaN` | — |
| `float` | `Float.NaN` | — |
| `char` | `Numbers.CHAR_NULL` | same |
| `IPv4` | `Numbers.IPv4_NULL` (`0`) | same |
| `Long256` | `Long256Impl.NULL_LONG256` | `core/src/main/java/io/questdb/std/Long256Impl.java` |

Always distinguish "not initialized yet" (sentinel for unset state) from "actual NULL value from data." Use separate boolean flags (e.g., `hasSimplePrev`) to track initialization.

## RecordSink Pattern

`RecordSink` copies a `Record`'s columns to a map key via `copy(Record, RecordSinkSPI)`. Instances have mutable fields and must not be shared across workers. Created via:
```java
RecordSink mapSink = RecordSinkFactory.getInstance(configuration, asm, base.getMetadata(), listColumnFilter);
```
Source: `core/src/main/java/io/questdb/cairo/RecordSink.java`

## Map Usage Pattern

QuestDB's `Map` (`io.questdb.cairo.map.Map`) is a custom hash map implementation used for GROUP BY and SAMPLE BY aggregation. It stores key/value pairs with typed access.

**Creation:**
```java
Map map = MapFactory.createOrderedMap(configuration, keyTypes, valueTypes);
```

**Key operations:**
- `MapKey withKey()` — get a key builder
- `MapValue createValue()` / `findValue()` — create or find a value
- `MapRecordCursor getCursor()` — iterate results
- `MapRecord getRecord()` — get the record proxy

**Lifecycle:** Always close via `Misc.free(map)` or in `_close()`.

Source: `core/src/main/java/io/questdb/cairo/map/Map.java`

## EntityColumnFilter

A simple `ColumnFilter` that maps position `i` to column index `i + 1`. Used when all columns participate in a key:
```java
EntityColumnFilter filter = new EntityColumnFilter();
filter.of(columnCount);
```
Source: `core/src/main/java/io/questdb/cairo/EntityColumnFilter.java`

## RecordCursorFactory Hierarchy for SAMPLE BY

The SAMPLE BY fill implementations follow this class hierarchy:

**Legacy (map-based, keyed):**
- `AbstractRecordCursorFactory` -> `AbstractSampleByRecordCursorFactory` -> `AbstractSampleByFillRecordCursorFactory`
  - `SampleByFillNoneRecordCursorFactory` — `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillNoneRecordCursorFactory.java`
  - `SampleByFillPrevRecordCursorFactory` — `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillPrevRecordCursorFactory.java`
  - `SampleByFillNullRecordCursorFactory` — `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillNullRecordCursorFactory.java`
  - `SampleByFillValueRecordCursorFactory` — `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillValueRecordCursorFactory.java`

**New (GROUP BY fast path, streaming):**
- `AbstractRecordCursorFactory` -> `SampleByFillRecordCursorFactory` — `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java`
  - Contains inner `SampleByFillCursor` implementing `NoRandomAccessRecordCursor`
  - Contains inner `FillRecord` implementing `Record`
  - Contains inner `FillTimestampConstant` extending `TimestampFunction`

**Key difference:** The new factory wraps a pre-sorted GROUP BY result (the "fast path"), while the legacy factories build their own maps from raw table data.

## Function.init Pattern

Before using functions with a cursor, always call `Function.init()`:
```java
Function.init(constantFills, baseCursor, executionContext, null);
fromFunc.init(baseCursor, executionContext);
toFunc.init(baseCursor, executionContext);
```
Source: `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java` lines 385-387

## toPlan Pattern

Factories implement `toPlan(PlanSink)` for EXPLAIN output:
```java
@Override
public void toPlan(PlanSink sink) {
    sink.type("Sample By Fill");
    sink.attr("stride").val('\'').val(samplingInterval).val(samplingIntervalUnit).val('\'');
    if (hasPrevFill) {
        sink.attr("fill").val("prev");
    }
    sink.child(base);
}
```

## Error Position Convention

`SqlException.$(position, msg)` — the position must point at the specific offending character, not the start of the expression.

## Active Voice in Comments and Documentation

Use active voice naming the acting subject:
- Good: "`savePrevValues()` stores the current row's column values as prev state"
- Avoid: "The current row's column values are stored as prev state"

## Underscores in Numbers

Use underscores as thousands separators in numbers with 5+ digits:
```java
timestamp_sequence('2024-01-01', 7_200_000_000)
long_sequence(100_000)
```

---

*Convention analysis: 2026-04-09*
