# Testing Patterns

**Analysis Date:** 2026-04-09

## Test Framework

**Runner:**
- JUnit 4 (`org.junit.Test`, `org.junit.Assert`)
- Config: Maven Surefire plugin in `core/pom.xml`

**Assertion Library:**
- JUnit 4 `Assert.*`
- QuestDB `TestUtils.assertEquals()`, `TestUtils.assertContains()`
- Custom assertion methods on `AbstractCairoTest`

**Run Commands:**
```bash
mvn test                                    # Run all tests
mvn -Dtest=SampleByTest test                # Run a specific test class
mvn -Dtest=SampleByTest#testBadFunction test # Run a specific test method
mvn -Dtest=SampleByFillTest test            # Run the new fill test class
```

Do not run multiple `mvn test` commands in parallel -- they interfere with each other.

## Test File Organization

**Location:** Co-located by package mirror under `core/src/test/java/io/questdb/test/`

**Key test files for SAMPLE BY fill:**
- `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByTest.java` — 18,163 lines, 302 `@Test` methods, the main SAMPLE BY test file
- `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java` — 72 lines, 3 `@Test` methods, the new test file for GROUP BY fast path fill
- `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByConfigTest.java` — configuration tests
- `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByNanoTimestampTest.java` — nanosecond timestamp tests
- `core/src/test/java/io/questdb/test/griffin/SampleBySqlParserTest.java` — parser-level tests

**Base class:**
- `core/src/test/java/io/questdb/test/AbstractCairoTest.java` — provides all assertion helpers, `engine`, `sqlExecutionContext`, `sink`, memory leak detection

## Test Structure

**All tests must use `assertMemoryLeak()`** (unless narrow unit tests that do not allocate native memory):
```java
@Test
public void testFillNullNonKeyed() throws Exception {
    assertMemoryLeak(() -> {
        execute("CREATE TABLE x AS (...)");
        assertSql(
                """
                        sum\tts
                        1.0\t2024-01-01T00:00:00.000000Z
                        """,
                "SELECT sum(val), ts FROM x SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR"
        );
    });
}
```

**Key assertion methods used in SampleByTest (by frequency):**

| Method | Count | Purpose |
|--------|-------|---------|
| `assertQuery(...)` | 162 | Full assertion: creates table, runs query, checks result + factory properties (supportsRandomAccess, expectSize, expectedTimestamp). Wraps in `assertMemoryLeak`. |
| `assertMemoryLeak(...)` | 103 | Memory leak detection wrapper |
| `assertQueryNoLeakCheck(...)` | 73 | Like `assertQuery` but without memory leak check (used inside already-wrapped blocks) |
| `assertSql(...)` | 42 | Data-only assertion. No factory property checks. For storage tests or when factory properties are irrelevant. |
| `assertException(...)` | 33 | Verifies SqlException with expected position and message fragment |

## Assertion Method Signatures

### assertQuery (preferred for query correctness)

Creates table, runs query, validates result + factory properties. Wraps in `assertMemoryLeak()` internally.

```java
// Most common form: expected, query, ddl, expectedTimestamp, supportsRandomAccess
assertQuery(
        """
                s\tk\tfirst
                TJW\t1970-01-03T00:00:00.000000Z\t010
                """,
        "select s, k, first(g1) from x sample by 30m fill(NULL)",
        "create table x as (...) timestamp(k) partition by NONE",
        "k",
        false   // supportsRandomAccess
);

// With insert-then-recheck (ddl2 + expected2):
assertQuery(expected1, query, ddl, "k", ddl2, expected2, true, true, false);
```

### assertQueryNoLeakCheck (inside assertMemoryLeak blocks)

Same as `assertQuery` but does not wrap in `assertMemoryLeak`. Use when you already have a `assertMemoryLeak` wrapper:

```java
assertMemoryLeak(() -> {
    execute("create table trades as (...)");
    assertQueryNoLeakCheck(
            """
                    ts\tsymbol\tswitch
                    1970-01-03T00:00:00.000000Z\tBTC-USD\t101.0
                    """,
            "SELECT ts, symbol, ... FROM trades SAMPLE BY 1h",
            "ts",
            true,   // supportsRandomAccess
            true    // expectSize
    );
});
```

### assertSql (data-only, no factory properties)

Use for tests that only verify data correctness. Does not check `supportsRandomAccess`, `expectSize`, or `expectedTimestamp`.

```java
assertSql(
        """
                sum\tts
                1.0\t2024-01-01T00:00:00.000000Z
                null\t2024-01-01T01:00:00.000000Z
                2.0\t2024-01-01T02:00:00.000000Z
                """,
        "SELECT sum(val), ts FROM x SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR"
);
```
Source: `core/src/test/java/io/questdb/test/AbstractCairoTest.java` line 2120

### assertException (error position + message)

Verifies that a query throws `SqlException` at a specific character position with a specific message fragment:

```java
// Two-arg form (no DDL, table already exists):
assertException(
        "select b, sum(a), sum(c), k from x sample by 3h fill(20.56)",
        22,
        "Invalid column: c"
);

// Four-arg form (creates table first):
assertException(
        "select b, sum(a), sum(c), k from x sample by 3h fill(20.56)",
        "create table x as (...) timestamp(k) partition by NONE",
        22,
        "Invalid column: c"
);
```

### assertPlanNoLeakCheck (EXPLAIN output)

Validates the query plan output from EXPLAIN:
```java
assertPlanNoLeakCheck(query, expectedPlan);
```

## DDL and Data Setup

### execute() for DDL

Use `execute()` for all non-query statements (CREATE TABLE, INSERT, ALTER):
```java
execute("CREATE TABLE x AS (" +
        "SELECT x::DOUBLE AS val, timestamp_sequence('2024-01-01', 7_200_000_000) AS ts " +
        "FROM long_sequence(3)) TIMESTAMP(ts) PARTITION BY DAY");
```

### Standard data generation patterns

**timestamp_sequence + long_sequence:**
```java
// Creates N rows with sequential timestamps
"CREATE TABLE x AS (" +
"SELECT x::DOUBLE AS val, " +
"       timestamp_sequence('2024-01-01', 7_200_000_000) AS ts " +
"FROM long_sequence(3)) TIMESTAMP(ts) PARTITION BY DAY"
```

**With symbol keys:**
```java
"CREATE TABLE x AS (" +
"SELECT x::DOUBLE AS val, " +
"       ('k' || (x % 2))::SYMBOL AS key, " +
"       timestamp_sequence('2024-01-01', 3_600_000_000) AS ts " +
"FROM long_sequence(4)) TIMESTAMP(ts) PARTITION BY DAY"
```

**Random data with rnd_* functions:**
```java
"create table x as (" +
"select" +
" rnd_double(0)*100 a," +
" rnd_symbol(5,4,4,1) b," +
" timestamp_sequence(172800000000, 3600000000) k" +
" from long_sequence(20)" +
") timestamp(k) partition by NONE"
```

**With all column types:**
```java
"select" +
" rnd_float(0)*100 a," +
" rnd_symbol(5,4,4,1) b," +
" rnd_double(0)*100 c," +
" abs(rnd_int()) d," +
" rnd_byte(2, 50) e," +
" abs(rnd_short()) f," +
" abs(rnd_long()) g," +
" timestamp_sequence(172800000000, 3600000000) k" +
" from long_sequence(20)"
```

### Static DDL constants

Large reusable DDL can be stored as static string constants:
```java
public static final String FROM_TO_DDL = """
        create table fromto as (
          SELECT timestamp_sequence(
                    to_timestamp('2018-01-01T00:00:00', 'yyyy-MM-ddTHH:mm:ss'),
                    1800000000L) as ts, \
        x, \
        ...
        FROM long_sequence(480)
        ) timestamp(ts)
        """;
```
Source: `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByTest.java` lines 81-101

## SQL Conventions in Tests

**UPPERCASE SQL keywords:**
```java
"SELECT sum(val), ts FROM x SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR"
"CREATE TABLE x AS (...) TIMESTAMP(ts) PARTITION BY DAY"
"INSERT INTO trades VALUES (...)"
```

**Multiline strings** for longer queries, expected results, and multi-row INSERTs:
```java
assertSql(
        """
                sum\tts
                1.0\t2024-01-01T00:00:00.000000Z
                null\t2024-01-01T01:00:00.000000Z
                """,
        "SELECT sum(val), ts FROM x SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR"
);
```

**Single INSERT for multiple rows:**
```java
execute("INSERT INTO trades VALUES" +
        "('BTC', 'buy',  100.0, 10.0, '2026-03-31T02:00:00.000000Z')," +
        "('BTC', 'buy',  101.0, 20.0, '2026-03-31T02:30:00.000000Z')," +
        "('BTC', 'sell', 102.0, 15.0, '2026-03-31T03:15:00.000000Z')");
```

**Underscores in numbers >= 5 digits:**
```java
timestamp_sequence('2024-01-01', 7_200_000_000)
long_sequence(100_000)
```

**Use `expr::TYPE` for casts** (not `CAST(expr, type)`):
```java
"SELECT x::DOUBLE AS val, ('k' || (x % 2))::SYMBOL AS key ..."
```

## Result Format

Test results use tab-separated columns with `\t`. The first line is the header. Timestamps use `YYYY-MM-DDTHH:MM:SS.ffffffZ` format:
```
sum\tts
1.0\t2024-01-01T00:00:00.000000Z
null\t2024-01-01T01:00:00.000000Z
2.0\t2024-01-01T02:00:00.000000Z
```

## Cached Plan Reuse Testing

Test that `getCursor()` called multiple times on the same factory returns consistent results:
```java
RecordCursorFactory factory = select(query);
try {
    String firstResult = null;
    for (int i = 0; i < 5; i++) {
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            StringSink s = new StringSink();
            CursorPrinter.println(factory.getMetadata(), s);
            CursorPrinter.println(cursor, factory.getMetadata(), s);
            if (firstResult == null) {
                firstResult = s.toString();
            } else {
                TestUtils.assertEquals("execution #" + (i + 1), firstResult, s.toString());
            }
        }
    }
} finally {
    Misc.free(factory);
}
```
Source: `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByTest.java` lines 391-408

## Debug Helpers

**printSql for exploratory debugging** (not for final tests):
```java
printSql("EXPLAIN SELECT ts, key, sum(val) FROM x SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR");
System.out.println("=== PLAN ===");
System.out.println(sink);
```

**Randomized copier type:**
```java
Rnd rnd = TestUtils.generateRandom(LOG);
setProperty(PropertyKey.DEBUG_CAIRO_COPIER_TYPE, rnd.nextInt(4));
```

## Test Types in SampleByTest

**Query correctness tests** (majority): Set up a table, run a SAMPLE BY query, assert exact output. Cover:
- All fill modes: NULL, PREV, LINEAR, VALUE (constant), NONE
- Keyed (with SYMBOL columns) and non-keyed queries
- Various stride intervals: minutes, hours, days
- ALIGN TO CALENDAR, TIME ZONE, WITH OFFSET
- FROM / TO range boundaries
- All column types: double, float, int, long, short, byte, boolean, char, geohash, decimal, IPv4, Long256, varchar, string, symbol, binary, array

**Error tests:** Verify `SqlException` with correct error position for invalid queries (bad interval, bad timezone, invalid fill values, etc.)

**Plan tests:** Verify EXPLAIN output matches expected plan shape (e.g., pushdown into SAMPLE BY)

**Concurrent tests:** Verify SAMPLE BY correctness under concurrent readers/writers using `WorkerPool` and `CyclicBarrier`

## SampleByFillTest Structure

The new `SampleByFillTest` class (`core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java`) tests the GROUP BY fast path fill implementation. Tests use `assertSql()` (not `assertQuery`) because the fast path cursor does not support random access and factory properties are irrelevant.

**Current test methods:**
- `testFillNullNonKeyed()` — non-keyed FILL(NULL)
- `testFillPrevNonKeyed()` — non-keyed FILL(PREV)
- `testFillNullKeyed()` — keyed FILL(NULL) (currently uses printSql for debugging)

**Pattern for new fill tests:**
```java
@Test
public void testFillNullNonKeyed() throws Exception {
    assertMemoryLeak(() -> {
        execute("CREATE TABLE x AS (" +
                "SELECT x::DOUBLE AS val, timestamp_sequence('2024-01-01', 7_200_000_000) AS ts " +
                "FROM long_sequence(3)) TIMESTAMP(ts) PARTITION BY DAY");
        // Data at 00:00, 02:00, 04:00 -- gaps at 01:00, 03:00
        assertSql(
                """
                        sum\tts
                        1.0\t2024-01-01T00:00:00.000000Z
                        null\t2024-01-01T01:00:00.000000Z
                        2.0\t2024-01-01T02:00:00.000000Z
                        null\t2024-01-01T03:00:00.000000Z
                        3.0\t2024-01-01T04:00:00.000000Z
                        """,
                "SELECT sum(val), ts FROM x SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR"
        );
    });
}
```

## Test Scenarios to Cover for Fill Implementation

Based on patterns in `SampleByTest.java`, new fill tests should cover:

**Fill modes:**
- FILL(NULL) — gap rows get null values
- FILL(PREV) — gap rows repeat last seen values
- FILL(constant) — gap rows get specified constant values (e.g., `FILL(0, 0, 0)`)
- Mixed fills per column: `FILL(NULL, PREV, 42.0)`

**Query shapes:**
- Non-keyed: `SELECT agg(col), ts FROM x SAMPLE BY ...`
- Keyed: `SELECT ts, key, agg(col) FROM x SAMPLE BY ...` (cartesian product of keys x buckets)
- FROM/TO range: `SAMPLE BY 1h FROM '...' TO '...'`
- ALIGN TO CALENDAR, TIME ZONE, WITH OFFSET
- DST transitions (e.g., 'Europe/Berlin', 'America/Anchorage')

**Column types:**
- DOUBLE, FLOAT, INT, LONG, SHORT, BYTE, BOOLEAN, CHAR
- GEOHASH (3-bit, 15-bit, 30-bit, 40-bit)
- DECIMAL (8, 16, 32, 64, 128, 256)
- IPv4, Long256, STRING, VARCHAR, SYMBOL

**Edge cases:**
- Empty table
- All rows in same bucket (no gaps)
- All buckets are gaps (FROM/TO with no data)
- Single row
- Cached plan reuse (getCursor called multiple times)

---

*Testing analysis: 2026-04-09*
