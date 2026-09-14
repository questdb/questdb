# Testing Patterns

**Analysis Date:** 2026-04-13

## Test Framework

**Runner:**
- JUnit 4 (version 4.13.2)
- Config: `core/pom.xml` with maven-surefire-plugin 3.5.3

**Assertion Library:**
- JUnit Assert class methods (`Assert.assertEquals()`, `Assert.assertTrue()`, etc.)

**Run Commands:**
```bash
mvn test                              # Run all tests
mvn -Dtest=ClassNameTest test         # Run specific test class
mvn -Dtest=ClassNameTest#methodName test  # Run specific test method
```

**Important:** Do not run multiple `mvn test` commands in parallel — each invocation triggers a full build and they interfere with each other.

## Test File Organization

**Location:**
- Tests co-located with code under `core/src/test/java/io/questdb/test/` following same package structure
- SQL/query tests primarily in `core/src/test/java/io/questdb/test/griffin/`
- Cairo storage tests in `core/src/test/java/io/questdb/test/cairo/`

**Naming:**
- Test classes: `[TopicName]Test.java`
- Examples: `InsertNullTest.java`, `GroupByFunctionTest.java`, `GroupByTest.java`
- Test methods: `test[DescriptionOfTest]()`

**Structure:**
```
core/src/test/java/io/questdb/test/
├── griffin/
│   ├── InsertNullTest.java
│   ├── GroupByFunctionTest.java
│   ├── GroupByTest.java
│   └── ...
├── cairo/
│   └── ...
└── AbstractCairoTest.java           # Base class for all Cairo-related tests
```

## Test Base Classes

**AbstractCairoTest:**
- Location: `core/src/test/java/io/questdb/test/AbstractCairoTest.java`
- All query and function tests extend this class
- Provides setUp() and tearDown() lifecycle hooks
- Manages test engine, SQL compiler, execution context

Example:
```java
public class InsertNullTest extends AbstractCairoTest {
    @Test
    public void testInsertNull() throws Exception {
        // Test body
    }
}
```

## Test Structure

**Test Method Pattern:**
```java
@Test
public void testInsertNull() throws Exception {
    // Setup/creation phase
    assertQuery(
        "expected_result_header",
        "query_sql",
        "create_table_ddl",
        expectedTimestampColumn,  // null if no expected timestamp
        create_additional_ddl,     // null if not needed
        expected_result_2,         // null if not needed
        supportsRandomAccess,      // boolean
        expectSize,               // boolean
        sizeCanBeVariable         // boolean
    );
}
```

**Lifecycle with Resource Cleanup:**
```java
@Test
public void testSomething() throws Exception {
    for (int i = 0; i < types.length; i++) {
        if (i > 0) {
            setUp();  // Reset for each iteration
        }
        try {
            assertQuery(...);
        } finally {
            tearDown();  // Always clean up
        }
    }
}
```

Example from `InsertNullTest.java`:
```java
@Test
public void testInsertNull() throws Exception {
    for (int i = 0; i < TYPES.length; i++) {
        if (i > 0) {
            setUp();
        }
        try {
            final String[] type = TYPES[i];
            assertQuery(
                    "value\n",
                    "x",
                    String.format("create table x (value %s)", type[0]),
                    null,
                    String.format("insert into x select null from long_sequence(%d)", NULL_INSERTS),
                    expectedNullInserts("value\n", type[1], NULL_INSERTS, true),
                    true,
                    true,
                    false
            );
        } finally {
            tearDown();
        }
    }
}
```

## Test Assertions

**assertMemoryLeak():**
- Wraps test code to verify no native memory leaks
- Required for all tests that allocate native memory (TableWriter, TableReader, etc.)
- Takes lambda/callable parameter: `assertMemoryLeak(() -> { /* test code */ })`
- Automatically checks for memory leaks in finally blocks
- Good for narrow unit tests that don't allocate native memory to skip this

Example from `InsertNullTest.java`:
```java
private void _testInsertNullFromSelectOnDesignatedColumnMustFail(String timestampType) throws Exception {
    assertMemoryLeak(() -> {
        try {
            assertQuery(
                    "sym\ty\n",
                    "xx",
                    "create table xx (sym symbol, y #TIMESTAMP_TYPE) timestamp(y)".replace("#TIMESTAMP_TYPE", timestampType),
                    "y",
                    "insert into xx select 'AA', null from long_sequence(1)",
                    "y\n",
                    true,
                    false,
                    false
            );
            Assert.fail();
        } catch (CairoException expected) {
            Assert.assertTrue(expected.getMessage().contains("designated timestamp column cannot be NULL"));
        }
    });
}
```

**assertQueryNoLeakCheck():**
- Asserts query results WITHOUT memory leak checking
- Verifies factory properties: `supportsRandomAccess`, `expectSize`, `expectedTimestamp`
- Use this for all query tests that involve function evaluation
- More thorough than assertSql() because it checks factory metadata

Signature:
```java
protected static void assertQueryNoLeakCheck(
    CharSequence expected,           // Expected query result as string
    CharSequence query,              // SQL query to execute
    CharSequence ddl,                // CREATE TABLE statement (null if table exists)
    @Nullable CharSequence expectedTimestamp,  // Expected timestamp column name or null
    @Nullable CharSequence ddl2,     // Optional second CREATE TABLE
    @Nullable CharSequence expected2, // Optional second expected result
    boolean supportsRandomAccess,    // Should factory support random access?
    boolean expectSize,              // Should factory report expected size?
    boolean sizeCanBeVariable        // Can expected size vary?
)
```

Example from `GroupByFunctionTest.java`:
```java
assertQuery(
    """
    y_utc_15m\ty_sf_position_mw
    1970-01-01T00:00:00.000000Z\t-0.2246301342497259
    ...""",
    """
    SELECT
        delivery_start_utc as y_utc_15m,
        sum(case
                when seller='sf' then -1.0*volume_mw
                when buyer='sf' then 1.0*volume_mw
                else 0.0
            end)
        as y_sf_position_mw
    FROM (
        SELECT delivery_start_utc, seller, buyer, volume_mw FROM trades
        WHERE (seller = 'sf' OR buyer = 'sf')
        )
    group by y_utc_15m
    order by y_utc_15m""",
    "create table trades as (" +
    "select" +
    " timestamp_sequence(0, 15*60*1000000L) delivery_start_utc," +
    " rnd_symbol('sf', null) seller," +
    " rnd_symbol('sf', null) buyer," +
    " rnd_double() volume_mw" +
    " from long_sequence(100)" +
    "), index(seller), index(buyer) timestamp(delivery_start_utc)",
    "y_utc_15m",
    true,
    true
);
```

**assertSql():**
- Minimal assertion for data persistence in storage tests
- Does NOT check factory properties
- Use only for cairo storage tests that verify data correctness without caring about factory metadata
- Listed in CLAUDE.md as option for storage tests only

**assertQuery():**
- Wraps assertQueryNoLeakCheck() inside assertMemoryLeak()
- Calls assertMemoryLeak() then assertQueryNoLeakCheck() with same parameters
- Use this when you want both leak checking and query result verification

**execute():**
- Runs non-query DDL statements (CREATE TABLE, ALTER TABLE, INSERT, etc.)
- Example: `execute("CREATE TABLE test (col INT)")`

## Error Testing

**Pattern for Expected Exceptions:**
```java
try {
    // Code expected to throw
    assertQuery(...);
    Assert.fail("Should have thrown exception");
} catch (CairoException expected) {
    Assert.assertTrue(expected.getMessage().contains("expected error text"));
} catch (SqlException expected) {
    Assert.assertEquals("[position] error message", expected.getMessage());
}
```

Full example from `InsertNullTest.java`:
```java
@Test
public void testInsertNullFromValuesOnDesignatedColumnMustFail() throws Exception {
    assertMemoryLeak(() -> {
        try {
            assertQuery(
                    "sym\ty\n",
                    "xx",
                    "create table xx (sym symbol, y timestamp) timestamp(y)",
                    "y",
                    "insert into xx values('AA', null)",
                    "y\n",
                    true,
                    false,
                    false
            );
            Assert.fail();
        } catch (SqlException expected) {
            Assert.assertEquals("[28] designated timestamp column cannot be NULL", expected.getMessage());
        }
    });
}
```

## Test Data Generation

**SQL Functions for Data:**
- `long_sequence(count)` - generates count rows with sequential `x` column
- `rnd_double()` - random double value
- `rnd_symbol(value1, value2, ...)` - random symbol from list
- `timestamp_sequence(start, increment)` - generates timestamp sequence
- Examples from `GroupByFunctionTest.java`:
  ```sql
  timestamp_sequence(0, 15*60*1000000L)  -- Start epoch, increment 15 minutes in micros
  rnd_symbol('sf', null)                  -- Returns 'sf' or null
  rnd_double()                            -- Random double in [0, 1)
  ```

**Test Helper Methods:**
```java
static String expectedNullInserts(String header, String nullValue, int count, boolean expectsOutput) {
    StringSink sb = Misc.getThreadLocalSink();
    sb.put(header);
    if (expectsOutput) {
        for (int i = 0; i < count; i++) {
            sb.put(nullValue).put("\n");
        }
    }
    return sb.toString();
}
```

## NULL Value Testing

**Handling NULL in Different Types:**
```java
// From InsertNullTest.java - type mappings
private static final String[][] TYPES = {
    // type name, null representation
    {"boolean", "false"},     // Cannot be null, defaults to false
    {"byte", "0"},           // Cannot be null, defaults to 0
    {"short", "0"},          // Cannot be null, defaults to 0
    {"int", "null"},         // Can be null
    {"long", "null"},        // Can be null
    {"float", "null"},       // Can be null
    {"double", "null"},      // Can be null
    {"string", ""},          // String null displays as empty line
    {"symbol", ""},          // Symbol null displays as empty line
    // ... more types
};
```

**NULL Behavior Assertions:**
```java
// Test IS NULL filter
assertQuery(
    "value\n",
    "x where value is null",
    "create table x (value int)",
    null,
    "insert into x select null from long_sequence(3)",
    expectedNullInserts("value\n", "null", 3, !isNotNullable("int")),  // expects output
    true,
    false,
    false
);

// Test IS NOT NULL filter
assertQuery(
    "value\n",
    "x where value is not null",
    "create table x (value int)",
    null,
    "insert into x select null from long_sequence(3)",
    expectedNullInserts("value\n", "null", 3, isNotNullable("int")),  // no output for nulls
    true,
    isNotNullable("int"),
    false
);
```

## Test Coverage

**Requirements:** Coverage is not explicitly enforced through build configuration

**View Coverage:**
- Coverage can be measured using IntelliJ IDEA's built-in test coverage tools
- Run tests with coverage from IDE: Right-click test class → Run with Coverage

## Test Organization by Type

**Unit Tests (GroupBy Functions):**
- Location: `core/src/test/java/io/questdb/test/griffin/GroupByFunctionTest.java`
- Test individual aggregate functions (sum, max, min, percentiles, etc.)
- Verify correct computation for various input scenarios
- Test NULL handling for each function type

**Integration Tests (Query Execution):**
- Location: `core/src/test/java/io/questdb/test/griffin/`
- Test end-to-end SQL query execution
- Test GROUP BY with various aggregates and expressions
- Test query optimization and execution plans
- Test error conditions and edge cases

**Storage Tests:**
- Location: `core/src/test/java/io/questdb/test/cairo/`
- Test data persistence and recovery
- Verify correct behavior on disk
- Test table operations (CREATE, DROP, ALTER)

**Example - Storage Test Pattern:**
```java
public class StorageTest extends AbstractCairoTest {
    @Test
    public void testTableCreation() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE test (id INT, value DOUBLE)");
            execute("INSERT INTO test VALUES (1, 1.5)");
            assertSql("id\tvalue\n1\t1.5\n", "SELECT * FROM test");
        });
    }
}
```

## Special Test Patterns

**Parametrized Testing (Manual):**
- Use loops with setUp/tearDown for testing multiple cases
- Example from `InsertNullTest.java`: Loop through TYPES array, setUp() between iterations

```java
for (int i = 0; i < TYPES.length; i++) {
    if (i > 0) {
        setUp();
    }
    try {
        // test with TYPES[i]
    } finally {
        tearDown();
    }
}
```

**Exception Testing:**
- Use try-catch with Assert.fail() when exception is required
- Check specific exception message/position for SQL errors
- Example: SqlException includes error position `[28]` in message

**Multiline SQL in Tests:**
- Use triple-quoted strings (Java text blocks) for readability
- Format SQL with proper indentation
- Use UPPERCASE for keywords

```java
String query = """
    SELECT
        col1,
        sum(col2) as total
    FROM table
    GROUP BY col1
    ORDER BY col1""";
```

## Resource Management in Tests

**Memory Leak Detection:**
- `assertMemoryLeak()` automatically tracks memory allocations and deallocations
- Fails test if native memory is not freed
- Catches leaks in error paths too

**Proper Cleanup:**
- Test lifecycle: setUp() initializes engine/compiler → test body → tearDown() releases resources
- Manual cleanup in finally blocks for specific test sections
- ObjList cleanup with `Misc.freeObjList()` for object arrays

Example:
```java
protected void setUp() throws Exception {
    // Initializes engine, compiler, execution context
}

protected void tearDown() throws Exception {
    // Releases all resources, clears engine cache
}
```

---

*Testing analysis: 2026-04-13*
