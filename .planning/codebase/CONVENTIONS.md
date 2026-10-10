# Coding Conventions

**Analysis Date:** 2026-04-13

## Naming Patterns

**Files:**
- GroupBy functions: `[FunctionName]GroupByFunction.java` (e.g., `SumDoubleGroupByFunction.java`)
- GroupBy factories: `[FunctionName]GroupByFunctionFactory.java` (e.g., `MaxDoubleGroupByFunctionFactory.java`)
- Window function factories: `[FunctionName]WindowFunctionFactory.java` (e.g., `VarDoubleWindowFunctionFactory.java`)
- Test files: `[TopicName]Test.java` (e.g., `InsertNullTest.java`, `GroupByFunctionTest.java`)

**Functions:**
- Use camelCase: `computeFirst()`, `computeNext()`, `isThreadSafe()`
- Boolean methods use `is` or `has` prefix: `isConstant()`, `hasTimestamp()`, `isThreadSafe()`
- Getter methods: `getDouble()`, `getLong()`, `getValueIndex()`
- Setter methods: `setEmpty()`, `setNull()`, `setDouble()`

**Variables:**
- Local variables: camelCase (e.g., `histogram`, `histogramIndex`, `percentile`)
- Instance fields: camelCase (e.g., `valueIndex`, `precision`, `arg`)
- Static constants: UPPER_SNAKE_CASE (e.g., `ARRAY_MASK`, `CONST_MASK`, `TYPE_MASK`)
- NULL sentinel values: `Numbers.LONG_NULL` for longs, `Double.NaN` for doubles

**Types:**
- Classes: PascalCase (e.g., `ApproxPercentileDoubleGroupByFunction`)
- Interfaces: PascalCase (e.g., `GroupByFunction`, `UnaryFunction`)
- Type variables: Single uppercase letter (e.g., `T`, `D`)

## Code Style

**Formatting:**
- IntelliJ IDEA code style is configured in `.idea/codeStyles/Project.xml`
- No auto-formatting tool (Prettier, spotless) enforced; relies on IDE configuration

**Linting:**
- No explicit linting configuration found; code follows Java conventions

**Member Organization:**
- Java class members grouped by kind (static vs. instance) and visibility
- Within each group, sorted alphabetically by name
- Do NOT use comments as "section headings" — methods must stay together after auto-sorting
- Order: Static public constants → Static protected → Static package-private → Static private → Instance public → Instance protected → Instance package-private → Instance private

Example from `ApproxPercentileDoubleGroupByFunction.java`:
```java
public class ApproxPercentileDoubleGroupByFunction extends DoubleFunction implements GroupByFunction, BinaryFunction {
    // static fields (none in this class)
    
    // instance fields (alphabetically)
    private final Function exprFunc;
    private final int funcPosition;
    private final ObjList<DoubleHistogram> histograms = new ObjList<>();
    private final Function percentileFunc;
    private final int precision;
    private int histogramIndex;
    private double percentile;
    private int valueIndex;

    // constructor
    public ApproxPercentileDoubleGroupByFunction(...) { }

    // public instance methods (alphabetical)
    @Override
    public void clear() { }

    @Override
    public void computeFirst(...) { }
    
    // continue alphabetically...
}
```

## Import Organization

**Order:**
1. Java standard library imports (e.g., `java.io.*`)
2. QuestDB core imports (e.g., `io.questdb.cairo.*`)
3. Third-party imports (if any)

**Path Aliases:**
- No path aliases observed; full package paths used throughout

## Error Handling

**Patterns:**
- Use `SqlException` with position information for SQL parsing/compilation errors
- Position passed to `SqlException.$()` should point at specific offending character, not expression start
- Example: `throw SqlException.$(funcPosition, "percentile must be between 0.0 and 1.0");`
- For validation failures, use `CairoException` for Cairo-level errors

**NULL Handling:**
- Doubles: Use `Double.NaN` as NULL sentinel, check with `Double.isNaN(value)` and `Numbers.isFinite(value)`
- Longs: Use `Numbers.LONG_NULL` as NULL sentinel, check with `Numbers.isNull(value)`
- Bytes/shorts/booleans: Cannot be NULL in QuestDB; always have default values (0 for numbers, false for booleans)
- Always consider behavior for NULL values; distinguish between NULL as "not initialized yet" vs. actual NULL column value

Example from `SumDoubleGroupByFunction.java`:
```java
// Check for NaN instead of null for doubles
if (!Double.isNaN(value)) {
    final double sum = mapValue.getDouble(valueIndex);
    if (!Double.isNaN(sum)) {
        mapValue.putDouble(valueIndex, sum + value);
    } else {
        mapValue.putDouble(valueIndex, value);
    }
}

// Use Double.NaN to represent NULL
mapValue.putDouble(valueIndex, Double.NaN);
```

## Logging

**Framework:** JUnit's Assert class for test assertions; no production logging framework enforced

**Patterns:**
- Log class: `io.questdb.log.Log` from `LogFactory.getLog()`
- Example: `protected static final Log LOG = LogFactory.getLog(AbstractCairoTest.class);`

## Comments

**When to Comment:**
- Use comments sparingly
- Document complex algorithms or non-obvious intent
- Avoid comments that restate code

**JavaDoc:**
- Used selectively for public APIs
- Example from `Decimal64LoaderFunctionFactory.java`: Detailed JavaDoc for factory methods

## Function Design

**Size:** Methods should be focused and typically under 50 lines
- `computeFirst()`, `computeNext()`, `merge()` are typically 10-20 lines
- Factory methods often 10 lines or less

**Parameters:**
- Constructor parameters typically match class name pattern conventions
- Method parameters use type prefix when ambiguous (e.g., `mapValue`, `record`, `rowId`)

**Return Values:**
- GroupBy functions implement interface methods with specific return types (void for compute, double/long/int for getters)
- Use Optional sparingly; prefer null or sentinel values

## Module Design

**Exports:**
- GroupBy function implementation classes extend specific base types: `DoubleFunction`, `LongFunction`, etc.
- Implement interfaces: `GroupByFunction`, `UnaryFunction`, `BinaryFunction` as appropriate
- Factories implement `FunctionFactory` interface

Example from `ApproxPercentileDoubleGroupByFunction.java`:
```java
public class ApproxPercentileDoubleGroupByFunction extends DoubleFunction implements GroupByFunction, BinaryFunction
```

**Barrel Files:**
- Not observed in codebase; individual function classes are imported directly

## Collections and Memory

**Standard Collections:**
- Use `ObjList<T>` instead of `T[]` for object arrays
- ObjList integrates with `Misc.freeObjList()` / `Misc.freeObjListIfCloseable()` for resource cleanup
- Examples from codebase: `ObjList<DoubleHistogram> histograms = new ObjList<>()`

**Resource Cleanup:**
- Always call `Misc.free()` or `Misc.freeObjList()` for resources
- Used in `close()` methods and error handling paths
- Example from `ApproxPercentileDoubleGroupByFunction` (if it had close): Would call `Misc.free()` on histogram list

## Modern Java Features

**Enhanced Switch:**
- Use modern switch expressions instead of switch statements
- Example: `return switch (type) { case 'D' -> DOUBLE; ... }`

**Multiline String Literals:**
- Use triple-quoted strings for SQL and long text
- Example from tests: `"""SELECT ... FROM ... WHERE ..."""`

**Pattern Variables:**
- Use pattern matching in instanceof checks
- Example: `if (func instanceof UnaryFunction unaryFunc) { unaryFunc.getArg(); }`

## SQL Dialect Patterns

**In Code/Tests:**
- Prefer `expr::TYPE` syntax for casts over `CAST(expr, TYPE)`
- Use underscore separators in numbers: `1_000_000`
- Use UPPERCASE for SQL keywords in test assertions: `CREATE TABLE`, `INSERT`, `SELECT ... FROM`
- Use multiline strings for complex SQL statements
- Single INSERT statement for multiple rows

Example from `GroupByFunctionTest.java`:
```java
assertQuery(
    "expected_result...",
    """
    SELECT
        delivery_start_utc as y_utc_15m,
        sum(case when seller='sf' then -1.0*volume_mw else 0.0 end) as y_sf_position_mw
    FROM trades
    WHERE (seller = 'sf' OR buyer = 'sf')
    GROUP BY y_utc_15m
    ORDER BY y_utc_15m""",
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

## Function Factory Pattern

Functions are registered through factory classes implementing `FunctionFactory`:

**Signature Format:** Function name followed by type codes in parentheses
- `D` = DOUBLE, `L` = LONG, `I` = INT, `S` = STRING, etc.
- Uppercase (e.g., `D`) = non-constant parameter
- Lowercase (e.g., `d`) = constant parameter
- `[]` suffix = array type

Examples from codebase:
- `"max(D)"` - takes one double parameter
- `"approx_percentile(DD)"` - takes two doubles
- `"variance(D)"` - takes one double

**Factory Implementation Pattern:**
```java
public class MaxDoubleGroupByFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "max(D)";
    }

    @Override
    public boolean isGroupBy() {
        return true;
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        return new MaxDoubleGroupByFunction(args.getQuick(0));
    }
}
```

---

*Convention analysis: 2026-04-13*
