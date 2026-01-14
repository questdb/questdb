# QuestDB Plugin Example

This is an example plugin demonstrating how to create custom functions for QuestDB using both the traditional FunctionFactory interface and the new simplified UDF API.

## Simplified UDF API (Recommended)

The simplified UDF API allows you to create functions with minimal boilerplate using Java lambdas:

### Scalar Functions (One-Liners!)

```java
// Square a number - just one line!
UDFRegistry.scalar("my_square", Double.class, Double.class, x -> x * x)

// String to uppercase
UDFRegistry.scalar("my_upper", String.class, String.class,
    s -> s == null ? null : s.toUpperCase())

// Absolute value using method reference
UDFRegistry.scalar("my_abs", Double.class, Double.class, Math::abs)
```

### Binary Functions (Two Arguments)

```java
// Power function
UDFRegistry.binary("my_power", Double.class, Double.class, Double.class,
    (base, exp) -> Math.pow(base, exp))

// String concatenation
UDFRegistry.binary("my_concat", String.class, String.class, String.class,
    (a, b) -> (a == null ? "" : a) + (b == null ? "" : b))
```

### Variadic Functions (N Arguments)

```java
// Maximum of any number of values
UDFRegistry.varargs("my_max", Double.class, Double.class,
    args -> args.stream().filter(Objects::nonNull).max(Double::compare).orElse(null))

// Concatenate any number of strings
UDFRegistry.varargs("my_concat_all", String.class, String.class,
    args -> args.stream().filter(Objects::nonNull).collect(Collectors.joining()))
```

### Aggregate Functions (GROUP BY)

```java
// Sum aggregate
UDFRegistry.aggregate("my_sum", Double.class, Double.class,
    () -> new AggregateUDF<Double, Double>() {
        private double sum = 0;
        public void accumulate(Double v) { if (v != null) sum += v; }
        public Double result() { return sum; }
        public void reset() { sum = 0; }
    })
```

### Timestamp and Date Functions

```java
// Extract hour from timestamp
UDFRegistry.scalar("my_hour", Timestamp.class, Integer.class,
    ts -> ts == null ? null : ts.toInstant().atZone(ZoneOffset.UTC).getHour())

// Add days to timestamp
UDFRegistry.binary("my_add_days", Timestamp.class, Integer.class, Timestamp.class,
    (ts, days) -> new Timestamp(ts.getMicros() + days * 24L * 60L * 60L * 1_000_000L))

// Extract year from date
UDFRegistry.scalar("my_year", Date.class, Integer.class,
    d -> d == null ? null : d.toLocalDate().getYear())
```

### Array Functions

```java
// Sum all elements in an array
UDFRegistry.scalar("my_array_sum", DoubleArray.class, Double.class,
    arr -> arr == null || arr.isEmpty() ? Double.NaN : arr.sum())

// Get array average
UDFRegistry.scalar("my_array_avg", DoubleArray.class, Double.class,
    arr -> arr == null || arr.isEmpty() ? Double.NaN : arr.avg())

// Get array length
UDFRegistry.scalar("my_array_len", DoubleArray.class, Integer.class,
    arr -> arr == null ? 0 : arr.length())

// Get element at index
UDFRegistry.binary("my_array_get", DoubleArray.class, Integer.class, Double.class,
    (arr, idx) -> arr == null || idx < 0 || idx >= arr.length() ? Double.NaN : arr.get(idx))

// Check if array contains a value
UDFRegistry.binary("my_array_contains", DoubleArray.class, Double.class, Boolean.class,
    (arr, val) -> {
        if (arr == null || val == null) return false;
        for (double v : arr) {
            if (v == val) return true;
        }
        return false;
    })
```

Note: Currently only `DoubleArray` is supported (1D arrays of DOUBLE).

### Decimal Functions

```java
import java.math.BigDecimal;
import java.math.RoundingMode;

// Round to 2 decimal places
UDFRegistry.scalar("my_round2", BigDecimal.class, BigDecimal.class,
    bd -> bd == null ? null : bd.setScale(2, RoundingMode.HALF_UP))

// Absolute value of decimal
UDFRegistry.scalar("my_decimal_abs", BigDecimal.class, BigDecimal.class,
    bd -> bd == null ? null : bd.abs())

// Add two decimals
UDFRegistry.binary("my_decimal_add", BigDecimal.class, BigDecimal.class, BigDecimal.class,
    (a, b) -> a == null || b == null ? null : a.add(b))

// Multiply two decimals
UDFRegistry.binary("my_decimal_mult", BigDecimal.class, BigDecimal.class, BigDecimal.class,
    (a, b) -> a == null || b == null ? null : a.multiply(b))

// Divide with precision
UDFRegistry.binary("my_decimal_div", BigDecimal.class, BigDecimal.class, BigDecimal.class,
    (a, b) -> {
        if (a == null || b == null || b.compareTo(BigDecimal.ZERO) == 0) return null;
        return a.divide(b, 6, RoundingMode.HALF_UP);
    })
```

Note: `BigDecimal` maps to QuestDB's `DECIMAL(18,6)` type by default. Input values are automatically converted from any DECIMAL type, and output values use 6 decimal places of precision.

## Supported Types

| Java Type | QuestDB Type | Signature Char |
|-----------|--------------|----------------|
| `Double` / `double` | DOUBLE | D |
| `Long` / `long` | LONG | L |
| `Integer` / `int` | INT | I |
| `String` | STRING | S |
| `Boolean` / `boolean` | BOOLEAN | T |
| `Float` / `float` | FLOAT | F |
| `Short` / `short` | SHORT | E |
| `Byte` / `byte` | BYTE | B |
| `Character` / `char` | CHAR | A |
| `Timestamp` | TIMESTAMP | N |
| `Date` | DATE | M |
| `DoubleArray` | DOUBLE[] | D[] |
| `BigDecimal` | DECIMAL(18,6) | Ξ |

## Complete Plugin Example

```java
@PluginFunctions(
    description = "My custom functions",
    version = "1.0.0",
    author = "My Company",
    license = "Apache-2.0"
)
public class MyPlugin implements PluginLifecycle {

    // Optional: Initialize resources when plugin loads
    @Override
    public void onLoad(CairoConfiguration configuration) {
        // Initialize connections, caches, etc.
    }

    // Optional: Clean up when plugin unloads
    @Override
    public void onUnload() {
        // Close connections, clean up resources
    }

    // Required: Return list of function factories
    public static ObjList<FunctionFactory> getFunctions() {
        return UDFRegistry.functions(
            UDFRegistry.scalar("my_square", Double.class, Double.class, x -> x * x),
            UDFRegistry.scalar("my_upper", String.class, String.class, String::toUpperCase),
            UDFRegistry.binary("my_power", Double.class, Double.class, Double.class, Math::pow)
        );
    }
}
```

## Error Handling

UDF exceptions are automatically caught and wrapped in meaningful error messages:

```java
UDFRegistry.scalar("my_safe_divide", Double.class, Double.class,
    x -> {
        if (x == 0) {
            throw new IllegalArgumentException("Cannot divide by zero");
        }
        return 1.0 / x;
    })
```

When called with `my_safe_divide(0)`, the error message will be:
`UDF 'my_safe_divide' threw exception: Cannot divide by zero`

## Traditional FunctionFactory API

For more control, you can still use the traditional FunctionFactory interface:

### Scalar Function Example

```java
public class MySquareFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() { return "my_square(D)"; }

    @Override
    public Function newInstance(int position, ObjList<Function> args,
            IntList argPositions, CairoConfiguration config,
            SqlExecutionContext ctx) {
        return new MySquareFunction(args.getQuick(0));
    }

    private static class MySquareFunction extends DoubleFunction implements UnaryFunction {
        private final Function arg;
        public MySquareFunction(Function arg) { this.arg = arg; }
        @Override public Function getArg() { return arg; }
        @Override public double getDouble(Record rec) {
            double v = arg.getDouble(rec);
            return Double.isNaN(v) ? Double.NaN : v * v;
        }
    }
}
```

## Building

```bash
cd examples/questdb-plugin-example
mvn clean package
```

The JAR will be created at `target/questdb-plugin-example-1.0.0.jar`.

## Installing

1. Copy the JAR to your QuestDB plugins directory:
   ```bash
   cp target/questdb-plugin-example-1.0.0.jar /path/to/questdb/plugins/
   ```

2. The plugin will be discovered on QuestDB startup.

## Using the Plugin

### Load the plugin
```sql
LOAD PLUGIN 'questdb-plugin-example-1.0.0';
```

### List available plugins
```sql
SHOW PLUGINS;
```

### Use plugin functions
Plugin functions use qualified names: `"plugin-name".function_name`
```sql
SELECT "questdb-plugin-example-1.0.0".simple_square(price) FROM trades;
```

### List plugin functions
```sql
SELECT * FROM functions() WHERE name LIKE 'questdb-plugin%';
```

### Unload the plugin
```sql
UNLOAD PLUGIN 'questdb-plugin-example-1.0.0';
```

## Dependencies

Plugins should declare QuestDB as a `provided` dependency since it's supplied by the runtime:

```xml
<dependency>
    <groupId>org.questdb</groupId>
    <artifactId>questdb</artifactId>
    <version>${questdb.version}</version>
    <scope>provided</scope>
</dependency>
```

## Functions Included in This Example

### Simplified UDF API Functions

| Function | Signature | Description |
|----------|-----------|-------------|
| `simple_square` | `(D)D` | Square a number |
| `simple_cube` | `(D)D` | Cube a number |
| `simple_abs` | `(D)D` | Absolute value |
| `simple_sqrt` | `(D)D` | Square root |
| `simple_reverse` | `(S)S` | Reverse a string |
| `simple_upper` | `(S)S` | Uppercase |
| `simple_lower` | `(S)S` | Lowercase |
| `simple_power` | `(DD)D` | Power (base^exp) |
| `simple_mod` | `(LL)L` | Modulo |
| `simple_sum` | `(D)D` | Sum aggregate |
| `simple_avg` | `(D)D` | Average aggregate |
| `simple_min` | `(D)D` | Minimum aggregate |
| `simple_max` | `(D)D` | Maximum aggregate |
| `simple_hour` | `(N)I` | Extract hour from timestamp |
| `simple_day` | `(N)I` | Extract day from timestamp |
| `simple_year` | `(N)I` | Extract year from timestamp |
| `simple_max_of` | `(V)D` | Maximum of N values |
| `simple_min_of` | `(V)D` | Minimum of N values |
| `simple_concat_all` | `(V)S` | Concatenate N strings |
| `simple_coalesce` | `(V)D` | First non-null value |
| `simple_array_sum` | `(D[])D` | Sum of array elements |
| `simple_array_avg` | `(D[])D` | Average of array elements |
| `simple_array_min` | `(D[])D` | Minimum of array elements |
| `simple_array_max` | `(D[])D` | Maximum of array elements |
| `simple_array_len` | `(D[])I` | Length of array |
| `simple_array_get` | `(D[]I)D` | Get element at index |
| `simple_array_contains` | `(D[]D)T` | Check if array contains value |
| `simple_round2` | `(Ξ)Ξ` | Round decimal to 2 places |
| `simple_decimal_abs` | `(Ξ)Ξ` | Absolute value of decimal |
| `simple_decimal_add` | `(ΞΞ)Ξ` | Add two decimals |
| `simple_decimal_mult` | `(ΞΞ)Ξ` | Multiply two decimals |
| `simple_decimal_div` | `(ΞΞ)Ξ` | Divide two decimals |

### Traditional API Functions

| Function | Signature | Description |
|----------|-----------|-------------|
| `example_square` | `(D)D` | Square a number |
| `example_reverse` | `(S)S` | Reverse a string |
| `example_weighted_avg` | `(DD)D` | Weighted average aggregate |

## License

Apache License 2.0
