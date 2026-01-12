# QuestDB Plugin Example

This is an example plugin demonstrating how to create custom functions for QuestDB. It includes examples of different function types:

## Function Types Included

### 1. Scalar Function: `example_square(D)`
A simple row-by-row transformation that squares a double value.

```sql
SELECT "questdb-plugin-example-1.0.0".example_square(4.0);
-- Returns: 16.0
```

### 2. String Function: `example_reverse(S)`
A string transformation function that reverses a string.

```sql
SELECT "questdb-plugin-example-1.0.0".example_reverse('hello');
-- Returns: 'olleh'
```

### 3. Aggregate Function: `example_weighted_avg(DD)`
A GROUP BY function that computes weighted averages.

```sql
SELECT category, "questdb-plugin-example-1.0.0".example_weighted_avg(value, weight)
FROM data
GROUP BY category;
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
SELECT "questdb-plugin-example-1.0.0".example_square(price) FROM trades;
```

### List plugin functions
```sql
SELECT * FROM functions() WHERE name LIKE 'questdb-plugin%';
```

### Unload the plugin
```sql
UNLOAD PLUGIN 'questdb-plugin-example-1.0.0';
```

## Creating Your Own Plugin

### FunctionFactory Interface

All custom functions implement `FunctionFactory`:

```java
public interface FunctionFactory {
    // Function signature: name(types)
    // Types: D=double, I=int, L=long, S=string, T=boolean, etc.
    String getSignature();

    // Mark as GROUP BY function
    default boolean isGroupBy() { return false; }

    // Mark as window function
    default boolean isWindow() { return false; }

    // Mark as cursor (table) function
    default boolean isCursor() { return false; }

    // Create function instance
    Function newInstance(int position, ObjList<Function> args,
                        IntList argPositions, CairoConfiguration config,
                        SqlExecutionContext ctx) throws SqlException;
}
```

### Scalar Functions

Extend the appropriate base class (`DoubleFunction`, `IntFunction`, `StrFunction`, etc.) and implement `UnaryFunction` or `BinaryFunction`:

```java
private static class MyFunction extends DoubleFunction implements UnaryFunction {
    private final Function arg;

    @Override
    public double getDouble(Record rec) {
        return transform(arg.getDouble(rec));
    }
}
```

### GROUP BY Functions

Implement `GroupByFunction` and its required methods:

- `initValueTypes()` - Register columns for storing state
- `computeFirst()` - Handle first row in a group
- `computeNext()` - Handle subsequent rows
- `merge()` - Merge partial results (for parallel execution)
- `setNull()` - Handle null values

### String Functions (Zero-GC Pattern)

Use `StringSink` buffers for efficient, allocation-free operation:

```java
private final StringSink sinkA = new StringSink();
private final StringSink sinkB = new StringSink();

@Override
public CharSequence getStrA(Record rec) {
    // Process into sinkA
    sinkA.clear();
    // ... process string ...
    return sinkA;
}
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

## License

Apache License 2.0
