# SQL Parser Fuzz Testing Design Document

## Overview

This document describes the design of a fuzz testing system for QuestDB's SQL parser. The goal is to generate complex
SQL queries—both valid and invalid—to verify that the parser either:

1. Parses successfully, or
2. Reports a proper `SqlException`

The parser must **never** crash, throw unexpected exceptions, hang, or exhibit undefined behavior.

## Goals

- **Robustness testing**: Ensure the parser handles all inputs gracefully
- **Edge case discovery**: Find parsing bugs through random complex queries
- **QuestDB feature coverage**: Test QuestDB-specific syntax (ASOF/LT joins, SAMPLE BY, LATEST ON, etc.)
- **Regression prevention**: Catch parser regressions early

## Non-Goals (Phase 1)

- Query execution testing
- Semantic validation (type checking, table existence)
- Performance benchmarking
- Mutation-based fuzzing (may add later)

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      SqlParserFuzzTest                          │
│  - Runs N iterations                                            │
│  - Classifies results: PASS | SYNTAX_ERROR | CRASH              │
│  - Reports failures                                             │
└─────────────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                      SqlFuzzGenerator                           │
│  - Orchestrates query generation                                │
│  - Selects generation mode (VALID, CORRUPT, GARBAGE)            │
│  - Configurable via GeneratorConfig                             │
└─────────────────────────────────────────────────────────────────┘
                          │
          ┌───────────────┼───────────────┐
          ▼               ▼               ▼
    ┌──────────┐   ┌───────────┐   ┌───────────┐
    │  Valid   │   │  Corrupt  │   │  Garbage  │
    │Generator │   │ Generator │   │ Generator │
    └──────────┘   └───────────┘   └───────────┘
          │
          ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Component Generators                         │
├─────────────────────────────────────────────────────────────────┤
│  SelectGenerator      │  Generates SELECT statements            │
│  CteGenerator         │  Generates WITH clauses                 │
│  JoinGenerator        │  Generates JOIN clauses (incl. ASOF/LT) │
│  SourceGenerator      │  Generates FROM sources (table/subquery)│
│  ExpressionGenerator  │  Generates expressions (WHERE, ON, etc.)│
│  SampleByGenerator    │  Generates SAMPLE BY clauses            │
│  LatestOnGenerator    │  Generates LATEST ON clauses            │
│  WindowGenerator      │  Generates window functions             │
│  LiteralGenerator     │  Generates literals and identifiers     │
└─────────────────────────────────────────────────────────────────┘
```

---

## Generation Modes

### 1. VALID Mode (~60% of generated queries)

Generates structurally correct SQL that follows QuestDB grammar. These queries may still be semantically invalid (
referencing non-existent tables/columns), but they are syntactically well-formed.

**Example output:**

```sql
WITH cte1 AS (SELECT a, b, c
              FROM t1
              WHERE x > 10),
     cte2 AS (SELECT *
              FROM t2 LIMIT 100
    )
SELECT cte1.a,
       sum(cte1.b) OVER (PARTITION BY cte1.c ORDER BY cte1.a),
last(cte2.val)
FROM cte1
    ASOF JOIN cte2
ON cte1.ts = cte2.ts
WHERE cte1.a BETWEEN 1
  AND 100
    SAMPLE BY 1h FILL(PREV)
GROUP BY cte1.a, cte1.c
ORDER BY cte1.a DESC
    LIMIT 50, 100
```

### 2. CORRUPT Mode (~30% of generated queries)

Takes valid SQL and applies random corruptions to test error handling:

| Corruption Type     | Description                      | Example                  |
|---------------------|----------------------------------|--------------------------|
| Token Drop          | Remove a random token            | `SELECT a, FROM t`       |
| Token Swap          | Swap two adjacent tokens         | `SELECT FROM a t`        |
| Token Duplicate     | Duplicate a random token         | `SELECT SELECT a FROM t` |
| Keyword Injection   | Insert keyword at wrong position | `SELECT a JOIN FROM t`   |
| Truncation          | Cut query at random point        | `SELECT a, b FROM`       |
| Paren Imbalance     | Add/remove parentheses           | `SELECT (a + b FROM t`   |
| Garbage Injection   | Replace token with random chars  | `SELECT a FROM @@#$`     |
| Character Insertion | Insert random character          | `SELECT a FROM t`        |

### 3. GARBAGE Mode (~10% of generated queries)

Generates completely random token sequences to test parser resilience against arbitrary input:

```sql
)
SELECT ASOF 123 'string' WHERE ( FROM JOIN != >=
```

---

## Component Specifications

### GeneratorConfig

Controls the behavior and complexity of generated queries.

```java
class GeneratorConfig {
    // Depth control
    int maxDepth = 5;                    // Maximum nesting depth for subqueries

    // Cardinality limits
    int maxCteCount = 3;                 // Maximum CTEs in WITH clause
    int maxJoins = 4;                    // Maximum JOINs per SELECT
    int maxColumns = 10;                 // Maximum columns in SELECT list
    int maxGroupByColumns = 5;           // Maximum GROUP BY columns
    int maxOrderByColumns = 5;           // Maximum ORDER BY columns
    int maxUnionCount = 3;               // Maximum UNIONed queries

    // Feature probabilities (0.0 - 1.0)
    double cteProb = 0.3;                // Probability of WITH clause
    double subqueryProb = 0.3;           // Probability of subquery as source
    double nestingProb = 0.5;            // Base probability of recursion (decreases with depth)
    double joinProb = 0.5;               // Probability of adding a JOIN
    double whereProb = 0.6;              // Probability of WHERE clause
    double groupByProb = 0.3;            // Probability of GROUP BY
    double havingProb = 0.2;             // Probability of HAVING (when GROUP BY exists)
    double orderByProb = 0.4;            // Probability of ORDER BY
    double limitProb = 0.3;              // Probability of LIMIT
    double distinctProb = 0.1;           // Probability of DISTINCT

    // QuestDB-specific feature probabilities
    double sampleByProb = 0.2;           // Probability of SAMPLE BY
    double latestOnProb = 0.1;           // Probability of LATEST ON
    double asofJoinProb = 0.15;          // Probability of ASOF JOIN (vs other joins)
    double ltJoinProb = 0.1;             // Probability of LT JOIN
    double spliceJoinProb = 0.05;        // Probability of SPLICE JOIN
    double windowFunctionProb = 0.15;    // Probability of window functions
    double timestampWithTimezoneProb = 0.1;

    // Expression complexity
    int maxExpressionDepth = 4;          // Maximum depth of expressions
    double functionCallProb = 0.3;       // Probability of function call in expression
    double caseWhenProb = 0.1;           // Probability of CASE expression
    double castProb = 0.1;               // Probability of CAST expression

    // Generation mode weights
    double validModeWeight = 0.6;
    double corruptModeWeight = 0.3;
    double garbageModeWeight = 0.1;
}
```

### GeneratorContext

Runtime context passed through recursive generation. Scope tracking is critical for generating syntactically coherent
queries—without it, we'd generate references to undefined CTEs or aliases.

```java
class GeneratorContext {
    final Rnd rnd;
    final GeneratorConfig config;
    final StringSink sink;

    // Current state
    int depth;                           // Current nesting depth
    int expressionDepth;                 // Current expression depth

    // Scope tracking - MUST be populated during generation
    List<String> cteNames;               // CTEs defined in current WITH clause
    List<String> tableAliases;           // Table/subquery aliases in current FROM
    List<String> columnNames;            // Columns available for reference

    // Flags
    boolean inAggregateContext;          // Inside aggregate function
    boolean hasTimestampColumn;          // Source has timestamp for SAMPLE BY
    boolean inWindowFunction;            // Inside window function

    // Methods
    void append(CharSequence s);

    void append(char c);

    void space();

    void comma();

    void openParen();

    void closeParen();

    boolean shouldRecurse();             // Checks depth and probability

    String randomIdentifier();

    String randomAlias();
}
```

### Scope Population Strategy

The context must be populated as we generate, so references are coherent:

```java
// Example: CTE generation populates scope for main query
void generateWithClause(GeneratorContext ctx) {
    int cteCount = 1 + ctx.rnd.nextInt(ctx.config.maxCteCount);
    ctx.append("WITH ");

    for (int i = 0; i < cteCount; i++) {
        if (i > 0) ctx.append(", ");

        String cteName = "cte" + (i + 1);  // cte1, cte2, ...
        ctx.cteNames.add(cteName);         // Register BEFORE generating body

        ctx.append(cteName).append(" AS (");
        generateSelectStatement(ctx, ctx.depth + 1);
        ctx.append(")");
    }
    ctx.space();
}

// Example: FROM clause populates table aliases
void generateFromClause(GeneratorContext ctx) {
    ctx.append("FROM ");
    String alias = generateSource(ctx);  // Returns alias
    ctx.tableAliases.add(alias);         // Register for column references

// JOINs add more aliases
    while (ctx.rnd.nextDouble() < ctx.config.joinProb) {
        String joinAlias = generateJoin(ctx);
        ctx.tableAliases.add(joinAlias);
    }
}

// Column references use what's in scope
String randomColumnReference(GeneratorContext ctx) {
    if (ctx.tableAliases.isEmpty()) {
        return randomColumnName(ctx);  // Unqualified
    }
    String alias = ctx.tableAliases.get(ctx.rnd.nextInt(ctx.tableAliases.size()));
    return alias + "." + randomColumnName(ctx);
}
```

**Key principle**: Generate-then-register. When creating a CTE, table alias, or subquery alias, add it to the context
*before* generating code that might reference it (for self-references) or *immediately after* for normal forward
references.

---

## SQL Feature Coverage

### Standard SQL Features

| Feature                | Generator           | Notes                                            |
|------------------------|---------------------|--------------------------------------------------|
| SELECT columns         | SelectGenerator     | Including *, expressions, aliases                |
| FROM clause            | SourceGenerator     | Tables, subqueries, CTE references               |
| WHERE clause           | ExpressionGenerator | Boolean expressions                              |
| GROUP BY               | SelectGenerator     | Column list, expressions                         |
| HAVING                 | SelectGenerator     | With GROUP BY only                               |
| ORDER BY               | SelectGenerator     | Columns, expressions, ASC/DESC, NULLS FIRST/LAST |
| LIMIT/OFFSET           | SelectGenerator     | Various forms                                    |
| DISTINCT               | SelectGenerator     | SELECT DISTINCT                                  |
| CTEs (WITH)            | CteGenerator        | Single and multiple, recursive potential         |
| JOINs                  | JoinGenerator       | INNER, LEFT, RIGHT, CROSS                        |
| UNION/EXCEPT/INTERSECT | SelectGenerator     | Set operations                                   |
| Subqueries             | SourceGenerator     | In FROM, WHERE, SELECT                           |
| CASE expressions       | ExpressionGenerator | Simple and searched CASE                         |
| CAST                   | ExpressionGenerator | Type conversions                                 |
| IN/NOT IN              | ExpressionGenerator | List and subquery forms                          |
| BETWEEN                | ExpressionGenerator | Range predicates                                 |
| LIKE/ILIKE             | ExpressionGenerator | Pattern matching                                 |
| IS NULL/IS NOT NULL    | ExpressionGenerator | Null checks                                      |
| EXISTS                 | ExpressionGenerator | Subquery existence                               |

### QuestDB-Specific Features

| Feature                | Generator           | Syntax                                                                    |
|------------------------|---------------------|---------------------------------------------------------------------------|
| ASOF JOIN              | JoinGenerator       | `ASOF JOIN t ON a.ts = b.ts`                                              |
| LT JOIN                | JoinGenerator       | `LT JOIN t ON a.ts = b.ts`                                                |
| SPLICE JOIN            | JoinGenerator       | `SPLICE JOIN t ON a.ts = b.ts`                                            |
| JOIN TOLERANCE         | JoinGenerator       | `ASOF JOIN t ON ... TOLERANCE 1h`                                         |
| SAMPLE BY              | SampleByGenerator   | `SAMPLE BY 1h`                                                            |
| SAMPLE BY FROM/TO      | SampleByGenerator   | `SAMPLE BY 1h FROM '2024-01-01' TO '2024-12-31'`                          |
| FILL                   | SampleByGenerator   | `FILL(PREV)`, `FILL(NULL)`, `FILL(LINEAR)`, `FILL(NONE)`, `FILL(x, y, z)` |
| ALIGN TO               | SampleByGenerator   | `ALIGN TO CALENDAR`, `ALIGN TO FIRST OBSERVATION`                         |
| TIME ZONE              | SampleByGenerator   | `ALIGN TO CALENDAR TIME ZONE 'UTC'`                                       |
| WITH OFFSET            | SampleByGenerator   | `ALIGN TO CALENDAR WITH OFFSET '00:30'`                                   |
| LATEST BY (deprecated) | LatestOnGenerator   | `LATEST BY sym`                                                           |
| LATEST ON              | LatestOnGenerator   | `LATEST ON ts PARTITION BY sym`                                           |
| timestamp()            | ExpressionGenerator | `timestamp(column)`                                                       |
| Geohash char literals  | LiteralGenerator    | `#sp052w`, `#sp052w/25`                                                   |
| Geohash bit literals   | LiteralGenerator    | `##01110001`                                                              |
| IPv4                   | LiteralGenerator    | `'192.168.1.1'::ipv4`                                                     |
| UUID                   | LiteralGenerator    | `'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid`                            |
| Long256                | LiteralGenerator    | `0x...` (64 hex digits)                                                   |
| Symbol                 | LiteralGenerator    | Symbol type handling                                                      |
| IP Operators           | ExpressionGenerator | `<<`, `>>`, `<<=`, `>>=` (subnet containment)                             |
| WITHIN                 | ExpressionGenerator | `col WITHIN (geohash_list)`                                               |

### Window Functions

| Feature            | Syntax                                                                      |
|--------------------|-----------------------------------------------------------------------------|
| OVER clause        | `func() OVER (...)`                                                         |
| PARTITION BY       | `OVER (PARTITION BY col)`                                                   |
| ORDER BY           | `OVER (ORDER BY col ASC/DESC)`                                              |
| Frame modes        | `ROWS`, `RANGE`, `GROUPS`                                                   |
| Frame bounds       | `UNBOUNDED PRECEDING`, `CURRENT ROW`, `n PRECEDING/FOLLOWING`               |
| Frame BETWEEN      | `ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING`                                  |
| Time unit in RANGE | `RANGE 1 HOUR PRECEDING` (QuestDB-specific)                                 |
| EXCLUDE            | `EXCLUDE CURRENT ROW`, `EXCLUDE GROUP`, `EXCLUDE TIES`, `EXCLUDE NO OTHERS` |
| IGNORE NULLS       | `first_value(col) IGNORE NULLS OVER (...)`                                  |
| RESPECT NULLS      | `first_value(col) RESPECT NULLS OVER (...)`                                 |

### Additional Statement Types (Lower Priority)

| Statement                | Generator            | Notes                                                    |
|--------------------------|----------------------|----------------------------------------------------------|
| INSERT INTO SELECT       | InsertGenerator      | `INSERT INTO t SELECT ...`                               |
| INSERT INTO VALUES       | InsertGenerator      | `INSERT INTO t VALUES (...)`                             |
| INSERT BATCH             | InsertGenerator      | `INSERT BATCH 10000 INTO t SELECT ...`                   |
| INSERT ATOMIC            | InsertGenerator      | `INSERT ATOMIC INTO t SELECT ...`                        |
| COPY                     | -                    | `COPY t FROM 'file.csv'`                                 |
| EXPLAIN                  | -                    | `EXPLAIN SELECT ...`, `EXPLAIN (FORMAT JSON) SELECT ...` |
| RENAME TABLE             | -                    | `RENAME TABLE t1 TO t2`                                  |
| CREATE TABLE             | CreateTableGenerator | DDL with columns, types, indexes                         |
| CREATE MATERIALIZED VIEW | -                    | Complex DDL with EVERY clause                            |

### Additional Expression Features

| Feature                  | Generator           | Syntax                                          |
|--------------------------|---------------------|-------------------------------------------------|
| Double colon cast        | ExpressionGenerator | `'value'::type`, `col::int`                     |
| PostgreSQL cast          | ExpressionGenerator | `type 'value'` (e.g., `timestamp '2024-01-01'`) |
| CAST function            | ExpressionGenerator | `CAST(col AS type)`                             |
| Escape strings           | LiteralGenerator    | `E'hello\nworld'`                               |
| Array constructor        | ExpressionGenerator | `ARRAY[1, 2, 3]`                                |
| Array access             | ExpressionGenerator | `col[1]`, `col[1:3]`                            |
| Array type               | LiteralGenerator    | `int[]`, `varchar[]`                            |
| DECIMAL type             | LiteralGenerator    | `DECIMAL(18, 3)`                                |
| GEOHASH type             | LiteralGenerator    | `GEOHASH(6c)`, `GEOHASH(30b)`                   |
| ALL operator             | ExpressionGenerator | `col <> ALL (subquery)`                         |
| EXISTS                   | ExpressionGenerator | `EXISTS (SELECT ...)`                           |
| DECLARE variables        | DeclareGenerator    | `DECLARE @var := expr SELECT ...`               |
| Variable assignment      | ExpressionGenerator | `@var := expr`                                  |
| Hints                    | HintGenerator       | `/*+ hint(param) */`                            |
| extract()                | ExpressionGenerator | `extract(hour from ts)`                         |
| AT TIME ZONE             | ExpressionGenerator | `ts AT TIME ZONE 'UTC'`                         |
| TIMESTAMP WITH TIME ZONE | LiteralGenerator    | `timestamp with time zone 'value'`              |

---

## Expression Generation

Expressions are generated recursively with controlled depth.

### Expression Types

```
Expression ::=
| Literal
| ColumnReference
| UnaryOp Expression
| Expression BinaryOp Expression
| FunctionCall
| CaseExpression
| CastExpression
| SubqueryExpression
| '(' Expression ')'
```

### Operators (from OperatorExpression.java)

| Precedence | Operators                                   | Type   | Description                   |
|------------|---------------------------------------------|--------|-------------------------------|
| 1          | `.`                                         | Binary | Member/column access          |
| 2          | `::`                                        | Binary | PostgreSQL-style cast         |
| 3          | `-` (unary), `~`                            | Unary  | Negation, bitwise complement  |
| 4          | `*`, `/`, `%`                               | Binary | Multiplicative                |
| 5          | `+`, `-`                                    | Binary | Additive                      |
| 6          | `<<`, `>>`, `<<=`, `>>=`                    | Binary | IP subnet containment         |
| 7          | `\|\|`                                      | Binary | String concatenation          |
| 8          | `&`                                         | Binary | Bitwise AND                   |
| 9          | `^`                                         | Binary | Bitwise XOR                   |
| 10         | `\|`                                        | Binary | Bitwise OR                    |
| 11         | `IN`, `BETWEEN`, `WITHIN`                   | Set    | Set membership operators      |
| 12         | `<`, `<=`, `>`, `>=`                        | Binary | Comparison                    |
| 13         | `=`, `!=`, `<>`, `~`, `!~`, `LIKE`, `ILIKE` | Binary | Equality/pattern matching     |
| 14         | `NOT`                                       | Unary  | Logical NOT                   |
| 15         | `AND`                                       | Binary | Logical AND                   |
| 16         | `OR`                                        | Binary | Logical OR                    |
| 17         | `:`                                         | Binary | Range/slice (internal)        |
| 100        | `:=`                                        | Binary | Variable assignment (DECLARE) |

**Notes:**

- `~` is both unary (bitwise complement) and binary (regex match)
- `NOT` can prefix `IN`, `BETWEEN`, `LIKE`, `ILIKE` as modifiers
- IP operators (`<<`, `>>`, `<<=`, `>>=`) test subnet containment, not bit shifts

### Literal Types

| Type      | Examples                                 |
|-----------|------------------------------------------|
| Integer   | `0`, `42`, `-1`, `9223372036854775807L`  |
| Float     | `3.14`, `1e10`, `NaN`, `Infinity`        |
| String    | `'hello'`, `'it''s'`, `''`               |
| Boolean   | `true`, `false`                          |
| Null      | `null`                                   |
| Timestamp | `'2024-01-15T10:30:00.000Z'`             |
| Date      | `'2024-01-15'`                           |
| Char      | `'x'`                                    |
| Binary    | `'\\x00\\xFF'`                           |
| Geohash   | `#sp052w`, `##s`                         |
| UUID      | `'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'` |
| IPv4      | `'192.168.1.1'`                          |
| Long256   | `0x...` (64 hex digits)                  |

---

## Corruption Strategies

### Token-Level Corruptions

```java
interface CorruptionStrategy {
    String apply(String sql, Rnd rnd);
}

class DropTokenStrategy implements CorruptionStrategy {
// Remove one random token
// "SELECT a, b FROM t" → "SELECT a, FROM t"
}

class SwapTokensStrategy implements CorruptionStrategy {
// Swap two adjacent tokens
// "SELECT a FROM t" → "SELECT FROM a t"
}

class DuplicateTokenStrategy implements CorruptionStrategy {
// Duplicate a random token
// "SELECT a FROM t" → "SELECT a a FROM t"
}

class InjectKeywordStrategy implements CorruptionStrategy {
// Insert SQL keyword at random position
// "SELECT a FROM t" → "SELECT a JOIN FROM t"
}

class TruncateStrategy implements CorruptionStrategy {
// Cut query at random point
// "SELECT a FROM t WHERE x > 1" → "SELECT a FROM t WHERE"
}

class UnbalanceParensStrategy implements CorruptionStrategy {
// Add or remove parenthesis
// "SELECT (a + b) FROM t" → "SELECT (a + b FROM t"
}

class GarbageTokenStrategy implements CorruptionStrategy {
// Replace random token with garbage
// "SELECT a FROM t" → "SELECT @#$% FROM t"
}

class CharacterInsertStrategy implements CorruptionStrategy {
// Insert random character in token
// "SELECT a FROM t" → "SELECT a FR0M t"
}
```

### Structural Corruptions

```java
class ClauseReorderStrategy implements CorruptionStrategy {
// Move clauses to wrong positions
// "SELECT a FROM t WHERE x > 1 ORDER BY a"
// → "SELECT a ORDER BY a FROM t WHERE x > 1"
}

class DuplicateClauseStrategy implements CorruptionStrategy {
// Duplicate a clause
// "SELECT a FROM t" → "SELECT a FROM t FROM t"
}

class MixKeywordsStrategy implements CorruptionStrategy {
// Use wrong keywords
// "SELECT a FROM t" → "INSERT a FROM t"
}
```

### Tokenization Strategy

**Problem**: Splitting SQL on whitespace fails for strings (`'hello world'`), comments, and operators without spaces (
`a+b`).

**Solution**: Operate on the token stream during valid generation, *before* serializing to string. The `SqlToken` class
holds both the token value and its type:

```java
class SqlToken {
    enum Type {KEYWORD, IDENTIFIER, LITERAL, OPERATOR, PUNCTUATION}

    final String value;
    final Type type;
}

class TokenizedQuery {
    List<SqlToken> tokens;

    String serialize() {
        // Reconstruct SQL from tokens with appropriate spacing
    }

    TokenizedQuery dropToken(int index) { ...}

    TokenizedQuery swapTokens(int i, int j) { ...}

    TokenizedQuery insertToken(int index, SqlToken token) { ...}
}
```

**Generation flow**:

1. VALID mode generates `TokenizedQuery` (list of tokens)
2. Serialize to string for parsing
3. CORRUPT mode operates on `TokenizedQuery` before serializing
4. GARBAGE mode generates random token sequences directly

This avoids the need for a separate SQL lexer and ensures corruptions are applied at token boundaries.

---

## Edge Cases

Explicit handling for parser edge cases:

| Category          | Test Cases                                                               |
|-------------------|--------------------------------------------------------------------------|
| Empty/minimal     | Empty string `""`, single space `" "`, single semicolon `";"`            |
| Single tokens     | Single keyword `"SELECT"`, single identifier `"x"`, single number `"42"` |
| Null bytes        | `"SELECT\0a FROM t"`, `"\0"`                                             |
| Long identifiers  | 1000+ char identifier, 10000+ char string literal                        |
| Unicode           | Emoji in identifiers `"SELECT 🎉 FROM t"`, mixed scripts                 |
| Nesting extremes  | 100+ levels of subquery nesting, 100+ levels of expression parens        |
| Whitespace        | Tabs, newlines, CRLF, multiple spaces, leading/trailing whitespace       |
| Comments          | `"SELECT /* comment */ a"`, nested comments, unterminated comments       |
| Number edge cases | `NaN`, `Infinity`, `-0`, `1e999`, `0x...` with 64+ hex digits            |
| String edge cases | `''` (empty), `''''` (escaped quote), unterminated strings               |

```java
void generateEdgeCases(GeneratorContext ctx) {
    String[] edgeCases = {
            "",
            " ",
            ";",
            "SELECT",
            "42",
            "SELECT " + "x".repeat(10000) + " FROM t",
            "SELECT '\0' FROM t",
            "SELECT a FROM " + "(".repeat(100) + "SELECT 1" + ")".repeat(100),
            // ... more edge cases
    };
// Mix into generation with low probability
}
```

---

## Test Harness

### Result Classification

```java
enum FuzzResult {
    PARSED_OK,           // Parser accepted the query
    SYNTAX_ERROR,        // Parser threw SqlException (expected for invalid SQL)
    CRASH                // Parser threw unexpected exception (BUG!)
}
```

### Test Implementation

```java

@Test
public void fuzzSqlParser() {
    Rnd rnd = TestUtils.generateRandom(LOG);
    SqlFuzzGenerator fuzzer = new SqlFuzzGenerator(rnd, GeneratorConfig.defaults());

    final int iterations = 100_000;
    final long timeoutMs = 1000;  // Per-query timeout

    int parsedOk = 0;
    int syntaxErrors = 0;
    List<FuzzFailure> failures = new ArrayList<>();

    try (SqlCompiler compiler = engine.getSqlCompiler()) {
        for (int i = 0; i < iterations; i++) {
            String sql = fuzzer.generate();
            long seed = fuzzer.getLastSeed();  // For reproduction

            FuzzResult result = testParse(compiler, sql, timeoutMs);

            switch (result) {
                case PARSED_OK:
                    parsedOk++;
                    break;
                case SYNTAX_ERROR:
                    syntaxErrors++;
                    break;
                case CRASH:
                    failures.add(new FuzzFailure(i, seed, sql, result.exception));
                    break;
            }

            // Progress logging
            if (i % 10000 == 0) {
                LOG.info().$("Progress: ").$(i).$("/").$(iterations).$();
            }
        }
    }

// Report results
    LOG.info().$("=== Fuzz Test Results ===").$();
    LOG.info().$("Iterations: ").$(iterations).$();
    LOG.info().$("Parsed OK: ").$(parsedOk).$();
    LOG.info().$("Syntax Errors: ").$(syntaxErrors).$();
    LOG.info().$("FAILURES: ").$(failures.size()).$();

// Report all failures for debugging
    for (FuzzFailure f : failures) {
        LOG.error().$("FAILURE #").$(f.iteration)
                .$(" seed=").$(f.seed)
                .$(" sql=[").$(f.sql).$("]").$();
        LOG.error().$(f.exception).$();
    }

    Assert.assertEquals("Parser crashed on " + failures.size() + " inputs", 0, failures.size());
}

private FuzzResult testParse(SqlCompiler compiler, String sql, long timeoutMs) {
    try {
        // Use appropriate parse method
        compiler.testParseExpression(sql, node -> {
        });
        return FuzzResult.PARSED_OK;
    } catch (SqlException e) {
        return FuzzResult.SYNTAX_ERROR;
    } catch (Throwable t) {
        return FuzzResult.crash(t);
    }
}
```

### Failure Record

```java
class FuzzFailure {
    final int iteration;
    final long seed;
    final String sql;
    final Throwable exception;
    final String generationMode;  // VALID, CORRUPT, GARBAGE

    String toReproductionCode() {
        return String.format(
                "// Reproduction for failure #%d\n" +
                        "Rnd rnd = new Rnd(%dL, %dL);\n" +
                        "String sql = \"%s\";\n",
                iteration, seed, seed, sql.replace("\"", "\\\"")
        );
    }
}
```

---

## Reproducibility

Every generated query must be reproducible from a seed value.

```java
public class SqlFuzzGenerator {
    private long lastSeed0;
    private long lastSeed1;

    public String generate() {
        // Save seed before generation
        lastSeed0 = rnd.getSeed0();
        lastSeed1 = rnd.getSeed1();

        // Generate query...
    }

    public long[] getLastSeeds() {
        return new long[]{lastSeed0, lastSeed1};
    }
}
```

Failures can be reproduced:

```java

@Test
public void reproduceFailure() {
    Rnd rnd = new Rnd(SEED0, SEED1);  // From failure report
    SqlFuzzGenerator fuzzer = new SqlFuzzGenerator(rnd);
    String sql = fuzzer.generate();
// Debug the specific failure...
}
```

---

## File Structure

```
core/src/test/java/io/questdb/test/griffin/fuzz/
├── SqlParserFuzzTest.java          # Main test class
├── SqlFuzzGenerator.java           # Orchestrates generation
├── GeneratorConfig.java            # Configuration
├── GeneratorContext.java           # Runtime context
├── FuzzResult.java                 # Result enum with PARSED_OK, SYNTAX_ERROR, TIMEOUT, CRASH
├── FuzzFailure.java                # Failure record for reproduction
├── SqlToken.java                   # Token representation for corruption
├── TokenizedQuery.java             # Token list with manipulation methods
├── generators/
│   ├── SelectGenerator.java        # SELECT statement
│   ├── CteGenerator.java           # WITH clause
│   ├── JoinGenerator.java          # JOIN clauses
│   ├── SourceGenerator.java        # FROM sources
│   ├── ExpressionGenerator.java    # Expressions
│   ├── SampleByGenerator.java      # SAMPLE BY clause
│   ├── LatestOnGenerator.java      # LATEST ON clause
│   ├── WindowGenerator.java        # Window functions
│   ├── LiteralGenerator.java       # Literals and identifiers
│   ├── InsertGenerator.java        # INSERT statements (Phase 5)
│   ├── CreateTableGenerator.java   # CREATE TABLE (Phase 5)
│   ├── DeclareGenerator.java       # DECLARE statements
│   └── HintGenerator.java          # Query hints /*+ ... */
├── corruption/
│   ├── CorruptionStrategy.java     # Interface
│   ├── TokenCorruptions.java       # Token-level corruptions
│   └── StructuralCorruptions.java  # Clause-level corruptions
├── minimizer/
│   └── SqlMinimizer.java           # Reduces failing inputs to minimal reproducer
└── SQL_PARSER_FUZZ_DESIGN.md       # This document
```

---

## Implementation Phases

### Phase 1: Core Infrastructure

- [ ] GeneratorConfig and GeneratorContext
- [ ] SqlFuzzGenerator skeleton
- [ ] Basic SelectGenerator (columns, FROM, WHERE)
- [ ] Basic ExpressionGenerator (literals, operators)
- [ ] Test harness with result classification

### Phase 2: Standard SQL Features

- [ ] Complete SelectGenerator (GROUP BY, ORDER BY, LIMIT, DISTINCT)
- [ ] CteGenerator
- [ ] JoinGenerator (standard joins)
- [ ] Subquery support in SourceGenerator
- [ ] UNION/EXCEPT/INTERSECT

### Phase 3: QuestDB-Specific Features

- [ ] ASOF/LT/SPLICE joins
- [ ] SampleByGenerator (SAMPLE BY, FILL, ALIGN TO)
- [ ] LatestOnGenerator
- [ ] WindowGenerator
- [ ] QuestDB-specific literals (geohash, IPv4, etc.)

### Phase 4: Corruption Layer

- [ ] Token-level corruptions
- [ ] Structural corruptions
- [ ] Garbage generation

### Phase 5: Refinement

- [ ] Coverage analysis
- [ ] Configuration tuning
- [ ] Performance optimization
- [ ] CI integration

---

## Success Criteria

1. **No crashes**: Parser never throws unexpected exceptions
2. **Coverage**: All QuestDB SQL features exercised
3. **Efficiency**: 100k+ queries/minute generation rate
4. **Reproducibility**: All failures reproducible from seed
5. **CI Integration**: Runs as part of regular test suite

---

## Design Decisions

1. **Parse method**: Use `SqlParser.parse()` directly - isolates parser bugs from later compilation stages
2. **Timeout handling**: Thread with timeout + `TIMEOUT` result type - parser hangs are bugs
3. **Function names**: Start with synthetic names (`func1`, `func2`) - real function names add complexity (arity, types)
   without parser coverage benefit; defer to Phase 5
4. **Table/column names**: Use synthetic names (`t1`, `t2`, `cte1`, `col1`) - keep it simple

### Timeout Implementation

```java
enum FuzzResult {
    PARSED_OK,
    SYNTAX_ERROR,
    TIMEOUT,      // Parser hung - this is a bug!
    CRASH
}

private FuzzResult testParseWithTimeout(SqlParser parser, String sql, long timeoutMs) {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    Future<FuzzResult> future = executor.submit(() -> {
        try {
            parser.parse(sql, sqlExecutionContext);
            return FuzzResult.PARSED_OK;
        } catch (SqlException e) {
            return FuzzResult.SYNTAX_ERROR;
        } catch (Throwable t) {
            return FuzzResult.crash(t);
        }
    });

    try {
        return future.get(timeoutMs, TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
        future.cancel(true);
        return FuzzResult.TIMEOUT;
    } catch (Exception e) {
        return FuzzResult.crash(e);
    } finally {
        executor.shutdownNow();
    }
}
```

**Note**: Thread interruption may not be clean in parsing code. Alternative approach: track wall-clock time and flag
queries that took suspiciously long (>100ms).

### Synthetic Names (Initial)

```java
// Tables: t1, t2, t3, ...
String randomTableName(GeneratorContext ctx) {
    return "t" + (1 + ctx.rnd.nextInt(5));
}

// CTEs: cte1, cte2, cte3, ...
String randomCteName(GeneratorContext ctx) {
    return "cte" + (1 + ctx.rnd.nextInt(ctx.config.maxCteCount));
}

// Columns: col1, col2, ..., ts, sym (for QuestDB-specific)
String randomColumnName(GeneratorContext ctx) {
    if (ctx.rnd.nextDouble() < 0.2) {
        return ctx.rnd.nextBoolean() ? "ts" : "sym";  // Common QuestDB columns
    }
    return "col" + (1 + ctx.rnd.nextInt(10));
}

// Functions: func1, func2, ... (Phase 5: switch to real FunctionFactory names)
String randomFunctionName(GeneratorContext ctx) {
    return "func" + (1 + ctx.rnd.nextInt(20));
}
```

---

## CI Configuration

Configurable iteration count via system property for different CI scenarios:

```java

@Test
public void fuzzSqlParser() {
// Fast mode: 10k iterations (~few seconds) - default for CI
// Overnight mode: 10M+ iterations - scheduled nightly job
    int iterations = Integer.getInteger("questdb.fuzz.iterations", 10_000);

// ...
}
```

### CI Modes

| Mode      | Iterations  | Duration    | Use Case                |
|-----------|-------------|-------------|-------------------------|
| Fast      | 10,000      | ~5 seconds  | Every PR, branch builds |
| Standard  | 100,000     | ~1 minute   | Main branch merges      |
| Thorough  | 1,000,000   | ~10 minutes | Release candidates      |
| Overnight | 10,000,000+ | Hours       | Nightly scheduled job   |

```bash
# Fast mode (default)
mvn test -Dtest=SqlParserFuzzTest

# Overnight mode
mvn test -Dtest=SqlParserFuzzTest -Dquestdb.fuzz.iterations=10000000
```

---

## Minimizer

When a failing input is found, automatically reduce it to a minimal reproducer. Invaluable for debugging.

```java
class SqlMinimizer {
    /**
     * Given a failing SQL query, find the smallest query that still triggers the same failure.
     * Uses delta debugging: binary search over query components.
     */
    String minimize(String failingSql, Predicate<String> stillFails) {
        TokenizedQuery tokens = tokenize(failingSql);

        // Try removing tokens one at a time
        for (int i = tokens.size() - 1; i >= 0; i--) {
            TokenizedQuery reduced = tokens.removeToken(i);
            String sql = reduced.serialize();
            if (stillFails.test(sql)) {
                tokens = reduced;  // Reduction successful
            }
        }

        // Try removing token ranges (binary search)
        int step = tokens.size() / 2;
        while (step >= 1) {
            for (int i = 0; i + step <= tokens.size(); i++) {
                TokenizedQuery reduced = tokens.removeRange(i, i + step);
                String sql = reduced.serialize();
                if (stillFails.test(sql)) {
                    tokens = reduced;
                    i--;  // Retry from same position
                }
            }
            step /= 2;
        }

        return tokens.serialize();
    }
}
```

### Usage

```java
// When a crash is detected:
if(result ==FuzzResult.CRASH){
String minimal = minimizer.minimize(sql, s -> {
    try {
        parser.parse(s, ctx);
        return false;
    } catch (SqlException e) {
        return false;  // Syntax error is OK
    } catch (Throwable t) {
        return t.getClass() == result.exception.getClass();  // Same crash type
    }
});

LOG.

error().

$("Minimal reproducer: ").

$(minimal).

$();
}
```

---

## Coverage Tracking (Phase 5)

Consider tracking which grammar productions are exercised to identify coverage gaps:

```java
class CoverageTracker {
    Map<String, AtomicLong> productionCounts = new ConcurrentHashMap<>();

    void record(String production) {
        productionCounts.computeIfAbsent(production, k -> new AtomicLong()).incrementAndGet();
    }

    void report() {
        LOG.info().$("=== Grammar Coverage ===").$();
        productionCounts.entrySet().stream()
                .sorted(Map.Entry.comparingByValue())
                .forEach(e -> LOG.info().$(e.getKey()).$(" = ").$(e.getValue()).$());

        // Identify productions never exercised
        Set<String> allProductions = getAllGrammarProductions();
        allProductions.removeAll(productionCounts.keySet());
        if (!allProductions.isEmpty()) {
            LOG.warn().$("Never exercised: ").$(allProductions).$();
        }
    }
}
```

---

## Notes

### LATEST BY vs LATEST ON

`LATEST BY` is **deprecated** syntax, kept for backward compatibility:

```sql
-- Deprecated (but still parsed)
SELECT *
FROM trades LATEST BY sym

-- Current syntax
SELECT *
FROM trades LATEST ON ts PARTITION BY sym
```

Both forms should be tested to ensure backward compatibility.

### Range/Slice Operator (`:`)

The `:` operator with precedence 17 is used internally for array slice notation:

```sql
-- Array slice (if/when supported)
SELECT arr[1:3]
FROM t

-- NOT a user-facing operator in most contexts
```

Currently this appears mainly in array access expressions like `col[1:3]`.

### SPLICE JOIN Probability

The default `spliceJoinProb = 0.05` was set conservatively. Since SPLICE JOIN is a distinct QuestDB feature worth
exercising thoroughly, consider:

```java
// Increase for better coverage of QuestDB-specific joins
double spliceJoinProb = 0.15;  // Was 0.05

// Or use a dedicated "QuestDB mode" with higher probabilities for QuestDB features
GeneratorConfig questdbFocused() {
    return new GeneratorConfig()
            .setAsofJoinProb(0.25)
            .setLtJoinProb(0.20)
            .setSpliceJoinProb(0.15)
            .setSampleByProb(0.30)
            .setLatestOnProb(0.20);
}
```

### Seed Consistency

The `Rnd` class uses two 64-bit seeds. Both must be captured and restored for exact reproduction:

```java
// CORRECT
long[] seeds = fuzzer.getLastSeeds();  // Returns [seed0, seed1]
Rnd rnd = new Rnd(seeds[0], seeds[1]);

// WRONG - loses precision
long seed = fuzzer.getLastSeed();  // Only one seed - cannot reproduce!
```

The `FuzzFailure.toReproductionCode()` method should use both seeds.

---

## References

- [SQLsmith](https://github.com/anse1/sqlsmith) - Grammar-based SQL fuzzer for PostgreSQL
- [SQLancer](https://github.com/sqlancer/sqlancer) - Detecting logic bugs in DBMS
- Existing QuestDB fuzz tests:
- `ExpressionParserFuzzTest.java` - Expression parsing
- `CreateTableFuzzTest.java` - DDL fuzzing
- `JsonExecuteApiFuzzTest.java` - HTTP API fuzzing