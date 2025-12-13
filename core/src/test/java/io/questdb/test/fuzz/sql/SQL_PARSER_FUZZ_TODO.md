# SQL Parser Fuzz Testing - Implementation TODO

**Status**: In Progress
**Last Updated**: 2025-12-14

---

## Ground Rules

1. **Iterative development**: Start with basic infrastructure, test each step before proceeding
2. **No workarounds, shortcuts, or hacks**: Proper implementation or not at all
3. **Design for maintainability**: This will grow; clean code and clear abstractions are critical
4. **Test-driven**: Each component should be testable in isolation

---

## Implementation Phases

### Phase 1: Core Infrastructure ✅

Foundation classes that everything else builds on.

- [x] **1.1 GeneratorConfig** - Configuration class with all generation parameters
    - All probability settings (cteProb, joinProb, etc.)
    - Cardinality limits (maxDepth, maxCteCount, etc.)
    - Mode weights (valid/corrupt/garbage)
    - Builder pattern for fluent configuration
    - `defaults()`, `questdbFocused()`, `simple()` factory methods
    - Unit tests: `GeneratorConfigTest.java`

- [x] **1.2 GeneratorContext** - Runtime context for generation
    - Holds Rnd, config, TokenizedQuery
    - Depth tracking (query depth, expression depth)
    - Scope tracking (cteNames, tableAliases, columnNames)
    - Token building helpers: keyword(), identifier(), literal(), operator(), punctuation()
    - Name generators: newTableName(), randomColumnName(), randomFunctionName()
    - `shouldRecurseQuery()` and `shouldRecurseExpression()` methods
    - `childContext()` for scoped recursion
    - Unit tests: `GeneratorContextTest.java`

- [x] **1.3 SqlToken and TokenizedQuery** - Token representation for corruption
    - SqlToken: enum Type (KEYWORD, IDENTIFIER, LITERAL, OPERATOR, PUNCTUATION), value, factory methods
    - TokenizedQuery: token list, serialize(), manipulation methods (removeToken, swapTokens, insertToken, replaceToken, duplicateToken, truncateAt)
    - Proper spacing rules in serialization
    - Unit tests: `SqlTokenTest.java`, `TokenizedQueryTest.java`

- [x] **1.4 FuzzResult and FuzzFailure** - Result classification
    - FuzzResult: PARSED_OK, SYNTAX_ERROR, TIMEOUT, CRASH with singleton instances
    - FuzzResult.crash(Throwable), syntaxError(message) factories
    - FuzzFailure: iteration, seed0, seed1, sql, result, generationMode
    - `toReproductionCode()` with proper string escaping
    - `toSummary()` for logging
    - Unit tests: `FuzzResultTest.java`, `FuzzFailureTest.java`

- [x] **1.5 SqlFuzzGenerator skeleton** - Main orchestrator
    - Constructor with Rnd and GeneratorConfig
    - `generate()` and `generateTokenized()` methods
    - Seed capture: `getLastSeeds()` returning long[2]
    - Mode selection based on weights
    - Placeholder generation (simple SELECT statements)
    - Basic corruption support
    - Garbage generation (random tokens)
    - Unit tests: `SqlFuzzGeneratorTest.java`

- [x] **1.6 SqlParserFuzzTest skeleton** - Test harness
    - Main fuzz test with configurable iterations
    - Mode-specific tests: validOnly, corruptOnly, garbageOnly, questdbFocused
    - Result classification loop with progress logging
    - Failure collection and detailed reporting
    - Seed-based reproduction test
    - System property configuration: `questdb.fuzz.iterations`, `questdb.fuzz.timeout`

### Phase 2: Basic Generation ✅

Minimal viable generation - simple SELECT statements.

- [x] **2.1 LiteralGenerator** - Generates literals
    - Integer literals (including edge cases: 0, negatives, Long.MAX_VALUE)
    - Float literals (including NaN, Infinity, -0)
    - String literals (including empty, escaped quotes)
    - Boolean literals (true, false)
    - Null literal
    - Timestamp literals
    - Unit tests: `LiteralGeneratorTest.java` (17 tests)

- [x] **2.2 ExpressionGenerator (basic)** - Simple expressions
    - Literal expressions (via LiteralGenerator)
    - Column references (qualified and unqualified)
    - Parenthesized expressions
    - Binary operators (+, -, *, /, =, !=, <, >, <=, >=, AND, OR)
    - Unary operators (-, NOT)
    - Function calls (basic)
    - Depth control via expressionDepth
    - Specialized generators: generateBooleanExpression, generateComparisonExpression, generateArithmeticExpression
    - Unit tests: `ExpressionGeneratorTest.java` (17 tests)

- [x] **2.3 SelectGenerator (basic)** - Simple SELECT
    - SELECT column list (expressions with aliases)
    - SELECT * (star)
    - FROM single table (with optional alias)
    - Optional WHERE clause
    - Token-based generation (builds TokenizedQuery)
    - Subquery support (basic)
    - Unit tests: `SelectGeneratorTest.java` (16 tests)

- [x] **2.4 Basic parsing test** - End-to-end validation
    - `testBasicGeneratorIntegration` in `SqlParserFuzzTest.java`
    - Generates 1000 queries using SelectGenerator
    - Parses with SqlCompiler.testCompileModel()
    - Verifies no crashes (unexpected exceptions)
    - Uses simple config for CI compatibility

### Phase 3: Standard SQL Features ✅

Complete standard SQL coverage.

- [x] **3.1 SelectGenerator (complete)**
    - DISTINCT
    - GROUP BY clause
    - ORDER BY clause (ASC/DESC, NULLS FIRST/LAST)
    - LIMIT/OFFSET (various forms)
    - JOIN integration
    - Note: QuestDB does not support HAVING
    - Unit tests: `SelectGeneratorTest.java` (24 tests)

- [ ] **3.2 SourceGenerator** - FROM clause sources (deferred to Phase 6)
    - Table reference with optional alias ✅ (in SelectGenerator)
    - Subquery as source (basic support added)
    - CTE reference
    - Parenthesized source

- [ ] **3.3 CteGenerator** - WITH clause (deferred to Phase 6)
    - Single CTE
    - Multiple CTEs
    - Scope registration (cte names available for main query)

- [x] **3.4 JoinGenerator (standard)** - Standard joins
    - INNER JOIN
    - LEFT [OUTER] JOIN
    - RIGHT [OUTER] JOIN
    - CROSS JOIN
    - JOIN ... ON condition
    - JOIN ... USING (columns)
    - Unit tests: `JoinGeneratorTest.java` (14 tests)

- [ ] **3.5 Set operations** (deferred to Phase 6)
    - UNION / UNION ALL
    - EXCEPT / EXCEPT ALL
    - INTERSECT / INTERSECT ALL

- [x] **3.6 ExpressionGenerator (complete)**
    - CASE WHEN ... THEN ... ELSE ... END
    - CAST(expr AS type)
    - IN (list), NOT IN
    - BETWEEN ... AND ..., NOT BETWEEN
    - LIKE, NOT LIKE
    - IS NULL / IS NOT NULL
    - Unit tests: `ExpressionGeneratorTest.java` (25 tests)

- [x] **3.7 Comprehensive parsing test**
    - `testBasicGeneratorIntegration` in `SqlParserFuzzTest.java`
    - Generates queries with all Phase 3 features
    - 1000 iterations (configurable)

### Phase 4: QuestDB-Specific Features ✅

QuestDB extensions to standard SQL.

- [x] **4.1 JoinGenerator (QuestDB)**
    - ASOF JOIN
    - LT JOIN
    - SPLICE JOIN
    - TOLERANCE clause
    - ON clause with timestamp equality
    - Unit tests: `JoinGeneratorTest.java` (24 tests total)

- [x] **4.2 SampleByGenerator**
    - SAMPLE BY interval (1h, 30m, etc.)
    - FILL clause (PREV, NULL, LINEAR, NONE, value list)
    - ALIGN TO CALENDAR
    - ALIGN TO FIRST OBSERVATION
    - TIME ZONE clause
    - WITH OFFSET clause
    - FROM/TO time range
    - Unit tests: `SampleByGeneratorTest.java` (15 tests)

- [x] **4.3 LatestOnGenerator**
    - LATEST ON timestamp PARTITION BY columns
    - LATEST BY (deprecated syntax)
    - Unit tests: `LatestOnGeneratorTest.java` (8 tests)

- [ ] **4.4 WindowGenerator** (deferred - complex feature)
    - Basic OVER clause
    - PARTITION BY
    - ORDER BY within window
    - Frame modes: ROWS, RANGE, GROUPS
    - Frame bounds: UNBOUNDED PRECEDING, CURRENT ROW, n PRECEDING/FOLLOWING
    - BETWEEN frame bounds
    - EXCLUDE clause
    - IGNORE NULLS / RESPECT NULLS
    - Unit tests

- [x] **4.5 LiteralGenerator (QuestDB types)**
    - Geohash char literals (#sp052w)
    - Geohash bit literals (##01110)
    - Geohash with precision (#sp052w/25)
    - IPv4 literals
    - UUID literals
    - Long256 literals
    - Unit tests: `LiteralGeneratorTest.java` (23 tests total)

- [ ] **4.6 ExpressionGenerator (QuestDB)** (deferred)
    - IP operators (<<, >>, <<=, >>=)
    - WITHIN (geohash)
    - timestamp() function
    - AT TIME ZONE
    - extract() function
    - Unit tests

- [ ] **4.7 Additional statements** (deferred)
    - INSERT INTO ... SELECT
    - INSERT INTO ... VALUES
    - INSERT BATCH / ATOMIC
    - EXPLAIN (with FORMAT)
    - Unit tests

- [x] **4.8 SelectGenerator integration**
    - Integrated SAMPLE BY into SELECT generation
    - Integrated LATEST ON into SELECT generation
    - Integrated QuestDB joins

### Phase 5: Corruption Layer ⏳

Invalid query generation for error handling testing.

- [ ] **5.1 CorruptionStrategy interface**
    - Interface definition
    - Base implementation with TokenizedQuery
    - Unit tests

- [ ] **5.2 Token-level corruptions**
    - DropTokenStrategy
    - SwapTokensStrategy
    - DuplicateTokenStrategy
    - InjectKeywordStrategy
    - TruncateStrategy
    - CharacterInsertStrategy
    - Unit tests for each

- [ ] **5.3 Structural corruptions**
    - UnbalanceParensStrategy
    - ClauseReorderStrategy
    - DuplicateClauseStrategy
    - MixKeywordsStrategy
    - Unit tests

- [ ] **5.4 GarbageGenerator**
    - Random token sequences
    - Random characters
    - Edge cases (empty, null bytes, long strings)
    - Unit tests

- [ ] **5.5 CorruptGenerator**
    - Select random corruption strategy
    - Apply to valid TokenizedQuery
    - Configurable corruption count
    - Unit tests

- [ ] **5.6 Complete fuzz test**
    - All three modes: VALID, CORRUPT, GARBAGE
    - Mode selection based on weights
    - Full iteration count test

### Phase 6: Advanced Features ⏳

Refinements and tooling.

- [ ] **6.1 SqlMinimizer**
    - Token-based reduction
    - Delta debugging algorithm
    - Same-failure predicate
    - Integration with test harness
    - Unit tests

- [ ] **6.2 Timeout handling**
    - Thread-based timeout execution
    - TIMEOUT result classification
    - Configurable timeout duration
    - Unit tests

- [ ] **6.3 Edge case generation**
    - Dedicated edge case generator
    - Empty/minimal inputs
    - Extreme nesting
    - Unicode/special characters
    - Very long identifiers/strings
    - Unit tests

- [ ] **6.4 Real function names**
    - Extract function names from FunctionFactory
    - Function arity awareness (optional)
    - Gradual replacement of synthetic names
    - Unit tests

- [ ] **6.5 Coverage tracking**
    - CoverageTracker class
    - Production counting
    - Gap identification
    - Report generation
    - Unit tests

- [ ] **6.6 CI integration**
    - Configurable iteration count via system property
    - Fast mode (10k) for PRs
    - Thorough mode (1M) for releases
    - Overnight mode for nightly jobs
    - Documentation

---

## Current Progress

### Completed

- [x] Design document created (`SQL_PARSER_FUZZ_DESIGN.md`)
- [x] Cross-checked with SqlParser and ExpressionParser
- [x] TODO document created
- [x] **Phase 1: Core Infrastructure** - All 6 components implemented and tested
    - GeneratorConfig, GeneratorContext, SqlToken, TokenizedQuery
    - FuzzResult, FuzzFailure
    - SqlFuzzGenerator, SqlParserFuzzTest
- [x] **Phase 2: Basic Generation** - All 4 components implemented and tested
    - LiteralGenerator (17 tests)
    - ExpressionGenerator (17 tests → 25 tests after Phase 3)
    - SelectGenerator (16 tests → 24 tests after Phase 3)
    - Basic parsing integration test
- [x] **Phase 3: Standard SQL Features** - Core features implemented
    - SelectGenerator enhanced: DISTINCT, GROUP BY, ORDER BY, LIMIT/OFFSET, JOINs
    - JoinGenerator (14 tests): INNER, LEFT, RIGHT, CROSS, ON, USING
    - ExpressionGenerator enhanced: CASE, CAST, IN, BETWEEN, LIKE, IS NULL
    - Total: 80 generator tests passing
- [x] **Phase 4: QuestDB-Specific Features** - Core QuestDB features implemented
    - JoinGenerator: ASOF, LT, SPLICE joins with TOLERANCE (24 tests total)
    - SampleByGenerator: SAMPLE BY, FILL, ALIGN TO (15 tests)
    - LatestOnGenerator: LATEST ON, LATEST BY deprecated (8 tests)
    - LiteralGenerator: Geohash, IPv4, UUID, Long256 (23 tests total)
    - SelectGenerator: Integrated SAMPLE BY and LATEST ON
    - Total: 119 generator tests passing

### In Progress

- [ ] Phase 5: Corruption Layer (next step)

### Bugs Found by Fuzzer

The fuzzer has already found real bugs in QuestDB's SQL parser:

1. **ClassCastException in ExpressionParser** - Triggered by malformed SQL patterns like:
   - `=.column` (dot after operator without left operand)
   - `AND.col` (keyword followed immediately by dot)
   - These patterns cause `ClassCastException: String cannot be cast to FloatingSequence`

### Blocked

(none)

### Files Created

```
core/src/test/java/io/questdb/test/fuzz/sql/
├── GeneratorConfig.java          # Configuration
├── GeneratorConfigTest.java      # Tests
├── GeneratorContext.java         # Runtime context
├── GeneratorContextTest.java     # Tests
├── SqlToken.java                 # Token representation
├── SqlTokenTest.java             # Tests
├── TokenizedQuery.java           # Token list with manipulation
├── TokenizedQueryTest.java       # Tests
├── FuzzResult.java               # Result classification
├── FuzzResultTest.java           # Tests
├── FuzzFailure.java              # Failure record
├── FuzzFailureTest.java          # Tests
├── SqlFuzzGenerator.java         # Main generator
├── SqlFuzzGeneratorTest.java     # Tests
├── SqlParserFuzzTest.java        # Test harness
├── SQL_PARSER_FUZZ_DESIGN.md     # Design document
├── SQL_PARSER_FUZZ_TODO.md       # This file
└── generators/
    ├── LiteralGenerator.java         # Literal generation (Geohash, IPv4, UUID, Long256)
    ├── LiteralGeneratorTest.java     # Tests (23)
    ├── ExpressionGenerator.java      # Expression generation (CASE, CAST, IN, BETWEEN, LIKE, IS NULL)
    ├── ExpressionGeneratorTest.java  # Tests (25)
    ├── SelectGenerator.java          # SELECT with SAMPLE BY, LATEST ON
    ├── SelectGeneratorTest.java      # Tests (24)
    ├── JoinGenerator.java            # JOIN clause generation (ASOF, LT, SPLICE, TOLERANCE)
    ├── JoinGeneratorTest.java        # Tests (24)
    ├── SampleByGenerator.java        # SAMPLE BY clause generation
    ├── SampleByGeneratorTest.java    # Tests (15)
    ├── LatestOnGenerator.java        # LATEST ON clause generation
    └── LatestOnGeneratorTest.java    # Tests (8)
```

---

## Testing Checklist

Each component should have:

- [ ] Unit tests covering normal operation
- [ ] Unit tests covering edge cases
- [ ] Integration test showing it works with the rest of the system

---

## Notes

- **Iteration count**: Start with small numbers (100-1000) during development
- **Seed preservation**: Always log seeds for reproducibility
- **Parser method**: Use `SqlParser.parse()` directly
- **Synthetic names**: Use t1, t2, cte1, col1, func1 etc. initially