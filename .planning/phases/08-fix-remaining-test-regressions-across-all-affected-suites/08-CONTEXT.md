# Phase 8 Context: Fix Remaining Test Regressions

## Domain Boundary

Fix 81 test failures across 7 suites caused by the fast-path fill cursor changes. No production code changes — test-only fixes using Phase 5 patterns.

## Decisions

### Fix strategies (same as Phase 5)

| Category | Count | Fix |
|----------|-------|-----|
| Plan text mismatch | ~15 | Update expected plan strings (Sample By → Sample By Fill + Async Group By) |
| "Should have failed" | ~4 | Replace with positive tests if functionality now works |
| Error message text | ~2 | Update expected error strings |
| Factory class mismatch | 3 | Update class assertions (old cursor factories → new fill + sort chain) |
| Parse tree mismatch | 4 | Update expected parse model structure |
| Metadata (timestamp index, random access) | 50 | Switch to assertSql or update expected values |
| Data/plan other | ~3 | Investigate individually |

### SampleByNanoTimestampTest (50 failures)

Same metadata fixes as SampleByTest Phase 5: timestamp index = -1 and random access mismatches. The nano test file mirrors SampleByTest but uses nanosecond timestamps. Apply the same assertSql/metadata fixes.

### RecordCursorMemoryUsageTest (3 failures)

Tests check exact factory class types. Old: `SampleByFillValueRecordCursorFactory`, `SampleByFillNullRecordCursorFactory`, `SampleByFillPrevRecordCursorFactory`. New: queries now go through GROUP BY → sort → fill chain, producing `SelectedRecordCursorFactory` wrapper. Update assertions to match new factory chain.

### No production code changes

All fixes are in test files. The production code is correct — tests just need to match the new execution plan structure.

## Canonical References

- `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByNanoTimestampTest.java`
- `core/src/test/java/io/questdb/test/griffin/ExplainPlanTest.java`
- `core/src/test/java/io/questdb/test/griffin/SqlOptimiserTest.java`
- `core/src/test/java/io/questdb/test/griffin/RecordCursorMemoryUsageTest.java`
- `core/src/test/java/io/questdb/test/griffin/SqlParserTest.java`
- `core/src/test/java/io/questdb/test/griffin/engine/functions/groupby/FirstArrayGroupByFunctionFactoryTest.java`
- `core/src/test/java/io/questdb/test/griffin/engine/functions/groupby/LastArrayGroupByFunctionFactoryTest.java`
