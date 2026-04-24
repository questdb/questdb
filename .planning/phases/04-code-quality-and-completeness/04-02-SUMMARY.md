---
phase: 04-code-quality-and-completeness
plan: 02
subsystem: sql
tags: [percentile, quantile, groupby, window-functions, function-registry, explain]

# Dependency graph
requires:
  - phase: 04-01
    provides: Shared DoubleSort utility extracted from duplicated sort implementations
provides:
  - toPlan() output includes percentile argument in all 5 group-by functions
  - 10 group-by factory registrations in function_list.txt
  - 4 new quantile_disc/quantile_cont window alias factory files
  - 4 window alias registrations in function_list.txt
affects: [testing, sql]

# Tech tracking
tech-stack:
  added: []
  patterns: [thin delegating factory for function aliases, toPlan includes all arguments]

key-files:
  created:
    - core/src/main/java/io/questdb/griffin/engine/functions/window/QuantileDiscDoubleWindowFunctionFactory.java
    - core/src/main/java/io/questdb/griffin/engine/functions/window/QuantileContDoubleWindowFunctionFactory.java
    - core/src/main/java/io/questdb/griffin/engine/functions/window/MultiQuantileDiscDoubleWindowFunctionFactory.java
    - core/src/main/java/io/questdb/griffin/engine/functions/window/MultiQuantileContDoubleWindowFunctionFactory.java
  modified:
    - core/src/main/java/io/questdb/griffin/engine/functions/groupby/PercentileDiscDoubleGroupByFunction.java
    - core/src/main/java/io/questdb/griffin/engine/functions/groupby/PercentileDiscLongGroupByFunction.java
    - core/src/main/java/io/questdb/griffin/engine/functions/groupby/PercentileContDoubleGroupByFunction.java
    - core/src/main/java/io/questdb/griffin/engine/functions/groupby/MultiPercentileDiscDoubleGroupByFunction.java
    - core/src/main/java/io/questdb/griffin/engine/functions/groupby/MultiPercentileContDoubleGroupByFunction.java
    - core/src/main/resources/function_list.txt

key-decisions:
  - "Multi-percentile functions use percentileFunc (singular) field name, not percentilesFunc as plan estimated"
  - "Window alias factories use delegation pattern matching existing QuantileDiscDoubleGroupByFunctionFactory approach"

patterns-established:
  - "Thin delegating factory: alias factories instantiate delegate and forward newInstance() calls"
  - "toPlan() convention: include all function arguments separated by commas for complete EXPLAIN output"

requirements-completed: [QUAL-02, REG-01, REG-02]

# Metrics
duration: 5min
completed: 2026-04-13
---

# Phase 4 Plan 2: toPlan Fixes, Factory Registration, and Window Aliases Summary

**Fixed EXPLAIN output for 5 group-by percentile functions, registered 10 group-by factories, and created 4 quantile window alias factories**

## Performance

- **Duration:** 5 min
- **Started:** 2026-04-13T16:04:50Z
- **Completed:** 2026-04-13T16:10:00Z
- **Tasks:** 2
- **Files modified:** 10

## Accomplishments
- All 5 group-by percentile toPlan() methods now include the percentile argument, producing output like `percentile_disc(value,0.5)` instead of `percentile_disc(value)`
- Registered all 10 missing group-by factories (5 percentile_disc/cont + 5 quantile_disc/cont aliases) in function_list.txt so percentile and quantile group-by functions resolve from SQL
- Created 4 new window alias factory files that delegate to canonical Percentile* window factories, enabling `quantile_disc()` and `quantile_cont()` as window function SQL aliases

## Task Commits

Each task was committed atomically:

1. **Task 1: Fix toPlan() in all 5 group-by percentile functions** - `3b4a0c4c42` (fix)
2. **Task 2: Register 10 group-by factories and create + register 4 window alias factories** - `f88119f838` (feat)

## Files Created/Modified
- `core/src/main/java/io/questdb/griffin/engine/functions/groupby/PercentileDiscDoubleGroupByFunction.java` - toPlan() includes percentileFunc argument
- `core/src/main/java/io/questdb/griffin/engine/functions/groupby/PercentileDiscLongGroupByFunction.java` - toPlan() includes percentileFunc argument
- `core/src/main/java/io/questdb/griffin/engine/functions/groupby/PercentileContDoubleGroupByFunction.java` - toPlan() includes percentileFunc argument
- `core/src/main/java/io/questdb/griffin/engine/functions/groupby/MultiPercentileDiscDoubleGroupByFunction.java` - toPlan() includes percentileFunc argument
- `core/src/main/java/io/questdb/griffin/engine/functions/groupby/MultiPercentileContDoubleGroupByFunction.java` - toPlan() includes percentileFunc argument
- `core/src/main/java/io/questdb/griffin/engine/functions/window/QuantileDiscDoubleWindowFunctionFactory.java` - Window alias factory delegating to PercentileDiscDoubleWindowFunctionFactory
- `core/src/main/java/io/questdb/griffin/engine/functions/window/QuantileContDoubleWindowFunctionFactory.java` - Window alias factory delegating to PercentileContDoubleWindowFunctionFactory
- `core/src/main/java/io/questdb/griffin/engine/functions/window/MultiQuantileDiscDoubleWindowFunctionFactory.java` - Window alias factory delegating to MultiPercentileDiscDoubleWindowFunctionFactory
- `core/src/main/java/io/questdb/griffin/engine/functions/window/MultiQuantileContDoubleWindowFunctionFactory.java` - Window alias factory delegating to MultiPercentileContDoubleWindowFunctionFactory
- `core/src/main/resources/function_list.txt` - Added 14 factory registrations (5 percentile group-by + 5 quantile group-by + 4 quantile window)

## Decisions Made
- Multi-percentile classes use `percentileFunc` (singular) as the field name, not `percentilesFunc` as the plan estimated. Verified by reading constructor code.
- Window alias factories follow the same thin delegation pattern as the existing `QuantileDiscDoubleGroupByFunctionFactory` group-by alias, instantiating the canonical factory and forwarding `newInstance()` calls.

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
None.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- All percentile/quantile functions now discoverable via function_list.txt registration
- EXPLAIN output for percentile group-by functions now includes the percentile argument
- quantile_disc/quantile_cont aliases work in both group-by and window contexts

## Self-Check: PASSED

All 4 created window alias factory files exist. Both task commits (3b4a0c4c42, f88119f838) verified in git log. SUMMARY.md exists at expected path.

---
*Phase: 04-code-quality-and-completeness*
*Completed: 2026-04-13*
