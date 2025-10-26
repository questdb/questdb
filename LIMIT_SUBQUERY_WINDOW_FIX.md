# Fix for "LIMIT in subquery misapplied when outer window function is used"

## Issue Description

When a SQL query contains a subquery with a LIMIT clause followed by window functions in the outer query, the LIMIT was being incorrectly moved from the subquery to the outer window model. This caused the LIMIT to be applied to the window function results instead of the subquery results.

### Example of the Bug

```sql
-- This query should apply LIMIT 1000 to the subquery first, then apply the window function
SELECT 
    *,
    row_number() OVER (PARTITION BY category ORDER BY value)
FROM (
    SELECT * FROM table ORDER BY id LIMIT 1000
);
```

**Expected behavior**: 
1. Apply LIMIT 1000 to the subquery, returning 1000 rows
2. Apply the window function to those 1000 rows
3. Final result: 1000 rows with window function values

**Actual behavior (before fix)**: 
1. Apply window function to all rows in the table  
2. Apply LIMIT 1000 to the window function results
3. Final result: Could be more or fewer rows than expected

## Root Cause

In `SqlOptimiser.java` at line 6466, the optimizer was unconditionally moving LIMIT clauses from subqueries to window models:

```java
windowModel.moveLimitFrom(limitSource);
```

This happened during the query optimization phase when window models were being constructed.

## Solution

The fix adds logic to detect when a LIMIT clause should remain with a subquery rather than being moved to a window model:

```java
// Fix for "limit in subquery misapplied when outer window function is used"
boolean shouldMoveLimit = true;
if (limitSource != null && limitSource.getLimitLo() != null) {
    // Check if this is a case where the limit should stay with the subquery
    // Use the nestedModelIsSubQuery flag to properly identify subqueries that should retain their limits
    if (limitSource.isNestedModelIsSubQuery() || 
        (limitSource != model && limitSource.getNestedModel() != null)) {
        // The limitSource represents a subquery that should retain its limit
        shouldMoveLimit = false;
    }
}

if (shouldMoveLimit) {
    windowModel.moveLimitFrom(limitSource);
    limitSource = windowModel;
}
```

## Detection Logic

The fix uses two conditions to identify when a LIMIT should stay with a subquery:

1. `limitSource.isNestedModelIsSubQuery()` - Uses the explicit subquery flag
2. `limitSource != model && limitSource.getNestedModel() != null` - Detects nested query structures

## Test Cases

Several test cases were added to verify the fix:

1. **Basic functionality**: Verify that LIMIT in subquery is applied before window functions
2. **Sign change test**: Verify that changing LIMIT sign affects results (proving it's applied to subquery)
3. **Complex window functions**: Test with multiple window functions and complex subqueries
4. **Query plan verification**: Ensure the query plan shows correct LIMIT placement

## Files Modified

- `core/src/main/java/io/questdb/griffin/SqlOptimiser.java` - Applied the fix
- `core/src/test/java/io/questdb/test/griffin/engine/window/WindowLimitSubqueryBugTest.java` - Basic tests
- `core/src/test/java/io/questdb/test/griffin/engine/window/WindowLimitSubqueryFixTest.java` - Comprehensive tests

## Impact

This fix ensures that:
- LIMIT clauses in subqueries are respected and applied before window functions
- Query results are consistent and predictable
- Complex analytical queries with window functions work correctly
- No regression in existing functionality (LIMIT is still moved when appropriate)