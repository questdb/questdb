# Fix for Issue #5820: Ensure function.cursorClosed() is called for all cursor types

## 📋 Issue Summary

**Issue:** The `function.cursorClosed()` lifecycle method, introduced in #4633, was only triggered for virtual record cursors. Other cursor types, especially those used in GROUP BY operations, were not calling this method, causing memory leaks and incomplete cleanup.

**Specific Problem:** `JsonExtractFunction.cursorClosed()` was never invoked in tests like `ParallelGroupByFuzzTest#testParallelJsonKeyGroupBy()`, leading to memory leaks when using JSON functions in GROUP BY operations.

## 🔧 Root Cause Analysis

The issue was found in multiple GROUP BY cursor implementations that were calling `Misc.clearObjList(groupByFunctions)` to clear function lists but were **not** calling `function.cursorClosed()` on each function before clearing them.

### Comparison: Working vs Broken Pattern

**✅ Working Pattern (AbstractVirtualFunctionRecordCursor):**
```java
@Override
public void close() {
    baseCursor = Misc.free(baseCursor);
    for (int i = 0, n = functions.size(); i < n; i++) {
        functions.getQuick(i).cursorClosed(); // ✅ Properly calls cursorClosed()
    }
}
```

**❌ Broken Pattern (GROUP BY cursors before fix):**
```java
@Override
public void close() {
    if (isOpen) {
        isOpen = false;
        Misc.free(dataMap);
        Misc.clearObjList(groupByFunctions); // ❌ Missing cursorClosed() calls
        super.close();
    }
}
```

## 🚀 Solution Implemented

Added proper `function.cursorClosed()` calls to all affected GROUP BY cursor types before clearing the function lists.

### Fixed Cursor Types

1. **GroupByRecordCursor** in `GroupByRecordCursorFactory.java`
2. **GroupByNotKeyedRecordCursor** in `GroupByNotKeyedRecordCursorFactory.java`
3. **AsyncGroupByRecordCursor** in `AsyncGroupByRecordCursorFactory.java`
4. **AsyncGroupByNotKeyedRecordCursor** in `AsyncGroupByNotKeyedRecordCursorFactory.java`
5. **SampleByInterpolateRecordCursor** in `SampleByInterpolateRecordCursorFactory.java`
6. **VirtualFunctionSkewedSymbolRecordCursor** (affects multiple factories)
7. **AbstractNoRecordSampleByCursor** (base class for sample cursors)

### Example Fix Applied

**Before:**
```java
@Override
public void close() {
    if (isOpen) {
        isOpen = false;
        Misc.free(dataMap);
        Misc.free(allocator);
        Misc.clearObjList(groupByFunctions); // ❌ Missing cursorClosed()
        super.close();
    }
}
```

**After:**
```java
@Override
public void close() {
    if (isOpen) {
        isOpen = false;
        Misc.free(dataMap);
        Misc.free(allocator);
        // Notify functions that their associated cursor has been closed
        for (int i = 0, n = groupByFunctions.size(); i < n; i++) {
            groupByFunctions.getQuick(i).cursorClosed(); // ✅ Proper cleanup
        }
        Misc.clearObjList(groupByFunctions);
        super.close();
    }
}
```

## 🧪 Testing & Impact

### Functions That Benefit From This Fix

1. **JsonExtractFunction** - Properly deflates internal state via `cursorClosed()`
2. **Window Functions** - Cleanup argument functions via `cursorClosed()`
3. **UnaryFunction, BinaryFunction, TernaryFunction** - Recursively call `cursorClosed()` on arguments
4. **Custom Functions** - Any function implementing `cursorClosed()` for cleanup

### Expected Impact

- **Memory Leaks Fixed:** Functions like `JsonExtractFunction` properly release memory
- **Resource Cleanup:** All functions get proper cleanup notification
- **Test Stability:** `ParallelGroupByFuzzTest#testParallelJsonKeyGroupBy` should run more reliably
- **Performance:** Reduced memory pressure in GROUP BY operations with complex functions

### Verification Steps

1. **Compile Test:** `mvn compile` - Ensures no syntax errors
2. **Run ParallelGroupByFuzzTest:** Specifically the `testParallelJsonKeyGroupBy` method
3. **Memory Profile:** Check for reduced memory leaks in GROUP BY operations with JSON functions
4. **Integration Tests:** Run full test suite to ensure no regressions

## 📁 Files Modified

```
core/src/main/java/io/questdb/griffin/engine/groupby/
├── GroupByRecordCursorFactory.java
├── GroupByNotKeyedRecordCursorFactory.java
├── SampleByInterpolateRecordCursorFactory.java
├── VirtualFunctionSkewedSymbolRecordCursor.java
└── AbstractNoRecordSampleByCursor.java

core/src/main/java/io/questdb/griffin/engine/table/
├── AsyncGroupByRecordCursor.java
└── AsyncGroupByNotKeyedRecordCursor.java
```

## 🔄 Backward Compatibility

This fix is **100% backward compatible**:
- Only adds missing functionality
- No API changes
- No behavior changes for existing working code
- Pure bug fix with no breaking changes

## 🎯 Related Issues

- **Closes:** #5820
- **Related:** #4633 (Original cursorClosed() implementation)
- **Improves:** ParallelGroupByFuzzTest reliability
- **Fixes:** Memory leaks in GROUP BY operations with complex functions

---

**Summary:** This fix ensures that all cursor types properly notify their functions when closing, not just virtual record cursors, preventing memory leaks and ensuring proper resource cleanup in GROUP BY operations.
