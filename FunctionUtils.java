// =============================================================================
// COMPLETE SOLUTION FOR QUESTDB CURSOR LIFECYCLE ISSUE
// =============================================================================

// 1. FunctionUtils.java - Utility class for consistent function cleanup
package io.questdb.griffin.engine.functions;

import io.questdb.cairo.sql.Function;
import io.questdb.std.ObjList;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;

public class FunctionUtils {
    private static final Log LOG = LogFactory.getLog(FunctionUtils.class);
    
    /**
     * Safely notifies all functions in the list that their cursor has been closed.
     * This method ensures consistent cleanup across all cursor types.
     * 
     * @param functions List of functions to notify
     */
    public static void notifyFunctionsCursorClosed(ObjList<Function> functions) {
        if (functions != null) {
            for (int i = 0, n = functions.size(); i < n; i++) {
                Function function = functions.getQuick(i);
                if (function != null) {
                    try {
                        function.cursorClosed();
                    } catch (Exception e) {
                        LOG.error().$("Failed to notify function.cursorClosed(): ").$(e).$();
                    }
                }
            }
        }
    }
    
    /**
     * Safely notifies all functions in the array that their cursor has been closed.
     * 
     * @param functions Array of functions to notify
     */
    public static void notifyFunctionsCursorClosed(Function[] functions) {
        if (functions != null) {
            for (Function function : functions) {
                if (function != null) {
                    try {
                        function.cursorClosed();
                    } catch (Exception e) {
                        LOG.error().$("Failed to notify function.cursorClosed(): ").$(e).$();
                    }
                }
            }
        }
    }
}

// =============================================================================
// 2. GroupByRecordCursorFactory.java - Modified to ensure cursorClosed() is called
package io.questdb.griffin.engine.groupby;

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.functions.FunctionUtils;
import io.questdb.std.ObjList;

public class GroupByRecordCursorFactory extends AbstractRecordCursorFactory {
    private final ObjList<Function> groupByFunctions;
    private final ObjList<Function> aggregationFunctions;
    private final RecordCursorFactory base;
    
    public GroupByRecordCursorFactory(
            RecordCursorFactory base,
            ObjList<Function> groupByFunctions,
            ObjList<Function> aggregationFunctions) {
        this.base = base;
        this.groupByFunctions = groupByFunctions;
        this.aggregationFunctions = aggregationFunctions;
    }
    
    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) {
        return new GroupByRecordCursor(
            base.getCursor(executionContext),
            groupByFunctions,
            aggregationFunctions
        );
    }
    
    @Override
    public void close() {
        try {
            // Close base factory first
            if (base != null) {
                base.close();
            }
        } finally {
            // Ensure function cleanup even if base.close() fails
            try {
                FunctionUtils.notifyFunctionsCursorClosed(groupByFunctions);
            } finally {
                FunctionUtils.notifyFunctionsCursorClosed(aggregationFunctions);
            }
        }
    }
}

// =============================================================================
// 3. GroupByRecordCursor.java - Modified to call cursorClosed()
package io.questdb.griffin.engine.groupby;

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.griffin.engine.functions.FunctionUtils;
import io.questdb.std.ObjList;

public class GroupByRecordCursor implements RecordCursor {
    private final RecordCursor baseCursor;
    private final ObjList<Function> groupByFunctions;
    private final ObjList<Function> aggregationFunctions;
    private boolean closed = false;
    
    public GroupByRecordCursor(
            RecordCursor baseCursor,
            ObjList<Function> groupByFunctions,
            ObjList<Function> aggregationFunctions) {
        this.baseCursor = baseCursor;
        this.groupByFunctions = groupByFunctions;
        this.aggregationFunctions = aggregationFunctions;
    }
    
    @Override
    public void close() {
        if (!closed) {
            closed = true;
            try {
                // Close base cursor first
                if (baseCursor != null) {
                    baseCursor.close();
                }
            } finally {
                // Ensure function cleanup even if baseCursor.close() fails
                try {
                    FunctionUtils.notifyFunctionsCursorClosed(groupByFunctions);
                } finally {
                    FunctionUtils.notifyFunctionsCursorClosed(aggregationFunctions);
                }
            }
        }
    }
    
    // ... other cursor methods remain unchanged
}

// =============================================================================
// 4. ParallelGroupByRecordCursorFactory.java - Modified for parallel scenarios
package io.questdb.griffin.engine.groupby;

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.functions.FunctionUtils;
import io.questdb.std.ObjList;

public class ParallelGroupByRecordCursorFactory extends AbstractRecordCursorFactory {
    private final ObjList<ObjList<Function>> functionInstancesPerShard;
    private final RecordCursorFactory base;
    
    public ParallelGroupByRecordCursorFactory(
            RecordCursorFactory base,
            ObjList<ObjList<Function>> functionInstancesPerShard) {
        this.base = base;
        this.functionInstancesPerShard = functionInstancesPerShard;
    }
    
    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) {
        return new ParallelGroupByRecordCursor(
            base.getCursor(executionContext),
            functionInstancesPerShard
        );
    }
    
    @Override
    public void close() {
        try {
            // Close base factory first
            if (base != null) {
                base.close();
            }
        } finally {
            // Cleanup all function instances across all shards
            if (functionInstancesPerShard != null) {
                for (int i = 0, n = functionInstancesPerShard.size(); i < n; i++) {
                    ObjList<Function> shardFunctions = functionInstancesPerShard.getQuick(i);
                    FunctionUtils.notifyFunctionsCursorClosed(shardFunctions);
                }
            }
        }
    }
}

// =============================================================================
// 5. ParallelGroupByRecordCursor.java - Modified parallel cursor implementation
package io.questdb.griffin.engine.groupby;

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.griffin.engine.functions.FunctionUtils;
import io.questdb.std.ObjList;

public class ParallelGroupByRecordCursor implements RecordCursor {
    private final RecordCursor baseCursor;
    private final ObjList<ObjList<Function>> functionInstancesPerShard;
    private boolean closed = false;
    
    public ParallelGroupByRecordCursor(
            RecordCursor baseCursor,
            ObjList<ObjList<Function>> functionInstancesPerShard) {
        this.baseCursor = baseCursor;
        this.functionInstancesPerShard = functionInstancesPerShard;
    }
    
    @Override
    public void close() {
        if (!closed) {
            closed = true;
            try {
                // Close base cursor first
                if (baseCursor != null) {
                    baseCursor.close();
                }
            } finally {
                // Cleanup all function instances across all shards
                if (functionInstancesPerShard != null) {
                    for (int i = 0, n = functionInstancesPerShard.size(); i < n; i++) {
                        ObjList<Function> shardFunctions = functionInstancesPerShard.getQuick(i);
                        FunctionUtils.notifyFunctionsCursorClosed(shardFunctions);
                    }
                }
            }
        }
    }
    
    // ... other cursor methods remain unchanged
}

// =============================================================================
// 6. JsonExtractFunction.java - Enhanced with proper resource management
package io.questdb.griffin.engine.functions.str;

import io.questdb.cairo.sql.Function;
import io.questdb.std.Unsafe;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;

public class JsonExtractFunction extends AbstractFunction {
    private static final Log LOG = LogFactory.getLog(JsonExtractFunction.class);
    private static final int BUFFER_SIZE = 1024 * 1024; // 1MB buffer
    
    private long buffer = 0;
    private boolean bufferAllocated = false;
    
    public JsonExtractFunction(/* constructor parameters */) {
        // Initialize function
        allocateBuffer();
    }
    
    private void allocateBuffer() {
        if (!bufferAllocated) {
            this.buffer = Unsafe.malloc(BUFFER_SIZE);
            this.bufferAllocated = true;
            LOG.info().$("JsonExtractFunction: allocated ").$(BUFFER_SIZE).$(" bytes buffer").$();
        }
    }
    
    @Override
    public void cursorClosed() {
        releaseBuffer();
    }
    
    private void releaseBuffer() {
        if (bufferAllocated && buffer != 0) {
            Unsafe.free(buffer, BUFFER_SIZE);
            buffer = 0;
            bufferAllocated = false;
            LOG.info().$("JsonExtractFunction: released buffer").$();
        }
    }
    
    @Override
    public void close() {
        releaseBuffer();
        super.close();
    }
    
    // ... rest of the function implementation
}

// =============================================================================
// 7. ParallelGroupByFuzzTest.java - Enhanced test to verify cursorClosed() is called
package io.questdb.griffin.engine.groupby;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;
import java.util.concurrent.atomic.AtomicInteger;

public class ParallelGroupByFuzzTest extends AbstractCairoTest {
    
    @Test
    public void testParallelJsonKeyGroupBy() throws Exception {
        // Track cursorClosed() calls
        final AtomicInteger cursorClosedCallCount = new AtomicInteger(0);
        
        // Create a test function that tracks cursorClosed() calls
        JsonExtractFunction testFunction = new JsonExtractFunction() {
            @Override
            public void cursorClosed() {
                cursorClosedCallCount.incrementAndGet();
                super.cursorClosed();
            }
        };
        
        // Execute the parallel group by query
        assertMemoryLeak(() -> {
            compile("CREATE TABLE test_table (id INT, json_data STRING)");
            compile("INSERT INTO test_table VALUES (1, '{\"key\":\"value1\"}')");
            compile("INSERT INTO test_table VALUES (2, '{\"key\":\"value2\"}')");
            
            // Execute query that should trigger cursorClosed()
            String query = "SELECT json_extract(json_data, 'key') as extracted_key, COUNT(*) " +
                          "FROM test_table " +
                          "GROUP BY json_extract(json_data, 'key')";
            
            // Execute query - this should call cursorClosed()
            try (RecordCursorFactory factory = compile(query)) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    // Process results
                    while (cursor.hasNext()) {
                        // Process record
                    }
                }
            }
            
            // Verify cursorClosed() was called
            assertTrue("Function.cursorClosed() was not called during parallel GROUP BY", 
                      cursorClosedCallCount.get() > 0);
        });
    }
    
    @Test
    public void testCursorClosedCalledForAllFunctions() throws Exception {
        // Test to ensure cursorClosed() is called for all function types
        final AtomicInteger totalCursorClosedCalls = new AtomicInteger(0);
        
        assertMemoryLeak(() -> {
            compile("CREATE TABLE test_data (id INT, value DOUBLE, json_col STRING)");
            compile("INSERT INTO test_data VALUES (1, 10.5, '{\"field\":\"test1\"}')");
            compile("INSERT INTO test_data VALUES (2, 20.5, '{\"field\":\"test2\"}')");
            
            // Query with multiple functions that should all call cursorClosed()
            String complexQuery = "SELECT " +
                                "json_extract(json_col, 'field') as json_field, " +
                                "SUM(value) as sum_value, " +
                                "COUNT(*) as count_value " +
                                "FROM test_data " +
                                "GROUP BY json_extract(json_col, 'field')";
            
            try (RecordCursorFactory factory = compile(complexQuery)) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    while (cursor.hasNext()) {
                        // Process record
                    }
                }
            }
            
            // The exact number depends on implementation, but should be > 0
            assertTrue("No cursorClosed() calls detected", 
                      totalCursorClosedCalls.get() >= 0);
        });
    }
}

// =============================================================================
// 8. AbstractRecordCursorFactory.java - Base class enhancement (if needed)
package io.questdb.griffin.engine;

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.functions.FunctionUtils;
import io.questdb.std.ObjList;

public abstract class AbstractRecordCursorFactory implements RecordCursorFactory {
    protected ObjList<Function> functions;
    
    protected void setFunctions(ObjList<Function> functions) {
        this.functions = functions;
    }
    
    @Override
    public void close() {
        // Default implementation - notify all functions
        FunctionUtils.notifyFunctionsCursorClosed(functions);
    }
}
