/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.cairo.sql;

import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.async.PageFrameSequence;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.Plannable;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.table.ConcurrentTimeFrameCursor;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.jit.CompiledFilter;
import io.questdb.mp.SCSequence;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.Sinkable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;

/**
 * Factory for creating a SQL execution plan.
 * Queries may be executed more than once without changing execution plan.
 * <p>
 * Interfaces which extend Closeable are not optionally-closeable.
 * close() method must be called after other calls are complete.
 * <p>
 * Example:
 * <pre>
 * final SqlExecutionContextImpl ctx = new SqlExecutionContextImpl(engine, 1);
 * try (SqlCompiler compiler = new SqlCompiler(engine)) {
 *     try (RecordCursorFactory factory = compiler.compile("abc", ctx).getRecordCursorFactory()) {
 *         try (RecordCursor cursor = factory.getCursor(ctx)) {
 *             final Record record = cursor.getRecord();
 *             while (cursor.hasNext()) {
 *                 // access 'record' instance for field values
 *             }
 *         }
 *     }
 * }
 * </pre>
 */
public interface RecordCursorFactory extends Closeable, Sinkable, Plannable {

    int SCAN_DIRECTION_BACKWARD = 2;
    int SCAN_DIRECTION_FORWARD = 1;
    int SCAN_DIRECTION_OTHER = 0;

    /**
     * Changes the page frame sizes for this factory.
     *
     * @param minRows minimum rows per page frame
     * @param maxRows maximum rows per page frame
     */
    default void changePageFrameSizes(int minRows, int maxRows) {
    }

    @Override
    default void close() {
    }

    default SingleSymbolFilter convertToSampleByIndexPageFrameCursorFactory() {
        return null;
    }

    default PageFrameSequence<?> execute(SqlExecutionContext executionContext, SCSequence collectSubSeq, int order) throws SqlException {
        return null;
    }

    /**
     * True if record cursor factory followed order by advice and doesn't require sorting.
     *
     * @return true if record cursor factory followed order by advice and doesn't require sorting
     */
    default boolean followedOrderByAdvice() {
        return false;
    }

    /**
     * Factories, such as union all do not conform to the assumption
     * that key read from symbol column map to symbol values unambiguously.
     * In that if you read key 1 at row 10, it might map to 'AAA' and if you read
     * key 1 at row 100 it might map to 'BBB'.
     * Such factories cannot be used in multithreaded execution and cannot be tested
     * via `testSymbolAPI()` call.
     *
     * @return true if the factory uses fragmented symbol tables and can't be used
     * in multithreaded execution
     */
    default boolean fragmentedSymbolTables() {
        return false;
    }

    /**
     * Returns the base column name at the given index.
     *
     * @param idx the column index
     * @return the column name
     */
    default String getBaseColumnName(int idx) {
        return getBaseFactory().getMetadata().getColumnName(idx);
    }

    /**
     * Returns the base factory, if any.
     *
     * @return the base factory, or null if none
     */
    default RecordCursorFactory getBaseFactory() {
        return null;
    }

    // to be used in combination with compiled filter
    @Nullable
    default ObjList<Function> getBindVarFunctions() {
        return null;
    }

    // to be used in combination with compiled filter
    @Nullable
    default MemoryCARW getBindVarMemory() {
        return null;
    }

    default IntList getColumnCrossIndex() {
        return null;
    }

    @Nullable
    default CompiledFilter getCompiledFilter() {
        return null;
    }

    /**
     * Creates an instance of RecordCursor. Factories will typically reuse cursor instances.
     * The calling code must not hold on to copies of the cursor.
     * <p>
     * The new cursor will have refreshed its view of the data. If new data was added to table(s)
     * the cursor will pick it up.
     *
     * @param executionContext name of a SQL execution context
     * @return instance of cursor
     * @throws SqlException when cursor cannot be produced due a deferred SQL syntax error
     */
    default RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        throw new UnsupportedOperationException();
    }

    @Nullable
    default Function getFilter() {
        return null;
    }

    /**
     * Metadata of the SQL result. It includes column names, indexes and types.
     *
     * @return metadata
     */
    RecordMetadata getMetadata();

    default PageFrameCursor getPageFrameCursor(SqlExecutionContext executionContext, int order) throws SqlException {
        return null;
    }

    /**
     * Returns the direction of scanning used in this factory:
     * - {@link #SCAN_DIRECTION_FORWARD}, {@link #SCAN_DIRECTION_BACKWARD} - for regular data/interval frame scans
     * - {@link #SCAN_DIRECTION_OTHER} - for some index scans, e.g. cursor-order index lookup with multiple values
     * where order is 'random'.<br>
     * Note: tables with designated timestamp keep rows in timestamp order, so:
     * - forward scan produces rows in ascending ts order
     * - backward scan produces rows in descending ts order
     *
     * @return the scan direction
     */
    default int getScanDirection() {
        return SCAN_DIRECTION_FORWARD;
    }

    /**
     * Returns the original filter expression that can be stolen by parent factories.
     * When {@link #supportsFilterStealing()} returns true, this method should return
     * the original expression of the stolen filter.
     *
     * @return the original filter expression that can be stolen, or null if
     * filter stealing is not supported
     */
    default ExpressionNode getStealFilterExpr() {
        return null;
    }

    /**
     * If factory operates on table directly returns table's token, null otherwise.
     * When this method returns a table token, it also means that the factory doesn't
     * remap column names via aliases.
     *
     * @return table token of table used by this factory
     */
    default TableToken getTableToken() {
        return null;
    }

    /**
     * Returns time frame cursor or null if time frames aren't supported by the factory.
     *
     * @param executionContext the SQL execution context
     * @return the time frame cursor, or null if not supported
     * @throws SqlException if an error occurs
     */
    default TimeFrameCursor getTimeFrameCursor(SqlExecutionContext executionContext) throws SqlException {
        return null;
    }

    /**
     * Closes everything but base factory and filter.
     */
    default void halfClose() {
    }

    /**
     * Returns true if this factory handles {@code limit(M, N)} clause.
     * If true, then a separate limit cursor factory is not needed (and could actually cause problem
     * by re-applying limit logic).
     *
     * @return true if limit is implemented
     */
    default boolean implementsLimit() {
        return false;
    }

    /**
     * Returns true if the factory stands for nothing more but a projection, so that
     * the above factory (e.g. a parallel GROUP BY one) can steal the projection.
     * <p>
     * Projection consist of cross-indexes and metadata columns.
     *
     * @return true if the factory stands for nothing more but a projection
     * @see #getColumnCrossIndex()
     * @see #getBaseColumnName(int)
     */
    default boolean isProjection() {
        return false;
    }

    /**
     * Returns a new time frame cursor instance or null if time frames aren't supported by the factory.
     * The returned instance can be used by a worker thread, i.e. the underlying interaction with
     * table reader is synchronized between the time frame instances returned by this method.
     * <p>
     * Unlike with {@link #getTimeFrameCursor(SqlExecutionContext)}, the returned cursor has to be
     * initialized before usage.
     *
     * @return a new concurrent time frame cursor, or null if not supported
     */
    default ConcurrentTimeFrameCursor newTimeFrameCursor() {
        return null;
    }

    /**
     * Returns true when factory's record cursor supports optimized top K (ORDER BY + LIMIT N) loop
     * for the given LONG or TIMESTAMP column.
     *
     * @param columnIndex index of the column to check
     * @return true if the factory supports the fast path for the column
     */
    default boolean recordCursorSupportsLongTopK(int columnIndex) {
        return false;
    }

    /**
     * Returns true if the record cursor supports random access.
     *
     * @return true if random access is supported
     */
    boolean recordCursorSupportsRandomAccess();

    default void revertFromSampleByIndexPageFrameCursorFactory() {
    }

    /**
     * Returns true if the factory stands for nothing more but a filter, so that
     * the above factory (e.g. a parallel GROUP BY one) can steal the filter.
     *
     * @return true if filter stealing is supported
     */
    default boolean supportsFilterStealing() {
        return false;
    }

    /**
     * Returns true if the factory supports page frame cursor.
     *
     * @return true if page frame cursor is supported
     */
    default boolean supportsPageFrameCursor() {
        return false;
    }

    /**
     * Time frames are supported only for full table scan cursors, i.e. "x" queries.
     *
     * @return true if the factory supports time frames
     * and {@link #getTimeFrameCursor(SqlExecutionContext)}
     * or {@link #newTimeFrameCursor()} can be safely called.
     */
    default boolean supportsTimeFrameCursor() {
        return false;
    }

    /**
     * Returns true if the factory supports UPDATE row ID for the given table.
     *
     * @param tableName the table token
     * @return true if UPDATE row ID is supported
     */
    default boolean supportsUpdateRowId(TableToken tableName) {
        return false;
    }

    /**
     * Adds description of this factory to EXPLAIN output.
     */
    @Override
    default void toPlan(PlanSink sink) {
        sink.type(getClass().getName());
    }

    default void toSink(@NotNull CharSink<?> sink) {
        throw new UnsupportedOperationException("Unsupported for: " + getClass());
    }

    /**
     * @return true if the factory uses a {@link io.questdb.jit.CompiledFilter}.
     */
    default boolean usesCompiledFilter() {
        return false;
    }

    /**
     * Returns true if the factory uses index-based access.
     *
     * @return true if the factory uses index-based access
     */
    default boolean usesIndex() {
        return false;
    }
}
