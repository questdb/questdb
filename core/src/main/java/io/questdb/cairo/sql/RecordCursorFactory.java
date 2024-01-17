/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.Plannable;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.mp.SCSequence;
import io.questdb.std.str.CharSinkBase;
import io.questdb.std.str.Sinkable;
import org.jetbrains.annotations.NotNull;

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

    @Override
    default void close() {
    }

    default SingleSymbolFilter convertToSampleByIndexDataFrameCursorFactory() {
        return null;
    }

    default PageFrameSequence<?> execute(SqlExecutionContext executionContext, SCSequence collectSubSeq, int order) throws SqlException {
        return null;
    }

    default boolean followedLimitAdvice() {
        return false;
    }

    /**
     * True if record cursor factory followed order by advice and doesn't require sorting.
     *
     * @return true if record cursor factory followed order by advice and doesn't require sorting
     */
    default boolean followedOrderByAdvice() {
        return false;
    }

    // Factories, such as union all do not conform to the assumption
    // that key read from symbol column map to symbol values unambiguously.
    // In that if you read key 1 at row 10, it might map to 'AAA' and if you read
    // key 1 at row 100 it might map to 'BBB'.
    // Such factories cannot be used in multi-threaded execution and cannot be tested
    // via `testSymbolAPI()` call.
    default boolean fragmentedSymbolTables() {
        return false;
    }

    default String getBaseColumnName(int idx) {
        return getBaseFactory().getMetadata().getColumnName(idx);
    }

    /**
     * Method is necessary for cases where row cursor uses index from table reader while record cursor can reorder columns (e.g. DataFrameRecordCursorFactory)
     *
     * @param idx idx of column
     * @return name of base column (no remapping)
     */
    default String getBaseColumnNameNoRemap(int idx) {
        return getBaseColumnName(idx);
    }

    default RecordCursorFactory getBaseFactory() {
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
     */
    default int getScanDirection() {
        return SCAN_DIRECTION_FORWARD;
    }

    /**
     * If factory operates on table directly returns table's token, null otherwise.
     *
     * @return table token of table used by this factory
     */
    default TableToken getTableToken() {
        return null;
    }

    /**
     * Returns true if this factory handles limit M , N clause already and false otherwise .
     * If true then separate limit cursor factory is not needed (and could actually cause problem by re-applying limit logic).
     */
    default boolean implementsLimit() {
        return false;
    }

    boolean recordCursorSupportsRandomAccess();

    default void revertFromSampleByIndexDataFrameCursorFactory() {
    }

    default boolean supportPageFrameCursor() {
        return false;
    }

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

    default void toSink(@NotNull CharSinkBase<?> sink) {
        throw new UnsupportedOperationException("Unsupported for: " + getClass());
    }

    /**
     * Returns true if the factory uses a {@link io.questdb.jit.CompiledFilter}.
     */
    default boolean usesCompiledFilter() {
        return false;
    }

    /**
     * Returns true if the factory uses index-based access.
     */
    default boolean usesIndex() {
        return false;
    }
}
