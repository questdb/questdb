/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

import io.questdb.cairo.sql.async.PageFrameSequence;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.mp.Sequence;
import io.questdb.std.Sinkable;
import io.questdb.std.str.CharSink;

import java.io.Closeable;

/**
 * Factory for creating a SQL execution plan.
 * Queries may be executed more than once without changing execution plan.
 *
 * Interfaces which extend Closeable are not optionally-closeable.
 * close() method must be called after other calls are complete.
 *
 * Example:
 *
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
 *
 */
public interface RecordCursorFactory extends Closeable, Sinkable {
    @Override
    default void close() {
    }

    /**
     * True if record cursor factory followed order by advice and doesn't require sorting .
     */
    default boolean followedOrderByAdvice() {
        return false;
    }

    default boolean followedLimitAdvice() {
        return false;
    }

    /**
     * Creates an instance of RecordCursor. Factories will typically reuse cursor instances.
     * The calling code must not hold on to copies of the cursor.
     *
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

    default PageFrameSequence<?> execute(SqlExecutionContext executionContext, Sequence collectSubSeq, int order) throws SqlException {
        return null;
    }

    boolean recordCursorSupportsRandomAccess();

    default boolean supportPageFrameCursor() {
        return false;
    }

    default boolean supportsUpdateRowId(CharSequence tableName) {
        return false;
    }

    default boolean usesCompiledFilter() {
        return false;
    }

    default void toSink(CharSink sink) {
        throw new UnsupportedOperationException();
    }

    default SingleSymbolFilter convertToSampleByIndexDataFrameCursorFactory() {
        return null;
    }

    /* Returns true if this factory handles limit M , N clause already and false otherwise .
     *  If true then separate limit cursor factory is not needed (and could actually cause problem by re-applying limit logic).   */
    default boolean implementsLimit() {
        return false;
    }

    default boolean hasDescendingOrder() {
        return false;
    }
}
