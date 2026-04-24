/*+*****************************************************************************
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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Iterates table forwards and finds the earliest values for a single symbol column,
 * where the symbol values come from a subquery (e.g. WHERE s IN (SELECT ...)).
 *
 * <p>When the key column has a bitmap index, per-symbol index lookups are used via
 * the {@code EarliestByValuesIndexed*} cursors. Otherwise a forward row scan with
 * a map of included keys runs via {@link EarliestByValueListRecordCursor}.
 */
public class EarliestBySubQueryRecordCursorFactory extends AbstractPageFrameRecordCursorFactory {
    private final int columnIndex;
    private final AbstractPageFrameRecordCursor cursor;
    private final Function filter;
    private final Record.CharSequenceFunction func;
    private final boolean indexed;
    private final RecordCursorFactory recordCursorFactory;
    private final DirectLongList rows;
    private final IntHashSet symbolKeys;

    public EarliestBySubQueryRecordCursorFactory(
            @NotNull CairoConfiguration configuration,
            @NotNull RecordMetadata metadata,
            @NotNull PartitionFrameCursorFactory partitionFrameCursorFactory,
            int columnIndex,
            @NotNull RecordCursorFactory recordCursorFactory,
            @Nullable Function filter,
            boolean indexed,
            @NotNull Record.CharSequenceFunction func,
            @NotNull IntList columnIndexes,
            @NotNull IntList columnSizeShifts
    ) {
        super(metadata, partitionFrameCursorFactory, columnIndexes, columnSizeShifts);
        try {
            this.symbolKeys = new IntHashSet();
            this.filter = filter;
            this.columnIndex = columnIndex;
            this.recordCursorFactory = recordCursorFactory;
            this.func = func;
            this.indexed = indexed;
            if (indexed) {
                this.rows = new DirectLongList(
                        configuration.getSqlEarliestByRowCount(),
                        MemoryTag.NATIVE_EARLIEST_BY_LONG_LIST
                );
                if (filter != null) {
                    this.cursor = new EarliestByValuesIndexedFilteredRecordCursor(
                            configuration, metadata, columnIndex, rows, symbolKeys, null, filter);
                } else {
                    this.cursor = new EarliestByValuesIndexedRecordCursor(
                            configuration, metadata, columnIndex, symbolKeys, null, rows);
                }
            } else {
                this.rows = null;
                this.cursor = new EarliestByValueListRecordCursor(
                        configuration,
                        metadata,
                        columnIndex,
                        filter,
                        configuration.getDefaultSymbolCapacity(),
                        true,
                        false
                );
            }
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return true;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("EarliestBySubQuery");
        sink.optAttr("filter", filter);
        sink.child("Subquery", recordCursorFactory);
        if (indexed) {
            sink.child(cursor);
        }
        sink.child(partitionFrameCursorFactory);
    }

    @Override
    public boolean usesIndex() {
        return indexed;
    }

    @Override
    protected void _close() {
        super._close();
        Misc.free(recordCursorFactory);
        Misc.free(filter);
        Misc.free(cursor);
        Misc.free(rows);
    }

    @Override
    protected RecordCursor initRecordCursor(
            PageFrameCursor pageFrameCursor,
            SqlExecutionContext executionContext
    ) throws SqlException {
        symbolKeys.clear();
        try (RecordCursor baseCursor = recordCursorFactory.getCursor(executionContext)) {
            final StaticSymbolTable symbolTable = pageFrameCursor.getSymbolTable(columnIndex);
            final Record record = baseCursor.getRecord();
            final StringSink sink = Misc.getThreadLocalSink();
            while (baseCursor.hasNext()) {
                int symbolKey = symbolTable.keyOf(func.get(record, 0, sink));
                if (symbolKey != SymbolTable.VALUE_NOT_FOUND) {
                    // Indexed cursors feed these keys directly to BitmapIndexReader,
                    // which expects +1-shifted keys (0 = NULL sentinel).
                    symbolKeys.add(indexed ? TableUtils.toIndexKey(symbolKey) : symbolKey);
                }
            }
        }
        if (!indexed) {
            // Non-indexed path: the ValueList cursor maintains its own included-keys
            // set (for restrictedByIncludedValues mode); mirror the subquery-resolved
            // keys into it. Indexed cursors read symbolKeys directly.
            EarliestByValueListRecordCursor valueListCursor = (EarliestByValueListRecordCursor) cursor;
            IntHashSet cursorKeys = valueListCursor.getIncludedSymbolKeys();
            cursorKeys.clear();
            cursorKeys.addAll(symbolKeys);
        }
        cursor.of(pageFrameCursor, executionContext);
        return cursor;
    }
}
