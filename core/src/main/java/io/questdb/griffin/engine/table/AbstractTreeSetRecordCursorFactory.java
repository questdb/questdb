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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import org.jetbrains.annotations.NotNull;

/**
 * Abstract base class for tree set record cursor factories.
 */
abstract class AbstractTreeSetRecordCursorFactory extends AbstractPageFrameRecordCursorFactory {
    /**
     * The row list for the tree set.
     */
    final DirectLongList rows;
    /**
     * The page frame record cursor.
     */
    protected PageFrameRecordCursor cursor;

    /**
     * Constructs a new tree set record cursor factory.
     *
     * @param configuration               the Cairo configuration
     * @param metadata                    the record metadata
     * @param partitionFrameCursorFactory the partition frame cursor factory
     * @param columnIndexes               the column indexes
     * @param columnSizeShifts            the column size shifts
     */
    public AbstractTreeSetRecordCursorFactory(
            @NotNull CairoConfiguration configuration,
            @NotNull RecordMetadata metadata,
            @NotNull PartitionFrameCursorFactory partitionFrameCursorFactory,
            @NotNull IntList columnIndexes,
            @NotNull IntList columnSizeShifts
    ) {
        super(configuration, metadata, partitionFrameCursorFactory, columnIndexes, columnSizeShifts);
        this.rows = new DirectLongList(configuration.getSqlLatestByRowCount(), MemoryTag.NATIVE_LATEST_BY_LONG_LIST);
    }

    @Override
    protected void _close() {
        super._close();
        Misc.free(rows);
    }

    @Override
    protected RecordCursor initRecordCursor(
            PageFrameCursor pageFrameCursor,
            SqlExecutionContext executionContext
    ) throws SqlException {
        cursor.of(pageFrameCursor, executionContext);
        return cursor;
    }
}
