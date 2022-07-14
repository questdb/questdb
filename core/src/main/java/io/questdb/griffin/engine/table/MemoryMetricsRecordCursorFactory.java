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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;

public final class MemoryMetricsRecordCursorFactory extends AbstractRecordCursorFactory {
    private static final RecordMetadata METADATA;
    private static final int METRIC_COUNT = MemoryTag.SIZE + 1; // 1 per each tag + 1 extra for total memory
    private final SingleRowRecordCursor cursor = new SingleRowRecordCursor();
    private final Object[] memoryMetrics = new Object[METRIC_COUNT];

    public MemoryMetricsRecordCursorFactory() {
        super(METADATA);
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        collectMetrics(memoryMetrics);
        cursor.of(memoryMetrics);
        return cursor;
    }

    private static void collectMetrics(Object[] collector) {
        assert collector.length == METRIC_COUNT;

        collector[0] = Unsafe.getMemUsed();
        for (int i = 0; i < MemoryTag.SIZE; i++) {
            collector[i + 1] = Unsafe.getMemUsedByTag(i);
        }
    }

    static {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(0, new TableColumnMetadata("TOTAL_USED", 0, ColumnType.LONG));
        for (int i = 0; i < MemoryTag.SIZE; i++) {
            String name = MemoryTag.nameOf(i);
            metadata.add(i + 1, new TableColumnMetadata(name, i + 1, ColumnType.LONG));
        }
        METADATA = metadata;
    }
}
