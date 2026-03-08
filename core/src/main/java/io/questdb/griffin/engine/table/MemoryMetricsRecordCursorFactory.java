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

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.MemoryTag;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;

public final class MemoryMetricsRecordCursorFactory extends AbstractRecordCursorFactory {
    private static final RecordMetadata METADATA;
    private static final int METRIC_COUNT = MemoryTag.SIZE + 2; // 1 per each tag + 2 extra for total memory and rss 
    private static final String[] KEYS = new String[METRIC_COUNT];
    private final StringLongTuplesRecordCursor cursor = new StringLongTuplesRecordCursor();
    private final long[] values = new long[METRIC_COUNT];

    public MemoryMetricsRecordCursorFactory() {
        super(METADATA);
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) {
        collectMetrics(values);
        cursor.of(KEYS, values);
        return cursor;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("memory_metrics");
    }

    private static void collectMetrics(long[] collector) {
        assert collector.length == METRIC_COUNT;

        collector[0] = Unsafe.getMemUsed();
        collector[1] = Os.getRss();
        for (int i = 0; i < MemoryTag.SIZE; i++) {
            collector[i + 2] = Unsafe.getMemUsedByTag(i);
        }
    }

    static {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(0, new TableColumnMetadata("memory_tag", ColumnType.STRING));
        metadata.add(1, new TableColumnMetadata("bytes", ColumnType.LONG));
        METADATA = metadata;

        KEYS[0] = "TOTAL_USED";
        KEYS[1] = "RSS";
        for (int i = 0; i < MemoryTag.SIZE; i++) {
            KEYS[i + 2] = MemoryTag.nameOf(i);
        }
    }
}
