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
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.DirectLongList;
import io.questdb.std.Rows;
import org.jetbrains.annotations.NotNull;

class LatestByAllRecordCursor extends AbstractDescendingRecordListCursor {
    private final Map map;
    private final RecordSink recordSink;

    public LatestByAllRecordCursor(
            @NotNull CairoConfiguration configuration,
            @NotNull RecordMetadata metadata,
            @NotNull Map map,
            @NotNull DirectLongList rows,
            @NotNull RecordSink recordSink
    ) {
        super(configuration, metadata, rows);
        this.map = map;
        this.recordSink = recordSink;
    }

    @Override
    public void close() {
        if (isOpen()) {
            map.close();
            super.close();
        }
    }

    @Override
    public void of(PageFrameCursor pageFrameCursor, SqlExecutionContext executionContext) throws SqlException {
        if (!isOpen) {
            isOpen = true;
            map.reopen();
        }
        super.of(pageFrameCursor, executionContext);
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Row backward scan");
    }

    @Override
    protected void buildTreeMap() {
        PageFrame frame;
        while ((frame = frameCursor.next()) != null) {
            circuitBreaker.statefulThrowExceptionIfTripped();
            final int frameIndex = frameCount;
            final long partitionLo = frame.getPartitionLo();
            final long partitionHi = frame.getPartitionHi() - 1;

            frameAddressCache.add(frameCount, frame);
            frameMemoryPool.navigateTo(frameCount++, recordA);
            recordA.setRowIndex(partitionHi);

            for (long row = partitionHi - partitionLo; row >= 0; row--) {
                recordA.setRowIndex(row);
                MapKey key = map.withKey();
                key.put(recordA, recordSink);
                if (key.create()) {
                    rows.add(Rows.toRowID(frameIndex, row));
                }
            }
        }
        map.clear();
    }
}
