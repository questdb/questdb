/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.*;
import io.questdb.std.Misc;
import io.questdb.std.Rows;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;

public abstract class AbstractPageFrameRecordCursor implements PageFrameRecordCursor {
    protected final PageFrameAddressCache frameAddressCache;
    protected final PageFrameMemoryPool frameMemoryPool;
    protected final PageFrameMemoryRecord recordA;
    protected final PageFrameMemoryRecord recordB;
    protected int frameCount = 0;
    protected PageFrameCursor frameCursor;

    public AbstractPageFrameRecordCursor(
            @NotNull CairoConfiguration configuration,
            @NotNull @Transient RecordMetadata metadata
    ) {
        recordA = new PageFrameMemoryRecord();
        recordB = new PageFrameMemoryRecord();
        frameAddressCache = new PageFrameAddressCache(configuration);
        frameAddressCache.of(metadata);
        frameMemoryPool = new PageFrameMemoryPool();
    }

    @Override
    public void close() {
        Misc.free(frameMemoryPool);
        frameCursor = Misc.free(frameCursor);
    }

    @Override
    public PageFrameCursor getPageFrameCursor() {
        return frameCursor;
    }

    @Override
    public Record getRecord() {
        return recordA;
    }

    @Override
    public Record getRecordB() {
        return recordB;
    }

    @Override
    public StaticSymbolTable getSymbolTable(int columnIndex) {
        return frameCursor.getSymbolTable(columnIndex);
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        return frameCursor.newSymbolTable(columnIndex);
    }

    @Override
    public void recordAt(Record record, long rowId) {
        final PageFrameMemoryRecord frameMemoryRecord = (PageFrameMemoryRecord) record;
        frameMemoryPool.navigateTo(Rows.toPartitionIndex(rowId), frameMemoryRecord);
        frameMemoryRecord.setRowIndex(Rows.toLocalRowID(rowId));
    }

    @Override
    public void toTop() {
        frameCount = 0;
        frameCursor.toTop();
    }

    protected void init() {
        frameAddressCache.clear();
        frameMemoryPool.of(frameAddressCache);
        frameCount = 0;
        frameCursor.toTop();
    }
}
