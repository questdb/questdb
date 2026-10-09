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
import io.questdb.cairo.sql.PageFrameAddressCache;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PageFrameMemoryPool;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.ParquetDecodeHint;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

public abstract class AbstractPageFrameRecordCursor implements PageFrameRecordCursor {
    protected final PageFrameAddressCache frameAddressCache;
    protected final PageFrameMemoryPool frameMemoryPool;
    protected final PageFrameMemoryRecord recordA;
    protected final PageFrameMemoryRecord recordB;
    private final RecordMetadata metadata;
    protected int frameCount = 0;
    protected PageFrameCursor frameCursor;

    public AbstractPageFrameRecordCursor(
            @NotNull CairoConfiguration configuration,
            @NotNull RecordMetadata metadata
    ) {
        this.metadata = metadata;
        try {
            recordA = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER);
            recordB = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_B_LETTER);
            frameAddressCache = new PageFrameAddressCache();
            frameMemoryPool = new PageFrameMemoryPool(configuration);
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    @Override
    public void close() {
        Misc.free(frameMemoryPool);
        Misc.free(recordA);
        Misc.free(recordB);
        Misc.free(frameAddressCache);
        frameCursor = Misc.free(frameCursor);
    }

    @TestOnly
    public PageFrameMemoryPool getFrameMemoryPool() {
        return frameMemoryPool;
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
        frameMemoryPool.recordAt(record, rowId);
    }

    @Override
    public void setParquetDecodeHint(ParquetDecodeHint hint) {
        frameMemoryPool.setParquetDecodeHint(hint);
    }

    @Override
    public void setRecordAtRows(@Nullable RowIdSource source) {
        frameMemoryPool.setRecordAtRows(source);
    }

    @Override
    public void toTop() {
        frameCount = 0;
        frameCursor.toTop();
    }

    protected void init(@Nullable MemoryTracker memoryTracker) {
        frameAddressCache.of(metadata, frameCursor.getColumnMapping(), frameCursor.isExternal());
        frameMemoryPool.setMemoryTracker(memoryTracker);
        frameMemoryPool.of(frameAddressCache);
        frameCount = 0;
        frameCursor.toTop();
    }

    /**
     * Drops every frame the address cache holds, so the next walk numbers its frames from zero and
     * fills the cache itself.
     * <p>
     * The cache maps a frame ordinal to that frame's addresses and page limits, and it deliberately
     * outlives a walk: {@link #toTop()} keeps it, and {@link PageFrameAddressCache#add} keeps whatever
     * the cache already holds under an ordinal. That holds only while every walk of this cursor cuts
     * frames the same way. A skip walk ({@link PageFrameCursor#next(long)}) does not - it cuts at the
     * skip target and may collapse a whole partition into one frame - so a walk that follows one, or
     * one that runs over frames an ordinary walk left behind, would read the other walk's addresses
     * and page limits with its own row counts. Callers that are about to change how frames are cut
     * must call this first, and only from the top of the cursor: it renumbers frames from zero.
     */
    protected void resetFrameCache() {
        frameAddressCache.of(metadata, frameCursor.getColumnMapping(), frameCursor.isExternal());
        // A decoded Parquet frame is keyed by frame ordinal, and so is the pool's bound frame memory;
        // both describe the numbering being dropped here. releaseParquetBuffers() ignores the pool's pin
        // bits and closes the DirectLongLists a bound record reads its addresses through, so abandon both
        // records first, as that method's contract demands. The pool's bindGeneration bump only guards the
        // NEXT navigateTo(); a read through a record still bound from an earlier recordAt() goes straight
        // to the freed lists.
        recordA.clear();
        recordB.clear();
        frameMemoryPool.releaseParquetBuffers();
        frameCount = 0;
    }
}
