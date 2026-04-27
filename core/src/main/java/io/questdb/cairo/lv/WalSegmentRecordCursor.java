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

package io.questdb.cairo.lv;

import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameAddressCache;
import io.questdb.cairo.sql.PageFrameMemoryPool;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.std.QuietCloseable;

/**
 * Adapts a {@link WalSegmentPageFrameCursor} (which yields at most one NATIVE page frame)
 * into a {@link RecordCursor} suitable for feeding into
 * {@link io.questdb.griffin.engine.window.WindowRecordCursorFactory#getIncrementalCursor}.
 * <p>
 * The cursor linearly scans rows {@code [0, partitionHi)} within the single frame.
 * Symbol resolution delegates to the underlying {@link WalSegmentPageFrameCursor}.
 */
public class WalSegmentRecordCursor implements RecordCursor, QuietCloseable {
    private final PageFrameAddressCache addressCache;
    private final PageFrameMemoryPool memoryPool;
    private final PageFrameMemoryRecord record = new PageFrameMemoryRecord();
    private PageFrame cachedFrame;
    private long currentRow;
    private WalSegmentPageFrameCursor frameCursor;
    private boolean isFrameLoaded;
    private long rowCount;

    public WalSegmentRecordCursor(PageFrameAddressCache addressCache, PageFrameMemoryPool memoryPool) {
        this.addressCache = addressCache;
        this.memoryPool = memoryPool;
    }

    @Override
    public void close() {
        // Reset iteration state only. The frame cursor is not owned by this cursor
        // and must be freed by the caller (LiveViewRefreshJob).
        isFrameLoaded = false;
        currentRow = -1;
        rowCount = 0;
        cachedFrame = null;
    }

    @Override
    public Record getRecord() {
        return record;
    }

    @Override
    public Record getRecordB() {
        throw new UnsupportedOperationException();
    }

    @Override
    public SymbolTable getSymbolTable(int columnIndex) {
        return frameCursor.getSymbolTable(columnIndex);
    }

    @Override
    public boolean hasNext() {
        if (!isFrameLoaded && loadFrame() == null) {
            return false;
        }
        if (++currentRow < rowCount) {
            record.setRowIndex(currentRow);
            return true;
        }
        return false;
    }

    /**
     * Eagerly loads the single WAL segment page frame and binds the record to it
     * without consuming any rows. Used by the JIT-compiled filter path, which
     * iterates filtered row ids via {@link #setRowIndex(long)} instead of
     * {@link #hasNext()}. Returns {@code null} when the cursor has no frame to yield.
     */
    public PageFrame loadFrame() {
        if (isFrameLoaded) {
            return cachedFrame;
        }
        PageFrame frame = frameCursor.next(-1);
        if (frame == null) {
            return null;
        }
        addressCache.clear();
        addressCache.add(0, frame);
        memoryPool.navigateTo(0, record);
        rowCount = frame.getPartitionHi();
        currentRow = -1;
        cachedFrame = frame;
        isFrameLoaded = true;
        return frame;
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        return frameCursor.newSymbolTable(columnIndex);
    }

    /**
     * Binds this cursor to the given frame cursor and metadata. The frame cursor
     * must already be positioned via {@link WalSegmentPageFrameCursor#of}.
     */
    public void of(WalSegmentPageFrameCursor frameCursor, RecordMetadata metadata) {
        this.frameCursor = frameCursor;
        addressCache.of(metadata, frameCursor.getColumnMapping(), false);
        memoryPool.of(addressCache);
        record.of(frameCursor);
        isFrameLoaded = false;
        currentRow = -1;
        rowCount = 0;
        cachedFrame = null;
    }

    @Override
    public long preComputedStateSize() {
        return 0;
    }

    @Override
    public void recordAt(Record record, long atRowId) {
        throw new UnsupportedOperationException();
    }

    /**
     * Positions {@link #getRecord()} at {@code rowIndex} within the loaded frame.
     * The caller must invoke {@link #loadFrame()} first; this method is the
     * iteration primitive for the JIT-compiled filter path, which reads filtered
     * row ids out of a row buffer and skips {@link #hasNext()}.
     */
    public void setRowIndex(long rowIndex) {
        record.setRowIndex(rowIndex);
    }

    @Override
    public long size() {
        return frameCursor != null ? frameCursor.size() : 0;
    }

    @Override
    public void toTop() {
        if (frameCursor != null) {
            frameCursor.toTop();
        }
        isFrameLoaded = false;
        currentRow = -1;
        rowCount = 0;
        cachedFrame = null;
    }
}
