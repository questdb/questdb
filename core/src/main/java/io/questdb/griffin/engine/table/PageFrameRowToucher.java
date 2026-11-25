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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.PageFrameAddressCache;
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.std.IntList;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Loads one byte of every column of a batch of rows of one page frame in a tight loop, ahead of
 * the code that reads those rows. When the rows are spread over the frame, as an index scan's are,
 * nearly every column value of every row is a cache and TLB miss. Paid one row at a time, between
 * the reader's own work, those misses come one after another; loaded together, independently of
 * each other, they overlap.
 * <p>
 * Only native frames that a covering index does not serve are touched: their addresses are stable
 * for the query, while a decoded Parquet frame can be evicted before the rows are read.
 */
public class PageFrameRowToucher {
    // per column of the current frame: the address of row 0, the bytes readable from it and the
    // row size as a shift; count columns in total, 0 when the frame is not touched
    private long[] addresses = new long[0];
    private int count;
    private long[] limits = new long[0];
    private int[] shifts = new int[0];
    // sink for the touched bytes, so that the loads are not eliminated
    private long sink;

    public void clear() {
        count = 0;
    }

    /**
     * The columns of the current frame this toucher loads.
     */
    @TestOnly
    public int getTouchedColumnCount() {
        return count;
    }

    public boolean isEnabled() {
        return count > 0;
    }

    /**
     * Prepares to touch rows of the given frame, or disables touching when the frame is not a
     * plain native one.
     */
    public void of(PageFrameAddressCache frameAddressCache, int frameIndex, PageFrameMemory frameMemory) {
        of(frameAddressCache, frameIndex, frameMemory, null);
    }

    /**
     * Like {@link #of(PageFrameAddressCache, int, PageFrameMemory)}, but touches only the columns
     * whose entry in {@code touchedColumns} is true, by the frame's column index: the reader knows
     * it reads no other. Null touches every column.
     */
    public void of(
            PageFrameAddressCache frameAddressCache,
            int frameIndex,
            PageFrameMemory frameMemory,
            @Nullable boolean[] touchedColumns
    ) {
        count = 0;
        if (frameMemory.getFrameFormat() != PartitionFormat.NATIVE || frameAddressCache.isFrameCovered(frameIndex)) {
            return;
        }
        final int columnCount = frameMemory.getColumnCount();
        if (addresses.length < columnCount) {
            addresses = new long[columnCount];
            limits = new long[columnCount];
            shifts = new int[columnCount];
        }
        final IntList columnTypes = frameAddressCache.getColumnTypes();
        for (int c = 0; c < columnCount; c++) {
            if (touchedColumns != null && (c >= touchedColumns.length || !touchedColumns[c])) {
                continue;
            }
            final int columnType = columnTypes.getQuick(c);
            final long address;
            final long limit;
            final int shift;
            if (ColumnType.isVarSize(columnType)) {
                address = frameMemory.getAuxPageAddress(c);
                limit = frameMemory.getAuxPageSizes().get(frameMemory.getColumnOffset() + c);
                shift = Long.numberOfTrailingZeros(ColumnType.getDriver(columnType).getAuxVectorOffset(1));
            } else {
                address = frameMemory.getPageAddress(c);
                limit = frameMemory.getPageSize(c);
                shift = ColumnType.pow2SizeOf(columnType);
            }
            if (address != 0 && shift >= 0 && limit > 0) {
                addresses[count] = address;
                limits[count] = limit;
                shifts[count] = shift;
                count++;
            }
        }
    }

    /**
     * Loads one byte of every column of the first {@code n} rows (frame row indexes) in
     * {@code rows}. A no-op unless {@link #isEnabled()}.
     */
    public void touch(long[] rows, int n) {
        long sink = 0;
        for (int c = 0; c < count; c++) {
            final long address = addresses[c];
            final long limit = limits[c];
            final int shift = shifts[c];
            for (int i = 0; i < n; i++) {
                final long offset = rows[i] << shift;
                // A touched column has data for every row of the frame, so a row out of range
                // means the address or the limit was derived wrongly. That loses the prefetch
                // but changes no result, so only an assertion can catch it. Unsigned, so that a
                // negative row id can never read below the column.
                assert Long.compareUnsigned(offset, limit) < 0 : "touch out of range [offset=" + offset + ", limit=" + limit + ']';
                if (Long.compareUnsigned(offset, limit) < 0) {
                    sink += Unsafe.getByte(address + offset);
                }
            }
        }
        this.sink += sink;
    }
}
