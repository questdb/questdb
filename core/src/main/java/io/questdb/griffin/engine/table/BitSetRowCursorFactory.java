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

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.cairo.sql.RowCursorFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.Nullable;

/**
 * Returns the rows of the current page frame in table (physical) order: drains every key cursor into a bit set of
 * the frame's rows, then walks the set bits in ascending order, applying the filter, if any, to each row. Unlike
 * {@link HeapRowCursorFactory} it pays no heap operation per row and, unlike {@link SequentialRowCursorFactory}, it
 * reads the column data in a single ascending pass, but it reads all key cursors of a frame before returning the
 * frame's first row.
 */
public class BitSetRowCursorFactory implements RowCursorFactory {
    private final BitSetRowCursor cursor = new BitSetRowCursor();
    private final ObjList<? extends RowCursorFactory> cursorFactories;
    // used to skip some cursor factories if values repeat
    private final int[] cursorFactoriesIdx;
    private final Function filter;
    private final PageFrameMemoryRecord record;
    // summaryWords has a bit per word of rowWords that has a bit set, so that a sparse frame walks no empty words
    private long[] rowWords = new long[64];
    private long[] summaryWords = new long[1];

    /**
     * @param filter the filter to apply to each row, owned by the caller
     */
    public BitSetRowCursorFactory(ObjList<? extends RowCursorFactory> cursorFactories, int[] cursorFactoriesIdx, @Nullable Function filter) {
        this.cursorFactories = cursorFactories;
        this.cursorFactoriesIdx = cursorFactoriesIdx;
        this.filter = filter;
        this.record = filter != null ? new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER) : null;
    }

    @Override
    public RowCursor getCursor(PageFrame pageFrame, PageFrameMemory pageFrameMemory) {
        cursor.clear();
        final long frameSize = pageFrame.getPartitionHi() - pageFrame.getPartitionLo();
        final int rowWordCount = (int) ((frameSize + 63) >>> 6);
        final int summaryWordCount = (rowWordCount + 63) >>> 6;
        if (rowWords.length < rowWordCount) {
            rowWords = new long[rowWordCount];
            summaryWords = new long[summaryWordCount];
        }
        cursor.of(summaryWordCount);
        final long[] rowWords = this.rowWords;
        final long[] summaryWords = this.summaryWords;
        for (int i = 0, n = cursorFactoriesIdx[0]; i < n; i++) {
            try (RowCursor keyCursor = cursorFactories.getQuick(i).getCursor(pageFrame, pageFrameMemory)) {
                while (keyCursor.hasNext()) {
                    final long row = keyCursor.next();
                    final int word = (int) (row >>> 6);
                    rowWords[word] |= 1L << row;
                    summaryWords[word >>> 6] |= 1L << word;
                }
            }
        }
        if (record != null) {
            record.init(pageFrameMemory);
        }
        return cursor;
    }

    @Override
    public void init(PageFrameCursor pageFrameCursor, SqlExecutionContext sqlExecutionContext) throws SqlException {
        RowCursorFactory.init(cursorFactories, pageFrameCursor, sqlExecutionContext);
    }

    @Override
    public boolean isEntity() {
        return false;
    }

    @Override
    public boolean isUsingIndex() {
        return true;
    }

    @Override
    public void prepareCursor(PageFrameCursor pageFrameCursor) {
        RowCursorFactory.prepareCursor(cursorFactories, pageFrameCursor);
        if (record != null) {
            record.of(pageFrameCursor);
        }
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Table-order scan");
        sink.optAttr("filter", filter);
        for (int i = 0, n = cursorFactories.size(); i < n; i++) {
            sink.child(cursorFactories.getQuick(i));
        }
    }

    private class BitSetRowCursor implements RowCursor {
        private long row;
        private long rowBits;
        private int rowWord;
        private long summaryBits;
        private int summaryHi;
        private int summaryWord;

        @Override
        public boolean hasNext() {
            while (true) {
                while (rowBits == 0) {
                    while (summaryBits == 0) {
                        if (summaryWord >= summaryHi) {
                            return false;
                        }
                        summaryBits = summaryWords[++summaryWord];
                        summaryWords[summaryWord] = 0;
                    }
                    rowWord = (summaryWord << 6) + Long.numberOfTrailingZeros(summaryBits);
                    summaryBits &= summaryBits - 1;
                    rowBits = rowWords[rowWord];
                    rowWords[rowWord] = 0;
                }
                row = ((long) rowWord << 6) + Long.numberOfTrailingZeros(rowBits);
                rowBits &= rowBits - 1;
                if (record == null) {
                    return true;
                }
                record.setRowIndex(row);
                if (filter.getBool(record)) {
                    return true;
                }
            }
        }

        @Override
        public long next() {
            return row;
        }

        // Zeroes the bits a previous frame left set, when its walk stopped early or its drain failed.
        private void clear() {
            while (summaryBits != 0) {
                rowWords[(summaryWord << 6) + Long.numberOfTrailingZeros(summaryBits)] = 0;
                summaryBits &= summaryBits - 1;
            }
            for (int i = summaryWord + 1; i < summaryHi + 1; i++) {
                long bits = summaryWords[i];
                summaryWords[i] = 0;
                while (bits != 0) {
                    rowWords[(i << 6) + Long.numberOfTrailingZeros(bits)] = 0;
                    bits &= bits - 1;
                }
            }
            rowBits = 0;
            summaryWord = summaryHi;
        }

        private void of(int summaryWordCount) {
            summaryWord = -1;
            summaryHi = summaryWordCount - 1;
        }
    }
}
