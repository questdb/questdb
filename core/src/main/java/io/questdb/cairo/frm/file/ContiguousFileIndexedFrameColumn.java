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

package io.questdb.cairo.frm.file;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.frm.FrameColumn;
import io.questdb.cairo.idx.IndexFactory;
import io.questdb.cairo.idx.IndexWriter;
import io.questdb.std.Misc;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;

public class ContiguousFileIndexedFrameColumn extends ContiguousFileFixFrameColumn {
    private final CairoConfiguration configuration;
    private byte indexType = IndexType.NONE;
    private IndexWriter indexWriter;
    private long upcomingTableTxn = -1L;

    public ContiguousFileIndexedFrameColumn(CairoConfiguration configuration) {
        super(configuration);
        this.configuration = configuration;
    }

    @Override
    public void append(long appendOffsetRowCount, FrameColumn sourceColumn, long sourceLo, long sourceHi, int commitMode) {
        super.append(appendOffsetRowCount, sourceColumn, sourceLo, sourceHi, commitMode);
        indexWrittenRows(appendOffsetRowCount, sourceHi - sourceLo);
    }

    @Override
    public void appendNulls(long rowCount, long sourceColumnTop, int commitMode) {
        super.appendNulls(rowCount, sourceColumnTop, commitMode);
        // Must come BEFORE rollbackConditionally, which can publish. See append().
        if (upcomingTableTxn >= 0) {
            indexWriter.setNextTxnAtSeal(upcomingTableTxn);
        }
        indexWriter.rollbackConditionally(rowCount);
        for (long i = 0; i < sourceColumnTop; i++) {
            indexWriter.add(0, rowCount + i);
        }
        indexWriter.setMaxValue(rowCount + sourceColumnTop - 1);
        indexWriter.commit();
    }

    @Override
    public void close() {
        Misc.free(indexWriter);
        upcomingTableTxn = -1L;
        super.close();
    }

    @Override
    public void merge(
            long appendOffsetRowCount,
            FrameColumn sourceColumn1,
            long source1Lo,
            long source1Hi,
            FrameColumn sourceColumn2,
            long source2Lo,
            long source2Hi,
            long mergeIndexAddr,
            long mergeIndexRows,
            int commitMode
    ) {
        super.merge(
                appendOffsetRowCount,
                sourceColumn1,
                source1Lo,
                source1Hi,
                sourceColumn2,
                source2Lo,
                source2Hi,
                mergeIndexAddr,
                mergeIndexRows,
                commitMode
        );
        // A merged row keeps its key but lands at a new row, so the index has to be told where it went. The
        // keys are read back out of what was just written rather than off either source, because that is the
        // only place the two sides are already in the order the index has to record.
        indexWrittenRows(appendOffsetRowCount, mergeIndexRows);
    }

    public void ofRW(
            Path partitionPath,
            CharSequence columnName,
            long columnTxn,
            int columnType,
            int indexBlockCapacity,
            byte indexType,
            long columnTop,
            int columnIndex,
            boolean isEmpty
    ) {
        super.ofRW(partitionPath, columnName, columnTxn, columnType, columnTop, columnIndex);
        this.upcomingTableTxn = -1L;
        try {
            if (indexWriter == null || this.indexType != indexType) {
                if (indexWriter != null) {
                    Misc.free(indexWriter);
                }
                this.indexType = indexType;
                this.indexWriter = IndexFactory.createWriter(indexType, configuration);
            }
            indexWriter.of(partitionPath, columnName, columnTxn, isEmpty ? indexBlockCapacity : 0);
        } catch (Throwable e) {
            close();
            throw e;
        }
    }

    // Keep old signature for backward compatibility
    public void ofRW(
            Path partitionPath,
            CharSequence columnName,
            long columnTxn,
            int columnType,
            int indexBlockCapacity,
            long columnTop,
            int columnIndex,
            boolean isEmpty
    ) {
        ofRW(partitionPath, columnName, columnTxn, columnType, indexBlockCapacity, IndexType.BITMAP, columnTop, columnIndex, isEmpty);
    }

    @Override
    public void ofRW(
            Path partitionPath,
            CharSequence columnName,
            long columnTxn,
            int columnType,
            long columnTop,
            int columnIndex
    ) {
        // close to reuse
        closed = false;
        super.close();
        throw new UnsupportedOperationException();
    }

    @Override
    public void setUpcomingTableTxn(long upcomingTableTxn) {
        this.upcomingTableTxn = upcomingTableTxn;
    }

    /**
     * Publishes index entries for {@code rowCount} rows this column has just written at
     * {@code appendOffsetRowCount}, reading their keys back out of the column file.
     */
    private void indexWrittenRows(long appendOffsetRowCount, long rowCount) {
        assert rowCount >= 0;
        if (rowCount == 0) {
            return;
        }

        final long fd = super.getPrimaryFd();
        final int shl = ColumnType.pow2SizeOf(getColumnType());
        final long offset = (appendOffsetRowCount - getColumnTop()) << shl;
        final long size = rowCount << shl;
        final long mappedAddress = TableUtils.mapAppendColumnBuffer(ff, fd, offset, size, false, MEMORY_TAG);
        try {
            // Must come BEFORE rollbackConditionally: that call publishes when the index still holds
            // rowids at or above the append offset (an O3 split shrank the partition without resealing
            // the parent), and ofRW's of() has just reset pendingTxnAtSeal to -1. Armed after it, the
            // republished entry would take publishToChain's pendingTxnAtSeal<0 fallback and land tagged
            // TXN_AT_SEAL=0 -- visible to every pinned reader and undroppable by the writer-open
            // recovery walk, whose predicate (txnAtSeal > committedTxn) can never fire on 0.
            if (upcomingTableTxn >= 0) {
                indexWriter.setNextTxnAtSeal(upcomingTableTxn);
            }
            indexWriter.rollbackConditionally(appendOffsetRowCount);
            for (long i = 0; i < rowCount; i++) {
                indexWriter.add(TableUtils.toIndexKey(Unsafe.getInt(mappedAddress + (i << shl))), appendOffsetRowCount + i);
            }
            indexWriter.setMaxValue(appendOffsetRowCount + rowCount - 1);
            indexWriter.commit();
        } finally {
            TableUtils.mapAppendColumnBufferRelease(ff, mappedAddress, offset, size, MEMORY_TAG);
        }
    }

    // Useful for debugging
    @SuppressWarnings("unused")
    private int keyCount(int key, long size, long mappedAddress) {
        int count = 0;
        for (long i = 0; i < size; i++) {
            if (TableUtils.toIndexKey(Unsafe.getInt(mappedAddress + (i << 2))) == key) {
                count++;
            }
        }
        return count;
    }
}
