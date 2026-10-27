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

import io.questdb.cairo.ReaderScanProfile;
import io.questdb.cairo.idx.AbstractPostingIndexReader;
import io.questdb.cairo.idx.IndexReader;
import io.questdb.cairo.idx.PostingIndexFwdReader;
import io.questdb.cairo.sql.ColumnMapping;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.Nullable;

/**
 * The page frames of a table scan, as they are, that also read one symbol's rows of a frame from
 * the frame's POSTING index: the parallel plan of a single-symbol index scan, see
 * {@link IndexKeyScanFrameFactory}. The Async Filter over it takes a frame's filtered rows from
 * {@link #readRows} instead of evaluating its filter, which it still evaluates for a frame
 * without a posting reader (Parquet, a partition without the column).
 * <p>
 * The posting readers of the frames are frozen once the frames have all been read, before any
 * frame is dispatched (the frame sequence reads every frame first), so that the workers'
 * detached cursors read them without changing them; they are unfrozen when this cursor closes or
 * rewinds, after the sequence has waited for every task.
 */
public class IndexKeyPageFrameCursor implements PageFrameCursor {
    // tests: frames read from the index, and frames left to the filter, counted under -ea
    public static final java.util.concurrent.atomic.AtomicLong TEST_FILTER_FRAMES = new java.util.concurrent.atomic.AtomicLong();
    public static final java.util.concurrent.atomic.AtomicLong TEST_INDEX_FRAMES = new java.util.concurrent.atomic.AtomicLong();
    private static final int[] NO_KEYS = new int[0];
    private final LongList frameHis = new LongList();
    // per frame, 1 when its reader was taken without a reload, which the first thread to read the
    // frame does, see prepare()
    private final IntList frameLazy = new IntList();
    private final LongList frameLos = new LongList();
    // per frame, the posting reader of its partition, or null when it has none
    private final ObjList<AbstractPostingIndexReader> frameReaders = new ObjList<>();
    private final ObjList<AbstractPostingIndexReader> frozenReaders = new ObjList<>();
    // the readers taken without a reload, each once; frozen by the thread that prepares them
    private final ObjList<AbstractPostingIndexReader> lazyReaders = new ObjList<>();
    private final int keyColumnIndex;
    private PageFrameCursor base;
    // the symbol's index key, or -1 when the table does not have the symbol
    private int indexKey;
    private boolean isExhausted;

    public IndexKeyPageFrameCursor(int keyColumnIndex) {
        this.keyColumnIndex = keyColumnIndex;
    }

    @Override
    public void calculateSize(RecordCursor.Counter counter) {
        base.calculateSize(counter);
    }

    @Override
    public void close() {
        unfreeze();
        clearFrames();
        base = Misc.free(base);
    }

    @Override
    public boolean deferFrameAddresses() {
        return base.deferFrameAddresses();
    }

    @Override
    public ColumnMapping getColumnMapping() {
        return base.getColumnMapping();
    }

    @Override
    public long getFrameCountHint() {
        return base.getFrameCountHint();
    }

    public int getIndexKey() {
        return indexKey;
    }

    @Override
    public long getRemainingRowsInInterval() {
        return base.getRemainingRowsInInterval();
    }

    @Override
    public StaticSymbolTable getSymbolTable(int columnIndex) {
        return base.getSymbolTable(columnIndex);
    }

    @Override
    public boolean hasActivePushdownFilter() {
        return base.hasActivePushdownFilter();
    }

    @Override
    public boolean isExternal() {
        return base.isExternal();
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        return base.newSymbolTable(columnIndex);
    }

    @Override
    public @Nullable PageFrame next(long skipTarget) {
        if (isExhausted) {
            return null;
        }
        final PageFrame frame = base.next(skipTarget);
        if (frame != null) {
            // the index is in file rows: a composite piece's frame sits at its partition rows plus
            // the piece's shift, see PageFrame#getIndexRowLo()
            frameLos.add(frame.getIndexRowLo());
            frameHis.add(frame.getIndexRowHi());
            IndexReader reader = null;
            boolean isLazy = false;
            if (frame.getFormat() == PartitionFormat.NATIVE) {
                // an open reader is taken as it is: its reload (a read of its chain header, a cache
                // miss per partition) and its warm-up are left to the thread that reads the frame
                reader = frame.getIndexReaderWithoutReload(keyColumnIndex, IndexReader.DIR_FORWARD);
                isLazy = reader instanceof PostingIndexFwdReader;
                if (reader == null) {
                    reader = frame.getIndexReader(keyColumnIndex, IndexReader.DIR_FORWARD);
                }
            }
            final AbstractPostingIndexReader posting = reader instanceof PostingIndexFwdReader p ? p : null;
            frameReaders.add(posting);
            frameLazy.add(isLazy ? 1 : 0);
            if (isLazy) {
                final int n = lazyReaders.size();
                if (n == 0 || lazyReaders.getQuick(n - 1) != posting) {
                    lazyReaders.add(posting);
                }
            }
            return frame;
        }
        isExhausted = true;
        freeze();
        return null;
    }

    /**
     * Opens over a table scan's page frames, for a symbol's index key, -1 for a symbol the table
     * does not have. Takes the cursor; closing this one closes it.
     */
    public IndexKeyPageFrameCursor of(PageFrameCursor base, int indexKey) {
        unfreeze();
        clearFrames();
        Misc.free(this.base);
        this.base = base;
        this.indexKey = indexKey;
        return this;
    }

    /**
     * Fills {@code rows}, after clearing it, with the frame's rows of the symbol, as row indexes
     * of the frame, ascending; returns false, leaving the frame to the filter, when the frame has
     * no posting reader. Any thread may call it once every frame has been read, until the cursor
     * rewinds or closes.
     */
    public boolean readRows(int frameIndex, DirectLongList rows) {
        rows.clear();
        final AbstractPostingIndexReader reader = frameReaders.getQuick(frameIndex);
        if (reader == null) {
            assert TEST_FILTER_FRAMES.incrementAndGet() > 0;
            return false;
        }
        assert TEST_INDEX_FRAMES.incrementAndGet() > 0;
        if (frameLazy.getQuick(frameIndex) == 1) {
            prepare(reader);
        }
        // frozen before it is read, see freeze() and prepare(): a reload would change what other
        // threads read
        assert reader.isFrozen();
        if (indexKey < 0) {
            return true;
        }
        final long lo = frameLos.getQuick(frameIndex);
        final long hi = frameHis.getQuick(frameIndex);
        if (hi <= lo) {
            return true;
        }
        final RowCursor cursor = reader.getDetachedCursor(indexKey, lo, hi - 1, null);
        try {
            // the rows are relative to the range's low row: the frame's row indexes
            cursor.drainTo(rows, 0, Long.MAX_VALUE);
        } finally {
            Misc.free(cursor);
        }
        return true;
    }

    @Override
    public void releaseOpenPartitions() {
        base.releaseOpenPartitions();
    }

    @Override
    public void resumeTimer() {
        base.resumeTimer();
    }

    @Override
    public void setScanProfile(ReaderScanProfile profile) {
        base.setScanProfile(profile);
    }

    @Override
    public long size() {
        return base.size();
    }

    @Override
    public boolean supportsSizeCalculation() {
        return base.supportsSizeCalculation();
    }

    @Override
    public void suspendTimer() {
        base.suspendTimer();
    }

    @Override
    public void toTop() {
        unfreeze();
        clearFrames();
        base.toTop();
    }

    // Brings a reader taken without a reload to this query's snapshot, warmed and frozen, once: the
    // first thread to read one of its frames does it, the others wait on the reader's monitor, which
    // also publishes the reader's state to them. Readers of distinct partitions are distinct objects,
    // and no other thread uses them until the cursor closes, after every task.
    private static void prepare(AbstractPostingIndexReader reader) {
        synchronized (reader) {
            if (!reader.isFrozen()) {
                // reloads to the reader's pinned txn, then maps the value memory in full
                reader.warmForKeys(NO_KEYS, null);
                reader.setFrozen(true);
            }
        }
    }

    private void clearFrames() {
        frameLos.clear();
        frameHis.clear();
        frameReaders.clear();
        frameLazy.clear();
        isExhausted = false;
    }

    private void freeze() {
        for (int i = 0, n = frameReaders.size(); i < n; i++) {
            final AbstractPostingIndexReader reader = frameReaders.getQuick(i);
            if (reader != null && frameLazy.getQuick(i) == 0 && !reader.isFrozen()) {
                // positioned at this query's index snapshot, with its value memory mapped in full,
                // so the workers' detached cursors read it without changing it
                reader.warmForKeys(NO_KEYS, null);
                reader.setFrozen(true);
                frozenReaders.add(reader);
            }
        }
    }

    private void unfreeze() {
        for (int i = 0, n = frozenReaders.size(); i < n; i++) {
            frozenReaders.getQuick(i).setFrozen(false);
        }
        frozenReaders.clear();
        // the lazily prepared readers; the sequence has waited for every task by now
        for (int i = 0, n = lazyReaders.size(); i < n; i++) {
            final AbstractPostingIndexReader reader = lazyReaders.getQuick(i);
            synchronized (reader) {
                reader.setFrozen(false);
            }
        }
        lazyReaders.clear();
    }
}
