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

package io.questdb.cairo;

import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.engine.table.parquet.PartitionDescriptor;
import io.questdb.std.DirectIntList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;

public class O3ParquetMergeContext extends ParquetConversionContext {
    // Initial size of the rebase aux arena (see getRebaseAuxMem). Grown on demand.
    private static final long REBASE_AUX_ARENA_PAGE_SIZE = 16 * 1024;
    private ObjList<O3ParquetMergeStrategy.MergeAction> actionsBuf;
    private DirectIntList bloomFilterColumns;
    // Non-owning descriptor for the O3-only writers (writeFreshParquetFromO3 +
    // copyO3ToRowGroup). Column pointers reference already-sorted/deduped O3
    // source buffers — and the merge index for the designated timestamp — so
    // nothing in this descriptor needs to be freed by the descriptor itself.
    private PartitionDescriptor freshPartitionDescriptor;
    private LongList gapO3Ranges;
    private LongList mergeDstBufs;
    private LongList nullBufs;
    private MemoryCARW rebaseAuxMem;
    private LongList rgO3Ranges;
    private LongList rowGroupBounds;
    private LongList srcPtrs;

    public O3ParquetMergeContext() {
        super(MemoryTag.NATIVE_O3);
        actionsBuf = new ObjList<>();
        bloomFilterColumns = new DirectIntList(16, MemoryTag.NATIVE_O3);
        freshPartitionDescriptor = new PartitionDescriptor();
        gapO3Ranges = new LongList();
        mergeDstBufs = new LongList();
        nullBufs = new LongList();
        rebaseAuxMem = Vm.getCARWInstance(REBASE_AUX_ARENA_PAGE_SIZE, Integer.MAX_VALUE, MemoryTag.NATIVE_O3);
        rgO3Ranges = new LongList();
        rowGroupBounds = new LongList();
        srcPtrs = new LongList();
    }

    @Override
    public void clear() {
        super.clear();
        bloomFilterColumns.clear();
        freshPartitionDescriptor.clear();
        gapO3Ranges.clear();
        mergeDstBufs.clear();
        nullBufs.clear();
        rgO3Ranges.clear();
        rowGroupBounds.clear();
        srcPtrs.clear();
    }

    @Override
    public void close() {
        actionsBuf = null;
        bloomFilterColumns = Misc.free(bloomFilterColumns);
        freshPartitionDescriptor = Misc.free(freshPartitionDescriptor);
        gapO3Ranges = null;
        // Each list stores [addr, size, addr, size] per column; the per-row-group
        // finally blocks normally free these, but on abnormal shutdown (worker
        // thread death) the lists may still hold native pointers. Walk and free
        // before dropping the references.
        mergeDstBufs = freeNativePairsAndNull(mergeDstBufs);
        nullBufs = freeNativePairsAndNull(nullBufs);
        rebaseAuxMem = Misc.free(rebaseAuxMem);
        rgO3Ranges = null;
        rowGroupBounds = null;
        srcPtrs = null;
        super.close();
    }

    public ObjList<O3ParquetMergeStrategy.MergeAction> getActionsBuf() {
        return actionsBuf;
    }

    public DirectIntList getBloomFilterColumns() {
        return bloomFilterColumns;
    }

    public PartitionDescriptor getFreshPartitionDescriptor() {
        return freshPartitionDescriptor;
    }

    public LongList getGapO3Ranges() {
        return gapO3Ranges;
    }

    public LongList getMergeDstBufs(int colCount) {
        final int requiredLen = colCount * 4;
        mergeDstBufs.setPos(requiredLen);
        mergeDstBufs.fill(0, requiredLen, 0);
        return mergeDstBufs;
    }

    public LongList getNullBufs(int colCount) {
        final int requiredLen = colCount * 4;
        nullBufs.setPos(requiredLen);
        nullBufs.fill(0, requiredLen, 0);
        return nullBufs;
    }

    /**
     * Reusable scratch arena for window-relative rebased var-column aux vectors.
     * The caller sizes it once per apply (a later resize moves earlier slots),
     * then hands out base+offset slots. Freed in {@link #close()}.
     */
    public MemoryCARW getRebaseAuxMem() {
        return rebaseAuxMem;
    }

    public LongList getRgO3Ranges() {
        return rgO3Ranges;
    }

    public LongList getRowGroupBounds() {
        return rowGroupBounds;
    }

    public LongList getSrcPtrs(int colCount) {
        final int requiredLen = colCount * 2;
        srcPtrs.setPos(requiredLen);
        srcPtrs.fill(0, requiredLen, 0);
        return srcPtrs;
    }
}
