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
import io.questdb.griffin.engine.table.parquet.RowGroupBuffers;
import io.questdb.std.DirectIntList;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;

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
    // Whether the first stage of the merge that keepRowGroupMerge() kept prepared the designated timestamp and
    // the dedup key columns only
    private boolean isPreparedMergeKeyColumnsOnly;
    // The MERGE among the unchanged merge actions whose row groups hold NULLs of a column without NULL that may
    // be the rows of the column top that the output keeps, -1 when there is none, see setKeptTopMergeAction()
    private int keptTopMergeAction = -1;
    // Decode buffers of the designated timestamp and the dedup key columns of a SKIP row group merge.
    // They are apart from getRowGroupBuffers() because a parquet rewrite can copy row groups through
    // ParquetRowGroupMaterializer, which decodes into the shared buffers, between the two stages of
    // the merge. See O3PartitionJob.mergeRowGroupPrepare().
    private RowGroupBuffers keyRowGroupBuffers;
    // The first stage of a row group merge hands the number of active columns, the active column
    // lists and the merge index, which this context owns, to the second stage. The lists are apart
    // from the shared lists of ParquetConversionContext for the same reason as keyRowGroupBuffers.
    private int mergeActiveColCount;
    private IntList mergeActiveColIndices;
    private IntList mergeActiveToDecodeIdx;
    private LongList mergeDstBufs;
    // the columns that the first stage of the row group merge decoded
    private int mergeFirstStageDecodedColumnCount;
    private long mergeIndexAddr;
    private long mergeIndexSize;
    // the stored designated timestamps of the row group merge, as its first stage decoded them
    private long mergeTimestampDataAddr;
    private LongList nullBufs;
    // The merge action whose row group merge keepRowGroupMerge() kept, -1 when there is none, and the rows that
    // its dedup dropped
    private int preparedMergeActionIndex = -1;
    private long preparedMergeDuplicateCount;
    private MemoryCARW rebaseAuxMem;
    private LongList rgO3Ranges;
    private LongList rowGroupBounds;
    private LongList rowGroupIndexRowLos;
    private IntList rowGroupOrder;
    private LongList srcPtrs;
    // The merge actions [unchangedMergeActionLo, unchangedMergeActionHi) of the partition whose MERGEs the merge loop
    // leaves as stored, see setUnchangedMergeActions()
    private int unchangedMergeActionHi;
    private int unchangedMergeActionLo;

    public O3ParquetMergeContext() {
        super(MemoryTag.NATIVE_O3);
        actionsBuf = new ObjList<>();
        bloomFilterColumns = new DirectIntList(16, MemoryTag.NATIVE_O3);
        freshPartitionDescriptor = new PartitionDescriptor();
        gapO3Ranges = new LongList();
        keyRowGroupBuffers = new RowGroupBuffers(MemoryTag.NATIVE_PARQUET_PARTITION_UPDATER);
        mergeActiveColIndices = new IntList();
        mergeActiveToDecodeIdx = new IntList();
        mergeDstBufs = new LongList();
        nullBufs = new LongList();
        rebaseAuxMem = Vm.getCARWInstance(REBASE_AUX_ARENA_PAGE_SIZE, Integer.MAX_VALUE, MemoryTag.NATIVE_O3);
        rgO3Ranges = new LongList();
        rowGroupBounds = new LongList();
        rowGroupIndexRowLos = new LongList();
        rowGroupOrder = new IntList();
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
        rowGroupIndexRowLos.clear();
        rowGroupOrder.clear();
        srcPtrs.clear();
        keptTopMergeAction = -1;
        unchangedMergeActionLo = 0;
        unchangedMergeActionHi = 0;
    }

    @Override
    public void close() {
        actionsBuf = null;
        bloomFilterColumns = Misc.free(bloomFilterColumns);
        freshPartitionDescriptor = Misc.free(freshPartitionDescriptor);
        gapO3Ranges = null;
        keyRowGroupBuffers = Misc.free(keyRowGroupBuffers);
        mergeActiveColIndices = null;
        mergeActiveToDecodeIdx = null;
        // The merge index and the lists below are normally freed per row group,
        // but on abnormal shutdown (worker thread death) they may still hold native
        // memory. Each list stores [addr, size, addr, size] per column. Walk and free
        // before dropping the references.
        if (mergeIndexAddr != 0) {
            mergeIndexAddr = Unsafe.free(mergeIndexAddr, mergeIndexSize, MemoryTag.NATIVE_O3);
            mergeIndexSize = 0;
        }
        mergeDstBufs = freeNativePairsAndNull(mergeDstBufs);
        nullBufs = freeNativePairsAndNull(nullBufs);
        rebaseAuxMem = Misc.free(rebaseAuxMem);
        rgO3Ranges = null;
        rowGroupBounds = null;
        rowGroupIndexRowLos = null;
        rowGroupOrder = null;
        srcPtrs = null;
        super.close();
    }

    /**
     * Frees the native memory of a row group merge: the merge index and the source buffers that
     * the merge allocated for converted, column top and missing columns. Call it once per row group,
     * after the merge, whichever way it ends. A second call frees nothing.
     */
    public void freeRowGroupMerge() {
        freeNativePairs(nullBufs);
        if (mergeIndexAddr != 0) {
            mergeIndexAddr = Unsafe.free(mergeIndexAddr, mergeIndexSize, MemoryTag.NATIVE_O3);
        }
        mergeIndexSize = 0;
        mergeActiveColCount = 0;
        mergeFirstStageDecodedColumnCount = 0;
        mergeTimestampDataAddr = 0;
        preparedMergeActionIndex = -1;
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

    // the MERGE that setKeptTopMergeAction() named, -1 when there is none
    public int getKeptTopMergeAction() {
        return keptTopMergeAction;
    }

    public RowGroupBuffers getKeyRowGroupBuffers() {
        return keyRowGroupBuffers;
    }

    public int getMergeActiveColCount() {
        return mergeActiveColCount;
    }

    public IntList getMergeActiveColIndices() {
        return mergeActiveColIndices;
    }

    public IntList getMergeActiveToDecodeIdx() {
        return mergeActiveToDecodeIdx;
    }

    public LongList getMergeDstBufs(int colCount) {
        final int requiredLen = colCount * 4;
        mergeDstBufs.setPos(requiredLen);
        mergeDstBufs.fill(0, requiredLen, 0);
        return mergeDstBufs;
    }

    // the columns that the first stage of the row group merge decoded, see setMergeFirstStage()
    public int getMergeFirstStageDecodedColumnCount() {
        return mergeFirstStageDecodedColumnCount;
    }

    public long getMergeIndexAddr() {
        return mergeIndexAddr;
    }

    // Number of rows of the merged row group, one merge index entry per row
    public long getMergeRowCount() {
        return mergeIndexSize / TableWriter.TIMESTAMP_MERGE_ENTRY_BYTES;
    }

    // the stored designated timestamps of the row group merge, see setMergeFirstStage()
    public long getMergeTimestampDataAddr() {
        assert mergeTimestampDataAddr != 0;
        return mergeTimestampDataAddr;
    }

    public LongList getNullBufs(int colCount) {
        final int requiredLen = colCount * 4;
        nullBufs.setPos(requiredLen);
        nullBufs.fill(0, requiredLen, 0);
        return nullBufs;
    }

    public int getPreparedMergeActionIndex() {
        return preparedMergeActionIndex;
    }

    public long getPreparedMergeDuplicateCount() {
        return preparedMergeDuplicateCount;
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

    public LongList getRowGroupIndexRowLos() {
        return rowGroupIndexRowLos;
    }

    public IntList getRowGroupOrder() {
        return rowGroupOrder;
    }

    public LongList getSrcPtrs(int colCount) {
        final int requiredLen = colCount * 2;
        srcPtrs.setPos(requiredLen);
        srcPtrs.fill(0, requiredLen, 0);
        return srcPtrs;
    }

    public boolean isPreparedMergeKeyColumnsOnly() {
        return isPreparedMergeKeyColumnsOnly;
    }

    // Whether the merge loop leaves the row groups of the MERGE at the given action index as stored, see
    // setUnchangedMergeActions()
    public boolean isUnchangedMergeAction(int actionIndex) {
        return actionIndex >= unchangedMergeActionLo && actionIndex < unchangedMergeActionHi;
    }

    /**
     * Keeps the row group merge that the first stage, or both stages, prepared for the merge action
     * at the given index, instead of freeing it: the merge loop completes it when it reaches the
     * action. {@link #freeRowGroupMerge()} frees it and forgets the action.
     *
     * @param actionIndex      the index of the merge action
     * @param duplicateCount   the rows that the dedup of the merge dropped
     * @param isKeyColumnsOnly whether the first stage prepared the designated timestamp and the dedup
     *                         key columns only, and the second stage has not run
     */
    public void keepRowGroupMerge(int actionIndex, long duplicateCount, boolean isKeyColumnsOnly) {
        assert mergeIndexAddr != 0;
        preparedMergeActionIndex = actionIndex;
        preparedMergeDuplicateCount = duplicateCount;
        isPreparedMergeKeyColumnsOnly = isKeyColumnsOnly;
    }

    /**
     * Takes the MERGE that {@link #setKeptTopMergeAction(int)} named out of the unchanged merge actions: the merge
     * loop merges it, because its merge writes its row groups otherwise than stored.
     */
    public void mergeKeptTopMergeAction() {
        assert isUnchangedMergeAction(keptTopMergeAction);
        unchangedMergeActionLo = keptTopMergeAction + 1;
        keptTopMergeAction = -1;
    }

    /**
     * Starts a row group merge with the given column count of the table. The previous merge must
     * have been freed with {@link #freeRowGroupMerge()}.
     */
    public void ofRowGroupMerge(int columnCount) {
        assert mergeIndexAddr == 0;
        mergeActiveColIndices.setPos(columnCount);
        mergeActiveToDecodeIdx.setPos(columnCount);
        mergeActiveColCount = 0;
    }

    /**
     * Allocates the merge index of a row group merge for the given number of rows, or resizes the
     * index the merge already has. This context owns the index until {@link #freeRowGroupMerge()}.
     */
    public long reallocMergeIndex(long rowCount) {
        final long size = rowCount * TableWriter.TIMESTAMP_MERGE_ENTRY_BYTES;
        if (mergeIndexAddr == 0) {
            mergeIndexAddr = Unsafe.malloc(size, MemoryTag.NATIVE_O3);
        } else {
            mergeIndexAddr = Unsafe.realloc(mergeIndexAddr, mergeIndexSize, size, MemoryTag.NATIVE_O3);
        }
        mergeIndexSize = size;
        return mergeIndexAddr;
    }

    /**
     * Names the MERGE among the merge actions of {@link #setUnchangedMergeActions(int, int)} that the merge loop leaves
     * as stored only when the partition updater finds that its merge writes its row groups as stored, see
     * {@code O3PartitionJob.isUpsertIdenticalUpToPartitionMax()}, otherwise {@link #mergeKeptTopMergeAction()} takes
     * it out. {@link #clear()} forgets it.
     */
    public void setKeptTopMergeAction(int actionIndex) {
        keptTopMergeAction = actionIndex;
    }

    public void setMergeActiveColCount(int mergeActiveColCount) {
        this.mergeActiveColCount = mergeActiveColCount;
    }

    /**
     * Records what the first stage of a row group merge decoded: the address of the stored designated
     * timestamps of its row groups, in the decode buffers of the stage, and the number of columns it
     * decoded. The address stays valid until the next decode into those buffers; the buffers of a stage
     * that decodes the keys only take no other decode before {@link #freeRowGroupMerge()}.
     */
    public void setMergeFirstStage(long timestampDataAddr, int decodedColumnCount) {
        mergeTimestampDataAddr = timestampDataAddr;
        mergeFirstStageDecodedColumnCount = decodedColumnCount;
    }

    /**
     * Sets the merge actions {@code [actionLo, actionHi)} whose MERGEs the UPSERT identity check found to reproduce
     * their row groups as stored, see {@code O3PartitionJob.isUpsertIdenticalUpToPartitionMax()}: the merge loop
     * leaves those row groups as they are. {@link #clear()} empties the range.
     */
    public void setUnchangedMergeActions(int actionLo, int actionHi) {
        unchangedMergeActionLo = actionLo;
        unchangedMergeActionHi = actionHi;
    }
}
