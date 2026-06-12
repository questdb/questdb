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

import io.questdb.cairo.sql.PageFrameAddressCache;
import io.questdb.cairo.sql.PageFrameMemoryPool;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.ParquetDecodeHint;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.async.UnorderedPageFrameSequence;
import io.questdb.griffin.engine.RecordComparator;
import io.questdb.griffin.engine.orderby.EncodedTopKBuffer;
import io.questdb.griffin.engine.orderby.LimitedSizeLongTreeChain;
import io.questdb.griffin.engine.orderby.SortKeyType;
import io.questdb.std.DirectLongList;
import io.questdb.std.Rows;
import io.questdb.std.Unsafe;

class AsyncTopKRecordCursor implements RecordCursor, RecordCursor.RowIdSource {
    private LimitedSizeLongTreeChain.TreeCursor chainCursor;
    private long consumedCount;
    private long currentAddr;
    private long emitEndAddr;
    private long emitStartAddr;
    private int entrySize;
    private PageFrameMemoryPool frameMemoryPool;
    private UnorderedPageFrameSequence<AsyncTopKAtom> frameSequence;
    private boolean isChainBuilt;
    private boolean isOpen = true;
    private PageFrameMemoryRecord recordA;
    private PageFrameMemoryRecord recordB;

    @Override
    public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
        ensureChainBuilt();
        if (frameSequence.getAtom().isEncoded()) {
            counter.add((emitEndAddr - currentAddr) / entrySize);
            currentAddr = emitEndAddr;
            return;
        }
        long size = size();
        counter.add(size - consumedCount);
        consumedCount = size;
    }

    @Override
    public void close() {
        if (isOpen) {
            try {
                if (frameSequence != null) {
                    frameSequence.await();
                    frameSequence.reset();
                }
            } finally {
                isOpen = false;
            }
        }
    }

    @Override
    public void copyParquetRowIdsTo(DirectLongList target, PageFrameAddressCache addressCache) {
        long parquetRowCount = 0;
        for (long addr = emitStartAddr; addr < emitEndAddr; addr += entrySize) {
            if (addressCache.getFrameFormat(Rows.toPartitionIndex(Unsafe.getLong(addr))) == PartitionFormat.PARQUET) {
                parquetRowCount++;
            }
        }
        if (parquetRowCount == 0) {
            return;
        }
        target.ensureCapacity(parquetRowCount);
        for (long addr = emitStartAddr; addr < emitEndAddr; addr += entrySize) {
            final long rowId = Unsafe.getLong(addr);
            if (addressCache.getFrameFormat(Rows.toPartitionIndex(rowId)) == PartitionFormat.PARQUET) {
                target.add(rowId);
            }
        }
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
    public SymbolTable getSymbolTable(int columnIndex) {
        return frameSequence.getSymbolTableSource().getSymbolTable(columnIndex);
    }

    @Override
    public boolean hasNext() {
        ensureChainBuilt();
        if (frameSequence.getAtom().isEncoded()) {
            if (currentAddr >= emitEndAddr) {
                return false;
            }
            recordAt(recordA, Unsafe.getLong(currentAddr));
            currentAddr += entrySize;
            return true;
        }
        if (consumedCount == size()) {
            return false;
        }
        if (chainCursor.hasNext()) {
            recordAt(recordA, chainCursor.next());
            consumedCount++;
            return true;
        }
        return false;
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        return frameSequence.getSymbolTableSource().newSymbolTable(columnIndex);
    }

    @Override
    public long preComputedStateSize() {
        return isChainBuilt ? 1 : 0;
    }

    @Override
    public void recordAt(Record record, long atRowId) {
        final PageFrameMemoryRecord frameMemoryRecord = (PageFrameMemoryRecord) record;
        frameMemoryPool.navigateTo(Rows.toPartitionIndex(atRowId), frameMemoryRecord);
        frameMemoryRecord.setRowIndex(Rows.toLocalRowID(atRowId));
    }

    @Override
    public long size() {
        if (!isChainBuilt) {
            return -1;
        }
        final AsyncTopKAtom atom = frameSequence.getAtom();
        if (atom.isEncoded()) {
            return (emitEndAddr - emitStartAddr) / entrySize;
        }
        return atom.getOwnerChain().size();
    }

    @Override
    public void toTop() {
        if (isChainBuilt && chainCursor != null) {
            chainCursor.toTop();
        }
        currentAddr = emitStartAddr;
        consumedCount = 0;
    }

    private void buildChain() {
        frameSequence.prepareForDispatch();
        frameSequence.getAtom().getFilterContext().initMemoryPools(frameSequence.getPageFrameAddressCache(), ParquetDecodeHint.SCATTERED);
        frameSequence.dispatchAndAwait();

        // merge everything into owner chain
        mergeChains();
    }

    private void ensureChainBuilt() {
        if (!isChainBuilt) {
            buildChain();
            isChainBuilt = true;
        }
    }

    private void mergeChains() {
        final AsyncTopKAtom atom = frameSequence.getAtom();
        if (atom.isEncoded()) {
            final EncodedTopKBuffer ownerTopK = atom.getTopK(-1);
            for (int i = 0, n = atom.getWorkerCount(); i < n; i++) {
                ownerTopK.mergeFrom(atom.getTopK(i));
            }
            atom.freePerWorkerChainsAndPools();
            ownerTopK.sort();
            final SortKeyType keyType = atom.getKeyType();
            entrySize = keyType.entrySize();
            final long emitCount = Math.min(ownerTopK.getCount(), atom.getLo());
            emitStartAddr = ownerTopK.getAddress() + keyType.rowIdOffset();
            emitEndAddr = emitStartAddr + emitCount * entrySize;
            currentAddr = emitStartAddr;
            if (emitStartAddr < emitEndAddr) {
                frameMemoryPool.setRecordAtRows(this);
            }
            return;
        }
        final LimitedSizeLongTreeChain ownerChain = atom.getOwnerChain();
        final RecordComparator ownerComparator = atom.getOwnerComparator();
        for (int i = 0, n = atom.getWorkerCount(); i < n; i++) {
            final LimitedSizeLongTreeChain workerChain = atom.getPerWorkerChains().getQuick(i);
            final LimitedSizeLongTreeChain.TreeCursor workerCursor = workerChain.getCursor();
            while (workerCursor.hasNext()) {
                recordAt(recordA, workerCursor.next());
                ownerChain.put(
                        recordA,
                        frameMemoryPool,
                        recordB,
                        ownerComparator
                );
            }
        }

        // free per worker pools and chains
        atom.freePerWorkerChainsAndPools();

        chainCursor = ownerChain.getCursor();
    }

    void of(UnorderedPageFrameSequence<AsyncTopKAtom> frameSequence) {
        final AsyncTopKAtom atom = frameSequence.getAtom();
        if (!isOpen) {
            isOpen = true;
            atom.reopen();
        }
        this.frameSequence = frameSequence;
        this.frameMemoryPool = atom.getFilterContext().getOwnerMemoryPool();
        this.recordA = atom.getOwnerRecordA();
        this.recordB = atom.getOwnerRecordB();
        isChainBuilt = false;
        consumedCount = 0;
        currentAddr = 0;
        emitStartAddr = 0;
        emitEndAddr = 0;
    }
}
