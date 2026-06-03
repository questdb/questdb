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

package io.questdb.cairo.sql;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.Reopenable;
import io.questdb.cairo.VarcharTypeDriver;
import io.questdb.griffin.engine.table.parquet.ParquetDecoder;
import io.questdb.griffin.engine.table.parquet.ParquetFileDecoder;
import io.questdb.griffin.engine.table.parquet.ParquetPartitionDecoder;
import io.questdb.griffin.engine.table.parquet.RowGroupBuffers;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.DirectIntList;
import io.questdb.std.DirectLongList;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntIntHashMap;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Rows;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Provides addresses for page frames in both native and Parquet formats.
 * Memory in native page frames is mmapped, so no additional actions are
 * necessary. Parquet frames must be explicitly deserialized into
 * the in-memory native format before being accessed directly or via a Record.
 * Thus, a {@link #navigateTo(int)} call is required before accessing memory
 * that belongs to a page frame.
 * <p>
 * Decoded Parquet frames live in a per-cursor LRU capped by total decoded
 * bytes ({@code cairo.sql.parquet.cache.memory.size}). Each entry tracks its
 * decoded size on insertion; inserting a new entry whose decode would push
 * the total over budget triggers LRU eviction of unused entries until the
 * total fits. Entries currently bound to a record or to the frame-memory
 * flyweight are never evicted, so the budget may be temporarily exceeded
 * when every cached entry is in use.
 * <p>
 * The access-pattern hint declared by the enclosing factory scales the
 * effective ceiling: {@link ParquetDecodeHint#MONOTONIC} cursors get a quarter
 * of the configured budget, {@link ParquetDecodeHint#SCATTERED} cursors get all
 * of it. Hints come in via {@link #of(PageFrameAddressCache, ParquetDecodeHint)}
 * or {@link #setParquetDecodeHint(ParquetDecodeHint)} and default to MONOTONIC.
 * <p>
 * Optional disk spill: when a SCATTERED-pattern cursor evicts a decoded
 * buffer that resides on a cold-tier (remote) partition, the pool may copy
 * the decoded bytes to a local-disk scratch file rather than discarding
 * them. Three gates must all hold: pattern == SCATTERED,
 * {@link PageFrameAddressCache#isColdParquetPartition(int)} == true, and
 * {@code cairo.sql.parquet.cache.disk.size} > 0. Spill files are tagged with
 * a per-cursor ID and deleted on {@link #close()}; the engine wipes the
 * spill directory on startup.
 * <p>
 * This pool is thread-unsafe as it may hold navigated Parquet partition data,
 * so it shouldn't be shared between multiple threads.
 */
public class PageFrameMemoryPool implements RecordRandomAccess, QuietCloseable, Mutable {
    public static final String SPILL_FILE_PREFIX = "qdb-parquet-spill-";
    private static final byte FRAME_MEMORY_MASK = 1 << 2;
    private static final Log LOG = LogFactory.getLog(PageFrameMemoryPool.class);
    private static final byte RECORD_A_MASK = 1;
    private static final byte RECORD_B_MASK = 1 << 1;
    private static final AtomicLong cursorIdGenerator = new AtomicLong();
    // LRU cache (most recently used buffers are to the right)
    private final ObjList<ParquetBuffers> cachedParquetBuffers;
    // Maps column ID (field_id / writer index) to parquet column index.
    // Rebuilt each time openParquet() encounters a new file.
    private final IntIntHashMap columnIdToParquetIdx;
    private final PageFrameMemoryImpl frameMemory;
    private final ObjList<ParquetBuffers> freeParquetBuffers;
    private final ParquetFileDecoder legacyDecoder;
    private final long maxCacheBytes;
    // Contains [parquet_column_index, column_type] pairs.
    // Each parquet column appears at most once even when multiple query
    // columns reference it (a SelectedRecord projection can list the same
    // base column twice). decode() iterates the query column mapping and
    // looks up the slot via parquetIdxToDecodeSlot.
    private final DirectIntList parquetColumns;
    // Maps parquet column index to its slot in parquetColumns / decoded
    // buffers. -1 when the parquet column is not part of the current
    // decode pass (excluded from the include/exclude filter, or absent
    // from the parquet file because it was added later).
    private final IntIntHashMap parquetIdxToDecodeSlot;
    private final ParquetPartitionDecoder parquetMetaDecoder;
    private final SpillManager spillManager;
    private ParquetDecodeHint accessPattern = ParquetDecodeHint.MONOTONIC;
    private ParquetDecoder activeDecoder;
    private PageFrameAddressCache addressCache;
    private long cachedBytes;
    private long effectiveBudgetBytes;

    public PageFrameMemoryPool(long maxCacheBytes) {
        this(maxCacheBytes, 0L, null, null, 0, null);
    }

    public PageFrameMemoryPool(
            long maxCacheBytes,
            long diskSpillBytes,
            @Nullable CharSequence diskSpillDir,
            @Nullable FilesFacade filesFacade,
            int mkDirMode,
            @Nullable ParquetDecodeMetrics metrics
    ) {
        try {
            this.maxCacheBytes = Math.max(maxCacheBytes, 0L);
            this.effectiveBudgetBytes = accessPattern.applyTo(this.maxCacheBytes);
            cachedParquetBuffers = new ObjList<>(8);
            freeParquetBuffers = new ObjList<>(8);
            columnIdToParquetIdx = new IntIntHashMap(16);
            frameMemory = new PageFrameMemoryImpl();
            parquetColumns = new DirectIntList(32, MemoryTag.NATIVE_DEFAULT, true);
            parquetIdxToDecodeSlot = new IntIntHashMap(16);
            parquetMetaDecoder = new ParquetPartitionDecoder();
            legacyDecoder = new ParquetFileDecoder();
            spillManager = (diskSpillBytes > 0 && diskSpillDir != null && filesFacade != null)
                    ? new SpillManager(filesFacade, diskSpillDir, diskSpillBytes, cursorIdGenerator.incrementAndGet(), mkDirMode, metrics)
                    : null;
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    public static void cleanStaleDiskSpillFiles(FilesFacade ff, @Nullable CharSequence dir) {
        if (dir == null || dir.length() == 0) {
            return;
        }
        Path path = Path.getThreadLocal(dir);
        if (ff.exists(path.$())) {
            SpillManager.deleteSpillFilesInDir(ff, path);
        }
    }

    @Override
    public void clear() {
        Misc.free(parquetMetaDecoder);
        Misc.free(legacyDecoder);
        activeDecoder = null;
        Misc.free(parquetColumns);
        releaseParquetBuffers();
    }

    @Override
    public void close() {
        Misc.free(parquetMetaDecoder);
        Misc.free(legacyDecoder);
        activeDecoder = null;
        Misc.free(parquetColumns);
        releaseParquetBuffers();
        if (spillManager != null) {
            spillManager.close();
        }
        addressCache = null;
    }

    /**
     * Navigates to the given frame, potentially deserializing it to in-memory format
     * (for Parquet partitions). After this call, the input record can be used to access
     * any row within the frame.
     */
    public void navigateTo(int frameIndex, PageFrameMemoryRecord record) {
        if (record.getFrameIndex() == frameIndex) {
            return;
        }

        final byte format = addressCache.getFrameFormat(frameIndex);
        final int columnOffset = addressCache.toColumnOffset(frameIndex);
        if (format == PartitionFormat.NATIVE) {
            record.init(
                    frameIndex,
                    format,
                    addressCache.getRowIdOffset(frameIndex),
                    addressCache.getPageAddresses(),
                    addressCache.getAuxPageAddresses(),
                    addressCache.getPageSizes(),
                    addressCache.getAuxPageSizes(),
                    columnOffset
            );
        } else if (format == PartitionFormat.PARQUET) {
            final byte usageBit = record.getLetter() == PageFrameMemoryRecord.RECORD_A_LETTER ? RECORD_A_MASK : RECORD_B_MASK;
            ParquetBuffers parquetBuffers = tryHit(frameIndex, usageBit);
            if (parquetBuffers == null) {
                openParquet(frameIndex);
                parquetBuffers = allocateMiss(frameIndex, usageBit);
                decodeAndAccount(frameIndex, parquetBuffers);
            }
            record.init(
                    frameIndex,
                    format,
                    addressCache.getRowIdOffset(frameIndex),
                    parquetBuffers.pageAddresses,
                    parquetBuffers.auxPageAddresses,
                    parquetBuffers.pageSizes,
                    parquetBuffers.auxPageSizes,
                    0
            );
        }
    }

    /**
     * Navigates to the given frame, potentially deserializing it to in-memory format
     * (for Parquet partitions). The returned PageFrameMemory object is a flyweight,
     * so it should be used immediately once returned. This method is useful for later
     * calls to native code.
     * <p>
     * If you need data access via {@link Record} API, use the
     * {@link #navigateTo(int, PageFrameMemoryRecord)} method.
     */
    public PageFrameMemory navigateTo(int frameIndex) {
        if (frameMemory.frameIndex == frameIndex) {
            return frameMemory;
        }

        final byte format = addressCache.getFrameFormat(frameIndex);
        final int columnOffset = addressCache.toColumnOffset(frameIndex);
        if (format == PartitionFormat.NATIVE) {
            frameMemory.pageAddresses = addressCache.getPageAddresses();
            frameMemory.auxPageAddresses = addressCache.getAuxPageAddresses();
            frameMemory.pageSizes = addressCache.getPageSizes();
            frameMemory.auxPageSizes = addressCache.getAuxPageSizes();
            frameMemory.columnOffset = columnOffset;
            frameMemory.currentRowGroupBuffer = null;
        } else if (format == PartitionFormat.PARQUET) {
            ParquetBuffers parquetBuffers = tryHit(frameIndex, FRAME_MEMORY_MASK);
            if (parquetBuffers == null) {
                openParquet(frameIndex);
                parquetBuffers = allocateMiss(frameIndex, FRAME_MEMORY_MASK);
                decodeAndAccount(frameIndex, parquetBuffers);
            }
            frameMemory.currentRowGroupBuffer = parquetBuffers;
            frameMemory.pageAddresses = parquetBuffers.pageAddresses;
            frameMemory.auxPageAddresses = parquetBuffers.auxPageAddresses;
            frameMemory.pageSizes = parquetBuffers.pageSizes;
            frameMemory.auxPageSizes = parquetBuffers.auxPageSizes;
            frameMemory.columnOffset = 0;
        }

        frameMemory.frameIndex = frameIndex;
        frameMemory.frameFormat = format;
        return frameMemory;
    }

    public PageFrameMemory navigateTo(int frameIndex, IntHashSet columnIndexes) {
        if (frameMemory.frameIndex == frameIndex) {
            return frameMemory;
        }

        final byte format = addressCache.getFrameFormat(frameIndex);
        final int columnOffset = addressCache.toColumnOffset(frameIndex);
        if (format == PartitionFormat.NATIVE) {
            frameMemory.pageAddresses = addressCache.getPageAddresses();
            frameMemory.auxPageAddresses = addressCache.getAuxPageAddresses();
            frameMemory.pageSizes = addressCache.getPageSizes();
            frameMemory.auxPageSizes = addressCache.getAuxPageSizes();
            frameMemory.columnOffset = columnOffset;
            frameMemory.currentRowGroupBuffer = null;
        } else if (format == PartitionFormat.PARQUET) {
            ParquetBuffers parquetBuffers = tryHit(frameIndex, FRAME_MEMORY_MASK);
            if (parquetBuffers == null) {
                openParquet(frameIndex, columnIndexes, true);
                parquetBuffers = allocateMiss(frameIndex, FRAME_MEMORY_MASK);
                decodeAndAccount(frameIndex, parquetBuffers);
            }
            frameMemory.currentRowGroupBuffer = parquetBuffers;
            frameMemory.pageAddresses = parquetBuffers.pageAddresses;
            frameMemory.auxPageAddresses = parquetBuffers.auxPageAddresses;
            frameMemory.pageSizes = parquetBuffers.pageSizes;
            frameMemory.auxPageSizes = parquetBuffers.auxPageSizes;
            frameMemory.columnOffset = 0;
        }

        frameMemory.frameIndex = frameIndex;
        frameMemory.frameFormat = format;
        return frameMemory;
    }

    public void of(PageFrameAddressCache addressCache) {
        of(addressCache, ParquetDecodeHint.MONOTONIC);
    }

    public void of(PageFrameAddressCache addressCache, ParquetDecodeHint hint) {
        releaseParquetBuffers();
        this.addressCache = addressCache;
        this.accessPattern = hint;
        this.effectiveBudgetBytes = hint.applyTo(maxCacheBytes);
        Misc.free(parquetMetaDecoder);
        Misc.free(legacyDecoder);
        activeDecoder = null;
    }

    @Override
    public void recordAt(Record record, long atRowId) {
        final PageFrameMemoryRecord frameMemoryRecord = (PageFrameMemoryRecord) record;
        navigateTo(Rows.toPartitionIndex(atRowId), frameMemoryRecord);
        frameMemoryRecord.setRowIndex(Rows.toLocalRowID(atRowId));
    }

    /**
     * Frees all decoded Parquet buffers and clears bookkeeping.
     * <p>
     * Bulk shutdown path: this does NOT honour the {@code usageFlags} pin
     * bits, so any {@link PageFrameMemoryRecord} or {@code frameMemory}
     * flyweight still bound to a cached entry will hold a dangling pointer
     * after this returns. Callers must ensure all records are abandoned
     * before invoking. Used from {@link #clear()} / {@link #close()} and
     * from cursor lifecycle points (toTop, close).
     */
    public void releaseParquetBuffers() {
        Misc.freeObjListAndKeepObjects(cachedParquetBuffers);
        freeParquetBuffers.addAll(cachedParquetBuffers);
        cachedParquetBuffers.clear();
        cachedBytes = 0;
        frameMemory.clear();
        if (spillManager != null) {
            spillManager.deleteAllSpilledFiles();
        }
    }

    public void setParquetDecodeHint(ParquetDecodeHint hint) {
        this.accessPattern = hint;
        this.effectiveBudgetBytes = hint.applyTo(maxCacheBytes);
        enforceBudget();
    }

    private void activateDecoder(int frameIndex) {
        final ParquetDecoder frameDecoder = addressCache.getParquetDecoder(frameIndex);
        if (frameDecoder instanceof ParquetPartitionDecoder parquetMetaFrame) {
            if (parquetMetaDecoder.getFileAddr() != parquetMetaFrame.getFileAddr() || parquetMetaDecoder.getFileSize() != parquetMetaFrame.getFileSize()) {
                parquetMetaDecoder.of(parquetMetaFrame);
                buildColumnIdMap(parquetMetaDecoder);
            }
            activeDecoder = parquetMetaDecoder;
        } else {
            ParquetFileDecoder legacyFrame = (ParquetFileDecoder) frameDecoder;
            if (legacyDecoder.getFileAddr() != legacyFrame.getFileAddr() || legacyDecoder.getFileSize() != legacyFrame.getFileSize()) {
                legacyDecoder.of(legacyFrame);
                buildColumnIdMap(legacyDecoder);
            }
            activeDecoder = legacyDecoder;
        }
    }

    private ParquetBuffers allocateMiss(int frameIndex, byte usageBit) {
        ParquetBuffers buffers;
        final int free = freeParquetBuffers.size();
        if (free > 0) {
            buffers = freeParquetBuffers.getQuick(free - 1);
            freeParquetBuffers.remove(free - 1);
        } else {
            buffers = new ParquetBuffers();
        }
        buffers.reopen();
        buffers.frameIndex = frameIndex;
        buffers.usageFlags = usageBit;
        buffers.decodedBytes = 0;
        cachedParquetBuffers.add(buffers);
        return buffers;
    }

    private void buildColumnIdMap(ParquetDecoder decoder) {
        final int parquetColumnCount = decoder.getColumnCount();
        columnIdToParquetIdx.clear();
        for (int i = 0; i < parquetColumnCount; i++) {
            final int id = decoder.getColumnId(i);
            // External parquet files may not have field IDs (all -1).
            // Fall back to positional index so the lookup in openParquet() works.
            columnIdToParquetIdx.put(id < 0 ? i : id, i);
        }
    }

    private void decodeAndAccount(int frameIndex, ParquetBuffers parquetBuffers) {
        if (spillManager != null) {
            boolean restored;
            try {
                restored = spillManager.restore(parquetBuffers, frameIndex, parquetColumns);
            } catch (Throwable th) {
                evictHalfInitialized(parquetBuffers);
                throw th;
            }
            if (restored) {
                try {
                    parquetBuffers.remapColumns();
                } catch (Throwable th) {
                    evictHalfInitialized(parquetBuffers);
                    throw th;
                }
                cachedBytes += parquetBuffers.decodedBytes;
                enforceBudget();
                return;
            }
        }
        final int rowGroupIndex = addressCache.getParquetRowGroup(frameIndex);
        final int rowGroupLo = addressCache.getParquetRowGroupLo(frameIndex);
        final int rowGroupHi = addressCache.getParquetRowGroupHi(frameIndex);
        try {
            parquetBuffers.decode(activeDecoder, parquetColumns, rowGroupIndex, rowGroupLo, rowGroupHi);
        } catch (Throwable th) {
            evictHalfInitialized(parquetBuffers);
            throw th;
        }
        cachedBytes += parquetBuffers.decodedBytes;
        enforceBudget();
    }

    private void enforceBudget() {
        if (cachedBytes <= effectiveBudgetBytes) {
            return;
        }
        // Keep at least one cached frame when the budget is positive: a single row group
        // larger than the budget must not ping-pong, but two retained frames silently
        // double the configured ceiling under tight budgets.
        final int minRetained = maxCacheBytes > 0 ? 1 : 0;
        final int size = cachedParquetBuffers.size();
        int retained = size;
        int write = 0;
        for (int read = 0; read < size; read++) {
            ParquetBuffers b = cachedParquetBuffers.getQuick(read);
            if (b.usageFlags == 0
                    && cachedBytes > effectiveBudgetBytes
                    && retained > minRetained) {
                spillAndFree(b);
                retained--;
            } else {
                if (write != read) {
                    cachedParquetBuffers.setQuick(write, b);
                }
                write++;
            }
        }
        if (write < size) {
            cachedParquetBuffers.setPos(write);
        }
    }

    private void evictHalfInitialized(ParquetBuffers buffers) {
        for (int i = cachedParquetBuffers.size() - 1; i >= 0; i--) {
            if (cachedParquetBuffers.getQuick(i) == buffers) {
                cachedParquetBuffers.remove(i);
                break;
            }
        }
        buffers.close();
        freeParquetBuffers.add(buffers);
    }

    private void openParquet(int frameIndex) {
        activateDecoder(frameIndex);

        parquetColumns.reopen();
        parquetColumns.clear();
        parquetIdxToDecodeSlot.clear();

        final ColumnMapping columnMapping = addressCache.getColumnMapping();
        final int readParquetColumnCount = columnMapping.getColumnCount();
        for (int i = 0; i < readParquetColumnCount; i++) {
            final int columnWriterIndex = columnMapping.getWriterIndex(i);
            final int parquetIdx = columnIdToParquetIdx.get(columnWriterIndex);
            if (parquetIdx < 0) {
                continue;
            }
            final int slotKey = parquetIdxToDecodeSlot.keyIndex(parquetIdx);
            if (parquetIdxToDecodeSlot.valueAt(slotKey) >= 0) {
                continue;
            }
            int columnType = addressCache.getColumnTypes().getQuick(i);
            if (ColumnType.tagOf(columnType) == ColumnType.VARCHAR) {
                columnType = ColumnType.VARCHAR_SLICE;
            }
            parquetIdxToDecodeSlot.putAt(slotKey, parquetIdx, (int) (parquetColumns.size() / 2));
            parquetColumns.add(parquetIdx);
            parquetColumns.add(columnType);
        }
    }

    private void openParquet(int frameIndex, IntHashSet columnIndexes, boolean include) {
        activateDecoder(frameIndex);

        parquetColumns.reopen();
        parquetColumns.clear();
        parquetIdxToDecodeSlot.clear();

        final ColumnMapping columnMapping = addressCache.getColumnMapping();
        final int readParquetColumnCount = columnMapping.getColumnCount();
        for (int i = 0; i < readParquetColumnCount; i++) {
            if (columnIndexes.contains(i) != include) {
                continue;
            }
            final int columnWriterIndex = columnMapping.getWriterIndex(i);
            final int parquetIdx = columnIdToParquetIdx.get(columnWriterIndex);
            if (parquetIdx < 0) {
                continue;
            }
            final int slotKey = parquetIdxToDecodeSlot.keyIndex(parquetIdx);
            if (parquetIdxToDecodeSlot.valueAt(slotKey) >= 0) {
                continue;
            }
            int columnType = addressCache.getColumnTypes().getQuick(i);
            if (ColumnType.tagOf(columnType) == ColumnType.VARCHAR) {
                columnType = ColumnType.VARCHAR_SLICE;
            }
            parquetIdxToDecodeSlot.putAt(slotKey, parquetIdx, (int) (parquetColumns.size() / 2));
            parquetColumns.add(parquetIdx);
            parquetColumns.add(columnType);
        }
    }

    private void reDecodeInitial(ParquetBuffers buffers, int frameIndex, IntHashSet filterColumnIndexes) {
        if (spillManager != null) {
            spillManager.dropSpilledFrame(frameIndex);
        }
        openParquet(frameIndex, filterColumnIndexes, true);
        final int rowGroupIndex = addressCache.getParquetRowGroup(frameIndex);
        final int rowGroupLo = addressCache.getParquetRowGroupLo(frameIndex);
        final int rowGroupHi = addressCache.getParquetRowGroupHi(frameIndex);
        cachedBytes -= buffers.decodedBytes;
        try {
            buffers.decode(activeDecoder, parquetColumns, rowGroupIndex, rowGroupLo, rowGroupHi);
        } catch (Throwable th) {
            evictHalfInitialized(buffers);
            throw th;
        }
        cachedBytes += buffers.decodedBytes;
        enforceBudget();
    }

    private void spillAndFree(ParquetBuffers buffers) {
        if (spillManager != null
                && accessPattern == ParquetDecodeHint.SCATTERED
                && addressCache != null
                && addressCache.isColdParquetPartition(buffers.frameIndex)) {
            try {
                spillManager.spill(buffers);
            } catch (Throwable th) {
                LOG.error().$("parquet spill failed; discarding buffer [frameIndex=").$(buffers.frameIndex)
                        .$(", error=").$(th).I$();
            }
        }
        cachedBytes -= buffers.decodedBytes;
        buffers.close();
        freeParquetBuffers.add(buffers);
    }

    @Nullable
    private ParquetBuffers tryHit(int frameIndex, byte usageBit) {
        final int cached = cachedParquetBuffers.size();
        int hitIdx = -1;
        for (int i = 0; i < cached; i++) {
            ParquetBuffers b = cachedParquetBuffers.getQuick(i);
            b.usageFlags &= (byte) ~usageBit;
            if (b.frameIndex == frameIndex) {
                hitIdx = i;
            }
        }
        if (hitIdx < 0) {
            return null;
        }
        ParquetBuffers hit = cachedParquetBuffers.getQuick(hitIdx);
        if (hitIdx != cached - 1) {
            cachedParquetBuffers.remove(hitIdx);
            cachedParquetBuffers.add(hit);
        }
        hit.usageFlags |= usageBit;
        return hit;
    }

    public static final class SpillManager implements QuietCloseable {
        // Spill file:
        //   header: int32 slotCount, int64 originalDecodedBytes,
        //           then per slot { int32 parquetIdx, int32 colType, long ds, long as }
        //   body:   per slot { ds bytes data, as bytes aux }
        // VARCHAR_SLICE slots are materialized into a contiguous blob (aux entries hold
        // offsets into it); restore rebases to absolute pointers. originalDecodedBytes
        // preserves the pre-spill chunk size so the budget accounting survives the
        // materialize step (which strictly shrinks VARCHAR_SLICE data).
        private static final int HEADER_PREFIX = Integer.BYTES + Long.BYTES;
        private static final int MAX_SPILL_SLOTS = 65_536;
        private static final int SLOT_META = 2 * Integer.BYTES + 2 * Long.BYTES;
        private final long cursorId;
        private final CharSequence dir;
        private final long diskBudget;
        private final FilesFacade ff;
        private final ParquetDecodeMetrics metrics;
        private final LongList spilledFrameBytes = new LongList();
        private final IntList spilledFrameIndexes = new IntList();
        private long headerScratch;
        private long headerScratchSize;
        private long spilledBytes;
        private long varcharScratch;
        private long varcharScratchSize;

        SpillManager(FilesFacade ff, CharSequence dir, long diskBudget, long cursorId, int mkDirMode, @Nullable ParquetDecodeMetrics metrics) {
            this.ff = ff;
            this.dir = dir;
            this.diskBudget = diskBudget;
            this.cursorId = cursorId;
            this.metrics = metrics;
            Path path = Path.getThreadLocal(dir);
            if (!ff.exists(path.$()) && ff.mkdirs(path.slash(), mkDirMode) != 0) {
                throw CairoException.critical(ff.errno())
                        .put("could not create parquet spill directory [path=").put(path).put(']');
            }
        }

        @Override
        public void close() {
            try {
                deleteAllSpilledFiles();
            } finally {
                if (headerScratch != 0) {
                    Unsafe.free(headerScratch, headerScratchSize, MemoryTag.NATIVE_DEFAULT);
                    headerScratch = 0;
                    headerScratchSize = 0;
                }
                if (varcharScratch != 0) {
                    Unsafe.free(varcharScratch, varcharScratchSize, MemoryTag.NATIVE_DEFAULT);
                    varcharScratch = 0;
                    varcharScratchSize = 0;
                }
            }
        }

        static void deleteSpillFilesInDir(FilesFacade ff, Path dir) {
            final int prefixLen = dir.size();
            final long pFind = ff.findFirst(dir.$());
            if (pFind <= 0) {
                return;
            }
            try {
                do {
                    if (ff.findType(pFind) != Files.DT_FILE) {
                        continue;
                    }
                    final long nameZ = ff.findName(pFind);
                    if (nameZ == 0 || !nameStartsWith(nameZ)) {
                        continue;
                    }
                    dir.trimTo(prefixLen).concat(nameZ).$();
                    ff.removeQuiet(dir.$());
                } while (ff.findNext(pFind) > 0);
            } finally {
                ff.findClose(pFind);
                dir.trimTo(prefixLen);
            }
        }

        private static boolean nameStartsWith(long nameZ) {
            for (int i = 0, n = PageFrameMemoryPool.SPILL_FILE_PREFIX.length(); i < n; i++) {
                byte b = Unsafe.getByte(nameZ + i);
                if (b == 0 || ((char) (b & 0xff)) != PageFrameMemoryPool.SPILL_FILE_PREFIX.charAt(i)) {
                    return false;
                }
            }
            return true;
        }

        private static void rebaseVarcharAux(long auxBase, long auxSize, long delta) {
            if (delta == 0) {
                return;
            }
            final long count = auxSize / VarcharTypeDriver.VARCHAR_AUX_WIDTH_BYTES;
            for (long i = 0; i < count; i++) {
                final long entry = auxBase + i * VarcharTypeDriver.VARCHAR_AUX_WIDTH_BYTES;
                int hdr = Unsafe.getInt(entry);
                if ((hdr & VarcharTypeDriver.VARCHAR_HEADER_FLAG_NULL) != 0) {
                    continue;
                }
                long oldPtr = Unsafe.getLong(entry + 8);
                Unsafe.putLong(entry + 8, oldPtr + delta);
            }
        }

        private static long sumVarcharStringBytes(long auxBase, long auxSize) {
            if (auxSize <= 0) {
                return 0;
            }
            final long count = auxSize / VarcharTypeDriver.VARCHAR_AUX_WIDTH_BYTES;
            long total = 0;
            for (long i = 0; i < count; i++) {
                final long entry = auxBase + i * VarcharTypeDriver.VARCHAR_AUX_WIDTH_BYTES;
                int hdr = Unsafe.getInt(entry);
                if ((hdr & VarcharTypeDriver.VARCHAR_HEADER_FLAG_NULL) != 0) {
                    continue;
                }
                total += hdr >>> 4;
            }
            return total;
        }

        private void buildPath(Path path, int frameIndex) {
            path.slash().put(SPILL_FILE_PREFIX).put(cursorId).put('-').put(frameIndex);
        }

        private void dropSpilledEntry(int idx) {
            if (idx < 0 || idx >= spilledFrameIndexes.size()) {
                return;
            }
            final int frameIndex = spilledFrameIndexes.getQuick(idx);
            final long bytes = spilledFrameBytes.getQuick(idx);
            Path path = Path.getThreadLocal(dir);
            buildPath(path, frameIndex);
            ff.removeQuiet(path.$());
            spilledFrameIndexes.removeIndex(idx);
            spilledFrameBytes.removeIndex(idx);
            spilledBytes -= bytes;
        }

        private void ensureHeaderScratch(long needed) {
            if (headerScratchSize >= needed) {
                return;
            }
            headerScratch = Unsafe.realloc(headerScratch, headerScratchSize, needed, MemoryTag.NATIVE_DEFAULT);
            headerScratchSize = needed;
        }

        private void ensureVarcharScratch(long needed) {
            if (varcharScratchSize >= needed) {
                return;
            }
            varcharScratch = Unsafe.realloc(varcharScratch, varcharScratchSize, needed, MemoryTag.NATIVE_DEFAULT);
            varcharScratchSize = needed;
        }

        private int findSpilled(int frameIndex) {
            for (int i = 0, n = spilledFrameIndexes.size(); i < n; i++) {
                if (spilledFrameIndexes.getQuick(i) == frameIndex) {
                    return i;
                }
            }
            return -1;
        }

        private void materializeVarchar(long auxBase, long auxSize, long totalBytes) {
            if (totalBytes <= 0) {
                return;
            }
            ensureVarcharScratch(totalBytes);
            final long count = auxSize / VarcharTypeDriver.VARCHAR_AUX_WIDTH_BYTES;
            long off = 0;
            for (long i = 0; i < count; i++) {
                final long entry = auxBase + i * VarcharTypeDriver.VARCHAR_AUX_WIDTH_BYTES;
                int hdr = Unsafe.getInt(entry);
                if ((hdr & VarcharTypeDriver.VARCHAR_HEADER_FLAG_NULL) != 0) {
                    continue;
                }
                int length = hdr >>> 4;
                long ptr = Unsafe.getLong(entry + 8);
                Vect.memcpy(varcharScratch + off, ptr, length);
                Unsafe.putLong(entry + 8, off);
                off += length;
            }
        }

        private void spill(ParquetBuffers buffers) {
            final int slotCount = buffers.slotCount;
            if (slotCount <= 0) {
                return;
            }
            if (findSpilled(buffers.frameIndex) >= 0) {
                return;
            }
            final long headerBytes = HEADER_PREFIX + (long) slotCount * SLOT_META;
            ensureHeaderScratch(headerBytes);
            Unsafe.putInt(headerScratch, slotCount);
            Unsafe.putLong(headerScratch + Integer.BYTES, buffers.decodedBytes);
            long mPtr = headerScratch + HEADER_PREFIX;
            long bodyBytes = 0;
            for (int s = 0; s < slotCount; s++) {
                final int parquetIdx = buffers.getSlotParquetIdx(s);
                final int colType = buffers.getSlotColumnType(s);
                final long as = buffers.getSlotAuxSize(s);
                final long ds = colType == ColumnType.VARCHAR_SLICE
                        ? sumVarcharStringBytes(buffers.getSlotAuxPtr(s), as)
                        : buffers.getSlotDataSize(s);
                Unsafe.putInt(mPtr, parquetIdx);
                mPtr += Integer.BYTES;
                Unsafe.putInt(mPtr, colType);
                mPtr += Integer.BYTES;
                Unsafe.putLong(mPtr, ds);
                mPtr += Long.BYTES;
                Unsafe.putLong(mPtr, as);
                mPtr += Long.BYTES;
                bodyBytes += ds + as;
            }
            if (bodyBytes <= 0) {
                return;
            }
            final long fileBytes = headerBytes + bodyBytes;
            if (spilledBytes + fileBytes > diskBudget) {
                return;
            }
            Path path = Path.getThreadLocal(dir);
            buildPath(path, buffers.frameIndex);
            final long fd = ff.openRW(path.$(), CairoConfiguration.O_NONE);
            if (fd < 0) {
                LOG.error().$("could not open spill file [errno=").$(ff.errno())
                        .$(", path=").$(path).I$();
                return;
            }
            boolean isSpillOk = false;
            try {
                if (!ff.allocate(fd, fileBytes)) {
                    LOG.error().$("could not allocate spill file [errno=").$(ff.errno())
                            .$(", size=").$(fileBytes).$(", path=").$(path).I$();
                    return;
                }
                if (ff.write(fd, headerScratch, headerBytes, 0) != headerBytes) {
                    LOG.error().$("could not write spill header [errno=").$(ff.errno())
                            .$(", path=").$(path).I$();
                    return;
                }
                mPtr = headerScratch + HEADER_PREFIX;
                long off = headerBytes;
                for (int s = 0; s < slotCount; s++) {
                    mPtr += Integer.BYTES;
                    final int colType = Unsafe.getInt(mPtr);
                    mPtr += Integer.BYTES;
                    final long ds = Unsafe.getLong(mPtr);
                    mPtr += Long.BYTES;
                    final long as = Unsafe.getLong(mPtr);
                    mPtr += Long.BYTES;
                    if (ds > 0) {
                        final long dataSrc;
                        if (colType == ColumnType.VARCHAR_SLICE) {
                            materializeVarchar(buffers.getSlotAuxPtr(s), as, ds);
                            dataSrc = varcharScratch;
                        } else {
                            dataSrc = buffers.getSlotDataPtr(s);
                        }
                        if (ff.write(fd, dataSrc, ds, off) != ds) {
                            LOG.error().$("could not write spill data [errno=").$(ff.errno())
                                    .$(", slot=").$(s).$(", path=").$(path).I$();
                            return;
                        }
                        off += ds;
                    }
                    if (as > 0) {
                        if (ff.write(fd, buffers.getSlotAuxPtr(s), as, off) != as) {
                            LOG.error().$("could not write spill aux [errno=").$(ff.errno())
                                    .$(", slot=").$(s).$(", path=").$(path).I$();
                            return;
                        }
                        off += as;
                    }
                }
                spilledFrameIndexes.add(buffers.frameIndex);
                spilledFrameBytes.add(fileBytes);
                spilledBytes += fileBytes;
                isSpillOk = true;
                if (metrics != null) {
                    metrics.incSpills();
                }
            } finally {
                ff.close(fd);
                if (!isSpillOk) {
                    ff.removeQuiet(path.$());
                }
            }
        }

        void deleteAllSpilledFiles() {
            if (spilledFrameIndexes.size() == 0) {
                return;
            }
            for (int i = 0, n = spilledFrameIndexes.size(); i < n; i++) {
                Path path = Path.getThreadLocal(dir);
                buildPath(path, spilledFrameIndexes.getQuick(i));
                ff.removeQuiet(path.$());
            }
            spilledFrameIndexes.clear();
            spilledFrameBytes.clear();
            spilledBytes = 0;
        }

        void dropSpilledFrame(int frameIndex) {
            dropSpilledEntry(findSpilled(frameIndex));
        }

        boolean restore(ParquetBuffers buffers, int frameIndex, DirectIntList parquetColumns) {
            final int idx = findSpilled(frameIndex);
            if (idx < 0) {
                return false;
            }
            Path path = Path.getThreadLocal(dir);
            buildPath(path, frameIndex);
            final long fd = ff.openRO(path.$());
            if (fd < 0) {
                LOG.error().$("could not open spill file for restore [errno=").$(ff.errno())
                        .$(", path=").$(path).I$();
                dropSpilledEntry(idx);
                return false;
            }
            try {
                ensureHeaderScratch(HEADER_PREFIX);
                if (ff.read(fd, headerScratch, HEADER_PREFIX, 0) != HEADER_PREFIX) {
                    LOG.error().$("could not read spill header prefix [errno=").$(ff.errno())
                            .$(", path=").$(path).I$();
                    dropSpilledEntry(idx);
                    return false;
                }
                final int slotCount = Unsafe.getInt(headerScratch);
                if (slotCount <= 0 || slotCount > MAX_SPILL_SLOTS) {
                    LOG.error().$("invalid spill slotCount [slotCount=").$(slotCount)
                            .$(", path=").$(path).I$();
                    dropSpilledEntry(idx);
                    return false;
                }
                final long originalDecodedBytes = Unsafe.getLong(headerScratch + Integer.BYTES);
                if (originalDecodedBytes < 0) {
                    LOG.error().$("negative originalDecodedBytes in spill header [bytes=").$(originalDecodedBytes)
                            .$(", path=").$(path).I$();
                    dropSpilledEntry(idx);
                    return false;
                }
                final long headerBytes = HEADER_PREFIX + (long) slotCount * SLOT_META;
                ensureHeaderScratch(headerBytes);
                if (ff.read(fd, headerScratch, headerBytes, 0) != headerBytes) {
                    LOG.error().$("could not read spill header [errno=").$(ff.errno())
                            .$(", path=").$(path).I$();
                    dropSpilledEntry(idx);
                    return false;
                }
                final int currentSlotCount = (int) (parquetColumns.size() / 2);
                // The spilled buffer may have more slots than the current cursor needs:
                // a previous cursor's late-materialised columns sit after its initial set.
                // Accept any prefix of the spilled slot list that matches the current decode;
                // bonus slots are kept in the restored buffer and remain unused.
                if (currentSlotCount > slotCount) {
                    dropSpilledEntry(idx);
                    return false;
                }
                long bodyBytes = 0;
                long mPtr = headerScratch + HEADER_PREFIX;
                for (int s = 0; s < slotCount; s++) {
                    final int spilledParquetIdx = Unsafe.getInt(mPtr);
                    mPtr += Integer.BYTES;
                    final int colType = Unsafe.getInt(mPtr);
                    mPtr += Integer.BYTES;
                    final long ds = Unsafe.getLong(mPtr);
                    mPtr += Long.BYTES;
                    final long as = Unsafe.getLong(mPtr);
                    mPtr += Long.BYTES;
                    if (s < currentSlotCount
                            && (spilledParquetIdx != parquetColumns.get(2L * s)
                            || colType != parquetColumns.get(2L * s + 1))) {
                        dropSpilledEntry(idx);
                        return false;
                    }
                    if (ds < 0 || as < 0) {
                        LOG.error().$("negative slot size in spill header [slot=").$(s)
                                .$(", colType=").$(colType).$(", dataSize=").$(ds).$(", auxSize=").$(as)
                                .$(", path=").$(path).I$();
                        dropSpilledEntry(idx);
                        return false;
                    }
                    bodyBytes += ds + as;
                }
                if (bodyBytes <= 0 || bodyBytes > diskBudget) {
                    LOG.error().$("invalid spill bodyBytes [bodyBytes=").$(bodyBytes)
                            .$(", diskBudget=").$(diskBudget).$(", path=").$(path).I$();
                    dropSpilledEntry(idx);
                    return false;
                }
                final long fileLen = ff.length(fd);
                if (fileLen != headerBytes + bodyBytes) {
                    LOG.error().$("spill file size mismatch [expected=").$(headerBytes + bodyBytes)
                            .$(", actual=").$(fileLen).$(", path=").$(path).I$();
                    dropSpilledEntry(idx);
                    return false;
                }
                final long bodyPtr = Unsafe.malloc(bodyBytes, MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);
                boolean isOk = false;
                try {
                    if (ff.read(fd, bodyPtr, bodyBytes, headerBytes) != bodyBytes) {
                        LOG.error().$("could not read spill body [errno=").$(ff.errno())
                                .$(", path=").$(path).I$();
                        dropSpilledEntry(idx);
                        return false;
                    }
                    // setupRestored hands bodyPtr to buffers; freeing here would double-free.
                    buffers.setupRestored(slotCount, bodyPtr, bodyBytes, originalDecodedBytes);
                    isOk = true;
                    long off = 0;
                    mPtr = headerScratch + HEADER_PREFIX;
                    for (int s = 0; s < slotCount; s++) {
                        final int parquetIdx = Unsafe.getInt(mPtr);
                        mPtr += Integer.BYTES;
                        final int colType = Unsafe.getInt(mPtr);
                        mPtr += Integer.BYTES;
                        final long ds = Unsafe.getLong(mPtr);
                        mPtr += Long.BYTES;
                        final long as = Unsafe.getLong(mPtr);
                        mPtr += Long.BYTES;
                        final long dataPtr = ds > 0 ? bodyPtr + off : 0;
                        off += ds;
                        final long auxPtr = as > 0 ? bodyPtr + off : 0;
                        off += as;
                        if (colType == ColumnType.VARCHAR_SLICE && auxPtr != 0 && dataPtr != 0) {
                            rebaseVarcharAux(auxPtr, as, dataPtr);
                        }
                        buffers.setRestoredSlot(s, parquetIdx, colType, dataPtr, ds, auxPtr, as);
                    }
                    if (metrics != null) {
                        metrics.incRestores();
                    }
                    return true;
                } finally {
                    if (!isOk) {
                        Unsafe.free(bodyPtr, bodyBytes, MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);
                    }
                }
            } finally {
                ff.close(fd);
            }
        }
    }

    private class PageFrameMemoryImpl implements PageFrameMemory, Mutable {
        private DirectLongList auxPageAddresses;
        private DirectLongList auxPageSizes;
        private int columnOffset;
        private ParquetBuffers currentRowGroupBuffer;
        private byte frameFormat = -1;
        private int frameIndex = -1;
        private DirectLongList pageAddresses;
        private DirectLongList pageSizes;

        @Override
        public void clear() {
            frameIndex = -1;
            frameFormat = -1;
            columnOffset = 0;
            pageAddresses = null;
            auxPageAddresses = null;
            pageSizes = null;
            auxPageSizes = null;
            currentRowGroupBuffer = null;
        }

        @Override
        public long getAuxPageAddress(int columnIndex) {
            return auxPageAddresses.get(columnOffset + columnIndex);
        }

        @Override
        public DirectLongList getAuxPageAddresses() {
            return auxPageAddresses;
        }

        @Override
        public DirectLongList getAuxPageSizes() {
            return auxPageSizes;
        }

        @Override
        public int getColumnCount() {
            return addressCache.getColumnCount();
        }

        @Override
        public int getColumnOffset() {
            return columnOffset;
        }

        @Override
        public byte getFrameFormat() {
            return frameFormat;
        }

        @Override
        public int getFrameIndex() {
            return frameIndex;
        }

        @Override
        public long getPageAddress(int columnIndex) {
            return pageAddresses.get(columnOffset + columnIndex);
        }

        @Override
        public DirectLongList getPageAddresses() {
            return pageAddresses;
        }

        @Override
        public long getPageSize(int columnIndex) {
            return pageSizes.get(columnOffset + columnIndex);
        }

        @Override
        public DirectLongList getPageSizes() {
            return pageSizes;
        }

        @Override
        public long getRowIdOffset() {
            return addressCache.getRowIdOffset(frameIndex);
        }

        @Override
        public boolean hasColumnTops() {
            for (int i = 0, n = addressCache.getColumnCount(); i < n; i++) {
                // VARCHAR column that contains short strings will have zero data vector,
                // so for such columns we also need to check that the aux (index) vector is zero.
                if (pageAddresses.get(columnOffset + i) == 0 && auxPageAddresses.get(columnOffset + i) == 0) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public boolean populateRemainingColumns(IntHashSet filterColumnIndexes, DirectLongList filteredRows, boolean fillWithNulls) {
            assert frameFormat == PartitionFormat.PARQUET;
            if (filterColumnIndexes.size() == addressCache.getColumnCount()) {
                return false;
            }

            // Late-mat writes to rowGroupBuffers; restored buffers store slots in restoredScratch.
            if (currentRowGroupBuffer.isRestored) {
                try {
                    reDecodeInitial(currentRowGroupBuffer, frameIndex, filterColumnIndexes);
                } catch (Throwable th) {
                    clear();
                    throw th;
                }
            }

            openParquet(frameIndex, filterColumnIndexes, false);
            final int rowGroupIndex = addressCache.getParquetRowGroup(frameIndex);
            final int rowGroupLo = addressCache.getParquetRowGroupLo(frameIndex);
            final int rowGroupHi = addressCache.getParquetRowGroupHi(frameIndex);
            if (filteredRows.size() != 0) {
                final long prevDecodedBytes = currentRowGroupBuffer.decodedBytes;
                final long extra;
                try {
                    extra = currentRowGroupBuffer.decodeRemainingColumns(
                            activeDecoder,
                            filterColumnIndexes,
                            parquetColumns,
                            rowGroupIndex,
                            rowGroupLo,
                            rowGroupHi,
                            filteredRows,
                            fillWithNulls
                    );
                } catch (Throwable th) {
                    cachedBytes -= prevDecodedBytes;
                    evictHalfInitialized(currentRowGroupBuffer);
                    clear();
                    throw th;
                }
                if (extra > 0) {
                    currentRowGroupBuffer.decodedBytes = prevDecodedBytes + extra;
                    cachedBytes += extra;
                    enforceBudget();
                }
                return true;
            }
            return false;
        }
    }

    private class ParquetBuffers implements QuietCloseable, Reopenable {
        private final DirectLongList auxPageAddresses;
        private final DirectLongList auxPageSizes;
        private final DirectLongList pageAddresses;
        private final DirectLongList pageSizes;
        private final DirectLongList restoredSlotMeta;
        private final RowGroupBuffers rowGroupBuffers;
        private final IntList slotColumnTypes = new IntList();
        private final IntList slotParquetIndexes = new IntList();
        private long decodedBytes;
        private int frameIndex = -1;
        private boolean isRestored;
        private long restoredScratch;
        private long restoredScratchSize;
        private int slotCount;
        // Contains bits FRAME_MEMORY_MASK, RECORD_A_MASK and RECORD_B_MASK.
        private byte usageFlags;

        public ParquetBuffers() {
            this.auxPageAddresses = new DirectLongList(16, MemoryTag.NATIVE_DEFAULT, true);
            this.auxPageSizes = new DirectLongList(16, MemoryTag.NATIVE_DEFAULT, true);
            this.pageAddresses = new DirectLongList(16, MemoryTag.NATIVE_DEFAULT, true);
            this.pageSizes = new DirectLongList(16, MemoryTag.NATIVE_DEFAULT, true);
            this.restoredSlotMeta = new DirectLongList(16, MemoryTag.NATIVE_DEFAULT, true);
            this.rowGroupBuffers = new RowGroupBuffers(MemoryTag.NATIVE_PARQUET_PARTITION_DECODER, true);
        }

        @Override
        public void close() {
            Misc.free(pageAddresses);
            Misc.free(pageSizes);
            Misc.free(auxPageAddresses);
            Misc.free(auxPageSizes);
            Misc.free(restoredSlotMeta);
            Misc.free(rowGroupBuffers);
            freeRestoredScratch();
            slotColumnTypes.clear();
            slotParquetIndexes.clear();
            isRestored = false;
            slotCount = 0;
            usageFlags = 0;
            frameIndex = -1;
            decodedBytes = 0;
        }

        public void decode(ParquetDecoder decoder, DirectIntList parquetColumns, int rowGroup, int rowLo, int rowHi) {
            clearAddresses();
            freeRestoredScratch();
            isRestored = false;
            slotColumnTypes.clear();
            slotParquetIndexes.clear();
            if (parquetColumns.size() > 0) {
                decoder.decodeRowGroup(rowGroupBuffers, parquetColumns, rowGroup, rowLo, rowHi);
                slotCount = (int) (parquetColumns.size() / 2);
                long bytes = 0;
                for (int s = 0; s < slotCount; s++) {
                    bytes += rowGroupBuffers.getChunkDataSize(s) + rowGroupBuffers.getChunkAuxSize(s);
                    slotParquetIndexes.add(parquetColumns.get(2L * s));
                    slotColumnTypes.add(parquetColumns.get(2L * s + 1));
                }
                decodedBytes = bytes;
            } else {
                slotCount = 0;
                decodedBytes = 0;
            }
            // Always size and zero the page-address lists, even when there are no
            // parquet columns to decode (every projected column was added after this
            // partition became parquet). remapColumns() is the only place that does
            // so; skipping it leaves stale/uninitialized native memory in the lists,
            // and a record reading an absent column then dereferences a wild address.
            // With no decoded columns the remap loop is a no-op, so every column
            // correctly resolves to address 0 (NULL).
            remapColumns();
        }

        public long decodeRemainingColumns(
                ParquetDecoder decoder,
                IntHashSet filterColumnIndexes,
                DirectIntList parquetColumns,
                int rowGroup,
                int rowLo,
                int rowHi,
                DirectLongList filteredRows,
                boolean fillWithNulls
        ) {
            assert !isRestored : "decodeRemainingColumns requires fresh-decode state; caller must reDecodeInitial first";
            if (parquetColumns.size() == 0) {
                return 0;
            }
            final int columnOffset = slotCount;
            if (fillWithNulls) {
                decoder.decodeRowGroupWithRowFilterFillNulls(rowGroupBuffers, columnOffset, parquetColumns, rowGroup, rowLo, rowHi, filteredRows);
            } else {
                decoder.decodeRowGroupWithRowFilter(rowGroupBuffers, columnOffset, parquetColumns, rowGroup, rowLo, rowHi, filteredRows);
            }
            final int extraSlots = (int) (parquetColumns.size() / 2);
            final long extra = sumChunkBytes(columnOffset, extraSlots);
            if (extraSlots > 0) {
                for (int s = 0; s < extraSlots; s++) {
                    slotParquetIndexes.add(parquetColumns.get(2L * s));
                    slotColumnTypes.add(parquetColumns.get(2L * s + 1));
                }
                slotCount += extraSlots;
            }
            remapRemainingColumns(columnOffset, filterColumnIndexes);
            return extra;
        }

        public long getSlotAuxPtr(int slot) {
            if (isRestored) {
                return restoredSlotMeta.get((long) slot * 4 + 2);
            }
            return rowGroupBuffers.getChunkAuxPtr(slot);
        }

        public long getSlotAuxSize(int slot) {
            if (isRestored) {
                return restoredSlotMeta.get((long) slot * 4 + 3);
            }
            return rowGroupBuffers.getChunkAuxSize(slot);
        }

        public int getSlotColumnType(int slot) {
            return slotColumnTypes.getQuick(slot);
        }

        public long getSlotDataPtr(int slot) {
            if (isRestored) {
                return restoredSlotMeta.get((long) slot * 4);
            }
            return rowGroupBuffers.getChunkDataPtr(slot);
        }

        public long getSlotDataSize(int slot) {
            if (isRestored) {
                return restoredSlotMeta.get((long) slot * 4 + 1);
            }
            return rowGroupBuffers.getChunkDataSize(slot);
        }

        public int getSlotParquetIdx(int slot) {
            return slotParquetIndexes.getQuick(slot);
        }

        @Override
        public void reopen() {
            pageAddresses.reopen();
            pageSizes.reopen();
            auxPageAddresses.reopen();
            auxPageSizes.reopen();
            restoredSlotMeta.reopen();
            rowGroupBuffers.reopen();
        }

        public void setRestoredSlot(int slot, int parquetIdx, int columnType, long dataPtr, long dataSize, long auxPtr, long auxSize) {
            final long base = (long) slot * 4;
            restoredSlotMeta.set(base, dataPtr);
            restoredSlotMeta.set(base + 1, dataSize);
            restoredSlotMeta.set(base + 2, auxPtr);
            restoredSlotMeta.set(base + 3, auxSize);
            slotParquetIndexes.setQuick(slot, parquetIdx);
            slotColumnTypes.setQuick(slot, columnType);
        }

        public void setupRestored(int slotCount, long scratchPtr, long scratchSize, long originalDecodedBytes) {
            freeRestoredScratch();
            restoredSlotMeta.clear();
            restoredSlotMeta.setCapacity((long) slotCount * 4);
            restoredSlotMeta.setPos((long) slotCount * 4);
            slotColumnTypes.setPos(slotCount);
            slotParquetIndexes.setPos(slotCount);
            this.slotCount = slotCount;
            this.restoredScratch = scratchPtr;
            this.restoredScratchSize = scratchSize;
            this.decodedBytes = originalDecodedBytes;
            isRestored = true;
        }

        private void clearAddresses() {
            pageAddresses.clear();
            pageSizes.clear();
            auxPageAddresses.clear();
            auxPageSizes.clear();
        }

        private void ensureCapacityAndZero(DirectLongList list, int size) {
            list.setCapacity(size);
            list.zero();
            list.setPos(size);
        }

        private void freeRestoredScratch() {
            if (restoredScratch != 0) {
                Unsafe.free(restoredScratch, restoredScratchSize, MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);
                restoredScratch = 0;
                restoredScratchSize = 0;
            }
        }

        // Fan the decoded buffers out to query columns. parquetColumns is
        // deduplicated, so when several query columns reference the same
        // parquet column they share one decode slot and copy the same
        // address pair into their respective query slots.
        private void remapColumns() {
            final int columnCount = addressCache.getColumnCount();
            if (columnCount == 0) {
                // The query reads no columns (e.g. count(*)). clearAddresses() already
                // left the lists empty, which is the correct state; there is nothing to
                // remap. Sizing them to 0 would trip DirectLongList.setCapacity()'s
                // assert capacity > 0.
                return;
            }
            ensureCapacityAndZero(pageAddresses, columnCount);
            ensureCapacityAndZero(pageSizes, columnCount);
            ensureCapacityAndZero(auxPageAddresses, columnCount);
            ensureCapacityAndZero(auxPageSizes, columnCount);

            if (parquetColumns.size() == 0) {
                // No parquet column was decoded (every projected column was added
                // after this partition became parquet). openParquet() only adds a
                // column to parquetColumns when columnIdToParquetIdx maps it to a
                // present parquet column, so an empty parquetColumns means every
                // column below would resolve to parquetIdx < 0 and continue. The
                // zeroing above already left them all at address 0 (NULL), so skip
                // the dead remap loop.
                return;
            }

            final ColumnMapping columnMapping = addressCache.getColumnMapping();
            final int readParquetColumnCount = columnMapping.getColumnCount();
            for (int q = 0; q < readParquetColumnCount; q++) {
                final int writerIndex = columnMapping.getWriterIndex(q);
                final int parquetIdx = columnIdToParquetIdx.get(writerIndex);
                if (parquetIdx < 0) {
                    continue; // ADD COLUMN: stays at address 0 (NULL).
                }
                final int slot = parquetIdxToDecodeSlot.get(parquetIdx);
                if (slot < 0) {
                    continue; // Not part of this decode pass.
                }
                final int columnType = addressCache.getColumnTypes().getQuick(q);
                pageAddresses.set(q, getSlotDataPtr(slot));
                pageSizes.set(q, getSlotDataSize(slot));
                if (ColumnType.isVarSize(columnType)) {
                    auxPageAddresses.set(q, getSlotAuxPtr(slot));
                    auxPageSizes.set(q, getSlotAuxSize(slot));
                }
            }
        }

        private void remapRemainingColumns(int columnOffset, IntHashSet filterColumnIndexes) {
            final ColumnMapping columnMapping = addressCache.getColumnMapping();
            final int readParquetColumnCount = columnMapping.getColumnCount();
            for (int q = 0; q < readParquetColumnCount; q++) {
                // Filter columns hold full data read by absolute index; never overwrite
                // them with the compacted buffer when a remaining column shares their
                // parquet column. Guard only: the optimizer keeps filters below
                // duplicating projections, so the late-mat frame has no duplicate today.
                if (filterColumnIndexes.contains(q)) {
                    continue;
                }
                final int writerIndex = columnMapping.getWriterIndex(q);
                final int parquetIdx = columnIdToParquetIdx.get(writerIndex);
                if (parquetIdx < 0) {
                    continue;
                }
                final int slot = parquetIdxToDecodeSlot.get(parquetIdx);
                if (slot < 0) {
                    continue; // Excluded from this decode pass; the previous decode set its address.
                }
                final int columnType = addressCache.getColumnTypes().getQuick(q);
                pageAddresses.set(q, rowGroupBuffers.getChunkDataPtr(columnOffset + slot));
                pageSizes.set(q, rowGroupBuffers.getChunkDataSize(columnOffset + slot));
                if (ColumnType.isVarSize(columnType)) {
                    auxPageAddresses.set(q, rowGroupBuffers.getChunkAuxPtr(columnOffset + slot));
                    auxPageSizes.set(q, rowGroupBuffers.getChunkAuxSize(columnOffset + slot));
                }
            }
        }

        private long sumChunkBytes(int startSlot, int slotCount) {
            long bytes = 0;
            for (int s = 0; s < slotCount; s++) {
                bytes += rowGroupBuffers.getChunkDataSize(startSlot + s)
                        + rowGroupBuffers.getChunkAuxSize(startSlot + s);
            }
            return bytes;
        }
    }
}
