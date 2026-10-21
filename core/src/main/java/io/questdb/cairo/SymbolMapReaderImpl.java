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

package io.questdb.cairo;

import io.questdb.cairo.idx.ConcurrentBitmapIndexFwdReader;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMR;
import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Chars;
import io.questdb.std.DirectIntCharSequenceHashMap;
import io.questdb.std.FilesFacade;
import io.questdb.std.Hash;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.NativeMemoryBudget;
import io.questdb.std.Numbers;
import io.questdb.std.Transient;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectString;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;

import static io.questdb.cairo.TableUtils.charFileName;
import static io.questdb.cairo.TableUtils.offsetFileName;

public class SymbolMapReaderImpl implements Closeable, SymbolMapReader {
    private static final Log LOG = LogFactory.getLog(SymbolMapReaderImpl.class);
    private final MemoryCMR charMem = Vm.getCMRInstance();
    private final StringSink columnNameSink = new StringSink();
    private final ConcurrentBitmapIndexFwdReader indexReader = new ConcurrentBitmapIndexFwdReader();
    // keyOf() must not rebind the valueOf() flyweight supplied by its caller.
    private final DirectString keyOfView = new DirectString();
    private final MemoryCMR offsetMem = Vm.getCMRInstance();
    private final Path path = new Path();
    private final DirectString valueViewA = new DirectString();
    private final DirectString valueViewB = new DirectString();
    private DirectIntCharSequenceHashMap cache;
    private SymbolMapCachePool.Entry cacheEntry;
    private boolean cached;
    private long columnNameTxn;
    private CairoConfiguration configuration;
    private long lowCharsBase;
    private int lowCount;
    private long lowOffsets;
    // Racy hint: a stale value only costs extra map calls.
    private int maxCachedKey = Integer.MAX_VALUE;
    private int maxHash;
    private long maxOffset;
    private boolean nullValue;
    private int symbolCapacity;
    private int symbolCount;
    private long symbolTableGeneration;
    private long truncateVersion;

    public SymbolMapReaderImpl() {
    }

    public SymbolMapReaderImpl(
            CairoConfiguration configuration,
            @Transient Utf8Sequence pathToTableDir,
            @Transient CharSequence columnName,
            long columnNameTxn,
            int symbolCount
    ) {
        of(configuration, pathToTableDir, columnName, columnNameTxn, symbolCount);
    }

    public SymbolMapReaderImpl(
            CairoConfiguration configuration,
            @Transient Utf8Sequence pathToTableDir,
            @Transient CharSequence columnName,
            long columnNameTxn,
            int symbolCount,
            @Nullable SymbolMapCachePool cachePool,
            long truncateVersion
    ) {
        of(configuration, pathToTableDir, columnName, columnNameTxn, symbolCount, cachePool, truncateVersion);
    }

    @Override
    public void close() {
        Misc.free(indexReader);
        Misc.free(charMem);
        releaseCache();
        long fd = offsetMem.getFd();
        Misc.free(offsetMem);
        Misc.free(path);
        LOG.debug().$("closed [fd=").$(fd).$(']').$();
    }

    @Override
    public boolean containsNullValue() {
        return nullValue;
    }

    @TestOnly
    public long getCacheAllocatedBytes() {
        return cache == null ? 0 : cache.allocatedBytes();
    }

    @TestOnly
    public int getCacheSize() {
        return cache == null ? 0 : cache.size();
    }

    @TestOnly
    public long getLowCacheAllocatedBytes() {
        return cacheEntry == null ? 0 : cacheEntry.getLowAllocatedBytes();
    }

    @TestOnly
    public int getLowCacheSize() {
        return lowCount;
    }

    @Override
    public int getSymbolCapacity() {
        return symbolCapacity;
    }

    @Override
    public int getSymbolCount() {
        return symbolCount;
    }

    @Override
    public long getSymbolTableGeneration() {
        return symbolTableGeneration;
    }

    @Override
    public MemoryR getSymbolOffsetsColumn() {
        return offsetMem;
    }

    @Override
    public MemoryR getSymbolValuesColumn() {
        return charMem;
    }

    @Override
    public boolean isCached() {
        return cached;
    }

    @Override
    public boolean isDeleted() {
        return offsetMem.isDeleted();
    }

    @Override
    public int keyOf(CharSequence value) {
        if (value != null) {
            int hash = Hash.boundedHash(value, maxHash);
            try (RowCursor cursor = indexReader.getCursor(hash, 0, maxOffset - Long.BYTES)) {
                while (cursor.hasNext()) {
                    final long offsetOffset = cursor.next();
                    if (Chars.equals(value, charMem.getStr(offsetMem.getLong(offsetOffset), keyOfView))) {
                        return SymbolMapWriter.offsetToKey(offsetOffset);
                    }
                }
            }
            return SymbolTable.VALUE_NOT_FOUND;
        }
        return SymbolTable.VALUE_IS_NULL;
    }

    public boolean needsReopen(long columnNameTxn) {
        return this.columnNameTxn != columnNameTxn;
    }

    public boolean needsReopen(long columnNameTxn, long truncateVersion) {
        return this.columnNameTxn != columnNameTxn
                || this.truncateVersion != truncateVersion
                || cached != offsetMem.getBool(SymbolMapWriter.HEADER_CACHE_ENABLED);
    }

    public StaticSymbolTable newSymbolTableView() {
        return new SymbolTableView();
    }

    public void of(
            CairoConfiguration configuration,
            @Transient Utf8Sequence pathToTableDir,
            @Transient CharSequence columnName,
            long columnNameTxn,
            int symbolCount
    ) {
        of(configuration, pathToTableDir, columnName, columnNameTxn, symbolCount, null, 0);
    }

    public void of(
            CairoConfiguration configuration,
            @Transient Utf8Sequence pathToTableDir,
            @Transient CharSequence columnName,
            long columnNameTxn,
            int symbolCount,
            @Nullable SymbolMapCachePool cachePool,
            long truncateVersion
    ) {
        releaseCache();
        final FilesFacade ff = configuration.getFilesFacade();
        this.configuration = configuration;
        this.path.of(pathToTableDir);
        this.columnNameSink.clear();
        this.columnNameSink.put(columnName);
        this.columnNameTxn = columnNameTxn;
        this.symbolCount = symbolCount;
        this.truncateVersion = truncateVersion;
        this.maxOffset = SymbolMapWriter.keyToOffset(symbolCount);
        final int plen = path.size();
        try {
            // this constructor does not create index. Index must exist,
            // and we use "offset" file to store "header"
            if (!ff.exists(offsetFileName(path.trimTo(plen), columnName, columnNameTxn))) {
                LOG.error().$(path).$(" is not found").$();
                throw CairoException.fileNotFound().put("SymbolMap does not exist: ").put(path);
            }

            // is there enough length in "offset" file for "header"?
            long len = ff.length(path.$());
            if (len < SymbolMapWriter.HEADER_SIZE) {
                LOG.error().$(path).$(" is too short [len=").$(len).$(']').$();
                throw CairoException.critical(0).put("SymbolMap is too short: ").put(path);
            }

            // open "offset" memory and make sure we start appending from where
            // we left off. Where we left off is stored externally to symbol map
            final long offsetMemSize = SymbolMapWriter.keyToOffset(symbolCount) + Long.BYTES;
            LOG.debug().$("offsetMem.of [columnName=").$(path).$(",offsetMemSize=").$(offsetMemSize).I$();
            offsetMem.of(ff, path.$(), offsetMemSize, offsetMemSize, MemoryTag.MMAP_INDEX_READER);
            this.symbolCapacity = offsetMem.getInt(SymbolMapWriter.HEADER_CAPACITY);
            assert symbolCapacity > 0;
            this.cached = offsetMem.getBool(SymbolMapWriter.HEADER_CACHE_ENABLED);
            this.nullValue = offsetMem.getBool(SymbolMapWriter.HEADER_NULL_FLAG);

            // index reader is used to identify attempts to store duplicate symbol value
            // partition txn does not matter, because symbol is at the root of the table dir (not at partition level)
            // Symbol-table de-dup index is BITMAP — metadata + cover params unused.
            indexReader.of(configuration, path.trimTo(plen), columnName, columnNameTxn, -1, 0, null, null, 0);

            long charSize = offsetMem.getLong(maxOffset);
            // char file size can be zero only if symbolCount is zero
            assert charSize > 0 || symbolCount == 0;
            // this is the place where symbol values are stored
            charMem.of(
                    ff,
                    charFileName(path.trimTo(plen), columnName, columnNameTxn),
                    ff.getMapPageSize(),
                    charSize,
                    MemoryTag.MMAP_INDEX_READER,
                    CairoConfiguration.O_NONE,
                    -1
            );

            // we use index hash maximum equals to half of symbol capacity, which
            // theoretically should require 2 value cells in index per hash
            // we use 4 cells to compensate for occasionally unlucky hash distribution
            this.maxHash = Math.max(Numbers.ceilPow2(symbolCapacity / 2) - 1, 1);
            if (cached && cachePool != null) {
                cacheEntry = cachePool.acquire(path.trimTo(plen), columnName, columnNameTxn, truncateVersion);
                cache = cacheEntry == null ? null : cacheEntry.getMap();
                if (cache != null) {
                    updateMaxCachedKey(cache);
                }
            }
            refreshLow();
            symbolTableGeneration = symbolTableGeneration == Long.MAX_VALUE ? 0 : symbolTableGeneration + 1;
            LOG.debug().$("open [columnName=").$(path.trimTo(plen).concat(columnName).$())
                    .$(", fd=").$(offsetMem.getFd())
                    .$(", capacity=").$(symbolCapacity)
                    .I$();
        } catch (Throwable e) {
            close();
            throw e;
        } finally {
            path.trimTo(plen);
        }
    }

    @Override
    public void updateSymbolCount(int symbolCount) {
        if (symbolCount > this.symbolCount) {
            this.symbolCount = symbolCount;
            this.maxOffset = SymbolMapWriter.keyToOffset(symbolCount);
            // offset mem contains offsets of symbolCount + 1
            // we need to make sure we have access to the last element
            // which will indicate size of the char column
            offsetMem.extend(maxOffset + Long.BYTES);

            long charSize = offsetMem.getLong(maxOffset);
            // char file size can be zero only if symbolCount is zero
            assert charSize > 0 || symbolCount == 0;
            charMem.extend(charSize);
        } else if (symbolCount < this.symbolCount) {
            this.symbolCount = symbolCount;
            this.maxOffset = SymbolMapWriter.keyToOffset(symbolCount);
        }
        // Refresh contains null flag.
        this.nullValue = offsetMem.getBool(SymbolMapWriter.HEADER_NULL_FLAG);
        // Refresh index reader to avoid memory remapping on keyOf() calls.
        // partition txn does not matter, because symbol is at the root of the table dir (not at partition level)
        indexReader.of(configuration, path, columnNameSink, columnNameTxn, -1, 0, null, null, 0);
        refreshLow();
    }

    @Override
    public CharSequence valueBOf(int key) {
        return cachedValueOf(key, valueViewB);
    }

    @Override
    public CharSequence valueOf(int key) {
        return cachedValueOf(key, valueViewA);
    }

    /**
     * Binds the caller's {@code view} to the value stored for {@code key}, reading the
     * mapped char file directly. Two differences from {@link #valueOf(int)} make this the
     * accessor a bulk consumer wants: the caller owns the view, so two live values can be
     * held at once without the A/B pair this class keeps for itself, and the read always
     * goes to the mapping without admitting bulk reads into the shared cache.
     * <p>
     * The view stays valid until this reader is rebound by {@link #of} or closed, or until
     * the caller rebinds it.
     */
    public CharSequence valueOf(int key, DirectString view) {
        if (key > -1 && key < symbolCount) {
            return charMem.getStr(offsetMem.getLong(SymbolMapWriter.keyToOffset(key)), view);
        }
        return null;
    }

    void bindLow(long offsets, long chars, long base, int count) {
        lowOffsets = offsets;
        lowCharsBase = chars - base;
        lowCount = count;
    }

    void logLowSliceOom() {
        LOG.info().$("symbol cache low slice allocation failed [table=").$(path)
                .$(", column=").$(columnNameSink).$(", cachedKeys=").$(lowCount).I$();
    }

    private CharSequence cachedValueOf(int key, DirectString view) {
        if (key > maxCachedKey) {
            return valueOf(key, view);
        }
        // Shared entries may contain keys committed after this reader's transaction.
        if (key < 0 || key >= symbolCount) {
            return null;
        }
        if (key < lowCount) {
            long record = lowCharsBase + Unsafe.getLong(lowOffsets + ((long) key << 3));
            return view.of(record + Integer.BYTES, Unsafe.getInt(record));
        }
        final DirectIntCharSequenceHashMap cache = this.cache;
        if (cache != null && cache.get(key, view) != null) {
            return view;
        }
        DirectString value = charMem.getStr(offsetMem.getLong(SymbolMapWriter.keyToOffset(key)), view);
        assert value != null;
        if (cache != null && maxCachedKey == Integer.MAX_VALUE) {
            int result = cache.tryPutIfAbsent(key, value.lo(), value.length());
            if (result != DirectIntCharSequenceHashMap.INSERTED) {
                updateMaxCachedKey(cache);
                if (result == DirectIntCharSequenceHashMap.STOPPED) {
                    logCacheStopped();
                }
            }
        }
        return value;
    }

    private void logCacheStopped() {
        NativeMemoryBudget budget = cacheEntry.getBudget();
        String reason = switch (cache.getStopReason()) {
            case DirectIntCharSequenceHashMap.STOP_BUDGET -> "budget";
            case DirectIntCharSequenceHashMap.STOP_OOM -> "oom";
            default -> "limit";
        };
        LOG.info().$("symbol cache stopped admitting new values [table=").$(path)
                .$(", column=").$(columnNameSink).$(", reason=").$(reason)
                .$(", cachedKeys=").$(cache.size()).$(", mapBytes=").$(cache.allocatedBytes())
                .$(", budgetUsed=").$(budget.getUsedBytes()).$(", budgetLimit=").$(budget.getLimit()).I$();
    }

    private void refreshLow() {
        if (cacheEntry == null) {
            bindLow(0, 0, 0, 0);
        } else {
            cacheEntry.ensureLow(this, offsetMem, charMem, Math.min(SymbolMapLowSlice.MAX_KEYS, symbolCount));
        }
    }

    private void releaseCache() {
        cacheEntry = Misc.free(cacheEntry);
        cache = null;
        maxCachedKey = Integer.MAX_VALUE;
        bindLow(0, 0, 0, 0);
    }

    private void updateMaxCachedKey(DirectIntCharSequenceHashMap cache) {
        if (maxCachedKey == Integer.MAX_VALUE) {
            int maxKey = cache.getMaxKeyIfStopped();
            if (maxKey != Integer.MAX_VALUE) {
                maxCachedKey = Math.max(SymbolMapLowSlice.MAX_KEYS - 1, maxKey);
            }
        }
    }

    private class SymbolTableView implements StaticSymbolTable {
        private final DirectString csviewA = new DirectString();
        private final DirectString csviewB = new DirectString();
        private final DirectString csviewInternal = new DirectString();
        private RowCursor rowCursor;

        @Override
        public boolean containsNullValue() {
            return nullValue;
        }

        @Override
        public int getSymbolCount() {
            return symbolCount;
        }

        @Override
        public long getSymbolTableGeneration() {
            return symbolTableGeneration;
        }

        @Override
        public int keyOf(CharSequence value) {
            if (value != null) {
                int hash = Hash.boundedHash(value, maxHash);
                // Here we need absolute row indexes within the partition while the cursor gives us relative ones.
                // But since the minimum row index (minValue) is 0, they match.
                rowCursor = indexReader.initCursor(rowCursor, hash, 0, maxOffset - Long.BYTES);
                while (rowCursor.hasNext()) {
                    final long offsetOffset = rowCursor.next();
                    if (Chars.equals(value, charMem.getStr(offsetMem.getLong(offsetOffset), csviewInternal))) {
                        return SymbolMapWriter.offsetToKey(offsetOffset);
                    }
                }
                return SymbolTable.VALUE_NOT_FOUND;
            }
            return SymbolTable.VALUE_IS_NULL;
        }

        @Override
        public CharSequence valueBOf(int key) {
            return SymbolMapReaderImpl.this.cachedValueOf(key, csviewB);
        }

        @Override
        public CharSequence valueOf(int key) {
            return SymbolMapReaderImpl.this.cachedValueOf(key, csviewA);
        }
    }
}
