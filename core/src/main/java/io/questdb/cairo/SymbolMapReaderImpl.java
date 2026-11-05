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
import io.questdb.std.FilesFacade;
import io.questdb.std.Hash;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.NativeMemoryBudget;
import io.questdb.std.Numbers;
import io.questdb.std.Transient;
import io.questdb.std.str.DirectString;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;

import static io.questdb.cairo.TableUtils.charFileName;
import static io.questdb.cairo.TableUtils.offsetFileName;

public class SymbolMapReaderImpl implements Closeable, SymbolMapReader {
    private static final Log LOG = LogFactory.getLog(SymbolMapReaderImpl.class);
    private static final int VALUE_HASHES_SIZE = 1024;
    private final AtomicReference<SymbolMapCachePool> cachePool = new AtomicReference<>();
    private final MemoryCMR charMem = Vm.getCMRInstance();
    private final StringSink columnNameSink = new StringSink();
    private final ConcurrentBitmapIndexFwdReader indexReader = new ConcurrentBitmapIndexFwdReader();
    // keyOf() must not rebind the valueOf() flyweight supplied by its caller.
    private final DirectString keyOfView = new DirectString();
    private final MemoryCMR offsetMem = Vm.getCMRInstance();
    private final SymbolTableView ownView = new SymbolTableView();
    private final Path path = new Path();
    private SymbolMapCachePool.Entry cacheEntry;
    private boolean cached;
    private long columnNameTxn;
    private CairoConfiguration configuration;
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
        return cacheEntry == null ? 0 : cacheEntry.getSegments().allocatedBytes();
    }

    @TestOnly
    public int getCacheSize() {
        return cacheEntry == null ? 0 : cacheEntry.getSegments().size();
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

    @TestOnly
    public boolean hasValueHash(int key) {
        return ownView.hasValueHash(key);
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

    @Override
    public int keyOf(CharSequence value, int cachedKey) {
        if (value == null) {
            return SymbolTable.VALUE_IS_NULL;
        }
        if (Chars.equalsNc(value, valueOf(cachedKey, keyOfView))) {
            return cachedKey;
        }
        return keyOf(value);
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
            if (cached) {
                this.cachePool.set(cachePool);
            }
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

    @TestOnly
    public void putValueHash(int key, int hash) {
        ownView.getValueHashes()[key & (VALUE_HASHES_SIZE - 1)] = valueHashEntry(key, hash);
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
    }

    @Override
    public CharSequence valueBOf(int key) {
        return ownView.valueBOf(key);
    }

    /**
     * Returns the reader's A view of the value. The next valueOf() call rebinds it, so a caller
     * that needs two values at once reads the second through {@link #valueBOf(int)} or a view
     * of its own.
     */
    @Override
    public CharSequence valueOf(int key) {
        return ownView.valueOf(key);
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

    private static long valueHashEntry(int key, int hash) {
        return ((long) (key + 1) << 32) | (hash & 0xFFFFFFFFL);
    }

    private void acquireCache(SymbolMapCachePool pool) {
        cacheEntry = pool.acquire(path, columnNameSink, columnNameTxn, truncateVersion);
    }

    private void logCacheStopped(SymbolMapCachePool.Entry entry) {
        final NativeMemoryBudget budget = entry.getBudget();
        final SymbolMapSegments segments = entry.getSegments();
        final String reason = segments.getStopReason() == SymbolMapSegments.STOP_OOM ? "oom" : "budget";
        LOG.info().$("symbol cache stopped admitting new values [table=").$(path)
                .$(", column=").$(columnNameSink).$(", reason=").$(reason)
                .$(", cachedKeys=").$(segments.size()).$(", cacheBytes=").$(segments.allocatedBytes())
                .$(", budgetUsed=").$(budget.getUsedBytes()).$(", budgetLimit=").$(budget.getLimit()).I$();
    }

    private void releaseCache() {
        cachePool.set(null);
        cacheEntry = Misc.free(cacheEntry);
    }

    private class SymbolTableView implements StaticSymbolTable {
        private final CachedHashView csviewA = new CachedHashView();
        private final CachedHashView csviewB = new CachedHashView();
        private final DirectString csviewInternal = new DirectString();
        private final DirectString csviewOnce = new DirectString();
        private RowCursor rowCursor;
        private long[] valueHashes;
        private long valueHashesGeneration;

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
                final RowCursor cursor = indexReader.initCursor(rowCursor, hash, 0, maxOffset - Long.BYTES);
                // initCursor() hands back the cursor it receives whenever it reuses it. Re-storing that
                // reference would still dirty a card under ParallelGC, and the views that parallel workers
                // use to translate SYMBOL join keys share card-table cache lines, so store only a different cursor.
                // Never store the shared empty cursor that a hash past the index's key count gets: it would
                // replace the real cursor, and the next in-range lookup would have to allocate a new one.
                if (cursor != rowCursor && cursor != EmptyRowCursor.INSTANCE) {
                    rowCursor = cursor;
                }
                while (cursor.hasNext()) {
                    final long offsetOffset = cursor.next();
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
            return cachedValueOf(key, csviewB);
        }

        @Override
        public CharSequence valueOf(int key) {
            return cachedValueOf(key, csviewA);
        }

        @Override
        public CharSequence valueOfOnce(int key) {
            return SymbolMapReaderImpl.this.valueOf(key, csviewOnce);
        }

        private CharSequence bindValueOf(int key, CachedHashView view) {
            // Entry's final fields make this racy read safe.
            final SymbolMapCachePool.Entry entry = cacheEntry;
            if (entry == null) {
                final SymbolMapCachePool pool = cachePool.getPlain();
                if (pool != null && key > -1 && key < symbolCount && cachePool.compareAndSet(pool, null)) {
                    acquireCache(pool);
                    return bindValueOf(key, view);
                }
                return SymbolMapReaderImpl.this.valueOf(key, view);
            }
            // Shared entries may contain keys committed after this reader's transaction.
            if (key < 0 || key >= symbolCount) {
                return null;
            }
            final SymbolMapSegments segments = entry.getSegments();
            if (segments.get(key, view, view.hint) != null) {
                return view;
            }
            final int result = segments.load(key, symbolCount, offsetMem, charMem);
            if (result == SymbolMapSegments.LOADED) {
                return segments.get(key, view, view.hint);
            }
            if (result == SymbolMapSegments.STOPPED) {
                logCacheStopped(entry);
            }
            return charMem.getStr(offsetMem.getLong(SymbolMapWriter.keyToOffset(key)), view);
        }

        private CharSequence cachedValueOf(int key, CachedHashView view) {
            final CharSequence value = bindValueOf(key, view);
            if (value != null) {
                view.key = key;
            }
            return value;
        }

        private long[] getValueHashes() {
            if (valueHashes == null) {
                valueHashes = new long[VALUE_HASHES_SIZE];
                valueHashesGeneration = symbolTableGeneration;
            } else if (valueHashesGeneration != symbolTableGeneration) {
                Arrays.fill(valueHashes, 0);
                valueHashesGeneration = symbolTableGeneration;
            }
            return valueHashes;
        }

        private boolean hasValueHash(int key) {
            return valueHashes != null && valueHashesGeneration == symbolTableGeneration
                    && (int) (valueHashes[key & (VALUE_HASHES_SIZE - 1)] >>> 32) == key + 1;
        }

        /**
         * The A and B value flyweights. The view sets the key on every bind, so the key names the
         * bound value, and hashCode() reuses the hash that valueHashes holds for that key.
         */
        private final class CachedHashView extends DirectString {
            private final SymbolMapSegments.Hint hint = new SymbolMapSegments.Hint();
            private int key = -1;

            @Override
            public int hashCode() {
                final int k = key;
                if (k < 0) {
                    return super.hashCode();
                }
                final long[] hashes = getValueHashes();
                final int slot = k & (VALUE_HASHES_SIZE - 1);
                final long entry = hashes[slot];
                if ((int) (entry >>> 32) == k + 1) {
                    return (int) entry;
                }
                final int h = super.hashCode();
                hashes[slot] = valueHashEntry(k, h);
                return h;
            }

            @Override
            protected boolean isRaw() {
                return true;
            }
        }
    }
}
