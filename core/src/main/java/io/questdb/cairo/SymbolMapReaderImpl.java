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
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import io.questdb.std.str.DirectString;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;

import static io.questdb.cairo.TableUtils.charFileName;
import static io.questdb.cairo.TableUtils.offsetFileName;

public class SymbolMapReaderImpl implements Closeable, SymbolMapReader {
    private static final Log LOG = LogFactory.getLog(SymbolMapReaderImpl.class);
    private final ObjList<String> cache = new ObjList<>();
    private final MemoryCMR charMem = Vm.getCMRInstance();
    private final StringSink columnNameSink = new StringSink();
    private final ConcurrentBitmapIndexFwdReader indexReader = new ConcurrentBitmapIndexFwdReader();
    private final MemoryCMR offsetMem = Vm.getCMRInstance();
    private final Path path = new Path();
    private boolean cached;
    private long columnNameTxn;
    private CairoConfiguration configuration;
    private int maxHash;
    private long maxOffset;
    private boolean nullValue;
    private int symbolCapacity;
    private int symbolCount;

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

    @Override
    public void close() {
        Misc.free(indexReader);
        Misc.free(charMem);
        cache.clear();
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
    public int getCacheSize() {
        return cache.size();
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
            final RowCursor cursor = indexReader.getCursor(true, hash, 0, maxOffset - Long.BYTES);
            while (cursor.hasNext()) {
                final long offsetOffset = cursor.next();
                if (Chars.equals(value, charMem.getStrA(offsetMem.getLong(offsetOffset)))) {
                    return SymbolMapWriter.offsetToKey(offsetOffset);
                }
            }
            return SymbolTable.VALUE_NOT_FOUND;
        }
        return SymbolTable.VALUE_IS_NULL;
    }

    public boolean needsReopen(long columnNameTxn) {
        return this.columnNameTxn != columnNameTxn;
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
        final FilesFacade ff = configuration.getFilesFacade();
        this.configuration = configuration;
        this.path.of(pathToTableDir);
        this.columnNameSink.clear();
        this.columnNameSink.put(columnName);
        this.columnNameTxn = columnNameTxn;
        this.symbolCount = symbolCount;
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
            indexReader.of(configuration, path.trimTo(plen), columnName, columnNameTxn, -1, 0);

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
                cache.setPos(symbolCapacity);
            }
            cache.clear();
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
            cache.remove(symbolCount + 1, this.symbolCount);
            this.symbolCount = symbolCount;
        }
        // Refresh contains null flag.
        this.nullValue = offsetMem.getBool(SymbolMapWriter.HEADER_NULL_FLAG);
        // Refresh index reader to avoid memory remapping on keyOf() calls.
        // partition txn does not matter, because symbol is at the root of the table dir (not at partition level)
        indexReader.of(configuration, path, columnNameSink, columnNameTxn, -1, 0);
    }

    @Override
    public CharSequence valueBOf(int key) {
        if (key > -1 && key < symbolCount) {
            if (cached) {
                return cachedValue(key);
            }
            return uncachedValue2(key);
        }
        return null;
    }

    @Override
    public CharSequence valueOf(int key) {
        if (key > -1 && key < symbolCount) {
            if (cached) {
                return cachedValue(key);
            }
            return uncachedValue(key);
        }
        return null;
    }

    private CharSequence cachedValue(int key) {
        final String symbol = cache.getQuiet(key);
        return symbol != null ? symbol : fetchAndCache(key);
    }

    private CharSequence fetchAndCache(int key) {
        final CharSequence cs = uncachedValue(key);
        assert cs != null;
        final String symbol = Chars.toString(cs);
        cache.extendAndSet(key, symbol);
        return symbol;
    }

    private CharSequence uncachedValue(int key) {
        return charMem.getStrA(offsetMem.getLong(SymbolMapWriter.keyToOffset(key)));
    }

    private CharSequence uncachedValue2(int key) {
        return charMem.getStrB(offsetMem.getLong(SymbolMapWriter.keyToOffset(key)));
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
            if (key > -1 && key < symbolCount) {
                return uncachedValue2(key);
            }
            return null;
        }

        @Override
        public CharSequence valueOf(int key) {
            if (key > -1 && key < symbolCount) {
                return uncachedValue(key);
            }
            return null;
        }

        private CharSequence uncachedValue(int key) {
            return charMem.getStr(offsetMem.getLong(SymbolMapWriter.keyToOffset(key)), csviewA);
        }

        private CharSequence uncachedValue2(int key) {
            return charMem.getStr(offsetMem.getLong(SymbolMapWriter.keyToOffset(key)), csviewB);
        }
    }
}
