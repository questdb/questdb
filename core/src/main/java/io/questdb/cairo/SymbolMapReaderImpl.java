/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.cairo.vm.api.MemoryCR;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
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

    public SymbolMapReaderImpl(CairoConfiguration configuration, Path path, CharSequence name, long columnNameTxn, int symbolCount) {
        of(configuration, path, name, columnNameTxn, symbolCount);
    }

    @Override
    public void close() {
        Misc.free(indexReader);
        Misc.free(charMem);
        this.cache.clear();
        int fd = this.offsetMem.getFd();
        Misc.free(offsetMem);
        Misc.free(path);
        LOG.debug().$("closed [fd=").$(fd).$(']').$();
    }

    @Override
    public boolean containsNullValue() {
        return nullValue;
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
                if (Chars.equals(value, charMem.getStr(offsetMem.getLong(offsetOffset)))) {
                    return SymbolMapWriter.offsetToKey(offsetOffset);
                }
            }
            return SymbolTable.VALUE_NOT_FOUND;
        }
        return SymbolTable.VALUE_IS_NULL;
    }

    public StaticSymbolTable newSymbolTableView() {
        return new SymbolTableView();
    }

    public void of(CairoConfiguration configuration, Path path, CharSequence columnName, long columnNameTxn, int symbolCount) {
        FilesFacade ff = configuration.getFilesFacade();
        this.configuration = configuration;
        this.path.of(path);
        this.columnNameSink.clear();
        this.columnNameSink.put(columnName);
        this.columnNameTxn = columnNameTxn;
        this.symbolCount = symbolCount;
        this.maxOffset = SymbolMapWriter.keyToOffset(symbolCount);
        final int plen = path.length();
        try {
            // this constructor does not create index. Index must exist,
            // and we use "offset" file to store "header"
            offsetFileName(path.trimTo(plen), columnName, columnNameTxn);
            if (!ff.exists(path)) {
                LOG.error().$(path).$(" is not found").$();
                throw CairoException.critical(0).put("SymbolMap does not exist: ").put(path);
            }

            // is there enough length in "offset" file for "header"?
            long len = ff.length(path);
            if (len < SymbolMapWriter.HEADER_SIZE) {
                LOG.error().$(path).$(" is too short [len=").$(len).$(']').$();
                throw CairoException.critical(0).put("SymbolMap is too short: ").put(path);
            }

            // open "offset" memory and make sure we start appending from where
            // we left off. Where we left off is stored externally to symbol map
            final long offsetMemSize = SymbolMapWriter.keyToOffset(symbolCount) + Long.BYTES;
            LOG.debug().$("offsetMem.of [columnName=").$(path).$(",offsetMemSize=").$(offsetMemSize).I$();
            this.offsetMem.of(ff, path, offsetMemSize, offsetMemSize, MemoryTag.MMAP_INDEX_READER);
            this.symbolCapacity = offsetMem.getInt(SymbolMapWriter.HEADER_CAPACITY);
            assert this.symbolCapacity > 0;
            this.cached = offsetMem.getBool(SymbolMapWriter.HEADER_CACHE_ENABLED);
            this.nullValue = offsetMem.getBool(SymbolMapWriter.HEADER_NULL_FLAG);

            // index reader is used to identify attempts to store duplicate symbol value
            this.indexReader.of(configuration, path.trimTo(plen), columnName, columnNameTxn, 0);

            // this is the place where symbol values are stored
            this.charMem.wholeFile(ff, charFileName(path.trimTo(plen), columnName, columnNameTxn), MemoryTag.MMAP_INDEX_READER);

            // move append pointer for symbol values in the correct place
            this.charMem.extend(this.offsetMem.getLong(maxOffset));

            // we use index hash maximum equals to half of symbol capacity, which
            // theoretically should require 2 value cells in index per hash
            // we use 4 cells to compensate for occasionally unlucky hash distribution
            this.maxHash = Math.max(Numbers.ceilPow2(symbolCapacity / 2) - 1, 1);
            if (cached) {
                this.cache.setPos(symbolCapacity);
            }
            this.cache.clear();
            LOG.debug().$("open [columnName=").$(path.trimTo(plen).concat(columnName).$()).$(", fd=").$(this.offsetMem.getFd()).$(", capacity=").$(symbolCapacity).$(']').$();
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
            this.offsetMem.extend(maxOffset + Long.BYTES);
            this.charMem.extend(this.offsetMem.getLong(maxOffset));
        } else if (symbolCount < this.symbolCount) {
            cache.remove(symbolCount + 1, this.symbolCount);
            this.symbolCount = symbolCount;
        }
        // Refresh index reader to avoid memory remapping on keyOf() calls.
        this.indexReader.of(configuration, path, columnNameSink, columnNameTxn, 0);
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
        return charMem.getStr(offsetMem.getLong(SymbolMapWriter.keyToOffset(key)));
    }

    private CharSequence uncachedValue2(int key) {
        return charMem.getStr2(offsetMem.getLong(SymbolMapWriter.keyToOffset(key)));
    }

    @TestOnly
    public int getCacheSize() {
        return cache.size();
    }

    private class SymbolTableView implements StaticSymbolTable {
        private final MemoryCR.CharSequenceView csview = new MemoryCR.CharSequenceView();
        private final MemoryCR.CharSequenceView csview2 = new MemoryCR.CharSequenceView();
        private final MemoryCR.CharSequenceView csviewInternal = new MemoryCR.CharSequenceView();
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
            return charMem.getStr(offsetMem.getLong(SymbolMapWriter.keyToOffset(key)), csview);
        }

        private CharSequence uncachedValue2(int key) {
            return charMem.getStr(offsetMem.getLong(SymbolMapWriter.keyToOffset(key)), csview2);
        }
    }
}
