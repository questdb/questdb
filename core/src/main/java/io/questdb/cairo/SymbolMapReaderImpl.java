/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMR;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.str.Path;

import java.io.Closeable;

public class SymbolMapReaderImpl implements Closeable, SymbolMapReader {
    private static final Log LOG = LogFactory.getLog(SymbolMapReaderImpl.class);
    private final BitmapIndexBwdReader indexReader = new BitmapIndexBwdReader();
    private final MemoryMR charMem = Vm.getMRInstance();
    private final MemoryMR offsetMem = Vm.getMRInstance();
    private final ObjList<String> cache = new ObjList<>();
    private int maxHash;
    private boolean cached;
    private int symbolCount;
    private long maxOffset;
    private int symbolCapacity;
    private boolean nullValue;

    public SymbolMapReaderImpl() {
    }

    public SymbolMapReaderImpl(CairoConfiguration configuration, Path path, CharSequence name, int symbolCount) {
        of(configuration, path, name, symbolCount);
    }

    @Override
    public void close() {
        Misc.free(indexReader);
        Misc.free(charMem);
        this.cache.clear();
        long fd = this.offsetMem.getFd();
        Misc.free(offsetMem);
        LOG.debug().$("closed [fd=").$(fd).$(']').$();
    }

    @Override
    public int getSymbolCapacity() {
        return symbolCapacity;
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
    public void updateSymbolCount(int symbolCount) {
        if (symbolCount > this.symbolCount) {
            this.symbolCount = symbolCount;
            this.maxOffset = SymbolMapWriter.keyToOffset(symbolCount);
            this.offsetMem.extend(maxOffset);
            growCharMemToSymbolCount(symbolCount);
        } else if (symbolCount < this.symbolCount) {
            cache.remove(symbolCount + 1, this.symbolCount);
            this.symbolCount = symbolCount;
        }
    }

    public void of(CairoConfiguration configuration, Path path, CharSequence name, int symbolCount) {
        FilesFacade ff = configuration.getFilesFacade();
        this.symbolCount = symbolCount;
        this.maxOffset = SymbolMapWriter.keyToOffset(symbolCount - 1);
        final int plen = path.length();
        try {
            // this constructor does not create index. Index must exist
            // and we use "offset" file to store "header"
            SymbolMapWriter.offsetFileName(path.trimTo(plen), name);
            if (!ff.exists(path)) {
                LOG.error().$(path).$(" is not found").$();
                throw CairoException.instance(0).put("SymbolMap does not exist: ").put(path);
            }

            // is there enough length in "offset" file for "header"?
            long len = ff.length(path);
            if (len < SymbolMapWriter.HEADER_SIZE) {
                LOG.error().$(path).$(" is too short [len=").$(len).$(']').$();
                throw CairoException.instance(0).put("SymbolMap is too short: ").put(path);
            }

            // open "offset" memory and make sure we start appending from where
            // we left off. Where we left off is stored externally to symbol map
            this.offsetMem.partialFile(ff, path, SymbolMapWriter.keyToOffset(symbolCount), MemoryTag.MMAP_DEFAULT);
            symbolCapacity = offsetMem.getInt(SymbolMapWriter.HEADER_CAPACITY);
            this.cached = offsetMem.getBool(SymbolMapWriter.HEADER_CACHE_ENABLED);
            this.nullValue = offsetMem.getBool(SymbolMapWriter.HEADER_NULL_FLAG);

            // index writer is used to identify attempts to store duplicate symbol value
            this.indexReader.of(configuration, path.trimTo(plen), name, 0, -1);

            // this is the place where symbol values are stored
            this.charMem.wholeFile(ff, SymbolMapWriter.charFileName(path.trimTo(plen), name), MemoryTag.MMAP_DEFAULT);

            // move append pointer for symbol values in the correct place
            growCharMemToSymbolCount(symbolCount);

            // we use index hash maximum equals to half of symbol capacity, which
            // theoretically should require 2 value cells in index per hash
            // we use 4 cells to compensate for occasionally unlucky hash distribution
            this.maxHash = Numbers.ceilPow2(symbolCapacity / 2) - 1;
            if (cached) {
                this.cache.setPos(symbolCapacity);
            }
            this.cache.clear();
            LOG.debug().$("open [name=").$(path.trimTo(plen).concat(name).$()).$(", fd=").$(this.offsetMem.getFd()).$(", capacity=").$(symbolCapacity).$(']').$();
        } catch (Throwable e) {
            close();
            throw e;
        } finally {
            path.trimTo(plen);
        }
    }

    @Override
    public int size() {
        return symbolCount;
    }

    @Override
    public int keyOf(CharSequence value) {
        if (value != null) {
            int hash = Hash.boundedHash(value, maxHash);
            RowCursor cursor = indexReader.getCursor(true, hash, 0, maxOffset);
            while (cursor.hasNext()) {
                long offsetOffset = cursor.next();
                if (Chars.equals(value, charMem.getStr(offsetMem.getLong(offsetOffset)))) {
                    return SymbolMapWriter.offsetToKey(offsetOffset);
                }
            }
            return SymbolTable.VALUE_NOT_FOUND;
        }
        return SymbolTable.VALUE_IS_NULL;
    }

    @Override
    public boolean containsNullValue() {
        return nullValue;
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

    private CharSequence cachedValue(int key) {
        String symbol = cache.getQuiet(key);
        return symbol != null ? symbol : fetchAndCache(key);
    }

    private CharSequence fetchAndCache(int key) {
        String symbol;
        CharSequence cs = charMem.getStr(offsetMem.getLong(SymbolMapWriter.keyToOffset(key)));
        assert cs != null;
        cache.extendAndSet(key, symbol = Chars.toString(cs));
        return symbol;
    }

    private void growCharMemToSymbolCount(int symbolCount) {
        long charMemLength;
        if (symbolCount > 0) {
            long lastSymbolOffset = this.offsetMem.getLong(SymbolMapWriter.keyToOffset(symbolCount - 1));
            this.charMem.extend(lastSymbolOffset + 4);
            charMemLength = lastSymbolOffset + Vm.getStorageLength(this.charMem.getStrLen(lastSymbolOffset));
        } else {
            charMemLength = 0;
        }
        this.charMem.extend(charMemLength);
    }

    private CharSequence uncachedValue(int key) {
        return charMem.getStr(offsetMem.getLong(SymbolMapWriter.keyToOffset(key)));
    }

    private CharSequence uncachedValue2(int key) {
        return charMem.getStr2(offsetMem.getLong(SymbolMapWriter.keyToOffset(key)));
    }

    @Override
    public long symbolCharsAddressOf(int symbolIndex) {
        if (symbolIndex < symbolCount) {
            long offset = offsetMem.getLong(SymbolMapWriter.keyToOffset(symbolIndex));
            return charMem.addressOf(offset);
        } else if (symbolIndex == symbolCount) {
            return charMem.addressOf(charMem.getGrownLength());
        }

        return -1;
    }
}
