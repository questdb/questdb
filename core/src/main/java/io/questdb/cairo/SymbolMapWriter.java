/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.str.Path;
import io.questdb.std.str.SingleCharCharSequence;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;

import static io.questdb.cairo.TableUtils.charFileName;
import static io.questdb.cairo.TableUtils.offsetFileName;

public class SymbolMapWriter implements Closeable, MapWriter {
    public static final int HEADER_SIZE = 64;
    public static final int HEADER_CAPACITY = 0;
    public static final int HEADER_CACHE_ENABLED = 4;
    public static final int HEADER_NULL_FLAG = 8;
    private static final Log LOG = LogFactory.getLog(SymbolMapWriter.class);
    private final BitmapIndexWriter indexWriter;
    private final MemoryMARW charMem;
    private final MemoryMARW offsetMem;
    private final CharSequenceIntHashMap cache;
    private final int maxHash;
    private final SymbolValueCountCollector valueCountCollector;
    private boolean nullValue = false;
    private int symbolIndexInTxWriter;

    public SymbolMapWriter(
            CairoConfiguration configuration,
            Path path,
            CharSequence name,
            long columnNameTxn,
            int symbolCount,
            int symbolIndexInTxWriter,
            @NotNull SymbolValueCountCollector valueCountCollector
    ) {
        final int plen = path.length();
        try {
            final FilesFacade ff = configuration.getFilesFacade();
            final long mapPageSize = configuration.getMiscAppendPageSize();

            // this constructor does not create index. Index must exist,
            // and we use "offset" file to store "header"
            offsetFileName(path.trimTo(plen), name, columnNameTxn);
            if (!ff.exists(path)) {
                LOG.error().$(path).$(" is not found").$();
                throw CairoException.instance(0).put("SymbolMap does not exist: ").put(path);
            }

            // is there enough length in "offset" file for "header"?
            long len = ff.length(path);
            if (len < HEADER_SIZE) {
                LOG.error().$(path).$(" is too short [len=").$(len).$(']').$();
                throw CairoException.instance(0).put("SymbolMap is too short: ").put(path);
            }

            // open "offset" memory and make sure we start appending from where
            // we left off. Where we left off is stored externally to symbol map
            this.offsetMem = Vm.getWholeMARWInstance(
                    ff,
                    path,
                    mapPageSize,
                    MemoryTag.MMAP_INDEX_WRITER,
                    configuration.getWriterFileOpenOpts()
            );
            // formula for calculating symbol capacity needs to be in agreement with symbol reader
            final int symbolCapacity = offsetMem.getInt(HEADER_CAPACITY);
            assert symbolCapacity > 0;
            final boolean useCache = offsetMem.getBool(HEADER_CACHE_ENABLED);
            this.offsetMem.jumpTo(keyToOffset(symbolCount) + Long.BYTES);

            // index writer is used to identify attempts to store duplicate symbol value
            // symbol table index stores int keys and long values, e.g. value = key * 2 storage size
            this.indexWriter = new BitmapIndexWriter(
                    configuration,
                    path.trimTo(plen),
                    name,
                    columnNameTxn,
                    configuration.getDataIndexKeyAppendPageSize(),
                    configuration.getDataIndexKeyAppendPageSize() * 2
            );

            // this is the place where symbol values are stored
            this.charMem = Vm.getWholeMARWInstance(
                    ff,
                    charFileName(path.trimTo(plen), name, columnNameTxn),
                    mapPageSize,
                    MemoryTag.MMAP_INDEX_WRITER,
                    configuration.getWriterFileOpenOpts()
            );

            // move append pointer for symbol values in the correct place
            jumpCharMemToSymbolCount(symbolCount);

            // we use index hash maximum equals to half of symbol capacity, which
            // theoretically should require 2 value cells in index per hash
            // we use 4 cells to compensate for occasionally unlucky hash distribution
            this.maxHash = Numbers.ceilPow2(symbolCapacity / 2) - 1;

            if (useCache) {
                this.cache = new CharSequenceIntHashMap(symbolCapacity);
            } else {
                this.cache = null;
            }

            this.symbolIndexInTxWriter = symbolIndexInTxWriter;
            this.valueCountCollector = valueCountCollector;
            LOG.debug()
                    .$("open [name=").$(path.trimTo(plen).concat(name).$())
                    .$(", fd=").$(this.offsetMem.getFd())
                    .$(", cache=").$(cache != null)
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
    public void close() {
        Misc.free(indexWriter);
        Misc.free(charMem);
        if (this.offsetMem != null) {
            long fd = this.offsetMem.getFd();
            Misc.free(offsetMem);
            LOG.debug().$("closed [fd=").$(fd).$(']').$();
        }
        nullValue = false;
    }

    public int getSymbolCount() {
        return offsetToKey(offsetMem.getAppendOffset() - Long.BYTES);
    }

    public boolean isCached() {
        return cache != null;
    }

    @Override
    public int put(char c) {
        return put(SingleCharCharSequence.get(c));
    }

    @Override
    public int put(CharSequence symbol) {
        return put(symbol, valueCountCollector);
    }

    public int put(CharSequence symbol, SymbolValueCountCollector valueCountCollector) {

        if (symbol == null) {
            if (!nullValue) {
                nullValue = true;
                updateNullFlag(true);
            }
            return SymbolTable.VALUE_IS_NULL;
        }

        if (cache != null) {
            int index = cache.keyIndex(symbol);
            return index < 0 ? cache.valueAt(index) : lookupPutAndCache(index, symbol, valueCountCollector);
        }
        return lookupAndPut(symbol, valueCountCollector);
    }

    @Override
    public void rollback(int symbolCount) {
        indexWriter.rollbackValues(keyToOffset(symbolCount - 1));
        offsetMem.jumpTo(keyToOffset(symbolCount) + Long.BYTES);
        jumpCharMemToSymbolCount(symbolCount);
        valueCountCollector.collectValueCount(symbolIndexInTxWriter, symbolCount);
        if (cache != null) {
            cache.clear();
        }
    }

    @Override
    public void setSymbolIndexInTxWriter(int symbolIndexInTxWriter) {
        this.symbolIndexInTxWriter = symbolIndexInTxWriter;
    }

    @Override
    public void truncate() {
        final int symbolCapacity = offsetMem.getInt(HEADER_CAPACITY);
        offsetMem.truncate();
        offsetMem.putInt(HEADER_CAPACITY, symbolCapacity);
        offsetMem.putBool(HEADER_CACHE_ENABLED, isCached());
        nullValue = false;
        updateNullFlag(nullValue);
        offsetMem.jumpTo(keyToOffset(0) + Long.BYTES);
        charMem.truncate();
        indexWriter.truncate();
        if (cache != null) {
            cache.clear();
        }
    }

    static int offsetToKey(long offset) {
        return (int) ((offset - HEADER_SIZE) / 8L);
    }

    static long keyToOffset(int key) {
        return HEADER_SIZE + key * 8L;
    }

    @Override
    public void updateCacheFlag(boolean flag) {
        offsetMem.putBool(HEADER_CACHE_ENABLED, flag);
    }

    private void jumpCharMemToSymbolCount(int symbolCount) {
        if (symbolCount > 0) {
            this.charMem.jumpTo(this.offsetMem.getLong(keyToOffset(symbolCount)));
        } else {
            this.charMem.jumpTo(0);
        }
    }

    private int lookupAndPut(CharSequence symbol, SymbolValueCountCollector countCollector) {
        int hash = Hash.boundedHash(symbol, maxHash);
        RowCursor cursor = indexWriter.getCursor(hash);
        while (cursor.hasNext()) {
            long offsetOffset = cursor.next();
            if (Chars.equals(symbol, charMem.getStr(offsetMem.getLong(offsetOffset)))) {
                return offsetToKey(offsetOffset);
            }
        }
        return put0(symbol, hash, countCollector);
    }

    private int lookupPutAndCache(int index, CharSequence symbol, SymbolValueCountCollector countCollector) {
        int result;
        result = lookupAndPut(symbol, countCollector);
        cache.putAt(index, symbol.toString(), result);
        return result;
    }

    private int put0(CharSequence symbol, int hash, SymbolValueCountCollector countCollector) {
        long offsetOffset = offsetMem.getAppendOffset() - Long.BYTES;
        offsetMem.putLong(charMem.putStr(symbol));
        indexWriter.add(hash, offsetOffset);
        final int symIndex = offsetToKey(offsetOffset);
        countCollector.collectValueCount(symbolIndexInTxWriter, symIndex + 1);
        return symIndex;
    }

    @Override
    public void updateNullFlag(boolean flag) {
        offsetMem.putBool(HEADER_NULL_FLAG, flag);
    }
}
