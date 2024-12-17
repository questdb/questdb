/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.SingleCharCharSequence;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;

import static io.questdb.cairo.TableUtils.charFileName;
import static io.questdb.cairo.TableUtils.offsetFileName;

public class SymbolMapWriter implements Closeable, MapWriter {
    public static final int HEADER_CACHE_ENABLED = 4;
    public static final int HEADER_CAPACITY = 0;
    public static final int HEADER_NULL_FLAG = 8;
    public static final int HEADER_SIZE = 64;
    private static final Log LOG = LogFactory.getLog(SymbolMapWriter.class);
    private final CharSequenceIntHashMap cache;
    private final MemoryMARW charMem;
    private final BitmapIndexWriter indexWriter;
    private final int maxHash;
    private final int symbolCapacity;
    private final SymbolValueCountCollector valueCountCollector;
    private boolean cachedFlag;
    private boolean nullValue = false;
    private MemoryMARW offsetMem;
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
        final int plen = path.size();
        try {
            final FilesFacade ff = configuration.getFilesFacade();
            final long mapPageSize = configuration.getMiscAppendPageSize();

            // this constructor does not create index. Index must exist,
            // and we use "offset" file to store "header"
            if (!ff.exists(offsetFileName(path.trimTo(plen), name, columnNameTxn))) {
                LOG.error().$(path).$(" is not found").$();
                throw CairoException.critical(0).put("SymbolMap does not exist: ").put(path);
            }

            // is there enough length in "offset" file for "header"?
            LPSZ lpsz = path.$();
            long len = ff.length(lpsz);
            if (len < HEADER_SIZE) {
                LOG.error().$(path).$(" is too short [len=").$(len).$(']').$();
                throw CairoException.critical(0).put("SymbolMap is too short: ").put(path);
            }

            // open "offset" memory and make sure we start appending from where
            // we left off. Where we left off is stored externally to symbol map
            this.offsetMem = Vm.getWholeMARWInstance(
                    ff,
                    lpsz,
                    mapPageSize,
                    MemoryTag.MMAP_INDEX_WRITER,
                    configuration.getWriterFileOpenOpts()
            );
            // formula for calculating symbol capacity needs to be in agreement with symbol reader
            this.symbolCapacity = offsetMem.getInt(HEADER_CAPACITY);
            assert symbolCapacity > 0;
            final boolean useCache = offsetMem.getBool(HEADER_CACHE_ENABLED);
            this.offsetMem.jumpTo(keyToOffset(symbolCount) + Long.BYTES);

            // index writer is used to identify attempts to store duplicate symbol value
            // symbol table index stores int keys and long values, e.g. value = key * 2 storage size
            this.indexWriter = new BitmapIndexWriter(configuration);
            this.indexWriter.of(path.trimTo(plen), name, columnNameTxn);

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
            this.maxHash = Math.max(Numbers.ceilPow2(symbolCapacity / 2) - 1, 1);

            if (useCache) {
                this.cache = new CharSequenceIntHashMap(symbolCapacity, 0.3, CharSequenceIntHashMap.NO_ENTRY_VALUE);
                cachedFlag = true;
            } else {
                this.cache = null;
                cachedFlag = false;
            }

            this.symbolIndexInTxWriter = symbolIndexInTxWriter;
            this.valueCountCollector = valueCountCollector;
            LOG.debug()
                    .$("open [name=").$(path.trimTo(plen).concat(name).$())
                    .$(", fd=").$(offsetMem.getFd())
                    .$(", cache=").$(cache != null)
                    .$(", capacity=").$(symbolCapacity)
                    .I$();

            // trust _txn file, not the key count in the files
            indexWriter.rollbackValues(keyToOffset(symbolCount - 1));
        } catch (Throwable e) {
            close();
            throw e;
        } finally {
            path.trimTo(plen);
        }
    }

    public static long keyToOffset(int key) {
        return HEADER_SIZE + key * 8L;
    }

    public static void mergeSymbols(final MapWriter dst, final SymbolMapReader src, final MemoryMARW map) {
        map.jumpTo(0);
        for (int srcId = 0, symbolCount = src.getSymbolCount(); srcId < symbolCount; srcId++) {
            map.putInt(dst.put(src.valueOf(srcId)));
        }
        dst.updateNullFlag(dst.getNullFlag() || src.containsNullValue());
    }

    public static boolean mergeSymbols(final MapWriter dst, final SymbolMapReader src) {
        boolean remapped = false;
        for (int srcId = 0, symbolCount = src.getSymbolCount(); srcId < symbolCount; srcId++) {
            if (dst.put(src.valueOf(srcId)) != srcId) {
                remapped = true;
            }
        }
        dst.updateNullFlag(dst.getNullFlag() || src.containsNullValue());
        return remapped;
    }

    @Override
    public void close() {
        Misc.free(indexWriter);
        Misc.free(charMem);
        if (offsetMem != null) {
            long fd = offsetMem.getFd();
            offsetMem = Misc.free(offsetMem);
            LOG.debug().$("closed [fd=").$(fd).$(']').$();
        }
        nullValue = false;
    }

    @Override
    public boolean getNullFlag() {
        return offsetMem.getBool(HEADER_NULL_FLAG);
    }

    @Override
    public int getSymbolCapacity() {
        return symbolCapacity;
    }

    public int getSymbolCount() {
        return offsetToKey(offsetMem.getAppendOffset() - Long.BYTES);
    }

    @Override
    public MemoryR getSymbolOffsetsMemory() {
        return offsetMem;
    }

    @Override
    public MemoryR getSymbolValuesMemory() {
        return charMem;
    }

    public boolean isCached() {
        return cachedFlag;
    }

    @Override
    public int put(char c) {
        return put(SingleCharCharSequence.get(c));
    }

    @Override
    public int put(CharSequence symbol) {
        return put(symbol, valueCountCollector);
    }

    @Override
    public int put(CharSequence symbol, SymbolValueCountCollector valueCountCollector) {
        if (symbol == null) {
            if (!nullValue) {
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
    public void sync(boolean async) {
        charMem.sync(async);
        offsetMem.sync(async);
        indexWriter.sync(async);
    }

    @Override
    public void truncate() {
        final int symbolCapacity = offsetMem.getInt(HEADER_CAPACITY);
        offsetMem.truncate();
        offsetMem.putInt(HEADER_CAPACITY, symbolCapacity);
        offsetMem.putBool(HEADER_CACHE_ENABLED, isCached());
        updateNullFlag(false);
        offsetMem.jumpTo(keyToOffset(0) + Long.BYTES);
        charMem.truncate();
        indexWriter.truncate();
        if (cache != null) {
            cache.clear();
        }
    }

    @Override
    public void updateCacheFlag(boolean flag) {
        offsetMem.putBool(HEADER_CACHE_ENABLED, flag);
        cachedFlag = flag;
    }

    @Override
    public void updateNullFlag(boolean flag) {
        offsetMem.putBool(HEADER_NULL_FLAG, flag);
        nullValue = flag;
    }

    private void jumpCharMemToSymbolCount(int symbolCount) {
        if (symbolCount > 0) {
            charMem.jumpTo(offsetMem.getLong(keyToOffset(symbolCount)));
        } else {
            charMem.jumpTo(0);
        }
    }

    private int lookupAndPut(CharSequence symbol, SymbolValueCountCollector countCollector) {
        int hash = Hash.boundedHash(symbol, maxHash);
        RowCursor cursor = indexWriter.getCursor(hash);
        while (cursor.hasNext()) {
            long offsetOffset = cursor.next();
            if (Chars.equals(symbol, charMem.getStrA(offsetMem.getLong(offsetOffset)))) {
                return offsetToKey(offsetOffset);
            }
        }
        return put0(symbol, hash, countCollector);
    }

    private int lookupPutAndCache(int index, CharSequence symbol, SymbolValueCountCollector countCollector) {
        final int result = lookupAndPut(symbol, countCollector);
        cache.putAt(index, symbol.toString(), result);
        return result;
    }

    private int put0(CharSequence symbol, int hash, SymbolValueCountCollector countCollector) {
        // offsetMem has N+1 entries, where N is the number of symbols
        // Last entry is the length of the symbol (.c) file after N symbols are already written
        final long nOffsetOffset = offsetMem.getAppendOffset() - 8L;
        final long nPlusOneValue = charMem.putStr(symbol);

        // Here we're adding the offset of in the offset file where the symbol started
        indexWriter.add(hash, nOffsetOffset);

        // Here we are adding a new symbol and writing offset file the offset AFTER the new symbol
        offsetMem.putLong(nPlusOneValue);

        final int symIndex = offsetToKey(nOffsetOffset);
        countCollector.collectValueCount(symbolIndexInTxWriter, symIndex + 1);
        return symIndex;
    }

    static int offsetToKey(long offset) {
        return (int) ((offset - HEADER_SIZE) / 8L);
    }
}
