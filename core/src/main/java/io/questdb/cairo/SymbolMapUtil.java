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

import io.questdb.cairo.idx.BitmapIndexWriter;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMOR;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.Hash;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;

import static io.questdb.cairo.SymbolMapWriter.*;
import static io.questdb.cairo.TableUtils.charFileName;
import static io.questdb.cairo.TableUtils.offsetFileName;

// The instance is re-usable but doesn't need to be Closable
// Native resources are deallocated after every call automatically but the wrappers are re-usable
public class SymbolMapUtil {
    private static final Log LOG = LogFactory.getLog(SymbolMapUtil.class);
    private MemoryCMOR charMem;
    private BitmapIndexWriter indexWriter;
    private MemoryMARW offsetMem;

    public void rebuildSymbolFiles(
            CairoConfiguration configuration,
            Path path,
            CharSequence name,
            long columnNameTxn,
            int symbolCount,
            int symbolCapacity
    ) {
        rebuildSymbolFiles(
                configuration,
                path,
                name,
                columnNameTxn,
                symbolCount,
                symbolCapacity,
                0
        );
    }

    public void rebuildSymbolFiles(
            CairoConfiguration configuration,
            Path path,
            CharSequence name,
            long columnNameTxn,
            int symbolCount,
            int symbolCapacity,
            int indexBlockCapacity
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
            long len = ff.length(path.$());
            if (len < SymbolMapWriter.HEADER_SIZE) {
                LOG.error().$(path).$(" is too short [len=").$(len).$(']').$();
                throw CairoException.critical(0).put("SymbolMap is too short: ").put(path);
            }
            offsetMem = open(configuration, ff, mapPageSize, len, path.$(), offsetMem);

            // formula for calculating symbol capacity needs to be in agreement with symbol reader
            if (symbolCapacity < 1) {
                symbolCapacity = offsetMem.getInt(HEADER_CAPACITY);
                assert symbolCapacity > 0;
            }
            int maxHash = Math.max(Numbers.ceilPow2(symbolCapacity / 2) - 1, 1);

            // index writer is used to identify attempts to store duplicate symbol value
            // symbol table index stores int keys and long values, e.g. value = key * 2 storage size
            if (this.indexWriter == null) {
                this.indexWriter = new BitmapIndexWriter(configuration);
            }
            int indexBlockCap = Math.max(TableUtils.MIN_INDEX_VALUE_BLOCK_SIZE, indexBlockCapacity);
            this.indexWriter.of(path.trimTo(plen), name, columnNameTxn, indexBlockCap);

            // clean the files, except .c file
            truncate(symbolCapacity);

            // open .c file
            long charFileLen = ff.length(charFileName(path.trimTo(plen), name, columnNameTxn));
            if (charFileLen <= 0) {
                // .c file is empty, nothing to do
                return;
            }
            charMem = open(configuration, ff, mapPageSize, charFileLen, path.$(), charMem);

            // Read .c file and rebuild symbol map
            long strOffset = 0;
            long offsetOffset = keyToOffset(0);
            offsetMem.putLong(0L);
            offsetOffset += Long.BYTES;

            for (int i = 0; i < symbolCount; i++) {
                if (strOffset > charMem.size() - 4) {
                    throw new CairoException().put("corrupted symbol map [name=").put(path).put(']');
                }
                // read symbol value
                CharSequence symbol = charMem.getStrA(strOffset);
                strOffset += Vm.getStorageLength(symbol);
                if (symbol.isEmpty()) {
                    LOG.info().$("symbol is empty [index=").$(i).$(']').$();
                }

                // write symbol value back to symbol map
                int hash = Hash.boundedHash(symbol, maxHash);
                indexWriter.add(hash, offsetOffset - Long.BYTES);

                // offset stores + 1 of symbols, write offset beyond the end of last symbol
                offsetMem.putLong(strOffset);
                offsetOffset += Long.BYTES;
            }

        } finally {
            Misc.free(charMem);
            if (offsetMem != null) {
                offsetMem.close(false);
            }
            Misc.free(indexWriter);
            path.trimTo(plen);
        }
    }

    private static MemoryCMOR open(
            CairoConfiguration configuration,
            FilesFacade ff,
            long mapPageSize,
            long size,
            LPSZ path,
            MemoryCMOR mem
    ) {
        assert size != -1;

        if (mem == null) {
            mem = Vm.getMemoryCMOR(false);
        }
        mem.of(
                ff,
                path,
                mapPageSize,
                size,
                MemoryTag.MMAP_INDEX_WRITER,
                configuration.getWriterFileOpenOpts()
        );
        return mem;
    }

    private static MemoryMARW open(CairoConfiguration configuration, FilesFacade ff, long mapPageSize, long size, LPSZ path, MemoryMARW mem) {
        if (mem == null) {
            mem = Vm.getCMARWInstance(
                    ff,
                    path,
                    mapPageSize,
                    size,
                    MemoryTag.MMAP_INDEX_WRITER,
                    configuration.getWriterFileOpenOpts()
            );
        } else {
            mem.of(
                    ff,
                    path,
                    mapPageSize,
                    size,
                    MemoryTag.MMAP_INDEX_WRITER,
                    configuration.getWriterFileOpenOpts()
            );
        }
        return mem;
    }

    private void truncate(int symbolCapacity) {
        final boolean useCache = offsetMem.getBool(HEADER_CACHE_ENABLED);
        boolean nullFlag = offsetMem.getBool(HEADER_NULL_FLAG);

        offsetMem.truncate();
        offsetMem.putInt(HEADER_CAPACITY, symbolCapacity);
        offsetMem.putBool(HEADER_CACHE_ENABLED, useCache);
        offsetMem.putBool(HEADER_NULL_FLAG, nullFlag);
        offsetMem.jumpTo(keyToOffset(0));

        indexWriter.truncate();
    }

    static long calculateExtendSegmentSize(CairoConfiguration configuration, long fileLen) {
        return Math.min(Numbers.floorPow2(Math.max(configuration.getSymbolTableMinAllocationPageSize(), fileLen)), configuration.getSymbolTableMaxAllocationPageSize());
    }
}
