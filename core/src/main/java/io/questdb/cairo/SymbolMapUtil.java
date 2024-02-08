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

import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.str.Path;

import static io.questdb.cairo.SymbolMapWriter.*;
import static io.questdb.cairo.TableUtils.charFileName;
import static io.questdb.cairo.TableUtils.offsetFileName;

// The instance is re-usable but doesn't need to be Closable
// Resources are deallocated after every call automatically
public class SymbolMapUtil {
    private static final Log LOG = LogFactory.getLog(SymbolMapUtil.class);
    private MemoryMARW charMem;
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
        final int plen = path.size();
        try {
            final FilesFacade ff = configuration.getFilesFacade();
            final long mapPageSize = configuration.getMiscAppendPageSize();

            // this constructor does not create index. Index must exist,
            // and we use "offset" file to store "header"
            offsetFileName(path.trimTo(plen), name, columnNameTxn);
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
            LOG.info().$("opening symbol [path=").$(path).$(", symbolCount=").$(symbolCount).$(']').$();

            // open "offset" memory and make sure we start appending from where
            // we left off. Where we left off is stored externally to symbol map
            if (offsetMem == null) {
                this.offsetMem = Vm.getWholeMARWInstance(
                        ff,
                        path,
                        mapPageSize,
                        MemoryTag.MMAP_INDEX_WRITER,
                        configuration.getWriterFileOpenOpts()
                );
            } else {
                offsetMem.of(ff, path, mapPageSize, MemoryTag.MMAP_INDEX_WRITER, configuration.getWriterFileOpenOpts());
            }
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
            this.indexWriter.of(path.trimTo(plen), name, columnNameTxn);

            // clean the files, except .c file
            truncate(symbolCapacity);

            // open .c file
            if (this.charMem == null) {
                this.charMem = Vm.getWholeMARWInstance(
                        ff,
                        charFileName(path.trimTo(plen), name, columnNameTxn),
                        mapPageSize,
                        MemoryTag.MMAP_INDEX_WRITER,
                        configuration.getWriterFileOpenOpts()
                );
            } else {
                this.charMem.of(
                        ff,
                        charFileName(path.trimTo(plen), name, columnNameTxn),
                        mapPageSize,
                        MemoryTag.MMAP_INDEX_WRITER,
                        configuration.getWriterFileOpenOpts()
                );
            }

            // Read .c file and rebuild symbol map
            long strOffset = 0;
            for (int i = 0; i < symbolCount; i++) {
                // read symbol value
                CharSequence symbol = charMem.getStr(strOffset);
                strOffset += Vm.getStorageLength(symbol);

                // write symbol value back to symbol map
                int hash = Hash.boundedHash(symbol, maxHash);
                long offsetOffset = offsetMem.getAppendOffset() - Long.BYTES;
                offsetMem.putLong(charMem.putStr(symbol));
                indexWriter.add(hash, offsetOffset);
            }
        } finally {
            Misc.free(charMem);
            Misc.free(offsetMem);
            Misc.free(indexWriter);
            path.trimTo(plen);
        }
    }

    private void truncate(int symbolCapacity) {
        final boolean useCache = offsetMem.getBool(HEADER_CACHE_ENABLED);
        boolean nullFlag = offsetMem.getBool(HEADER_NULL_FLAG);

        offsetMem.truncate();
        offsetMem.putInt(HEADER_CAPACITY, symbolCapacity);
        offsetMem.putBool(HEADER_CACHE_ENABLED, useCache);
        offsetMem.putBool(HEADER_NULL_FLAG, nullFlag);
        offsetMem.jumpTo(keyToOffset(0) + Long.BYTES);

        indexWriter.truncate();
    }
}
