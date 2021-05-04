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

package io.questdb.cutlass.line.tcp;

import java.io.Closeable;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.SymbolMapReaderImpl;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.vm.MappedReadOnlyMemory;
import io.questdb.cairo.vm.SinglePageMappedReadOnlyPageMemory;
import io.questdb.std.Chars;
import io.questdb.std.FilesFacade;
import io.questdb.std.ObjIntHashMap;
import io.questdb.std.str.Path;

class SymbolCache implements Closeable {
    private final ObjIntHashMap<CharSequence> indexBySym;
    private final MappedReadOnlyMemory txMem = new SinglePageMappedReadOnlyPageMemory();
    private final SymbolMapReaderImpl symMapReader = new SymbolMapReaderImpl();
    private long transientSymCountOffset;

    public SymbolCache(CairoConfiguration configuration) {
        indexBySym = new ObjIntHashMap<>(configuration.getDefaultSymbolCapacity(), 0.5, SymbolTable.VALUE_NOT_FOUND);
    }

    void of(CairoConfiguration configuration, Path path, CharSequence name, int symIndex) {
        FilesFacade ff = configuration.getFilesFacade();
        transientSymCountOffset = TableUtils.getSymbolWriterTransientIndexOffset(symIndex);
        int plen = path.length();
        txMem.of(ff, path.concat(TableUtils.TXN_FILE_NAME).$(), ff.getPageSize(), transientSymCountOffset + Integer.BYTES);
        int symCount = txMem.getInt(transientSymCountOffset);
        path.trimTo(plen);
        symMapReader.of(configuration, path, name, symCount);
        indexBySym.clear(symCount);
    }

    int getSymIndex(CharSequence symValue) {
        final int index = indexBySym.keyIndex(symValue);
        if (index < 0) {
            return indexBySym.valueAt(index);
        }

        int symCount = txMem.getInt(transientSymCountOffset);
        symMapReader.updateSymbolCount(symCount);
        int symIndex = symMapReader.keyOf(symValue);

        if (SymbolTable.VALUE_NOT_FOUND != symIndex) {
            indexBySym.putAt(index, Chars.toString(symValue), symIndex);
        }

        return symIndex;
    }

    int getNCached() {
        return indexBySym.size();
    }

    @Override
    public void close() {
        symMapReader.close();
        indexBySym.clear();
        txMem.close();
    }
}
