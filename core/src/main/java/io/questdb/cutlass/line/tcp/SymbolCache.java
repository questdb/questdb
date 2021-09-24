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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.SymbolMapReaderImpl;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMR;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjIntHashMap;
import io.questdb.std.str.Path;

import java.io.Closeable;

class SymbolCache implements Closeable {
    private final ObjIntHashMap<CharSequence> indexBySym = new ObjIntHashMap<>(256, 0.5, SymbolTable.VALUE_NOT_FOUND);
    private final MemoryMR txMem = Vm.getMRInstance();
    private final SymbolMapReaderImpl symMapReader = new SymbolMapReaderImpl();
    private long transientSymCountOffset;

    SymbolCache() {
    }

    @Override
    public void close() {
        symMapReader.close();
        indexBySym.clear();
        txMem.close();
    }

    int getNCached() {
        return indexBySym.size();
    }

    int getSymIndex(CharSequence symValue) {
        int symIndex = indexBySym.get(symValue);

        if (SymbolTable.VALUE_NOT_FOUND != symIndex) {
            return symIndex;
        }

        int symCount = txMem.getInt(transientSymCountOffset);
        symMapReader.updateSymbolCount(symCount);
        symIndex = symMapReader.keyOf(symValue);

        if (SymbolTable.VALUE_NOT_FOUND != symIndex) {
            indexBySym.put(symValue.toString(), symIndex);
        }

        return symIndex;
    }

    void of(CairoConfiguration configuration, Path path, CharSequence name, int symIndex) {
        FilesFacade ff = configuration.getFilesFacade();
        transientSymCountOffset = TableUtils.getSymbolWriterTransientIndexOffset(symIndex);
        final int plen = path.length();
        txMem.partialFile(ff, path.concat(TableUtils.TXN_FILE_NAME).$(), transientSymCountOffset + Integer.BYTES, MemoryTag.MMAP_DEFAULT);
        int symCount = txMem.getInt(transientSymCountOffset);
        path.trimTo(plen);
        symMapReader.of(configuration, path, name, symCount);
        indexBySym.clear(symCount);
    }
}
