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
import java.io.IOException;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ReadOnlyMemory;
import io.questdb.cairo.SymbolMapReaderImpl;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.std.FilesFacade;
import io.questdb.std.ObjIntHashMap;
import io.questdb.std.str.Path;

class SymbolCache implements Closeable {
    private final ObjIntHashMap<CharSequence> indexBySym = new ObjIntHashMap<CharSequence>(8, 0.5, SymbolTable.VALUE_NOT_FOUND);
    private final ReadOnlyMemory txMem = new ReadOnlyMemory();
    private final SymbolMapReaderImpl symMapReader = new SymbolMapReaderImpl();
    private long transientSymCountOffset;

    SymbolCache() {
    }

    void of(CairoConfiguration configuration, Path path, CharSequence name, int colIndex) {
        FilesFacade ff = configuration.getFilesFacade();
        long transientSymCountOffset = TableUtils.getSymbolWriterTransientIndexOffset(colIndex);
        int plen = path.length();
        txMem.of(ff, path.concat(TableUtils.TXN_FILE_NAME).$(), ff.getPageSize(), transientSymCountOffset + Integer.BYTES);
        int symCount = txMem.getInt(transientSymCountOffset);
        path.trimTo(plen);
        symMapReader.of(configuration, path, name, symCount);
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

    void clear() {
        symMapReader.close();
        indexBySym.clear();
        txMem.close();
    }

    @Override
    public void close() throws IOException {
        symMapReader.close();
        indexBySym.clear();
        txMem.close();
    }
}
