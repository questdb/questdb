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

import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMR;
import io.questdb.std.*;
import io.questdb.std.str.Path;

import java.io.Closeable;

public class WalReaderMetadata extends BaseRecordMetadata implements Closeable {
    private final Path path;
    private final FilesFacade ff;
    private final MemoryMR metaMem;
    private final ObjList<SymbolMapDiff> symbolMapDiffs;
    private final long segmentId;

    public WalReaderMetadata(FilesFacade ff, long segmentId) {
        this.path = new Path();
        this.ff = ff;
        this.segmentId = segmentId;
        this.metaMem = Vm.getMRInstance();
        this.columnMetadata = new ObjList<>(columnCount);
        this.columnNameIndexMap = new LowerCaseCharSequenceIntHashMap();
        this.symbolMapDiffs = new ObjList<>();
    }

    @Override
    public void close() {
        // WalReaderMetadata is re-usable after close, don't assign nulls
        Misc.free(metaMem);
        Misc.free(path);
    }

    public WalReaderMetadata of(Path p, int expectedVersion) {
        path.of(p).slash().put(segmentId).concat(TableUtils.META_FILE_NAME).$();
        try {
            metaMem.smallFile(ff, path, MemoryTag.MMAP_DEFAULT);
            columnNameIndexMap.clear();
            TableUtils.loadWalMetadata(metaMem, columnMetadata, columnNameIndexMap, symbolMapDiffs, expectedVersion);
            columnCount = metaMem.getInt(TableUtils.WAL_META_OFFSET_COUNT);
            timestampIndex = -1;
        } catch (Throwable e) {
            close();
            throw e;
        }
        return this;
    }

    public SymbolMapDiff getSymbolMapDiff(int columnIndex) {
        return symbolMapDiffs.get(columnIndex);
    }
}
