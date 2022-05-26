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
    private int version;

    public WalReaderMetadata(FilesFacade ff) {
        this.path = new Path();
        this.ff = ff;
        this.metaMem = Vm.getMRInstance();
        this.columnMetadata = new ObjList<>(columnCount);
        this.columnNameIndexMap = new LowerCaseCharSequenceIntHashMap();
    }

    public WalReaderMetadata(FilesFacade ff, Path path) {
        this(ff);
        of(path, ColumnType.VERSION);
    }

    @Override
    public void close() {
        // WalReaderMetadata is re-usable after close, don't assign nulls
        Misc.free(metaMem);
        Misc.free(path);
    }

    @Override
    public int getColumnCount() {
        return columnCount;
    }

    public int getVersion() {
        return version;
    }

    public WalReaderMetadata of(Path path, int expectedVersion) {
        this.path.of(path).$();
        try {
            metaMem.smallFile(ff, path, MemoryTag.MMAP_DEFAULT);
            columnNameIndexMap.clear();
            TableUtils.validateWalMeta(metaMem, columnMetadata, columnNameIndexMap, expectedVersion);
            version = metaMem.getInt(TableUtils.WAL_META_OFFSET_VERSION);
            columnCount = metaMem.getInt(TableUtils.WAL_META_OFFSET_COLUMNS);
            timestampIndex = -1;
        } catch (Throwable e) {
            close();
            throw e;
        }
        return this;
    }
}
