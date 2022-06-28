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
import io.questdb.cairo.vm.api.MemoryMAR;
import io.questdb.std.*;
import io.questdb.std.str.Path;

import java.io.Closeable;

import static io.questdb.cairo.TableUtils.*;

class WalWriterMetadata extends BaseRecordMetadata implements Closeable {
    private final FilesFacade ff;
    private final MemoryMAR metaMem = Vm.getMARInstance();

    WalWriterMetadata(FilesFacade ff) {
        this.ff = ff;
        columnMetadata = new ObjList<>();
        columnNameIndexMap = new LowerCaseCharSequenceIntHashMap();
    }

    void of(TableDescriptor descriptor) {
        reset();

        timestampIndex = descriptor.getTimestampIndex();

        for (int i = 0; i < descriptor.getColumnCount(); i++) {
            final CharSequence name = descriptor.getColumnName(i);
            final int type = descriptor.getColumnType(i);
            addColumn(i, name, type);
        }
    }

    private void reset() {
        columnMetadata.clear();
        columnNameIndexMap.clear();
        columnCount = 0;
        timestampIndex = -1;
    }

    @Override
    public void close() {
        Misc.free(metaMem);
    }

    void addColumn(int columnIndex, CharSequence columnName, int columnType) {
        final String name = columnName.toString();
        columnNameIndexMap.put(name, columnNameIndexMap.size());
        columnMetadata.add(new TableColumnMetadata(name, -1L, columnType, false, 0, false, null, columnIndex));
        columnCount++;
    }

    void removeColumn(int columnIndex) {
        final TableColumnMetadata deletedMeta = columnMetadata.getQuick(columnIndex);
        deletedMeta.markDeleted();
        columnNameIndexMap.remove(deletedMeta.getName());
    }

    void openMetaFile(Path path, int pathLen, int liveColumnCount) {
        openSmallFile(ff, path, pathLen, metaMem, META_FILE_NAME, MemoryTag.MMAP_TABLE_WAL_WRITER);
        metaMem.putInt(WalWriter.WAL_FORMAT_VERSION);
        metaMem.putInt(liveColumnCount);
        metaMem.putInt(timestampIndex);
        for (int i = 0; i < columnCount; i++) {
            int type = getColumnType(i);
            if (type > 0) {
                metaMem.putInt(type);
                metaMem.putStr(getColumnName(i));
            }
        }
    }
}
