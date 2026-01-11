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

import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.str.Path;

import java.io.Closeable;

import static io.questdb.cairo.TableUtils.*;
import static io.questdb.cairo.wal.WalUtils.TABLE_REGISTRY_NAME_FILE;

public class GrowOnlyTableNameRegistryStore implements Closeable {
    public static final int OPERATION_ADD = 0;
    public static final int OPERATION_REMOVE = -1;
    protected final static long TABLE_NAME_ENTRY_RESERVED_LONGS = 8;
    protected final MemoryMARW tableNameMemory = Vm.getCMARWInstance();
    private final FilesFacade ff;

    public GrowOnlyTableNameRegistryStore(FilesFacade ff) {
        this.ff = ff;
    }

    @Override
    public void close() {
        tableNameMemory.close(false);
    }

    public synchronized void logAddTable(final TableToken tableToken) {
        writeEntry(tableToken, OPERATION_ADD);
        tableNameMemory.sync(false);
    }

    public synchronized void logDropTable(final TableToken tableToken) {
        writeEntry(tableToken, OPERATION_REMOVE);
        tableNameMemory.sync(false);
    }

    public GrowOnlyTableNameRegistryStore of(Path rootPath, int version) {
        rootPath.concat(TABLE_REGISTRY_NAME_FILE).putAscii('.').put(version);
        tableNameMemory.smallFile(ff, rootPath.$(), MemoryTag.MMAP_DEFAULT);
        tableNameMemory.putLong(0);
        return this;
    }

    protected void writeEntry(TableToken tableToken, int operation) {
        tableNameMemory.putInt(operation);
        tableNameMemory.putStr(tableToken.getTableName());
        tableNameMemory.putStr(tableToken.getDirName());
        tableNameMemory.putInt(tableToken.getTableId());
        final int tableType =
                tableToken.isView() ? TABLE_TYPE_VIEW
                        : tableToken.isMatView() ? TABLE_TYPE_MAT
                        : tableToken.isWal() ? TABLE_TYPE_WAL
                        : TABLE_TYPE_NON_WAL;
        tableNameMemory.putInt(tableType);

        if (operation != OPERATION_REMOVE) {
            for (int i = 0; i < TABLE_NAME_ENTRY_RESERVED_LONGS; i++) {
                tableNameMemory.putLong(0);
            }
        }
        tableNameMemory.putLong(0L, tableNameMemory.getAppendOffset());
    }
}
