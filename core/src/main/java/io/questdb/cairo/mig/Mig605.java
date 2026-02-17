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

package io.questdb.cairo.mig;

import io.questdb.cairo.TableUtils;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.str.Path;

import static io.questdb.cairo.TableUtils.META_FILE_NAME;

final class Mig605 {
    static void migrate(MigrationContext migrationContext) {
        MigrationActions.LOG.info().$("updating column type IDs [table=").$(migrationContext.getTablePath()).I$();
        final FilesFacade ff = migrationContext.getFf();
        Path path = migrationContext.getTablePath();
        path.concat(META_FILE_NAME);

        if (!ff.exists(path.$())) {
            MigrationActions.LOG.error().$("meta file does not exist, nothing to migrate [path=").$(path).I$();
            return;
        }

        // Metadata file should already be backed up
        try (final MemoryMARW rwMem = migrationContext.getRwMemory()) {
            rwMem.of(ff, path.$(), ff.getPageSize(), ff.length(path.$()), MemoryTag.NATIVE_MIG_MMAP);

            // column count
            final int columnCount = rwMem.getInt(TableUtils.META_OFFSET_COUNT);

            long offset = TableUtils.META_OFFSET_COLUMN_TYPES;
            for (int i = 0; i < columnCount; i++) {
                final byte oldTypeId = rwMem.getByte(offset);
                final long oldFlags = rwMem.getLong(offset + 1);
                final int blockCapacity = rwMem.getInt(offset + 1 + 8);
                // column type id is int now
                // we grabbed 3 reserved bytes for extra type info
                // extra for old types is zeros
                rwMem.putInt(offset, oldTypeId == 13 ? 18 : oldTypeId + 1); // ColumnType.VERSION_420 - ColumnType.VERSION_419 = 1 except for BINARY, old 13 new 18
                rwMem.putLong(offset + 4, oldFlags);
                rwMem.putInt(offset + 4 + 8, blockCapacity);
                offset += 16; // old TableUtils.META_COLUMN_DATA_SIZE;
            }
        }
    }
}
