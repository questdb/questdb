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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.str.Path;

import static io.questdb.cairo.TableUtils.META_FILE_NAME;
import static io.questdb.cairo.TableUtils.TXN_FILE_NAME;

final class Mig614 {
    private static final Log LOG = LogFactory.getLog(EngineMigration.class);
    private static final long META_OFFSET_STRUCTURE_VERSION = 32;
    private static final long TX_OFFSET_STRUCT_VERSION = 40;

    private static void openFileSafe(MemoryMARW metaMem, FilesFacade ff, Path path, long readOffset) {
        long fileLen = ff.length(path.$());

        if (fileLen < 0) {
            throw CairoException.critical(ff.errno()).put("cannot read file length: ").put(path);
        }

        if (fileLen < readOffset + Long.BYTES) {
            throw CairoException.critical(0).put("File length ").put(fileLen).put(" is too small at ").put(path);
        }

        metaMem.of(
                ff,
                path.$(),
                ff.getPageSize(),
                fileLen,
                MemoryTag.NATIVE_MIG_MMAP
        );
    }

    static void migrate(MigrationContext migrationContext) {
        final FilesFacade ff = migrationContext.getFf();
        final Path path = migrationContext.getTablePath();
        final Path path2 = migrationContext.getTablePath2();

        try (MemoryMARW rwMemory = migrationContext.getRwMemory()) {
            path2.concat(TXN_FILE_NAME).$();
            openFileSafe(rwMemory, ff, path2, TX_OFFSET_STRUCT_VERSION);

            // Copy structure version from txn to meta.
            long structureVersion = rwMemory.getLong(TX_OFFSET_STRUCT_VERSION);
            path.concat(META_FILE_NAME).$();
            openFileSafe(rwMemory, ff, path, META_OFFSET_STRUCTURE_VERSION);

            LOG.advisory().$("copying structure version [version=").$(structureVersion)
                    .$(", migration=614, metadata=").$(path)
                    .I$();

            rwMemory.putLong(META_OFFSET_STRUCTURE_VERSION, structureVersion);
        }
    }
}
