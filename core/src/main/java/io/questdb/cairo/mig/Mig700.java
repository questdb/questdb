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

package io.questdb.cairo.mig;

import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.std.*;
import io.questdb.std.str.Path;

import static io.questdb.cairo.TableUtils.*;

public class Mig700 {
    static void migrate(MigrationContext migrationContext) {
        final FilesFacade ff = migrationContext.getFf();
        final Path path = migrationContext.getTablePath();
        final int pathLen = path.length();

        path.concat(TXN_FILE_NAME).$();
        if (!ff.exists(path)) {
            MigrationActions.LOG.error().$("txn file does not exist, nothing to migrate [path=").$(path).I$();
            return;
        }
        EngineMigration.backupFile(ff, path, migrationContext.getTablePath2().trimTo(pathLen), TXN_FILE_NAME, 426);

        MigrationActions.LOG.debug().$("opening for rw [path=").$(path).I$();
        try (MemoryMARW txMem = migrationContext.createRwMemoryOf(ff, path)) {
            final long version = txMem.getLong(TX_BASE_OFFSET_VERSION_64);

            final boolean isA = (version & 1L) == 0L;
            final int baseOffset = isA ? txMem.getInt(TX_BASE_OFFSET_A_32) : txMem.getInt(TX_BASE_OFFSET_B_32);

            final long txn = txMem.getLong(baseOffset + TX_OFFSET_TXN_64);
            assert version == txn;

            final long absolutAddress = ((MemoryCARW) txMem).getAddress() + baseOffset + TX_OFFSET_SEQ_TXN_64;
            Vect.memcpy(absolutAddress + Long.BYTES, absolutAddress, TX_OFFSET_MAP_WRITER_COUNT_32 - TX_BASE_HEADER_SIZE - 2 * Long.BYTES);
            txMem.putLong(baseOffset + TX_OFFSET_SEQ_TXN_64, -1);
        }
    }
}
