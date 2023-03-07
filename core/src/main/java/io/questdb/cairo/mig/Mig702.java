/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.Path;

import static io.questdb.cairo.mig.MigrationUtils.openFileSafe;

public class Mig702 {
    public static final long TX_BASE_OFFSET_A_32 = 8;
    public static final long TX_BASE_OFFSET_B_32 = 32;
    private static final String TXN_FILE_NAME_MIG = "_txn";
    private static final long TXN_VERSION_OFFSET_MIG = 0;
    private static final long TX_OFFSET_LAG_MAX_TIMESTAMP_64 = 104;
    private static final long TX_OFFSET_LAG_MIN_TIMESTAMP_64 = 96;
    private static final long TX_OFFSET_LAG_ROW_COUNT_32 = 92;
    private static final long TX_OFFSET_LAG_TXN_COUNT_32 = 88;
    private static final long TX_OFFSET_MAP_WRITER_COUNT_MIG = 128;

    public static void migrate(MigrationContext migrationContext) {
        final FilesFacade ff = migrationContext.getFf();
        final Path path = migrationContext.getTablePath();

        path.concat(TXN_FILE_NAME_MIG).$();
        EngineMigration.backupFile(
                ff,
                path,
                migrationContext.getTablePath2(),
                TXN_FILE_NAME_MIG,
                426
        );

        try (MemoryMARW txMemory = openFileSafe(ff, path, TX_OFFSET_MAP_WRITER_COUNT_MIG + 8)) {
            long version = txMemory.getLong(TXN_VERSION_OFFSET_MIG);
            boolean isA = (version & 1L) == 0L;

            long baseOffset = isA ? txMemory.getInt(TX_BASE_OFFSET_A_32) : txMemory.getInt(TX_BASE_OFFSET_B_32);
            txMemory.putInt(baseOffset + TX_OFFSET_LAG_TXN_COUNT_32, 0);
            txMemory.putInt(baseOffset + TX_OFFSET_LAG_ROW_COUNT_32, 0);
            txMemory.putLong(baseOffset + TX_OFFSET_LAG_MIN_TIMESTAMP_64, 0);
            txMemory.putLong(baseOffset + TX_OFFSET_LAG_MAX_TIMESTAMP_64, 0);
        }
    }
}
