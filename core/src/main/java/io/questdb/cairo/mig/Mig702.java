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

class Mig702 {
    public static final long TX_BASE_OFFSET_A_32 = 8;
    public static final long TX_BASE_OFFSET_B_32 = 32;
    public static final long TX_OFFSET_SEQ_TXN_64_MIG = 80;
    public static final long TX_OFFSET_CHECKSUM_32_MIG = TX_OFFSET_SEQ_TXN_64_MIG + 8;
    public static final long TX_OFFSET_LAG_TXN_COUNT_32_MIG = TX_OFFSET_CHECKSUM_32_MIG + 4;
    public static final long TX_OFFSET_LAG_ROW_COUNT_32_MIG = TX_OFFSET_LAG_TXN_COUNT_32_MIG + 4;
    public static final long TX_OFFSET_LAG_MIN_TIMESTAMP_64_MIG = TX_OFFSET_LAG_ROW_COUNT_32_MIG + 4;
    public static final long TX_OFFSET_LAG_MAX_TIMESTAMP_64_MIG = TX_OFFSET_LAG_MIN_TIMESTAMP_64_MIG + 8;
    private static final String TXN_FILE_NAME_MIG = "_txn";
    private static final long TXN_VERSION_OFFSET_MIG = 0;
    private static final long TX_OFFSET_MAP_WRITER_COUNT_MIG = 128;

    private static int getInt(MemoryMARW txMemory, long baseOffset, long txOffsetTransientRowCount64) {
        return txMemory.getInt(baseOffset + txOffsetTransientRowCount64);
    }

    private static long getLong(MemoryMARW txMemory, long baseOffset, long txOffsetTransientRowCount64) {
        return txMemory.getLong(baseOffset + txOffsetTransientRowCount64);
    }

    static int calculateTxnLagChecksum(long txn1, long seqTxn1, int lagRowCount1, long lagMinTimestamp1, long lagMaxTimestamp1, int lagTxnCount1) {
        long checkSum = txn1 +
                seqTxn1 +
                lagRowCount1 +
                lagMinTimestamp1 +
                lagMaxTimestamp1 +
                lagTxnCount1;

        return (int) (checkSum ^ (checkSum >>> 32));
    }

    static void migrate(MigrationContext migrationContext) {
        final FilesFacade ff = migrationContext.getFf();
        final Path path = migrationContext.getTablePath();

        path.concat(TXN_FILE_NAME_MIG).$();

        try (MemoryMARW txMemory = openFileSafe(ff, path, TX_OFFSET_MAP_WRITER_COUNT_MIG + 8)) {
            long version = txMemory.getLong(TXN_VERSION_OFFSET_MIG);
            boolean isA = (version & 1L) == 0L;

            long baseOffset = isA ? txMemory.getInt(TX_BASE_OFFSET_A_32) : txMemory.getInt(TX_BASE_OFFSET_B_32);

            long seqTxn = getLong(txMemory, baseOffset, TX_OFFSET_SEQ_TXN_64_MIG);
            int lagRowCount = getInt(txMemory, baseOffset, TX_OFFSET_LAG_ROW_COUNT_32_MIG);
            long lagMinTimestamp = getLong(txMemory, baseOffset, TX_OFFSET_LAG_MIN_TIMESTAMP_64_MIG);
            long lagMaxTimestamp = getLong(txMemory, baseOffset, TX_OFFSET_LAG_MAX_TIMESTAMP_64_MIG);
            int lagTxnCountRaw = getInt(txMemory, baseOffset, TX_OFFSET_LAG_TXN_COUNT_32_MIG);
            int expectedChecksum = calculateTxnLagChecksum(
                    version,
                    seqTxn,
                    lagRowCount,
                    lagMinTimestamp,
                    lagMaxTimestamp,
                    lagTxnCountRaw
            );
            int actualCheckSum = getInt(txMemory, baseOffset, TX_OFFSET_CHECKSUM_32_MIG);

            if (actualCheckSum != expectedChecksum) {
                EngineMigration.backupFile(
                        ff,
                        path,
                        migrationContext.getTablePath2(),
                        TXN_FILE_NAME_MIG,
                        426
                );

                txMemory.putInt(baseOffset + TX_OFFSET_LAG_TXN_COUNT_32_MIG, 0);
                txMemory.putInt(baseOffset + TX_OFFSET_LAG_ROW_COUNT_32_MIG, 0);
                txMemory.putLong(baseOffset + TX_OFFSET_LAG_MIN_TIMESTAMP_64_MIG, 0);
                txMemory.putLong(baseOffset + TX_OFFSET_LAG_MAX_TIMESTAMP_64_MIG, 0);
                int zeroChecksum = calculateTxnLagChecksum(
                        version,
                        seqTxn,
                        0,
                        0,
                        0,
                        0
                );
                txMemory.putInt(baseOffset + TX_OFFSET_CHECKSUM_32_MIG, zeroChecksum);
            }
        }
    }
}
