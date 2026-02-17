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

import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.Path;

import static io.questdb.cairo.mig.MigrationUtils.openFileSafe;

public class Mig702 {
    private static final Log LOG = LogFactory.getLog(EngineMigration.class);
    private static final String TXN_FILE_NAME_MIG = "_txn";
    private static final long TXN_VERSION_OFFSET_MIG = 0;
    private static final long TX_BASE_OFFSET_A_32 = 8;
    private static final long TX_BASE_OFFSET_B_32 = 32;
    private static final long TX_OFFSET_MAP_WRITER_COUNT_MIG = 128;
    private static final long TX_OFFSET_SEQ_TXN_64_MIG = 80;
    private static final long TX_OFFSET_CHECKSUM_32_MIG = TX_OFFSET_SEQ_TXN_64_MIG + 8;
    private static final long TX_OFFSET_LAG_TXN_COUNT_32_MIG = TX_OFFSET_CHECKSUM_32_MIG + 4;
    private static final long TX_OFFSET_LAG_ROW_COUNT_32_MIG = TX_OFFSET_LAG_TXN_COUNT_32_MIG + 4;
    private static final long TX_OFFSET_LAG_MIN_TIMESTAMP_64_MIG = TX_OFFSET_LAG_ROW_COUNT_32_MIG + 4;
    private static final long TX_OFFSET_LAG_MAX_TIMESTAMP_64_MIG = TX_OFFSET_LAG_MIN_TIMESTAMP_64_MIG + 8;

    public static int calculateTxnLagChecksum(long txn, long seqTxn, int lagRowCount, long lagMinTimestamp, long lagMaxTimestamp, int lagTxnCount) {
        long checkSum = lagMinTimestamp;
        checkSum = checkSum * 31 + lagMaxTimestamp;
        checkSum = checkSum * 31 + txn;
        checkSum = checkSum * 31 + seqTxn;
        checkSum = checkSum * 31 + lagRowCount;
        checkSum = checkSum * 31 + lagTxnCount;
        return (int) (checkSum ^ (checkSum >>> 32));
    }

    private static int getInt(MemoryMARW txMemory, long baseOffset, long txOffsetTransientRowCount64) {
        return txMemory.getInt(baseOffset + txOffsetTransientRowCount64);
    }

    private static long getLong(MemoryMARW txMemory, long baseOffset, long txOffsetTransientRowCount64) {
        return txMemory.getLong(baseOffset + txOffsetTransientRowCount64);
    }

    static void migrate(MigrationContext migrationContext) {
        final FilesFacade ff = migrationContext.getFf();
        final Path path = migrationContext.getTablePath();

        path.concat(TXN_FILE_NAME_MIG);
        if (!ff.exists(path.$())) {
            // no transaction file, nothing to do. Broken table.
            LOG.error().$("7.0.2 migration is skipped for the table, no _txn file exists [path=").$(path).I$();
            return;
        }

        try (MemoryMARW txMemory = openFileSafe(ff, path.$(), TX_BASE_OFFSET_B_32)) {
            long version = txMemory.getLong(TXN_VERSION_OFFSET_MIG);
            boolean isA = (version & 1L) == 0L;

            long baseOffset = isA ? txMemory.getInt(TX_BASE_OFFSET_A_32) : txMemory.getInt(TX_BASE_OFFSET_B_32);

            if (baseOffset + TX_OFFSET_MAP_WRITER_COUNT_MIG <= txMemory.size()) {
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
                    txMemory.putLong(baseOffset + TX_OFFSET_LAG_MIN_TIMESTAMP_64_MIG, Long.MAX_VALUE);
                    txMemory.putLong(baseOffset + TX_OFFSET_LAG_MAX_TIMESTAMP_64_MIG, Long.MIN_VALUE);
                    int zeroChecksum = calculateTxnLagChecksum(
                            version,
                            seqTxn,
                            0,
                            Long.MAX_VALUE,
                            Long.MIN_VALUE,
                            0
                    );
                    txMemory.putInt(baseOffset + TX_OFFSET_CHECKSUM_32_MIG, zeroChecksum);
                }
            } else {
                LOG.error().$("7.0.2 migration is skipped for the table, _txn file is too small [path=")
                        .$(path)
                        .$(", fileLength=").$(txMemory.size())
                        .$(", expectedLength=").$(baseOffset + TX_OFFSET_MAP_WRITER_COUNT_MIG).I$();
            }
        }
    }
}
