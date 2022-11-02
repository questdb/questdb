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

import io.questdb.cairo.TableUtils;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongList;
import io.questdb.std.str.Path;

import static io.questdb.cairo.TableUtils.*;

public class Mig655 {

    static void migrate(MigrationContext context) {
        // Update transaction file
        // Before there where 4 longs per partition entry in the partition table,
        // now there are 8. The 5th is the partition mask, whose 64th bit flags
        // the partition as RO. Prior to version 427 all partitions were RW

        MigrationActions.LOG.info().$("extending partition table on tx file [table=").$(context.getTablePath()).I$();

        final FilesFacade ff = context.getFf();
        final Path path = context.getTablePath();   // preloaded with tha table's path
        final int pathLen = path.length();
        final Path other = context.getTablePath2(); // tmp path whose content we don't care about
        other.of(path).trimTo(pathLen);

        // backup current _txn file to _txn.v426
        path.concat(TableUtils.TXN_FILE_NAME).$();
        if (!ff.exists(path)) {
            MigrationActions.LOG.error().$("tx file does not exist, nothing to migrate [path=").$(path).I$();
            return;
        }
        EngineMigration.backupFile(ff, path, other, TableUtils.TXN_FILE_NAME, 426);

        // open _txn file for RW
        try (MemoryMARW txnFile = context.createRwMemoryOf(ff, path)) {

            boolean isA = (txnFile.getLong(TX_BASE_OFFSET_VERSION_64) & 1L) == 0L;
            final int baseOffset = txnFile.getInt(isA ? TX_BASE_OFFSET_A_32 : TX_BASE_OFFSET_B_32);
            final int symbolsSize = txnFile.getInt(isA ? TX_BASE_OFFSET_SYMBOLS_SIZE_A_32 : TX_BASE_OFFSET_SYMBOLS_SIZE_B_32);
            final long partitionSegmentSizeOffset = isA ? TX_BASE_OFFSET_PARTITIONS_SIZE_A_32 : TX_BASE_OFFSET_PARTITIONS_SIZE_B_32;
            final int partitionSegmentSize = txnFile.getInt(partitionSegmentSizeOffset);
            if (partitionSegmentSize > 0) {

                final int newPartitionSegmentSize = partitionSegmentSize * 2;

                MigrationActions.LOG.info().$("partition table [table=").$(context.getTablePath()).$(", size=").$(partitionSegmentSize).$(", partitions=").$(partitionSegmentSize / (4 * Long.BYTES)) // version 426 -> 4 longs
                        .I$();

                // read/extend current partition table
                final LongList partitionTable = new LongList();
                final long readOffset = baseOffset + TX_OFFSET_MAP_WRITER_COUNT_32 + Integer.BYTES + symbolsSize + Integer.BYTES;
                for (int i = 0, limit = partitionSegmentSize; i < limit; i += Long.BYTES) { // for each long
                    partitionTable.add(txnFile.getLong(readOffset + i));
                    if ((i + Long.BYTES) % (4 * Long.BYTES) == 0) {
                        partitionTable.add(0L); // mask
                        partitionTable.add(0L); // available0
                        partitionTable.add(0L); // available1
                        partitionTable.add(0L); // available2
                    }
                }

                // overwrite partition table with parched version
                txnFile.putInt(partitionSegmentSizeOffset, newPartitionSegmentSize); // A/B header
                txnFile.jumpTo(readOffset - Integer.BYTES);
                txnFile.putInt(newPartitionSegmentSize);
                for (int i = 0, limit = partitionTable.size(); i < limit; i++) {
                    txnFile.putLong(partitionTable.getQuick(i));
                }
            }
        }
    }
}
