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
import io.questdb.cairo.vm.api.MemoryARW;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.Path;

public class Mig630 {

    static void migrate(MigrationContext context) {

        // Update transaction file
        // Before there where 4 longs per partition entry in the partition table,
        // now there are 8. The 5th is the partition mask, whose 64th bits flags
        // it as RO. Prior to version 427 all partitions were RW

        MigrationActions.LOG.info().$("extending partition table on tx file [table=").$(context.getTablePath()).I$();

        final FilesFacade ff = context.getFf();
        final Path path = context.getTablePath();
        final int pathDirLen = path.length();

        // backup current _txn file
        path.concat(TableUtils.TXN_FILE_NAME).$();
        if (!ff.exists(path)) {
            MigrationActions.LOG.error().$("tx file does not exist, nothing to migrate [path=").$(path).I$();
            return;
        }
        EngineMigration.backupFile(ff, path, context.getTablePath2(), TableUtils.TXN_FILE_NAME, 426);

        try (MemoryMARW originalTxnFile = context.createRwMemoryOf(ff, path)) {

            final int symbolColumnCount = originalTxnFile.getInt(TableUtils.TX_OFFSET_MAP_WRITER_COUNT_32);
            final long attachedPartitionSizeOffset = TableUtils.TX_OFFSET_MAP_WRITER_COUNT_32 + (symbolColumnCount * 2 * Integer.BYTES);

            final MemoryARW newTxnFile = context.getTempVirtualMem();
            final long startAppendOffset = newTxnFile.getAppendOffset();
            newTxnFile.jumpTo(0L);

            // from 4 longs/partition in version 426, to 8 longs/partition in version 427
            final int attachedPartitionsSize = originalTxnFile.getInt(attachedPartitionSizeOffset);
            newTxnFile.putInt(attachedPartitionsSize * 2);

            long dstOffset = startAppendOffset + Integer.BYTES;
            for (int i = 0; i < attachedPartitionsSize; i++) {
                newTxnFile.putLong(dstOffset, originalTxnFile.getLong(i));
                dstOffset += Long.BYTES;
                if ((i + 1) % 4 == 0) {
                    newTxnFile.putLong(dstOffset, 0L);
                    dstOffset += Long.BYTES;
                    newTxnFile.putLong(dstOffset, 0L);
                    dstOffset += Long.BYTES;
                    newTxnFile.putLong(dstOffset, 0L);
                    dstOffset += Long.BYTES;
                    newTxnFile.putLong(dstOffset, 0L);
                    dstOffset += Long.BYTES;
                }
            }

            originalTxnFile.jumpTo(attachedPartitionSizeOffset);
            long updateSize = newTxnFile.getAppendOffset() - startAppendOffset;
            for (int i = 0, size = 1; i < size && updateSize > 0; i++) {
                long writeSize = Math.min(updateSize, newTxnFile.getPageSize());
                originalTxnFile.putBlockOfBytes(newTxnFile.getPageAddress(i), writeSize);
                updateSize -= writeSize;
            }
            assert updateSize == 0;
        }
    }
}
