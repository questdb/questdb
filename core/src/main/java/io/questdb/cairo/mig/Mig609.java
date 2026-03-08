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

import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.vm.MemoryCMARWImpl;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.str.Path;

import static io.questdb.cairo.TableUtils.*;

final class Mig609 {
    private static final Log LOG = LogFactory.getLog(EngineMigration.class);
    private static final long TX_OFFSET_FIXED_ROW_COUNT_505 = 16;
    private static final long TX_OFFSET_MAP_WRITER_COUNT_608 = 128;

    static void migrate(MigrationContext migrationContext) {
        final FilesFacade ff = migrationContext.getFf();
        final Path path = migrationContext.getTablePath();
        final int plen = path.size();

        path.trimTo(plen).concat(META_FILE_NAME);
        try (MemoryMARW metaMem = migrationContext.getRwMemory()) {
            metaMem.of(ff, path.$(), ff.getPageSize(), ff.length(path.$()), MemoryTag.NATIVE_MIG_MMAP);

            // we require partition by value to avoid processing non-partitioned tables
            final int partitionBy = metaMem.getInt(4);

            try (MemoryMARW txMem = new MemoryCMARWImpl(
                    ff,
                    path.trimTo(plen).concat(TXN_FILE_NAME).$(),
                    ff.getPageSize(),
                    ff.length(path.$()),
                    MemoryTag.NATIVE_MIG_MMAP,
                    migrationContext.getConfiguration().getWriterFileOpenOpts()
            )
            ) {
                // this is a variable length file; we need to count of symbol maps before we get to the partition
                // table data
                final int symbolMapCount = txMem.getInt(TX_OFFSET_MAP_WRITER_COUNT_608);
                final long partitionCountOffset = TX_OFFSET_MAP_WRITER_COUNT_608 + 4 + symbolMapCount * 8L;

                // walk only non-active partition to extract sizes
                final int partitionCount = txMem.getInt(partitionCountOffset) / Long.BYTES / LONGS_PER_TX_ATTACHED_PARTITION - 1;


                if (PartitionBy.isPartitioned(partitionBy)) {
                    long calculatedFixedRowCount = 0;
                    for (int partitionIndex = 0; partitionIndex < partitionCount; partitionIndex++) {
                        final long partitionDataOffset = partitionCountOffset + Integer.BYTES + partitionIndex * 8L * LONGS_PER_TX_ATTACHED_PARTITION;
                        // the row count may not be stored in _txn file for the last partition
                        // we need to use transient row count instead
                        calculatedFixedRowCount += txMem.getLong(partitionDataOffset + Long.BYTES);
                    }
                    long currentFixedRowCount = txMem.getLong(TX_OFFSET_FIXED_ROW_COUNT_505);
                    if (currentFixedRowCount != calculatedFixedRowCount) {
                        txMem.putLong(TX_OFFSET_FIXED_ROW_COUNT_505, calculatedFixedRowCount);
                        LOG.info()
                                .$("fixed row count is out [table=").$(path.trimTo(plen).$())
                                .$(", currentFixedRowCount=").$(currentFixedRowCount)
                                .$(", calculatedFixedRowCount=").$(calculatedFixedRowCount)
                                .I$();
                    }
                }
            }
        }
    }
}
