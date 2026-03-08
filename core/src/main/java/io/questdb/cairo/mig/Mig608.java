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
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Rnd;
import io.questdb.std.Vect;
import io.questdb.std.str.Path;

import static io.questdb.cairo.TableUtils.META_FILE_NAME;
import static io.questdb.cairo.TableUtils.TXN_FILE_NAME;

final class Mig608 {
    static void migrate(MigrationContext migrationContext) {
        //  META_COLUMN_DATA_SIZE = 16 -> 32;
        //  TX_OFFSET_MAP_WRITER_COUNT = 72 -> 128

        final FilesFacade ff = migrationContext.getFf();
        final Path path = migrationContext.getTablePath();
        final int plen = path.size();

        path.concat(META_FILE_NAME);
        if (!ff.exists(path.$())) {
            MigrationActions.LOG.error().$("meta file does not exist, nothing to migrate [path=").$(path).I$();
            return;
        }

        // modify metadata
        try (final MemoryMARW rwMem = migrationContext.getRwMemory()) {
            final long thatMetaColumnDataSize = 16;
            final long thisMetaColumnDataSize = 32;

            rwMem.of(ff, path.$(), ff.getPageSize(), ff.length(path.$()), MemoryTag.NATIVE_MIG_MMAP);

            // column count
            final int columnCount = rwMem.getInt(TableUtils.META_OFFSET_COUNT);
            long offset = TableUtils.META_OFFSET_COLUMN_TYPES;
            // 32L here is TableUtils.META_COLUMN_DATA_SIZE at the time of writing this migration
            long newNameOffset = offset + thisMetaColumnDataSize * columnCount;

            // the intent is to resize the _meta file and move the variable length (names) segment
            // to do that we need to work out size of the variable length segment first
            long oldNameOffset = offset + thatMetaColumnDataSize * columnCount;
            long o = oldNameOffset;
            for (int i = 0; i < columnCount; i++) {
                int len = rwMem.getStrLen(o);
                o += Vm.getStorageLength(len);
            }

            final long nameSegmentLen = o - oldNameOffset;

            // resize the file
            rwMem.extend(newNameOffset + nameSegmentLen);
            // move name segment
            Vect.memmove(rwMem.addressOf(newNameOffset), rwMem.addressOf(oldNameOffset), nameSegmentLen);

            // copy column information in reverse order
            o = offset + thatMetaColumnDataSize * (columnCount - 1);
            long o2 = offset + thisMetaColumnDataSize * (columnCount - 1);
            final Rnd rnd = SharedRandom.getRandom(migrationContext.getConfiguration());
            while (o >= offset) {
                rwMem.putInt(o2, rwMem.getInt(o)); // type
                rwMem.putLong(o2 + 4, rwMem.getInt(o + 4)); // flags
                rwMem.putInt(o2 + 12, rwMem.getInt(o + 12)); // index block capacity
                rwMem.putLong(o2 + 20, rnd.nextLong()); // column hash
                o -= thatMetaColumnDataSize;
                o2 -= thisMetaColumnDataSize;
            }
            rwMem.jumpTo(newNameOffset + nameSegmentLen);
        }

        // update _txn file
        path.trimTo(plen).concat(TXN_FILE_NAME);
        if (!ff.exists(path.$())) {
            MigrationActions.LOG.error().$("tx file does not exist, nothing to migrate [path=").$(path).I$();
            return;
        }
        EngineMigration.backupFile(ff, path, migrationContext.getTablePath2(), TXN_FILE_NAME, 422);

        MigrationActions.LOG.debug().$("opening for rw [path=").$(path).I$();
        try (MemoryMARW txMem = migrationContext.createRwMemoryOf(ff, path.$())) {

            // calculate size of the _txn file
            final long thatTxOffsetMapWriterCount = 72;
            final long thisTxOffsetMapWriterCount = 128;
            final int longsPerAttachedPartition = 4;

            int symbolCount = txMem.getInt(thatTxOffsetMapWriterCount);
            int partitionTableSize = txMem.getInt(thatTxOffsetMapWriterCount + 4 + symbolCount * 8L) * 8 * longsPerAttachedPartition;

            // resize existing file:
            // thisTxOffsetMapWriterCount + symbolCount + symbolData + partitionTableEntryCount + partitionTableSize
            long thatSize = thatTxOffsetMapWriterCount + 4 + symbolCount * 8L + 4L + partitionTableSize;
            long thisSize = thisTxOffsetMapWriterCount + 4 + symbolCount * 8L + 4L + partitionTableSize;
            txMem.extend(thisSize);
            txMem.jumpTo(thisSize);
            Vect.memmove(txMem.addressOf(thisTxOffsetMapWriterCount), txMem.addressOf(thatTxOffsetMapWriterCount), thatSize - thatTxOffsetMapWriterCount);

            // zero out reserved area
            Vect.memset(txMem.addressOf(thatTxOffsetMapWriterCount), thisTxOffsetMapWriterCount - thatTxOffsetMapWriterCount, 0);
        }
    }
}
