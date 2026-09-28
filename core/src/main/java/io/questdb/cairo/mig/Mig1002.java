/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnVersionReader;
import io.questdb.cairo.SymbolMapWriter;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMR;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;

import static io.questdb.cairo.TableUtils.COLUMN_VERSION_FILE_NAME;
import static io.questdb.cairo.TableUtils.META_FILE_NAME;
import static io.questdb.cairo.TableUtils.TXN_FILE_NAME;

/**
 * Sets the symbol map null flag of every SYMBOL column that has column-top rows in at
 * least one partition. Earlier releases left the flag unset when
 * {@code ALTER TABLE ... ALTER COLUMN ... TYPE SYMBOL} converted a column whose partitions
 * carried column tops, and when {@code ATTACH PARTITION} brought in column tops, so the
 * NULL group was invisible to every reader that consults the flag.
 */
public final class Mig1002 {
    private static final Log LOG = LogFactory.getLog(EngineMigration.class);

    public static void migrate(MigrationContext migrationContext) {
        final FilesFacade ff = migrationContext.getFf();
        final Path path = migrationContext.getTablePath();
        final int plen = path.size();
        try {
            if (!ff.exists(path.concat(META_FILE_NAME).$())) {
                LOG.error().$("meta file does not exist, nothing to migrate [path=").$(path).I$();
                return;
            }
            final long metaFileSize = ff.length(path.$());
            try (MemoryCMR metaMem = Vm.getCMRInstance(ff, path.$(), metaFileSize, MemoryTag.NATIVE_MIG_MMAP)) {
                final int columnCount = metaMem.getInt(TableUtils.META_OFFSET_COUNT);
                final int partitionBy = metaMem.getInt(TableUtils.META_OFFSET_PARTITION_BY);
                final int timestampIndex = metaMem.getInt(TableUtils.META_OFFSET_TIMESTAMP_INDEX);
                final int timestampType = timestampIndex > -1 && timestampIndex < columnCount
                        ? TableUtils.getColumnType(metaMem, timestampIndex)
                        : ColumnType.TIMESTAMP;

                if (!ff.exists(path.trimTo(plen).concat(TXN_FILE_NAME).$())) {
                    LOG.error().$("tx file does not exist, nothing to migrate [path=").$(path).I$();
                    return;
                }
                if (!ff.exists(path.trimTo(plen).concat(COLUMN_VERSION_FILE_NAME).$())) {
                    LOG.error().$("column version file does not exist, nothing to migrate [path=").$(path).I$();
                    return;
                }

                try (
                        TxReader txReader = new TxReader(ff).ofRO(path.trimTo(plen).concat(TXN_FILE_NAME).$(), timestampType, partitionBy);
                        ColumnVersionReader cvReader = new ColumnVersionReader().ofRO(ff, path.trimTo(plen).concat(COLUMN_VERSION_FILE_NAME).$())
                ) {
                    if (!txReader.unsafeLoadAll()) {
                        throw CairoException.critical(0).put("migration failed, could not read tx file [path=").put(path).put(']');
                    }
                    cvReader.readUnsafe();

                    long nameOffset = TableUtils.getColumnNameOffset(columnCount);
                    for (int i = 0; i < columnCount; i++) {
                        final CharSequence columnName = metaMem.getStrA(nameOffset);
                        nameOffset += Vm.getStorageLength(columnName);
                        final int columnType = TableUtils.getColumnType(metaMem, i);
                        if (columnType > 0 && ColumnType.isSymbol(columnType) && hasColumnTopRows(txReader, cvReader, i)) {
                            setNullFlag(migrationContext, path.trimTo(plen), columnName, cvReader.getSymbolTableNameTxn(i));
                        }
                    }
                }
            }
        } finally {
            path.trimTo(plen);
        }
    }

    private static boolean hasColumnTopRows(TxReader txReader, ColumnVersionReader cvReader, int columnIndex) {
        for (int i = 0, n = txReader.getPartitionCount(); i < n; i++) {
            if (txReader.getPartitionSize(i) > 0
                    && cvReader.getColumnTop(txReader.getPartitionTimestampByIndex(i), columnIndex) != 0) {
                return true;
            }
        }
        return false;
    }

    private static void setNullFlag(MigrationContext migrationContext, Path path, CharSequence columnName, long columnNameTxn) {
        final FilesFacade ff = migrationContext.getFf();
        TableUtils.offsetFileName(path, columnName, columnNameTxn);
        if (!ff.exists(path.$()) || ff.length(path.$()) < SymbolMapWriter.HEADER_SIZE) {
            LOG.error().$("symbol offset file is missing or too short, skipping [path=").$(path).I$();
            return;
        }
        final long fd = TableUtils.openRW(ff, path.$(), LOG, migrationContext.getConfiguration().getWriterFileOpenOpts());
        try {
            final long flagMem = migrationContext.getTempMemory(Byte.BYTES);
            if (ff.read(fd, flagMem, Byte.BYTES, SymbolMapWriter.HEADER_NULL_FLAG) != Byte.BYTES) {
                throw CairoException.critical(ff.errno()).put("could not read symbol null flag [path=").put(path).put(']');
            }
            if (Unsafe.getByte(flagMem) == 1) {
                return;
            }
            Unsafe.putByte(flagMem, (byte) 1);
            if (ff.write(fd, flagMem, Byte.BYTES, SymbolMapWriter.HEADER_NULL_FLAG) != Byte.BYTES) {
                throw CairoException.critical(ff.errno()).put("could not write symbol null flag [path=").put(path).put(']');
            }
            LOG.info().$("set symbol null flag [path=").$(path).I$();
        } finally {
            ff.close(fd);
        }
    }
}
