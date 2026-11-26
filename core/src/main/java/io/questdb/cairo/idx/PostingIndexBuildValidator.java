/*+*****************************************************************************
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

package io.questdb.cairo.idx;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnVersionReader;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMR;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.Nullable;

import static io.questdb.cairo.idx.PostingIndexUtils.*;

/**
 * Read-only validation of partition-local work left by an interrupted ADD INDEX.
 */
public final class PostingIndexBuildValidator {
    private PostingIndexBuildValidator() {
    }

    /**
     * Accepts only the single dense generation that seal() leaves after a complete build. The caller holds the
     * table writer lock and supplies the upcoming ADD INDEX txn, not the last committed txn: the build's files
     * are complete but not yet committed. Check before {@link PostingIndexUtils#resetIndex}, which would discard them.
     * Column/partition versions in the paths identify the source files; the generation must cover the expected
     * physical row extent. A checkpoint can copy .pk without the .pv or covering files it names, so check their
     * published extents too. This reads metadata only, never the indexed column or the encoded postings.
     */
    public static boolean isComplete(
            FilesFacade ff,
            Path path,
            CharSequence columnName,
            long columnNameTxn,
            long expectedTxn,
            long expectedRowCount,
            @Nullable IntList coveringColumns,
            ColumnVersionReader columnVersionReader,
            long partitionTimestamp
    ) {
        final int plen = path.size();
        try {
            final long keySize = ff.length(keyFileName(path, columnName, columnNameTxn));
            if (keySize < KEY_FILE_RESERVED || expectedRowCount <= 0) {
                return false;
            }
            final MemoryCMR keyMem = Vm.getCMRInstance();
            try {
                keyMem.of(ff, path.$(), ff.getMapPageSize(), keySize, MemoryTag.MMAP_INDEX_READER);
                final PostingIndexChainHeader.Snapshot header = new PostingIndexChainHeader.Snapshot();
                if (!PostingIndexChainHeader.readUnderSeqlock(keyMem, header)
                        || !isSupportedFormatVersion(header.formatVersion)) {
                    return false;
                }
                final int coverCount = coveringColumns == null ? 0 : coveringColumns.size();
                final long entryOffset = header.headEntryOffset;
                final int entrySize = PostingIndexChainEntry.entrySize(1, coverCount);
                // Bound every entry read by both the published region and the actual file, before dereferencing.
                if (header.regionBase < KEY_FILE_RESERVED
                        || entryOffset < header.regionBase
                        || header.regionLimit < header.regionBase
                        || header.regionLimit > keySize
                        || entryOffset > header.regionLimit - entrySize
                        || keyMem.getLong(entryOffset + V2_ENTRY_OFFSET_LEN) != entrySize
                        || keyMem.getInt(entryOffset + V2_ENTRY_OFFSET_GEN_COUNT) != 1) {
                    return false;
                }
                final int rawFormat = keyMem.getInt(entryOffset + V2_ENTRY_OFFSET_COVERING_FORMAT);
                if (rawFormat != COVERING_FORMAT_LEGACY
                        && rawFormat != PostingIndexChainEntry.packCoveringFormat(COVERING_FORMAT_DEALIASED, coverCount)) {
                    return false;
                }
                final PostingIndexChainEntry.Snapshot entry = new PostingIndexChainEntry.Snapshot();
                PostingIndexChainEntry.read(keyMem, entryOffset, coverCount, entry);
                final long genOffset = entry.genDirOffset;
                if (entry.txnAtSeal != expectedTxn
                        || entry.maxValue != expectedRowCount - 1
                        || entry.sealTxn < 0
                        || entry.valueMemSize <= 0
                        || entry.keyCount <= 0
                        || keyMem.getInt(genOffset + GEN_DIR_OFFSET_KEY_COUNT) != entry.keyCount
                        || keyMem.getLong(genOffset + GEN_DIR_OFFSET_FILE_OFFSET) != 0
                        || keyMem.getLong(genOffset + GEN_DIR_OFFSET_SIZE) != entry.valueMemSize
                        // setMaxValue() updates the entry header BEFORE the final flush. Only the generation's
                        // own high-water proves that the postings themselves reached the end of the partition.
                        || keyMem.getLong(genOffset + GEN_DIR_OFFSET_MAX_VALUE) != expectedRowCount - 1
                        || ff.length(valueFileName(path.trimTo(plen), columnName, columnNameTxn, entry.sealTxn)) < entry.valueMemSize) {
                    return false;
                }
                if (coverCount == 0) {
                    return true;
                }
                final long infoFd = ff.openRO(coverInfoFileName(path.trimTo(plen), columnName, columnNameTxn));
                if (infoFd < 0) {
                    return false;
                }
                try {
                    if (ff.length(infoFd) < (2L + coverCount) * Integer.BYTES
                            || ff.readNonNegativeInt(infoFd, 0) != COVER_INFO_MAGIC
                            || ff.readNonNegativeInt(infoFd, Integer.BYTES) != coverCount) {
                        return false;
                    }
                    for (int c = 0; c < coverCount; c++) {
                        final int columnIndex = coveringColumns.getQuick(c);
                        // ADD INDEX resolves only live columns, never tombstoned INCLUDE slots.
                        assert columnIndex >= 0;
                        if (ff.readNonNegativeInt(infoFd, (2L + c) * Integer.BYTES) != columnIndex) {
                            return false;
                        }
                        final long endOffset = entry.coverFileEndOffsets.getQuick(c);
                        final long coveredColumnNameTxn = columnVersionReader.getColumnNameTxn(partitionTimestamp, columnIndex);
                        if (endOffset <= PC_HEADER_SIZE || ff.length(coverDataFileName(
                                path.trimTo(plen), columnName, c, columnNameTxn, coveredColumnNameTxn, entry.sealTxn
                        )) < endOffset) {
                            return false;
                        }
                    }
                    return true;
                } finally {
                    ff.close(infoFd);
                }
            } finally {
                keyMem.close();
            }
        } catch (CairoException e) {
            // A failed read-only probe cannot justify skipping work. Let the normal recovery/build path handle it.
            return false;
        } finally {
            path.trimTo(plen);
        }
    }
}
