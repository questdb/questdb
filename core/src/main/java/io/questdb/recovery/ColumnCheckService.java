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

package io.questdb.recovery;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableUtils;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;

/**
 * Validates column files within a partition. For each column, checks that
 * fixed-size data files have the expected size, and that var-size columns
 * have internally consistent aux files (monotonic offsets, valid inlining
 * flags, correct N+1 offset models for string/binary).
 *
 * <p>Aux files are read in chunks to avoid allocating buffers proportional
 * to partition row count. Results are collected into {@link ColumnCheckResult}.
 */
public class ColumnCheckService {
    private static final int CHUNK_ENTRIES = 4096;
    private final FilesFacade ff;

    public ColumnCheckService(FilesFacade ff) {
        this.ff = ff;
    }

    public ColumnCheckResult checkPartition(
            CharSequence tableDir,
            String partitionDirName,
            long partitionTimestamp,
            long partitionRowCount,
            MetaState metaState,
            ColumnVersionState cvState
    ) {
        ObjList<ColumnCheckEntry> entries = new ObjList<>();
        ObjList<MetaColumnState> columns = metaState.getColumns();

        for (int colIdx = 0, n = columns.size(); colIdx < n; colIdx++) {
            MetaColumnState col = columns.getQuick(colIdx);
            int type = col.getType();

            if (type < 0) {
                entries.add(new ColumnCheckEntry(
                        colIdx, col.getName(), col.getTypeName(),
                        ColumnCheckStatus.SKIPPED, "dropped column",
                        -1, -1, -1
                ));
                continue;
            }

            long columnTop = cvState != null ? cvState.getColumnTop(partitionTimestamp, colIdx) : 0;
            long columnNameTxn = cvState != null ? cvState.getColumnNameTxn(partitionTimestamp, colIdx) : -1;

            if (columnTop == -1) {
                entries.add(new ColumnCheckEntry(
                        colIdx, col.getName(), col.getTypeName(),
                        ColumnCheckStatus.SKIPPED, "not in partition",
                        -1, -1, -1
                ));
                continue;
            }

            long effectiveRows = partitionRowCount - columnTop;
            if (effectiveRows <= 0) {
                entries.add(new ColumnCheckEntry(
                        colIdx, col.getName(), col.getTypeName(),
                        ColumnCheckStatus.OK, "no data expected",
                        columnTop, -1, -1
                ));
                continue;
            }

            if (ColumnType.isVarSize(type)) {
                checkVarSizeColumn(tableDir, partitionDirName, col, colIdx, type, effectiveRows, columnTop, columnNameTxn, entries);
            } else {
                checkFixedSizeColumn(tableDir, partitionDirName, col, colIdx, type, effectiveRows, columnTop, columnNameTxn, entries);
            }
        }

        return new ColumnCheckResult(partitionDirName, entries);
    }

    private String checkArrayColumn(long auxFd, long dataSize, long effectiveRows) {
        final int entrySize = 16;
        final long scratchSize = (long) CHUNK_ENTRIES * entrySize;
        final long scratch = Unsafe.malloc(scratchSize, MemoryTag.NATIVE_DEFAULT);
        try {
            long prevEnd = 0;
            long fileOffset = 0;
            long rowsChecked = 0;

            while (rowsChecked < effectiveRows) {
                long remainingRows = effectiveRows - rowsChecked;
                int chunkRows = (int) Math.min(remainingRows, CHUNK_ENTRIES);
                long chunkBytes = (long) chunkRows * entrySize;
                long bytesRead = ff.read(auxFd, scratch, chunkBytes, fileOffset);
                if (bytesRead < chunkBytes) {
                    return "aux file read failed at row " + rowsChecked;
                }

                for (int i = 0; i < chunkRows; i++) {
                    long entryAddr = scratch + (long) i * entrySize;
                    long offset = Unsafe.getUnsafe().getLong(entryAddr) & ((1L << 48) - 1);
                    int size = Unsafe.getUnsafe().getInt(entryAddr + Long.BYTES);
                    long row = rowsChecked + i;

                    if (size == 0) {
                        continue; // null array
                    }
                    if (offset < prevEnd) {
                        return "data offset goes backward at row " + row
                                + " [prev_end=" + prevEnd + ", offset=" + offset + ']';
                    }
                    long end = offset + size;
                    if (end > dataSize) {
                        return "data file too short at row " + row
                                + " [needed=" + end + ", actual=" + dataSize + ']';
                    }
                    prevEnd = end;
                }

                rowsChecked += chunkRows;
                fileOffset += chunkBytes;
            }
            return null; // OK
        } finally {
            Unsafe.free(scratch, scratchSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private void checkFixedSizeColumn(
            CharSequence tableDir,
            String partitionDirName,
            MetaColumnState col,
            int colIdx,
            int type,
            long effectiveRows,
            long columnTop,
            long columnNameTxn,
            ObjList<ColumnCheckEntry> entries
    ) {
        try (Path path = new Path()) {
            path.of(tableDir).slash().concat(partitionDirName).slash();

            TableUtils.dFile(path, col.getName(), columnNameTxn);
            long actualSize = ff.length(path.$());

            if (actualSize < 0) {
                entries.add(new ColumnCheckEntry(
                        colIdx, col.getName(), col.getTypeName(),
                        ColumnCheckStatus.ERROR, "file missing",
                        columnTop, -1, -1
                ));
                return;
            }

            long expectedSize = effectiveRows * ColumnType.sizeOf(type);
            if (actualSize < expectedSize) {
                entries.add(new ColumnCheckEntry(
                        colIdx, col.getName(), col.getTypeName(),
                        ColumnCheckStatus.ERROR,
                        "file too short [expected=" + expectedSize + ", actual=" + actualSize + ']',
                        columnTop, expectedSize, actualSize
                ));
            } else {
                entries.add(new ColumnCheckEntry(
                        colIdx, col.getName(), col.getTypeName(),
                        ColumnCheckStatus.OK, null,
                        columnTop, expectedSize, actualSize
                ));
            }
        }
    }

    private String checkStringColumn(long auxFd, long dataSize, long effectiveRows, boolean isBinary) {
        final int entrySize = 8;
        final long totalEntries = effectiveRows + 1; // N+1 model
        final int minGap = isBinary ? 8 : 4;
        final long scratchSize = (long) CHUNK_ENTRIES * entrySize;
        final long scratch = Unsafe.malloc(scratchSize, MemoryTag.NATIVE_DEFAULT);
        try {
            long prevOffset = -1;
            long fileOffset = 0;
            long entriesChecked = 0;

            while (entriesChecked < totalEntries) {
                long remainingEntries = totalEntries - entriesChecked;
                int chunkEntries = (int) Math.min(remainingEntries, CHUNK_ENTRIES);
                long chunkBytes = (long) chunkEntries * entrySize;
                long bytesRead = ff.read(auxFd, scratch, chunkBytes, fileOffset);
                if (bytesRead < chunkBytes) {
                    return "aux file read failed at entry " + entriesChecked;
                }

                for (int i = 0; i < chunkEntries; i++) {
                    long offset = Unsafe.getUnsafe().getLong(scratch + (long) i * entrySize);
                    long entryIndex = entriesChecked + i;

                    if (entryIndex == 0 && offset != 0) {
                        return "first offset is not zero [offset=" + offset + ']';
                    }
                    if (prevOffset >= 0) {
                        long gap = offset - prevOffset;
                        if (gap < 0) {
                            return "offset goes backward at row " + (entryIndex - 1)
                                    + " [prev=" + prevOffset + ", curr=" + offset + ']';
                        }
                        if (entryIndex <= effectiveRows && gap < minGap) {
                            return "gap too small at row " + (entryIndex - 1)
                                    + " [gap=" + gap + ", min=" + minGap + ']';
                        }
                    }
                    prevOffset = offset;
                }

                entriesChecked += chunkEntries;
                fileOffset += chunkBytes;
            }

            // final boundary check: prevOffset is offset[N]
            if (prevOffset > dataSize) {
                return "data file too short [expected=" + prevOffset + ", actual=" + dataSize + ']';
            }
            return null; // OK
        } finally {
            Unsafe.free(scratch, scratchSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private String checkVarcharColumn(long auxFd, long dataSize, long effectiveRows) {
        final int entrySize = 16;
        final long scratchSize = (long) CHUNK_ENTRIES * entrySize;
        final long scratch = Unsafe.malloc(scratchSize, MemoryTag.NATIVE_DEFAULT);
        try {
            long prevDataEnd = 0;
            long fileOffset = 0;
            long rowsChecked = 0;

            while (rowsChecked < effectiveRows) {
                long remainingRows = effectiveRows - rowsChecked;
                int chunkRows = (int) Math.min(remainingRows, CHUNK_ENTRIES);
                long chunkBytes = (long) chunkRows * entrySize;
                long bytesRead = ff.read(auxFd, scratch, chunkBytes, fileOffset);
                if (bytesRead < chunkBytes) {
                    return "aux file read failed at row " + rowsChecked;
                }

                for (int i = 0; i < chunkRows; i++) {
                    long entryAddr = scratch + (long) i * entrySize;
                    int raw = Unsafe.getUnsafe().getInt(entryAddr);
                    long row = rowsChecked + i;

                    if (raw == 0) {
                        return "invalid aux header at row " + row + " (zero)";
                    }
                    // NULL: no data consumed
                    if ((raw & 4) != 0) {
                        continue;
                    }
                    // INLINED: no data consumed in .d file
                    if ((raw & 1) != 0) {
                        continue;
                    }
                    // non-inlined entry
                    long dataOffset = Unsafe.getUnsafe().getLong(entryAddr + Long.BYTES) >>> 16;
                    int size = (raw >>> 4) & ((1 << 28) - 1);
                    if (dataOffset < prevDataEnd) {
                        return "data offset goes backward at row " + row
                                + " [prev_end=" + prevDataEnd + ", offset=" + dataOffset + ']';
                    }
                    long end = dataOffset + size;
                    if (end > dataSize) {
                        return "data file too short at row " + row
                                + " [needed=" + end + ", actual=" + dataSize + ']';
                    }
                    prevDataEnd = end;
                }

                rowsChecked += chunkRows;
                fileOffset += chunkBytes;
            }
            return null; // OK
        } finally {
            Unsafe.free(scratch, scratchSize, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private void checkVarSizeColumn(
            CharSequence tableDir,
            String partitionDirName,
            MetaColumnState col,
            int colIdx,
            int type,
            long effectiveRows,
            long columnTop,
            long columnNameTxn,
            ObjList<ColumnCheckEntry> entries
    ) {
        try (Path path = new Path()) {
            path.of(tableDir).slash().concat(partitionDirName).slash();
            int pathLen = path.size();

            // check .d file exists
            path.trimTo(pathLen);
            TableUtils.dFile(path, col.getName(), columnNameTxn);
            long dataSize = ff.length(path.$());

            if (dataSize < 0) {
                entries.add(new ColumnCheckEntry(
                        colIdx, col.getName(), col.getTypeName(),
                        ColumnCheckStatus.ERROR, "data file missing",
                        columnTop, -1, -1
                ));
                return;
            }

            // check .i file exists
            path.trimTo(pathLen);
            TableUtils.iFile(path, col.getName(), columnNameTxn);
            long auxSize = ff.length(path.$());

            if (auxSize < 0) {
                entries.add(new ColumnCheckEntry(
                        colIdx, col.getName(), col.getTypeName(),
                        ColumnCheckStatus.ERROR, "aux file missing",
                        columnTop, -1, -1
                ));
                return;
            }

            long expectedAuxSize = computeExpectedAuxSize(type, effectiveRows);
            if (auxSize < expectedAuxSize) {
                entries.add(new ColumnCheckEntry(
                        colIdx, col.getName(), col.getTypeName(),
                        ColumnCheckStatus.ERROR,
                        "aux file too short [expected=" + expectedAuxSize + ", actual=" + auxSize + ']',
                        columnTop, expectedAuxSize, auxSize
                ));
                return;
            }

            // per-row validation: open aux file and dispatch to type-specific check
            long auxFd = ff.openRO(path.$());
            if (auxFd < 0) {
                entries.add(new ColumnCheckEntry(
                        colIdx, col.getName(), col.getTypeName(),
                        ColumnCheckStatus.ERROR, "cannot open aux file",
                        columnTop, -1, -1
                ));
                return;
            }

            try {
                short tag = ColumnType.tagOf(type);
                String error = switch (tag) {
                    case ColumnType.STRING -> checkStringColumn(auxFd, dataSize, effectiveRows, false);
                    case ColumnType.BINARY -> checkStringColumn(auxFd, dataSize, effectiveRows, true);
                    case ColumnType.VARCHAR -> checkVarcharColumn(auxFd, dataSize, effectiveRows);
                    default -> checkArrayColumn(auxFd, dataSize, effectiveRows); // ARRAY
                };

                if (error != null) {
                    entries.add(new ColumnCheckEntry(
                            colIdx, col.getName(), col.getTypeName(),
                            ColumnCheckStatus.ERROR, error,
                            columnTop, -1, dataSize
                    ));
                } else {
                    entries.add(new ColumnCheckEntry(
                            colIdx, col.getName(), col.getTypeName(),
                            ColumnCheckStatus.OK, null,
                            columnTop, expectedAuxSize, auxSize
                    ));
                }
            } finally {
                ff.close(auxFd);
            }
        }
    }

    private static long computeExpectedAuxSize(int type, long effectiveRows) {
        short tag = ColumnType.tagOf(type);
        return switch (tag) {
            case ColumnType.STRING, ColumnType.BINARY -> (effectiveRows + 1) << ColumnType.LEGACY_VAR_SIZE_AUX_SHL;
            case ColumnType.VARCHAR -> effectiveRows << ColumnType.VARCHAR_AUX_SHL;
            default -> effectiveRows * 16L; // ARRAY and other future var-size types: 16 bytes per row
        };
    }
}
