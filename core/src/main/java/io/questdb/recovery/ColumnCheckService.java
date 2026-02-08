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
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;

public class ColumnCheckService {
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
            } else {
                entries.add(new ColumnCheckEntry(
                        colIdx, col.getName(), col.getTypeName(),
                        ColumnCheckStatus.OK, null,
                        columnTop, expectedAuxSize, auxSize
                ));
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
