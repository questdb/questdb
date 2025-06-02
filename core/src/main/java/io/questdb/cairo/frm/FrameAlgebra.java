/*******************************************************************************
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

package io.questdb.cairo.frm;

/**
 * Used for partition squashing in {@link io.questdb.cairo.TableWriter}.
 */
public class FrameAlgebra {

    public static void append(Frame target, Frame source, int commitMode) {
        append(target, source, 0, source.getRowCount(), commitMode);
    }

    public static void append(Frame target, Frame source, long sourceLo, long sourceHi, int commitMode) {
        if (sourceLo < sourceHi) {
            for (int i = 0, n = source.columnCount(); i < n; i++) {
                try (
                        FrameColumn sourceColumn = source.createColumn(i);
                        FrameColumn targetColumn = target.createColumn(i)
                ) {
                    if (sourceColumn.getColumnType() >= 0) {
                        append(targetColumn, target.getRowCount(), sourceColumn, sourceLo, sourceHi, commitMode);
                        target.saveChanges(targetColumn);
                    }
                }
            }
            target.setRowCount(target.getRowCount() + (sourceHi - sourceLo));
        }
    }

    private static void append(FrameColumn targetColumn, long targetRowCount, FrameColumn sourceColumn, long sourceLo, long sourceHi, int commitMode) {
        int columnType = sourceColumn.getColumnType();
        if (columnType != targetColumn.getColumnType()) {
            throw new UnsupportedOperationException();
        }

        final long sourceColumnTop = sourceColumn.getColumnTop();
        final long nullPaddingRowCount = Math.max(0, Math.min(sourceColumnTop, sourceHi) - sourceLo);
        if (nullPaddingRowCount > 0) {
            long targetColTop = targetColumn.getColumnTop();
            if (targetColTop == targetRowCount) {
                // Increase target column top
                targetColumn.addTop(nullPaddingRowCount);
            } else {
                // Pad target with NULLs
                targetColumn.appendNulls(targetRowCount, nullPaddingRowCount, commitMode);
            }
        }

        if (sourceColumnTop < sourceHi) {
            targetColumn.append(
                    targetRowCount + nullPaddingRowCount,
                    sourceColumn,
                    sourceLo + nullPaddingRowCount,
                    sourceHi,
                    commitMode
            );
        }
    }
}
