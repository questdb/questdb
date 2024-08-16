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
        if (source.getRowCount() > 0) {
            for (int i = 0, n = source.columnCount(); i < n; i++) {
                try (
                        FrameColumn sourceColumn = source.createColumn(i);
                        FrameColumn targetColumn = target.createColumn(i)
                ) {
                    if (sourceColumn.getColumnType() >= 0) {
                        append(targetColumn, target.getRowCount(), sourceColumn, source.getRowCount(), commitMode);
                        target.saveChanges(targetColumn);
                    }
                }
            }
            target.setRowCount(source.getRowCount() + target.getRowCount());
        }
    }

    private static void append(FrameColumn targetColumn, long targetRowCount, FrameColumn sourceColumn, long sourceRowCount, int commitMode) {
        int columnType = sourceColumn.getColumnType();
        if (columnType != targetColumn.getColumnType()) {
            throw new UnsupportedOperationException();
        }

        final long sourceColumnTop = sourceColumn.getColumnTop();
        if (sourceColumnTop > 0) {
            long targetColTop = targetColumn.getColumnTop();
            if (targetColTop == targetRowCount) {
                // Increase target column top
                targetColumn.addTop(sourceColumnTop);
            } else {
                // Pad target with NULLs
                targetColumn.appendNulls(targetRowCount, sourceColumnTop, commitMode);
            }
        }
        targetColumn.append(targetRowCount + sourceColumnTop, sourceColumn, sourceColumnTop, sourceRowCount, commitMode);
    }
}
