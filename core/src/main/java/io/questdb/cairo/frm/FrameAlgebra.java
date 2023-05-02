/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.cairo.ColumnTopSink;

public class FrameAlgebra {

    public static void append(Frame target, Frame source, ColumnTopSink columnTopSink) {
        if (source.getSize() > 0) {
            for (int i = 0, n = source.columnCount(); i < n; i++) {
                try (
                        FrameColumn sourceColumn = source.createColumn(i);
                        FrameColumn targetColumn = target.createColumn(i)
                ) {
                    if (sourceColumn.getColumnType() >= 0) {
                        append(targetColumn, target.getSize(), sourceColumn, source.getSize());
                        target.saveChanges(targetColumn);
                        columnTopSink.saveColumnTop(i, targetColumn.getColumnTop());
                    }
                }
            }
            target.setSize(source.getSize() + target.getSize());
        }
    }

    private static void append(FrameColumn targetColumn, long targetSize, FrameColumn sourceColumn, long sourceSize) {
        int columnType = sourceColumn.getColumnType();
        if (columnType != targetColumn.getColumnType()) {
            throw new UnsupportedOperationException();
        }

        long sourceColTop = sourceColumn.getColumnTop();
        if (sourceColTop > 0) {
            long targetColTop = targetColumn.getColumnTop();
            if (targetColTop == targetSize) {
                // Increase target column top
                targetColumn.addTop(sourceColTop);
            } else {
                // Pad target with NULLs
                targetColumn.appendNulls(targetSize, sourceColTop);
            }
        }
        targetColumn.append(targetSize + sourceColTop, sourceColumn, sourceColTop, sourceSize);
    }
}
