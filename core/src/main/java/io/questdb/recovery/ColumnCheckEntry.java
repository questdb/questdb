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

public final class ColumnCheckEntry {
    private final long actualSize;
    private final int columnIndex;
    private final String columnName;
    private final long columnTop;
    private final String columnTypeName;
    private final long expectedSize;
    private final String message;
    private final ColumnCheckStatus status;

    public ColumnCheckEntry(
            int columnIndex,
            String columnName,
            String columnTypeName,
            ColumnCheckStatus status,
            String message,
            long columnTop,
            long expectedSize,
            long actualSize
    ) {
        this.columnIndex = columnIndex;
        this.columnName = columnName;
        this.columnTypeName = columnTypeName;
        this.status = status;
        this.message = message;
        this.columnTop = columnTop;
        this.expectedSize = expectedSize;
        this.actualSize = actualSize;
    }

    public long getActualSize() {
        return actualSize;
    }

    public int getColumnIndex() {
        return columnIndex;
    }

    public String getColumnName() {
        return columnName;
    }

    public long getColumnTop() {
        return columnTop;
    }

    public String getColumnTypeName() {
        return columnTypeName;
    }

    public long getExpectedSize() {
        return expectedSize;
    }

    public String getMessage() {
        return message;
    }

    public ColumnCheckStatus getStatus() {
        return status;
    }
}
