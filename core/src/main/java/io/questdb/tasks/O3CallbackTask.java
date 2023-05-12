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

package io.questdb.tasks;

import io.questdb.cairo.TableWriter;
import io.questdb.mp.CountDownLatchSPI;

public class O3CallbackTask {
    private int columnIndex;
    private int columnType;
    private CountDownLatchSPI countDownLatchSPI;
    private long mergedTimestampsAddr;
    private long row1Count;
    private long row2Hi;
    private long row2Lo;
    private TableWriter.O3ColumnUpdateMethod writerCallbackMethod;

    public int getColumnIndex() {
        return columnIndex;
    }

    public int getColumnType() {
        return columnType;
    }

    public CountDownLatchSPI getCountDownLatchSPI() {
        return countDownLatchSPI;
    }

    public long getMergedTimestampsAddr() {
        return mergedTimestampsAddr;
    }

    public long getRow1Count() {
        return row1Count;
    }

    public long getRow2Hi() {
        return row2Hi;
    }

    public long getRow2Lo() {
        return row2Lo;
    }

    public TableWriter.O3ColumnUpdateMethod getWriterCallbackMethod() {
        return writerCallbackMethod;
    }

    public void of(
            CountDownLatchSPI countDownLatchSPI,
            int columnIndex,
            int columnType,
            long mergedTimestampsAddr,
            long row1Count,
            long row2Lo,
            long row2Hi,
            TableWriter.O3ColumnUpdateMethod writerCallbackMethod
    ) {
        this.countDownLatchSPI = countDownLatchSPI;
        this.columnIndex = columnIndex;
        this.columnType = columnType;
        this.mergedTimestampsAddr = mergedTimestampsAddr;
        this.row1Count = row1Count;
        this.row2Lo = row2Lo;
        this.row2Hi = row2Hi;
        this.writerCallbackMethod = writerCallbackMethod;
    }
}
