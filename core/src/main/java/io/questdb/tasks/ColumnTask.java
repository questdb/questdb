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

package io.questdb.tasks;

import io.questdb.cairo.TableWriter;
import io.questdb.mp.CountDownLatchSPI;

public class ColumnTask {
    private int columnIndex;
    private int columnType;
    private CountDownLatchSPI countDownLatchSPI;
    private long long0;
    private long long1;
    private long long2;
    private long long3;
    private long long4;
    private TableWriter.ColumnTaskHandler taskHandler;
    private long timestampColumnIndex;

    public int getColumnIndex() {
        return columnIndex;
    }

    public int getColumnType() {
        return columnType;
    }

    public CountDownLatchSPI getCountDownLatchSPI() {
        return countDownLatchSPI;
    }

    public long getLong0() {
        return long0;
    }

    public long getLong1() {
        return long1;
    }

    public long getLong2() {
        return long2;
    }

    public long getLong3() {
        return long3;
    }

    public long getLong4() {
        return long4;
    }

    public TableWriter.ColumnTaskHandler getTaskHandler() {
        return taskHandler;
    }

    public long getTimestampColumnIndex() {
        return timestampColumnIndex;
    }

    public void of(
            CountDownLatchSPI countDownLatchSPI,
            int columnIndex,
            int columnType,
            long timestampIndex,
            long long0,
            long long1,
            long long2,
            long long3,
            long long4,
            TableWriter.ColumnTaskHandler taskHandler
    ) {
        this.countDownLatchSPI = countDownLatchSPI;
        this.columnIndex = columnIndex;
        this.columnType = columnType;
        this.timestampColumnIndex = timestampIndex;
        this.long0 = long0;
        this.long1 = long1;
        this.long2 = long2;
        this.long3 = long3;
        this.long4 = long4;
        this.taskHandler = taskHandler;
    }
}
