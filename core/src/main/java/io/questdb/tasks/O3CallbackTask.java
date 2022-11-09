/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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
import io.questdb.std.AbstractLockable;

public class O3CallbackTask extends AbstractLockable {
    private int columnIndex;
    private int columnType;
    private CountDownLatchSPI countDownLatchSPI;
    private long mergedTimestampsAddr;
    private long valueCount;
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

    public long getValueCount() {
        return valueCount;
    }

    public TableWriter.O3ColumnUpdateMethod getWriterCallbackMethod() {
        return writerCallbackMethod;
    }

    public void of(
            CountDownLatchSPI countDownLatchSPI,
            int columnIndex,
            int columnType,
            long mergedTimestampsAddr,
            long rowCount,
            TableWriter.O3ColumnUpdateMethod writerCallbackMethod
    ) {
        of(columnIndex);
        this.countDownLatchSPI = countDownLatchSPI;
        this.columnIndex = columnIndex;
        this.columnType = columnType;
        this.mergedTimestampsAddr = mergedTimestampsAddr;
        this.valueCount = rowCount;
        this.writerCallbackMethod = writerCallbackMethod;
    }
}
