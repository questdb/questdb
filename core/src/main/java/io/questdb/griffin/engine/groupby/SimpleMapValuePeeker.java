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

package io.questdb.griffin.engine.groupby;

import io.questdb.cairo.sql.Record;

public class SimpleMapValuePeeker {
    private AbstractNoRecordSampleByCursor cursor;
    private final SimpleMapValue currentRecord;
    private final SimpleMapValue nextRecord;
    private boolean nextHasNext = false;
    private long nextLocalEpoch = -1;

    SimpleMapValuePeeker(SimpleMapValue currentRecord, SimpleMapValue nextRecord) {
        this.currentRecord = currentRecord;
        this.nextRecord = nextRecord;
    }

    void setCursor(AbstractNoRecordSampleByCursor cursor) {
        this.cursor = cursor;
    }

    Record peek() {
        final long localEpochTemp = cursor.localEpoch;
        nextHasNext = cursor.notKeyedLoop(nextRecord);
        nextLocalEpoch = cursor.localEpoch;
        cursor.localEpoch = localEpochTemp;
        return nextRecord;
    }

    boolean reset() {
        cursor.localEpoch = nextLocalEpoch;
        currentRecord.copy(nextRecord);
        return nextHasNext;
    }
}
