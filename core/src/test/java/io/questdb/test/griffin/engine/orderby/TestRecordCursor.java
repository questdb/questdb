/*+*****************************************************************************
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

package io.questdb.test.griffin.engine.orderby;

import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.std.LongList;

/**
 * Serves a fixed list of long values as {@link TestRecord}s, addressable by row id.
 */
class TestRecordCursor implements RecordCursor {
    final Record left = new TestRecord();
    int position = -1;
    final Record right = new TestRecord();
    final LongList values = new LongList();

    TestRecordCursor(long... newValues) {
        for (int i = 0; i < newValues.length; i++) {
            this.values.add(newValues[i]);
        }
    }

    @Override
    public void close() {
        // nothing to do here
    }

    @Override
    public Record getRecord() {
        return left;
    }

    @Override
    public Record getRecordB() {
        return right;
    }

    @Override
    public boolean hasNext() {
        if (position < values.size() - 1) {
            position++;
            recordAt(left, position);
            return true;
        }

        return false;
    }

    @Override
    public long preComputedStateSize() {
        return 0;
    }

    @Override
    public void recordAt(Record record, long atRowId) {
        ((TestRecord) record).value = values.get((int) atRowId);
        ((TestRecord) record).position = atRowId;
    }

    public void recordAtValue(Record record, long value) {
        for (int i = 0; i < values.size(); i++) {
            if (values.get(i) == value) {
                recordAt(record, i);
                return;
            }
        }

        throw new RuntimeException("Can't find value " + value + " in " + values);
    }

    @Override
    public long size() {
        return values.size();
    }

    @Override
    public void toTop() {
        // hasNext() pre-increments, so the pre-iteration position is -1, not 0. Resetting to 0
        // would skip the first element on a second pass.
        position = -1;
    }
}
