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

package io.questdb.griffin.engine;

import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;

public class SingleValueRecordCursor implements RecordCursor {
    private final Record record;
    private int remaining = 1;

    public SingleValueRecordCursor(Record record) {
        this.record = record;
    }

    @Override
    public void close() {
    }

    @Override
    public Record getRecord() {
        return record;
    }

    @Override
    public Record getRecordB() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasNext() {
        return remaining-- > 0;
    }

    @Override
    public long preComputedStateSize() {
        return 0;
    }

    @Override
    public void recordAt(Record record, long atRowId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long size() {
        return 1;
    }

    @Override
    public void toTop() {
        remaining = 1;
    }
}
