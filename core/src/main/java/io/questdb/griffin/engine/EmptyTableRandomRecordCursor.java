/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.VirtualRecordNoRowid;
import io.questdb.std.ObjList;

final public class EmptyTableRandomRecordCursor implements RecordCursor {
    public static final EmptyTableRandomRecordCursor INSTANCE = new EmptyTableRandomRecordCursor();

    private final Record recordA;
    private final Record recordB;

    public EmptyTableRandomRecordCursor() {
        ObjList<Function> empty = new ObjList<>();
        recordA = new VirtualRecordNoRowid(empty);
        recordB = new VirtualRecordNoRowid(empty);
    }

    @Override
    public void close() {
    }

    @Override
    public Record getRecord() {
        return recordA;
    }

    @Override
    public Record getRecordB() {
        return recordB;
    }

    @Override
    public void recordAt(Record record, long atRowId) {
    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public void toTop() {
    }

    @Override
    public long size() {
        return 0;
    }
}
