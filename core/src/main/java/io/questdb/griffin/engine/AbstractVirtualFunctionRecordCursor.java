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
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;

public abstract class AbstractVirtualFunctionRecordCursor implements RecordCursor {
    protected final VirtualRecord record;
    protected RecordCursor baseCursor;

    public AbstractVirtualFunctionRecordCursor(ObjList<Function> functions) {
        this.record = new VirtualRecord(functions);
    }

    @Override
    public void close() {
        Misc.free(baseCursor);
    }

    @Override
    public Record getRecord() {
        return record;
    }

    @Override
    public boolean hasNext() {
        return baseCursor.hasNext();
    }

    @Override
    public long size() {
        return baseCursor.size();
    }

    @Override
    public Record newRecord() {
        final VirtualRecord record = new VirtualRecord(this.record.getFunctions());
        record.of(baseCursor.newRecord());
        return record;
    }

    @Override
    public void link(Record record) {
        baseCursor.link(((VirtualRecord) record).getBaseRecord());
    }

    @Override
    public void recordAt(Record record, long atRowId) {
        baseCursor.recordAt(((VirtualRecord) record).getBaseRecord(), atRowId);
    }

    @Override
    public void toTop() {
        baseCursor.toTop();
    }

    public void of(RecordCursor cursor) {
        this.baseCursor = cursor;
        record.of(baseCursor.getRecord());
    }
}
