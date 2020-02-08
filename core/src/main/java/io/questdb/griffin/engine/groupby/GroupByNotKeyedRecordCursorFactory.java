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

package io.questdb.griffin.engine.groupby;

import io.questdb.cairo.sql.*;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.EmptyTableRecordCursor;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;

public class GroupByNotKeyedRecordCursorFactory implements RecordCursorFactory {

    protected final RecordCursorFactory base;
    private final GroupByNotKeyedRecordCursor cursor;
    private final ObjList<GroupByFunction> groupByFunctions;
    // this sink is used to copy recordKeyMap keys to dataMap
    private final RecordMetadata metadata;
    private final SimpleMapValue simpleMapValue;
    private final VirtualRecord virtualRecordA;
    private final VirtualRecord virtualRecordB;

    public GroupByNotKeyedRecordCursorFactory(
            RecordCursorFactory base,
            RecordMetadata groupByMetadata,
            ObjList<GroupByFunction> groupByFunctions,
            ObjList<Function> recordFunctions,
            int valueCount
    ) {
        this.simpleMapValue = new SimpleMapValue(valueCount);
        this.base = base;
        this.metadata = groupByMetadata;
        this.groupByFunctions = groupByFunctions;
        this.virtualRecordA = new VirtualRecordNoRowid(recordFunctions);
        this.virtualRecordA.of(simpleMapValue);

        this.virtualRecordB = new VirtualRecordNoRowid(recordFunctions);
        this.virtualRecordB.of(simpleMapValue);

        this.cursor = new GroupByNotKeyedRecordCursor();
    }

    @Override
    public void close() {
        Misc.freeObjList(groupByFunctions);
        Misc.free(base);
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) {
        final RecordCursor baseCursor = base.getCursor(executionContext);
        try {
            final Record baseRecord = baseCursor.getRecord();
            final int n = groupByFunctions.size();

            if (baseCursor.hasNext()) {
                GroupByUtils.updateNew(groupByFunctions, n, simpleMapValue, baseRecord);
            } else {
                return EmptyTableRecordCursor.INSTANCE;
            }

            while (baseCursor.hasNext()) {
                GroupByUtils.updateExisting(groupByFunctions, n, simpleMapValue, baseRecord);
            }

            cursor.toTop();
            // init all record function for this cursor, in case functions require metadata and/or symbol tables
            for (int i = 0, m = groupByFunctions.size(); i < m; i++) {
                groupByFunctions.getQuick(i).init(cursor, executionContext);
            }
        } finally {
            Misc.free(baseCursor);
        }
        return cursor;
    }

    @Override
    public RecordMetadata getMetadata() {
        return metadata;
    }

    @Override
    public boolean isRandomAccessCursor() {
        return false;
    }

    private class GroupByNotKeyedRecordCursor implements NoRandomAccessRecordCursor {

        private int recordsRemaining = 1;

        @Override
        public void close() {
        }

        @Override
        public Record getRecord() {
            return virtualRecordA;
        }

        @Override
        public Record getRecordB() {
            return virtualRecordB;
        }

        @Override
        public boolean hasNext() {
            return recordsRemaining-- > 0;
        }

        @Override
        public void toTop() {
            recordsRemaining = 1;
        }

        @Override
        public long size() {
            return 1;
        }
    }
}
