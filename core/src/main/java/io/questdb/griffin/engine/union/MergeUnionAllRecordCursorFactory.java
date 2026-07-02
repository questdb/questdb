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

package io.questdb.griffin.engine.union;

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.PlanSink;
import io.questdb.std.ObjList;

public class MergeUnionAllRecordCursorFactory extends AbstractSetRecordCursorFactory {
    private final boolean isAscending;
    private final int timestampIndex;

    public MergeUnionAllRecordCursorFactory(
            RecordMetadata metadata,
            RecordCursorFactory factoryA,
            RecordCursorFactory factoryB,
            ObjList<Function> castFunctionsA,
            ObjList<Function> castFunctionsB,
            int timestampIndex,
            boolean isAscending
    ) {
        super(metadata, factoryA, factoryB, castFunctionsA, castFunctionsB);
        this.isAscending = isAscending;
        this.timestampIndex = timestampIndex;
        try {
            this.cursor = new MergeUnionAllRecordCursor(castFunctionsA, castFunctionsB, timestampIndex, isAscending);
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    @Override
    public boolean followedOrderByAdvice() {
        return true;
    }

    @Override
    public boolean fragmentedSymbolTables() {
        return true;
    }

    @Override
    public int getScanDirection() {
        return isAscending ? SCAN_DIRECTION_FORWARD : SCAN_DIRECTION_BACKWARD;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type(getOperation());
        sink.attr("order").val('[');
        sink.putBaseColumnName(timestampIndex);
        sink.val(isAscending ? " asc]" : " desc]");
        sink.child(factoryA);
        sink.child(factoryB);
    }

    protected CharSequence getOperation() {
        return "Union All Merge";
    }
}
