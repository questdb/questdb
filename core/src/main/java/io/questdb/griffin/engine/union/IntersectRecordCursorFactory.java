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

package io.questdb.griffin.engine.union;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;

public class IntersectRecordCursorFactory implements RecordCursorFactory {
    private final RecordMetadata metadata;
    private final RecordCursorFactory factoryA;
    private final RecordCursorFactory factoryB;
    private final IntersectRecordCursor cursor;
    private final Map map;
    private final ObjList<Function> castFunctionsB;

    public IntersectRecordCursorFactory(
            CairoConfiguration configuration,
            RecordMetadata metadata,
            RecordCursorFactory factoryA,
            RecordCursorFactory factoryB,
            ObjList<Function> castFunctionsA,   // unused, we keep it here to comply with an interface
            ObjList<Function> castFunctionsB,
            RecordSink recordSink,
            ColumnTypes valueTypes
    ) {
        this.metadata = metadata;
        this.factoryA = factoryA;
        this.factoryB = factoryB;
        this.map = MapFactory.createMap(configuration, metadata, valueTypes);
        this.cursor = new IntersectRecordCursor(map, recordSink, castFunctionsB);
        this.castFunctionsB = castFunctionsB;
    }

    @Override
    public void close() {
        Misc.free(factoryA);
        Misc.free(factoryB);
        Misc.free(map);
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        RecordCursor cursorA = null;
        RecordCursor cursorB = null;
        try {
            cursorA = factoryA.getCursor(executionContext);
            cursorB = factoryB.getCursor(executionContext);
            Function.init(castFunctionsB, cursorB, executionContext);
            cursor.of(cursorA, cursorB, executionContext.getCircuitBreaker());
            return cursor;
        } catch (Throwable ex) {
            Misc.free(cursorA);
            Misc.free(cursorB);
            throw ex;
        }
    }

    @Override
    public RecordMetadata getMetadata() {
        return metadata;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return factoryA.recordCursorSupportsRandomAccess();
    }
}
