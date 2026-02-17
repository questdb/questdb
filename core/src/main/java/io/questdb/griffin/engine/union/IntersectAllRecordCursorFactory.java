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

package io.questdb.griffin.engine.union;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;

public class IntersectAllRecordCursorFactory extends AbstractSetRecordCursorFactory {

    public IntersectAllRecordCursorFactory(
            CairoConfiguration configuration,
            RecordMetadata metadata,
            RecordCursorFactory factoryA,
            RecordCursorFactory factoryB,
            ObjList<Function> castFunctionsA,
            ObjList<Function> castFunctionsB,
            RecordSink recordSink,
            @Transient @NotNull ColumnTypes mapKeyTypes,
            @Transient @NotNull ColumnTypes mapValueTypes
    ) {
        super(metadata, factoryA, factoryB, castFunctionsA, castFunctionsB);
        try {
            Map map = MapFactory.createOrderedMap(configuration, mapKeyTypes, mapValueTypes);
            if (castFunctionsA == null && castFunctionsB == null) {
                cursor = new IntersectAllRecordCursor(map, recordSink);
            } else {
                assert castFunctionsA != null && castFunctionsB != null;
                cursor = new IntersectAllCastRecordCursor(map, recordSink, castFunctionsA, castFunctionsB);
            }
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    @Override
    public int getScanDirection() {
        return factoryA.getScanDirection();
    }

    @Override
    protected void _close() {
        Misc.free(cursor);
        super._close();
    }

    @Override
    protected CharSequence getOperation() {
        return "Intersect All";
    }

    @Override
    protected boolean isSecondFactoryHashed() {
        return true;
    }
}
