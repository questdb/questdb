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

package io.questdb.griffin.engine.groupby;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.RecordSinkFactory;
import io.questdb.cairo.Reopenable;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;

public abstract class AbstractSampleByFillRecordCursorFactory extends AbstractSampleByRecordCursorFactory {
    protected final ObjList<GroupByFunction> groupByFunctions;
    // factory keeps a reference but allocation lifecycle is governed by cursor
    protected final Map map;
    protected final RecordSink mapSink;

    public AbstractSampleByFillRecordCursorFactory(
            @Transient @NotNull BytecodeAssembler asm,
            CairoConfiguration configuration,
            RecordCursorFactory base,
            @Transient @NotNull ListColumnFilter listColumnFilter,
            @Transient @NotNull ArrayColumnTypes keyTypes,
            @Transient @NotNull ArrayColumnTypes valueTypes,
            RecordMetadata groupByMetadata,
            ObjList<GroupByFunction> groupByFunctions,
            ObjList<Function> recordFunctions
    ) {
        super(base, groupByMetadata, recordFunctions);
        try {
            this.groupByFunctions = groupByFunctions;
            // sink will be storing record columns to map key
            mapSink = RecordSinkFactory.getInstance(asm, base.getMetadata(), listColumnFilter, configuration);
            // this is the map itself, which we must not forget to free when factory closes
            map = MapFactory.createOrderedMap(configuration, keyTypes, valueTypes);
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        AbstractNoRecordSampleByCursor cursor = null;
        try {
            cursor = getRawCursor();
            if (cursor instanceof Reopenable) {
                ((Reopenable) cursor).reopen();
            }
        } catch (Throwable th) {
            Misc.free(cursor);
            throw th;
        }

        final RecordCursor baseCursor = base.getCursor(executionContext);
        return initFunctionsAndCursor(executionContext, baseCursor);
    }

    @Override
    protected void _close() {
        super._close();
        Misc.free(getRawCursor());
    }
}
