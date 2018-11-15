/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.griffin.engine.groupby;

import com.questdb.cairo.ArrayColumnTypes;
import com.questdb.cairo.CairoConfiguration;
import com.questdb.cairo.ListColumnFilter;
import com.questdb.cairo.RecordSink;
import com.questdb.cairo.map.Map;
import com.questdb.cairo.sql.Function;
import com.questdb.cairo.sql.RecordCursor;
import com.questdb.cairo.sql.RecordCursorFactory;
import com.questdb.griffin.FunctionParser;
import com.questdb.griffin.SqlException;
import com.questdb.griffin.SqlExecutionContext;
import com.questdb.griffin.engine.EmptyTableRecordCursor;
import com.questdb.griffin.engine.functions.GroupByFunction;
import com.questdb.griffin.engine.functions.bind.BindVariableService;
import com.questdb.griffin.model.QueryModel;
import com.questdb.std.BytecodeAssembler;
import com.questdb.std.IntIntHashMap;
import com.questdb.std.ObjList;
import com.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;

public class SampleByFillNoneRecordCursorFactory extends AbstractSampleByRecordCursorFactory {
    private final static SampleByCursorLambda CURSOR_LAMBDA = SampleByFillNoneRecordCursorFactory::createCursor;

    public SampleByFillNoneRecordCursorFactory(
            CairoConfiguration configuration,
            RecordCursorFactory base,
            @NotNull TimestampSampler timestampSampler,
            @Transient @NotNull QueryModel model,
            @Transient @NotNull ListColumnFilter listColumnFilter,
            @Transient @NotNull FunctionParser functionParser,
            @Transient @NotNull SqlExecutionContext executionContext,
            @Transient @NotNull BytecodeAssembler asm,
            @Transient @NotNull ArrayColumnTypes keyTypes,
            @Transient @NotNull ArrayColumnTypes valueTypes
    ) throws SqlException {
        super(
                configuration,
                base,
                timestampSampler,
                model,
                listColumnFilter,
                functionParser,
                executionContext,
                asm,
                CURSOR_LAMBDA,
                keyTypes,
                valueTypes
        );
    }

    @Override
    public RecordCursor getCursor(BindVariableService bindVariableService) {
        final RecordCursor baseCursor = base.getCursor(bindVariableService);
        if (baseCursor.hasNext()) {
            map.clear();
            return initFunctionsAndCursor(bindVariableService, baseCursor);
        }

        baseCursor.close();
        return EmptyTableRecordCursor.INSTANCE;
    }

    @NotNull
    private static SampleByFillNoneRecordCursor createCursor(
            Map map,
            RecordSink mapSink,
            @NotNull TimestampSampler timestampSampler,
            int timestampIndex,
            ObjList<GroupByFunction> groupByFunctions,
            ObjList<Function> recordFunctions,
            IntIntHashMap symbolTableIndex
    ) {
        return new SampleByFillNoneRecordCursor(
                map,
                mapSink,
                groupByFunctions,
                recordFunctions,
                timestampIndex,
                timestampSampler,
                symbolTableIndex
        );
    }
}
