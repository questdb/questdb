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

package io.questdb.griffin.engine.groupby;

import io.questdb.cairo.*;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.SqlCodeGenerator;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.groupby.vect.CountVectorAggregateFunction;
import io.questdb.griffin.engine.groupby.vect.GroupByRecordCursorFactory;
import io.questdb.griffin.engine.groupby.vect.VectorAggregateFunction;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;

public class DistinctKeyRecordCursorFactory extends AbstractRecordCursorFactory {
    private static final TableColumnMetadata COUNT_COLUMN_META = new TableColumnMetadata("count", 1, ColumnType.LONG);

    private final GroupByRecordCursorFactory baseAggregatorFactory;

    public DistinctKeyRecordCursorFactory(
            CairoConfiguration configuration,
            RecordCursorFactory base,
            RecordMetadata metadata,
            @Transient ArrayColumnTypes columnTypes,
            @Transient ObjList<VectorAggregateFunction> vafList,
            int workerCount,
            @Transient IntList symbolTableSkewIndex) {

        super(metadata);

        GenericRecordMetadata internalMeta = new GenericRecordMetadata();
        GenericRecordMetadata.copyColumns(metadata, internalMeta);
        internalMeta.add(COUNT_COLUMN_META);

        columnTypes.clear();
        columnTypes.add(metadata.getColumnType(0));

        vafList.clear();
        CountVectorAggregateFunction countFunction = new CountVectorAggregateFunction(SqlCodeGenerator.GKK_VANILLA_INT);
        countFunction.pushValueTypes(columnTypes);
        vafList.add(countFunction);

        symbolTableSkewIndex.clear();
        symbolTableSkewIndex.add(0);

        this.baseAggregatorFactory = new GroupByRecordCursorFactory(
                configuration,
                base,
                internalMeta,
                columnTypes,
                workerCount,
                vafList,
                0,
                0,
                symbolTableSkewIndex
        );
    }

    @Override
    protected void _close() {
        this.baseAggregatorFactory.close();
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        return this.baseAggregatorFactory.getCursor(executionContext);
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return this.baseAggregatorFactory.recordCursorSupportsRandomAccess();
    }
}
