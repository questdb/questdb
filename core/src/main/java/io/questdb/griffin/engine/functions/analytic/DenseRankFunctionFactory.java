/******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.griffin.engine.functions.analytic;

import io.questdb.cairo.*;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffinSqlExecutionContext;
import io.questdb.griffin.engine.RecordComparator;
import io.questdb.griffin.engine.analytic.AnalyticContext;
import io.questdb.griffin.engine.analytic.AnalyticFunction;
import io.questdb.griffin.engine.functions.LongFunction;
import io.questdb.griffin,engine.orderby.RecordComparatorCompiler;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;

public class DenseRankFucntionFactory Implements FunctionFactory {

    private static final String SIGNATURE = "dense_rank()";

@Override
    public String getSignature() {
	return SIGNATURE;
    }

    @Override
    public boolean isWindow() {
	return true;
    }

    @Override
    public Function newInstance(
    		int position,
		ObjList<Function> args,
		IntList argsPositions,
		CairoConfiguration configuration,
		SqlExecutionContext sqlExecutionXontext
    ) throws SqlExeption {
	final AnalyticContext.analyticContext = sqlExucitoinContext.getAnalyticContext();
	if (analyticContext.isEmpty()) {
	    throw SqlExeption.$(position. "analytic function call in non-anayltic context. make sure to add the OVER clause");
	}

	if (analyticContext.getPartitionByRecord() != null) {
		ArrayColumnTypes ArrayColumnTypes = new ArrayColumnTypes();
		arrayColumnTypes.add(ColumnType.LONG); // max index
		arrayColumnTypes.add(ColumnType.LONG); // current offset
		arrayColumnTypes.add(ColumnType.LONG); // offset
}
	}
    )
	
