/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
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

package io.questdb.griffin.engine.groupby;

import io.questdb.cairo.RecordSink;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.sql.DelegatingRecordCursor;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.std.IntIntHashMap;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;

@FunctionalInterface
public interface SampleByCursorLambda {
    @NotNull
    DelegatingRecordCursor createCursor(
            Map map,
            RecordSink sink,
            @NotNull TimestampSampler timestampSampler,
            int timestampIndex,
            ObjList<GroupByFunction> groupByFunctions,
            ObjList<Function> recordFunctions,
            IntIntHashMap symbolTableIndex
    ) throws SqlException;
}
