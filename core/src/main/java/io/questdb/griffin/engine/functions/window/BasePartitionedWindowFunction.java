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

package io.questdb.griffin.engine.functions.window;

import io.questdb.cairo.RecordSink;
import io.questdb.cairo.Reopenable;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Misc;

public abstract class BasePartitionedWindowFunction extends BaseWindowFunction implements Reopenable {
    protected final Map map;
    protected final VirtualRecord partitionByRecord;
    protected final RecordSink partitionBySink;

    public BasePartitionedWindowFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg) {
        super(arg);
        this.map = map;
        this.partitionByRecord = partitionByRecord;
        this.partitionBySink = partitionBySink;
    }

    @Override
    public void close() {
        super.close();
        Misc.free(map);
        Misc.freeObjList(partitionByRecord.getFunctions());
    }

    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        super.init(symbolTableSource, executionContext);
        Function.init(partitionByRecord.getFunctions(), symbolTableSource, executionContext, null);
    }

    @Override
    public void reopen() {
        if (map != null) {
            map.reopen();
        }
    }

    @Override
    public void reset() {
        Misc.free(map);
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.val(getName());
        if (arg != null) {
            sink.val('(').val(arg).val(')');
        } else {
            sink.val("(*)");
        }
        if (isIgnoreNulls()) {
            sink.val(" ignore nulls");
        }
        sink.val(" over (");
        sink.val("partition by ");
        sink.val(partitionByRecord.getFunctions());
        sink.val(')');
    }

    @Override
    public void toTop() {
        super.toTop();
        Misc.clear(map);
    }
}
