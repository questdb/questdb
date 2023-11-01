/*******************************************************************************
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

package io.questdb.griffin.engine.functions.table;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.CursorFunction;
import io.questdb.griffin.engine.table.SequentialRecordCursorFactory;
import io.questdb.griffin.engine.table.ShowPartitionsRecordCursorFactory;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjHashSet;
import io.questdb.std.ObjList;

public class AllTablePartitionsFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "all_table_partitions()";
    }

    @Override
    public boolean isCursor() {
        return true;
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPos, CairoConfiguration config, SqlExecutionContext context) throws SqlException {
        ObjHashSet<TableToken> tableTokensBucket = new ObjHashSet<>();
        CairoEngine engine = context.getCairoEngine();
        engine.getTableTokens(tableTokensBucket, false);
        ObjList<ShowPartitionsRecordCursorFactory> factories = new ObjList<>();

        for (int i = 0; i < tableTokensBucket.size(); i++) {
            try {
                TableToken tt = tableTokensBucket.get(i);

                ShowPartitionsRecordCursorFactory factory = new ShowPartitionsRecordCursorFactory(tt);
                factories.add(factory);

            } catch (CairoException e) {
                throw SqlException.$(argPos.getQuick(i), e.getFlyweightMessage());
            }
        }
        return new CursorFunction(new SequentialRecordCursorFactory<>(factories)) {
            @Override
            public void close() {
                tableTokensBucket.clear();
                factories.clear();

            }
        };
    }
}
