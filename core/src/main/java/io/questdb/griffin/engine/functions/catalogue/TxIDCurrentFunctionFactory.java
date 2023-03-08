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

package io.questdb.griffin.engine.functions.catalogue;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.LongFunction;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.TestOnly;

import java.util.concurrent.atomic.AtomicLong;

public class TxIDCurrentFunctionFactory implements FunctionFactory {
    private static final AtomicLong PG_TX_ID = new AtomicLong();

    private static final String SIGNATURE = "txid_current()";

    @TestOnly
    public static long getTxID() {
        return PG_TX_ID.get();
    }

    @Override
    public String getSignature() {
        return SIGNATURE;
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        return new LongFunction() {
            @Override
            public long getLong(Record rec) {
                return PG_TX_ID.incrementAndGet();
            }

            @Override
            public void toPlan(PlanSink sink) {
                sink.val(SIGNATURE);
            }
        };
    }
}
