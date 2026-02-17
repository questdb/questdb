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

package io.questdb.griffin.engine.functions.table;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.Os;

public class WaitWalTableFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "wait_wal_table(s)";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        final CharSequence tableName = args.getQuick(0).getStrA(null);
        return new WaitWalFunction(tableName);
    }

    private static class WaitWalFunction extends BooleanFunction implements Function {
        private final CharSequence tableName;
        private SqlExecutionContext executionContext;
        private long seqTxn;
        private SeqTxnTracker seqTxnTracker;

        public WaitWalFunction(CharSequence tableName) {
            this.tableName = tableName;
        }

        @Override
        public boolean getBool(Record rec) {
            if (seqTxnTracker != null) {
                for (int i = 0; seqTxnTracker.getWriterTxn() < seqTxn; i++) {
                    Os.sleep(1);
                    executionContext.getCircuitBreaker().statefulThrowExceptionIfTripped();
                    if (i % 1000 == 0 && seqTxnTracker.isSuspended()) {
                        throw CairoException.nonCritical().put("table is suspended [tableName=").put(tableName).put("]");
                    }
                }
            }
            return true;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            TableToken tt = executionContext.getCairoEngine().verifyTableName(tableName);
            if (tt.isWal()) {
                seqTxnTracker = executionContext.getCairoEngine().getTableSequencerAPI().getTxnTracker(tt);
                seqTxn = seqTxnTracker.getSeqTxn();
                this.executionContext = executionContext;
            } else {
                seqTxnTracker = null;
                this.executionContext = null;
            }
            super.init(symbolTableSource, executionContext);
        }

        @Override
        public boolean isRuntimeConstant() {
            return true;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("wait_wal_table(").val(tableName).val(')');
        }
    }
}
