/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.ScalarFunction;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.griffin.FunctionFactory;
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
        return new WaitWFunction(tableName);
    }

    private static class WaitWFunction extends BooleanFunction implements ScalarFunction {
        private final CharSequence tableName;
        private SeqTxnTracker seqTxnTracker;
        private long seqTxn;

        public WaitWFunction(CharSequence tableName) {
            this.tableName = tableName;
        }

        @Override
        public boolean getBool(Record rec) {
            while (seqTxnTracker.getWriterTxn() < seqTxn) {
                Os.sleep(1);
            }
            return true;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            TableToken tt = executionContext.getCairoEngine().verifyTableName(tableName);
            seqTxnTracker = executionContext.getCairoEngine().getTableSequencerAPI().getTxnTracker(tt);
            seqTxn = seqTxnTracker.getSeqTxn();
            super.init(symbolTableSource, executionContext);
        }

        @Override
        public boolean isRuntimeConstant() {
            return true;
        }
    }
}
