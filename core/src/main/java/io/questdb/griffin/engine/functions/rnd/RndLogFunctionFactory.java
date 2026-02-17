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

package io.questdb.griffin.engine.functions.rnd;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.functions.constants.BooleanConstant;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.log.LogRecord;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;

public class RndLogFunctionFactory implements FunctionFactory {
    private static final Log LOG = LogFactory.getLog(RndLogFunctionFactory.class);

    @Override
    public String getSignature() {
        return "rnd_log(ld)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        if (configuration.isDevModeEnabled()) {
            return new TestLogFunction(
                    args.getQuick(0).getLong(null),
                    args.getQuick(1).getDouble(null) % 100d
            );
        }
        return BooleanConstant.FALSE;
    }

    private static class TestLogFunction extends BooleanFunction {
        private final double errorRatio; // error log percentage from the total log lines
        private final long totalLogLines;
        private Rnd rnd;

        public TestLogFunction(long totalLogLines, double errorRatio) {
            this.totalLogLines = totalLogLines;
            this.errorRatio = errorRatio;
        }

        @Override
        public boolean getBool(Record rec) {
            for (long l = 0; l < totalLogLines; l++) {
                final LogRecord log = rnd.nextDouble() * 10000 < errorRatio * 100 ? LOG.error() : LOG.info();
                log.$(rnd.nextString(25)).$();
            }
            return true;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            executionContext.getSecurityContext().authorizeSystemAdmin();
            super.init(symbolTableSource, executionContext);
            rnd = executionContext.getRandom();
        }

        @Override
        public boolean isNonDeterministic() {
            return true;
        }

        @Override
        public boolean isRandom() {
            return true;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("rnd_log(").val(totalLogLines).val(',').val(errorRatio).val(')');
        }
    }
}
