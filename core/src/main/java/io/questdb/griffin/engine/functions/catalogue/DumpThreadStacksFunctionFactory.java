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

package io.questdb.griffin.engine.functions.catalogue;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.log.LogRecord;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;

public class DumpThreadStacksFunctionFactory implements FunctionFactory {

    private static final Log LOG = LogFactory.getLog("dump-thread-stacks");

    private static final String SIGNATURE = "dump_thread_stacks()";

    public static void dumpThreadStacks() {
        final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
        final ThreadInfo[] threadInfos = threadMXBean.getThreadInfo(threadMXBean.getAllThreadIds(), 20);
        // Each thread stack on its own LOG message to avoid overrunning log buffer
        // Generally overrun will truncate the log message. We are likely to overrun considering how
        // many threads we could be running
        for (ThreadInfo threadInfo : threadInfos) {
            // it turns out it is possible to have null "infos"
            if (threadInfo != null) {
                final LogRecord record = LOG.advisory();
                try {
                    final Thread.State state = threadInfo.getThreadState();
                    record.$('\n');
                    record.$('\'').$(threadInfo.getThreadName()).$("': ").$(state);
                    final StackTraceElement[] stackTraceElements = threadInfo.getStackTrace();
                    for (final StackTraceElement stackTraceElement : stackTraceElements) {
                        record.$("\n\t\tat ").$(stackTraceElement);
                    }
                    record.$("\n\n");
                } catch (Throwable th) {
                    record.$("error dumping threads: ").$(th);
                } finally {
                    record.$();
                }
            }
        }
    }

    @Override
    public String getSignature() {
        return SIGNATURE;
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) {
        return new DumpThreadStacksFunction();
    }

    private static class DumpThreadStacksFunction extends BooleanFunction {
        @Override
        public boolean getBool(Record rec) {
            dumpThreadStacks();
            return true;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            executionContext.getSecurityContext().authorizeSystemAdmin();
            super.init(symbolTableSource, executionContext);
        }

        @Override
        public boolean isThreadSafe() {
            return true;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(SIGNATURE);
        }
    }
}
