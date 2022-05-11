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

package io.questdb.griffin.engine.functions.catalogue;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
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

    @Override
    public String getSignature() {
        return "dump_thread_stacks()";
    }

    @Override
    public Function newInstance(int position,
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
            final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
            final ThreadInfo[] threadInfos = threadMXBean.getThreadInfo(threadMXBean.getAllThreadIds(), 20);
            // Each thread stack on its own LOG message to avoid overrunning log buffer
            // Generally overrun will truncate the log message. We are likely to overrun considering how
            // many threads we could be running
            for (ThreadInfo threadInfo : threadInfos) {
                final LogRecord record = LOG.advisory();
                final Thread.State state = threadInfo.getThreadState();
                record.$('\n');
                record.$('\'').$(threadInfo.getThreadName()).$("': ").$(state);
                final StackTraceElement[] stackTraceElements = threadInfo.getStackTrace();
                for (final StackTraceElement stackTraceElement : stackTraceElements) {
                    record.$("\n\t\tat ").$(stackTraceElement);
                }
                record.$("\n\n");
                record.$();
            }
            return true;
        }

        @Override
        public boolean isStateless() {
            return true;
        }
    }
}
