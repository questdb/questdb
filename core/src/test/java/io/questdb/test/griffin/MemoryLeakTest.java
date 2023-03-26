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

package io.questdb.test.griffin;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.BindVariableService;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractGriffinTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class MemoryLeakTest extends AbstractGriffinTest {
    private static final Log LOG = LogFactory.getLog(MemoryLeakTest.class);

    @Test
    public void testQuestDbForLeaks() throws Exception {
        LOG.info().$("testQuestDbForLeaks").$();
        assertMemoryLeak(() -> {
            int N = 1_000_000;
            populateUsersTable(engine, N);
            final BindVariableService bindVariableService = new BindVariableServiceImpl(configuration);
            bindVariableService.setLong("low", 0L);
            bindVariableService.setLong("high", 0L);
            try (final SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(engine, bindVariableService)) {
                StringSink sink = new StringSink();
                sink.clear();
                sink.put("users");
                sink.put(" where sequence > :low and sequence < :high latest on timestamp partition by id");
                try (RecordCursorFactory rcf = compiler.compile(sink, executionContext).getRecordCursorFactory()) {
                    bindVariableService.setLong("low", 0);
                    bindVariableService.setLong("high", N + 1);
                    Misc.free(rcf.getCursor(executionContext));
                }
            } finally {
                Assert.assertEquals(Unsafe.getMemUsed(), getUsed());
                engine.clear();
                Assert.assertEquals(Unsafe.getMemUsed(), getUsed());
            }
        });
    }

    private long getUsed() {
        long used = 0;
        for (int i = 0; i < MemoryTag.SIZE; i++) {
            used += Unsafe.getMemUsedByTag(i);
        }
        return used;
    }

    private void populateUsersTable(CairoEngine engine, int n) throws SqlException {
        try (
                final SqlCompiler compiler = new SqlCompiler(engine);
                final SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(engine)
        ) {
            compiler.compile("create table users (sequence long, event binary, timestamp timestamp, id long) timestamp(timestamp)", executionContext);
            long buffer = Unsafe.malloc(1024, MemoryTag.NATIVE_DEFAULT);
            try {
                try (TableWriter writer = getWriter("users")) {
                    // time can go backwards if asked too quickly, add I to offset the chance (on mac M1 at least)
                    long baseTimestamp = Os.currentTimeMicros(); // call_j can yield a lower value than call_i thus resulting in an unordered
                    for (int i = 0; i < n; i++) {                // table, so we add i to make sure the timestamps are ordered
                        long sequence = 20 + i * 2L;
                        TableWriter.Row row = writer.newRow(baseTimestamp + i);
                        row.putLong(0, sequence);
                        row.putBin(1, buffer, 1024);
                        row.putLong(3, i);
                        row.append();
                    }
                    writer.commit();
                }
            } finally {
                Unsafe.free(buffer, 1024, MemoryTag.NATIVE_DEFAULT);
            }
        }
    }
}
