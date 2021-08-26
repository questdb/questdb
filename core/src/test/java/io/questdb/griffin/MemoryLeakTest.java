/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.griffin;

import io.questdb.MessageBusImpl;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cairo.sql.BindVariableService;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import io.questdb.std.Misc;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.std.str.StringSink;
import org.junit.Assert;
import org.junit.Test;

public class MemoryLeakTest extends AbstractGriffinTest {
    @Test
    public void testQuestDbForLeaks() throws Exception {
        assertMemoryLeak(() -> {
            int N = 1_000_000;
            populateUsersTable(engine, N);
            try (SqlCompiler compiler = new SqlCompiler(engine)) {
                final BindVariableService bindVariableService = new BindVariableServiceImpl(configuration);
                bindVariableService.setLong("low", 0L);
                bindVariableService.setLong("high", 0L);
                try (
                        final SqlExecutionContextImpl executionContext = new SqlExecutionContextImpl(
                                engine,
                                1,
                                new MessageBusImpl(configuration)
                        ).with(AllowAllCairoSecurityContext.INSTANCE,
                                bindVariableService,
                                null)
                ) {
                    StringSink sink = new StringSink();
                    sink.clear();
                    sink.put("users");
                    sink.put(" latest by id where sequence > :low and sequence < :high");
                    try (RecordCursorFactory rcf = compiler.compile(sink, executionContext).getRecordCursorFactory()) {
                        bindVariableService.setLong("low", 0);
                        bindVariableService.setLong("high", N + 1);
                        Misc.free(rcf.getCursor(executionContext));
                    }
                }
            } finally {
                engine.clear();
            }
        });
    }

    @Test
    public void testBreakOS() {
        // On MAC M1 at least afaik, two subsequent calls to Os.currentTimeMicros
        // can result in decreasing values, as when time goes backwards. This test
        // is here to showcase this. We let it try to surface the issue, report it
        // and and fail for 5 seconds. So there is a window for this test to actually
        // fail and be flaky. If you run this test locally with repeat until failure,
        // you will eventually get a failure (depending on system architecture).
        final long start = Os.currentTimeMicros();
        final long limit = start + 5_000_000; // 5 sec
        long prev = start;
        boolean broken = false;
        while (true) {
            long curr = Os.currentTimeMicros();
            if (curr < prev) {
                broken = true;
                break;
            }
            if (curr >= limit) {
                break;
            }
            prev = curr;
        }
        Assert.assertTrue(Os.currentTimeMicros() - start >= 5_000_000 || broken);
        if (broken) {
            System.out.printf("BROKE THE OS.currentTimeMicros call%n");
            Assert.fail();
        }
    }

    private void populateUsersTable(CairoEngine engine, int n) throws SqlException {
        try (
                final SqlCompiler compiler = new SqlCompiler(engine);
                final SqlExecutionContextImpl executionContext = new SqlExecutionContextImpl(
                        engine,
                        1,
                        new MessageBusImpl(configuration)).with(
                        AllowAllCairoSecurityContext.INSTANCE,
                        new BindVariableServiceImpl(configuration),
                        null
                )
        ) {
            compiler.compile("create table users (sequence long, event binary, timestamp timestamp, id long) timestamp(timestamp)", executionContext);
            long buffer = Unsafe.malloc(1024);
            try {
                try (TableWriter writer = engine.getWriter(executionContext.getCairoSecurityContext(), "users", "testing")) {
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
                Unsafe.free(buffer, 1024);
            }
        }
    }
}
