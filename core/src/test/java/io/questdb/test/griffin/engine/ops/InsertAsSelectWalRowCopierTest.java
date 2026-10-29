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

package io.questdb.test.griffin.engine.ops;

import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriterAPI;
import io.questdb.cairo.pool.WriterSource;
import io.questdb.cairo.sql.InsertMethod;
import io.questdb.cairo.sql.InsertOperation;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.std.Misc;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicInteger;

public class InsertAsSelectWalRowCopierTest extends AbstractCairoTest {

    @Test
    public void testEligibleWalInsertSelectUsesRowCopier() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE src AS (SELECT x, timestamp_sequence(0, 1000000) ts FROM long_sequence(5)) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE TABLE dst (x LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();

            final AtomicInteger newRowCalls = new AtomicInteger();
            final WriterSource source = new WriterSource() {
                @Override
                public TableWriterAPI getTableWriterAPI(TableToken tableToken, String lockReason) {
                    return counting(engine.getTableWriterAPI(tableToken, lockReason), newRowCalls);
                }

                @Override
                public TableWriterAPI getTableWriterAPI(CharSequence tableName, String lockReason) {
                    return getTableWriterAPI(engine.verifyTableName(tableName), lockReason);
                }
            };
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                final CompiledQuery cq = compiler.compile("INSERT INTO dst SELECT x, ts FROM src", sqlExecutionContext);
                Assert.assertEquals(CompiledQuery.INSERT_AS_SELECT, cq.getType());
                try (InsertOperation insert = cq.popInsertOperation();
                     InsertMethod method = insert.createMethod(sqlExecutionContext, source)) {
                    Assert.assertEquals(5, method.execute(sqlExecutionContext));
                    method.commit();
                    Misc.free(method.popWriter());
                }
            }
            Assert.assertEquals("row copier must write every row of an ordinary WAL table", 5, newRowCalls.get());
            drainWalQueue();
            assertQuery("SELECT count(), sum(x) FROM dst").expectSize().noRandomAccess().returns("count\tsum\n5\t15\n");
        });
    }

    private static TableWriterAPI counting(TableWriterAPI delegate, AtomicInteger newRowCalls) {
        return (TableWriterAPI) Proxy.newProxyInstance(
                TableWriterAPI.class.getClassLoader(),
                new Class[]{TableWriterAPI.class},
                (proxy, method, args) -> {
                    if ("newRow".equals(method.getName())) {
                        newRowCalls.incrementAndGet();
                    }
                    try {
                        return method.invoke(delegate, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                }
        );
    }
}
