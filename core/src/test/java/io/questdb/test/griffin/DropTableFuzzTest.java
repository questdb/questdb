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

package io.questdb.test.griffin;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.OperationFuture;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.ops.Operation;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

public class DropTableFuzzTest extends AbstractCairoTest {
    private final Rnd rnd = TestUtils.generateRandom(LOG);

    @Test
    public void testDropTable() throws Exception {
        assertMemoryLeak(() -> {
            String dropDdl = "DROP TABLE " + rndCase();
            execute("CREATE TABLE tango (ts TIMESTAMP)");
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                CompiledQuery dropQuery = compiler.compile(dropDdl, sqlExecutionContext);
                boolean tableRenamed = rnd.nextBoolean();
                if (tableRenamed) {
                    execute("RENAME TABLE tango TO samba");
                }
                try (Operation op = dropQuery.getOperation()) {
                    try (OperationFuture fut = op.execute(sqlExecutionContext, null)) {
                        fut.await();
                        assertFalse("Table renamed, yet DROP operation didn't fail", tableRenamed);
                    } catch (SqlException e) {
                        TestUtils.assertContains(e.getMessage(), "[11] table does not exist");
                    }
                    try (OperationFuture fut = op.execute(sqlExecutionContext, null)) {
                        fut.await();
                        fail("Table is already dropped, yet repeating DROP succeeded");
                    } catch (SqlException e) {
                        TestUtils.assertContains(e.getMessage(), "[11] table does not exist");
                    }
                    execute("CREATE TABLE tango (ts TIMESTAMP)");
                    try (OperationFuture fut = op.execute(sqlExecutionContext, null)) {
                        fut.await();
                    }
                }
            }
        });
    }

    @Test
    public void testDropTableDoesNotReturnErrorThatTableIsDropped() throws Exception {
        assertMemoryLeak(() -> {
            for (int c = 0; c < 100; c++) {
                execute("CREATE TABLE tango (ts TIMESTAMP) timestamp(ts) PARTITION BY DAY WAL");

                final int dropThreads = 5;
                CyclicBarrier dropsStart = new CyclicBarrier(dropThreads);
                AtomicReference<Throwable> exception = new AtomicReference<>();

                ObjList<Thread> threads = new ObjList<>();
                for (int i = 0; i < dropThreads; i++) {
                    threads.add(new Thread(() -> {
                        try (SqlExecutionContextImpl sqlExecutionContext = new SqlExecutionContextImpl(engine, 1)) {
                            sqlExecutionContext.with(AllowAllSecurityContext.INSTANCE);
                            dropsStart.await();
                            engine.execute("DROP TABLE tango;", sqlExecutionContext);
                        } catch (CairoException ex) {
                            if (!ex.isTableDoesNotExist()) {
                                exception.set(ex);
                            }
                        } catch (SqlException ex) {
                            if (!ex.isTableDoesNotExist()) {
                                exception.set(ex);
                            }
                        } catch (Throwable e) {
                            exception.set(e);
                        }
                    }));
                    threads.getLast().start();
                }

                for (int i = 0; i < dropThreads; i++) {
                    threads.get(i).join();
                }

                if (exception.get() != null) {
                    throw new RuntimeException(exception.get());
                }
            }
        });
    }

    @Test
    public void testDropTableIfExistsDoesNotFailWhenTableDoesNotExist() throws Exception {
        assertMemoryLeak(() -> {
            for (int c = 0; c < 10; c++) {
                execute("CREATE TABLE tango (ts TIMESTAMP) timestamp(ts) PARTITION BY DAY WAL");

                final int dropThreads = 5;
                CyclicBarrier dropsStart = new CyclicBarrier(dropThreads);
                AtomicReference<Throwable> exception = new AtomicReference<>();

                ObjList<Thread> threads = new ObjList<>();
                for (int i = 0; i < dropThreads; i++) {
                    threads.add(new Thread(() -> {
                        try (SqlExecutionContextImpl sqlExecutionContext = new SqlExecutionContextImpl(engine, 1)) {
                            sqlExecutionContext.with(AllowAllSecurityContext.INSTANCE);
                            dropsStart.await();
                            engine.execute("DROP TABLE IF EXISTS tango;", sqlExecutionContext);
                        } catch (Throwable e) {
                            exception.set(e);
                        }
                    }));
                    threads.getLast().start();
                }

                for (int i = 0; i < dropThreads; i++) {
                    threads.get(i).join();
                }

                if (exception.get() != null) {
                    throw new RuntimeException(exception.get());
                }
            }
        });
    }

    private String rndCase() {
        final String word = "tango";
        StringBuilder result = new StringBuilder(word.length());
        for (int i = 0; i < word.length(); i++) {
            char c = word.charAt(i);
            if (c <= 'z' && Character.isLetter(c) && rnd.nextBoolean()) {
                c = (char) (c ^ 32); // flips the case
            }
            result.append(c);
        }
        return result.toString();
    }
}
