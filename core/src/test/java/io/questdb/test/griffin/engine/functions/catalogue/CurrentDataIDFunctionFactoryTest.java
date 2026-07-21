/*+*****************************************************************************
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

package io.questdb.test.griffin.engine.functions.catalogue;

import io.questdb.PropertyKey;
import io.questdb.ServerMain;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.DataID;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.catalogue.CurrentDataIdFunctionFactory;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class CurrentDataIDFunctionFactoryTest extends AbstractBootstrapTest {

    @Test
    public void testCurrentDataID() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final StringSink sink = new StringSink();

            // Test the `current_data_id()` SQL function with a `.data_id` generated at start-up.
            try (
                    ServerMain serverMain = startWithEnvVariables();
                    SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(serverMain.getEngine())
            ) {
                Assert.assertTrue(serverMain.getEngine().getDataID().isInitialized());
                sink.put(serverMain.getEngine().getDataID().get());
                final String expected = sink.toString();
                assertQuery("select current_data_id();")
                        .withEngine(serverMain.getEngine())
                        .withContext(executionContext)
                        .noLeakCheck()
                        .returnsOnce("current_data_id\n" + expected + "\n");
            }

            // Now that we have a full DB, we can remove the data id and make restart it as read-only.
            Assert.assertTrue(java.nio.file.Paths.get(root, "db", DataID.FILENAME).toFile().delete());
            try (
                    ServerMain serverMain = startWithEnvVariables(
                            PropertyKey.READ_ONLY_INSTANCE.getEnvVarName(), "true"
                    );
                    SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(serverMain.getEngine())
            ) {
                sink.clear();

                Assert.assertFalse(serverMain.getEngine().getDataID().isInitialized());
                assertQuery("select current_data_id();")
                        .withEngine(serverMain.getEngine())
                        .withContext(executionContext)
                        .noLeakCheck()
                        .returnsOnce("current_data_id\n\n");
            }
        });
    }

    /**
     * Regression guard for the actual fixed callsite. {@code CurrentDataIdFunctionFactory#newInstance}
     * builds its {@link io.questdb.griffin.engine.functions.constants.UuidConstant} from
     * {@link DataID#getSnapshot()}, which reads the 128-bit id under a single monitor acquisition. The
     * pre-fix code read the pair with two separate calls -- {@code new UuidConstant(id.getLo(), id.getHi())}
     * -- which can tear across a concurrent DataID publish, yielding a constant that is neither the old nor
     * the new id.
     * <p>
     * This hammers the <b>real</b> factory while a writer re-stamps the id via
     * {@link DataID#change(long, long)} (the point-in-time-recovery path), asserting the produced constant
     * is always a whole published value. GREEN with the atomic snapshot; reverting the factory to the two
     * separate accessor calls turns it RED. Unlike the {@code getSnapshot()} tests in {@code DataIDTest},
     * this exercises the exact production read site, so it -- not those -- is what protects the fix.
     */
    @Test
    public void testCurrentDataIdStaysAtomicUnderConcurrentChange() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (
                    ServerMain serverMain = startWithEnvVariables();
                    SqlExecutionContext executionContext = TestUtils.createSqlExecutionCtx(serverMain.getEngine())
            ) {
                final CairoEngine engine = serverMain.getEngine();
                final DataID id = engine.getDataID();
                Assert.assertTrue(id.isInitialized());

                // Two whole values with all four halves distinct, so any torn mix -- (aLo,bHi) or (bLo,aHi)
                // -- equals neither A nor B and is unambiguously detectable.
                final long aLo = 0x1111111111111111L;
                final long aHi = 0x2222222222222222L;
                final long bLo = 0x3333333333333333L;
                final long bHi = 0x4444444444444444L;

                // Establish A as the baseline before the race so the reader only ever legitimately observes
                // A or B, never the random boot id.
                id.change(aLo, aHi);

                final CurrentDataIdFunctionFactory factory = new CurrentDataIdFunctionFactory();
                final ObjList<Function> args = new ObjList<>();
                final IntList argPositions = new IntList();
                final CairoConfiguration configuration = engine.getConfiguration();

                final SOCountDownLatch ready = new SOCountDownLatch(1);
                final AtomicBoolean stop = new AtomicBoolean(false);
                final AtomicBoolean torn = new AtomicBoolean(false);
                final AtomicLong badLo = new AtomicLong();
                final AtomicLong badHi = new AtomicLong();
                final AtomicReference<Throwable> error = new AtomicReference<>();

                // Writer re-stamps the id back and forth, modelling the point-in-time-recovery change() path.
                final Thread writer = new Thread(() -> {
                    try {
                        ready.await();
                        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                        while (System.nanoTime() - deadline < 0 && !torn.get()) {
                            id.change(bLo, bHi);
                            id.change(aLo, aHi);
                        }
                    } catch (Throwable t) {
                        error.set(t);
                    } finally {
                        stop.set(true);
                    }
                }, "dataid-change-writer");

                // Reader hammers the real SQL factory callsite and checks the constant is never torn.
                final Thread reader = new Thread(() -> {
                    try {
                        ready.await();
                        while (!stop.get() && !torn.get()) {
                            final Function fn = factory.newInstance(0, args, argPositions, configuration, executionContext);
                            final long lo = fn.getLong128Lo(null);
                            final long hi = fn.getLong128Hi(null);
                            final boolean isA = lo == aLo && hi == aHi;
                            final boolean isB = lo == bLo && hi == bHi;
                            if (!isA && !isB) {
                                badLo.set(lo);
                                badHi.set(hi);
                                torn.set(true);
                                break;
                            }
                        }
                    } catch (Throwable t) {
                        error.set(t);
                    }
                }, "dataid-current-reader");

                writer.start();
                reader.start();
                ready.countDown(); // release writer and reader together to maximise overlap
                writer.join();
                reader.join();

                if (error.get() != null) {
                    throw new AssertionError(error.get());
                }
                Assert.assertFalse(
                        "current_data_id() factory produced a torn id belonging to neither the old nor the "
                                + "new value: lo=" + badLo.get() + " hi=" + badHi.get(),
                        torn.get());
            }
        });
    }
}