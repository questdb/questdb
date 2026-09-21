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

package io.questdb.test.griffin;

import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.table.ReaderPoolRecordCursorFactory;
import io.questdb.griffin.engine.table.WriterPoolRecordCursorFactory;
import io.questdb.mp.CarrierIdentity;
import io.questdb.mp.continuation.Fiber;
import io.questdb.mp.continuation.FiberRuntime;
import io.questdb.mp.continuation.FiberRuntimeState;
import io.questdb.mp.continuation.FiberTask;
import io.questdb.mp.continuation.LaunchResult;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.HashSet;
import java.util.Set;

@RunWith(Parameterized.class)
public class PoolCursorFiberTest extends AbstractCairoTest {
    private final boolean isReaderPool;

    public PoolCursorFiberTest(boolean isReaderPool) {
        this.isReaderPool = isReaderPool;
    }

    @Parameterized.Parameters(name = "readerPool={0}")
    public static Object[][] parameters() {
        return new Object[][]{{false}, {true}};
    }

    @Test
    public void testInterleavedCursorsOnOneCarrier() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a as (select 1 x)");
            execute("create table b as (select 1 x)");
            execute("create table c as (select 1 x)");
            if (isReaderPool) {
                try (var a = getReader("a"); var b = getReader("b"); var c = getReader("c")) {
                    Assert.assertNotNull(a);
                    Assert.assertNotNull(b);
                    Assert.assertNotNull(c);
                }
            }
            final FiberRuntime runtime = new FiberRuntime(2);
            try (RecordCursorFactory factory = isReaderPool
                    ? new ReaderPoolRecordCursorFactory(engine)
                    : new WriterPoolRecordCursorFactory(engine)) {
                final EnumerationTask first = new EnumerationTask(factory, true);
                final EnumerationTask second = new EnumerationTask(factory, false);
                Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(first));
                Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(second));
                Assert.assertEquals(1, runtime.drain(1));
                Assert.assertFalse(first.isDone());
                Assert.assertEquals(1, first.rows.size());
                Assert.assertEquals(1, runtime.drain(1));
                Assert.assertTrue(second.isDone());
                Assert.assertEquals(1, runtime.drain(1));
                Assert.assertTrue(first.isDone());
                Assert.assertNull(first.failure);
                Assert.assertNull(second.failure);
                Assert.assertEquals(first.carrier, second.carrier);
                Assert.assertNotSame(first.fiber, second.fiber);
                Assert.assertEquals(Set.of("a", "b", "c"), first.rows);
                Assert.assertEquals(Set.of("a", "b", "c"), second.rows);
            } finally {
                runtime.beginQuiesce();
                final long deadline = System.nanoTime() + 5_000_000_000L;
                while (runtime.state() != FiberRuntimeState.CLOSED && System.nanoTime() < deadline) {
                    runtime.drain(64);
                }
                Assert.assertTrue(runtime.awaitClosed(deadline));
                runtime.closeAfterDrained();
            }
        });
    }

    private static final class EnumerationTask extends FiberTask {
        private final RecordCursorFactory factory;
        private final boolean isYieldRequired;
        private final Set<String> rows = new HashSet<>();
        private int carrier;
        private Throwable failure;
        private Fiber fiber;

        private EnumerationTask(RecordCursorFactory factory, boolean isYieldRequired) {
            this.factory = factory;
            this.isYieldRequired = isYieldRequired;
        }

        @Override
        protected void onError(Throwable th) {
            failure = th;
        }

        @Override
        protected boolean runStep() {
            carrier = CarrierIdentity.current();
            fiber = Fiber.current();
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                while (cursor.hasNext()) {
                    Assert.assertTrue(rows.add(cursor.getRecord().getStrA(0).toString()));
                    if (isYieldRequired && rows.size() == 1) {
                        Assert.assertTrue(Fiber.yieldCooperatively());
                        Assert.assertEquals(carrier, CarrierIdentity.current());
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return true;
        }
    }
}
