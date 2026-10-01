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

import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.mp.continuation.Fiber;
import io.questdb.mp.continuation.FiberRuntime;
import io.questdb.mp.continuation.FiberRuntimeState;
import io.questdb.mp.continuation.FiberTask;
import io.questdb.mp.continuation.LaunchResult;
import io.questdb.mp.continuation.SuspensionScope;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class SqlExecutionContextFiberRandomTest extends AbstractCairoTest {

    @Test
    public void testConcurrentFibersDoNotShareExecutionContextRandoms() throws Exception {
        assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(2);
            try (SqlExecutionContextImpl executionContext = new SqlExecutionContextImpl(engine, 1)
                    .with(AllowAllSecurityContext.INSTANCE)) {
                final Rnd sharedRandom = SharedRandom.getRandom(configuration);
                final Rnd sharedAsyncRandom = SharedRandom.getAsyncRandom(configuration);
                Assert.assertSame(sharedRandom, executionContext.getRandom());
                Assert.assertSame(sharedAsyncRandom, executionContext.getAsyncRandom());

                final RandomTask first = new RandomTask(executionContext);
                final RandomTask second = new RandomTask(executionContext);
                Assert.assertSame(LaunchResult.LAUNCHED, runtime.launch(first));
                Assert.assertSame(LaunchResult.LAUNCHED, runtime.launch(second));
                Assert.assertEquals(2, runtime.drain(2));

                Assert.assertNotSame(first.random, second.random);
                Assert.assertNotSame(first.asyncRandom, second.asyncRandom);
                Assert.assertNotSame(first.random, first.asyncRandom);

                final Rnd injected = new Rnd(1, 2);
                executionContext.setRandom(injected);
                Assert.assertSame(injected, executionContext.getRandom());
            } finally {
                runtime.beginQuiesce();
                final long deadline = System.nanoTime() + 1_000_000_000L;
                while (runtime.state() != FiberRuntimeState.CLOSED && System.nanoTime() < deadline) {
                    runtime.drain(2);
                }
                Assert.assertTrue(runtime.awaitClosed(deadline));
                runtime.closeAfterDrained();
            }
        });
    }

    @Test
    public void testFiberRandomsUseExecutionContextClocks() throws Exception {
        setCurrentMicros(1_234);
        assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(1);
            try (SqlExecutionContextImpl executionContext = new SqlExecutionContextImpl(engine, 1)
                    .with(AllowAllSecurityContext.INSTANCE)) {
                final RandomTask task = new RandomTask(executionContext);
                Assert.assertSame(LaunchResult.LAUNCHED, runtime.launch(task));
                Assert.assertEquals(1, runtime.drain(1));

                Assert.assertEquals(1_234_000, task.random.getSeed0());
                Assert.assertEquals(1_234, task.random.getSeed1());
                Assert.assertEquals(1_234_000, task.asyncRandom.getSeed0());
                Assert.assertEquals(1_234, task.asyncRandom.getSeed1());
            } finally {
                runtime.beginQuiesce();
                final long deadline = System.nanoTime() + 1_000_000_000L;
                while (runtime.state() != FiberRuntimeState.CLOSED && System.nanoTime() < deadline) {
                    runtime.drain(1);
                }
                Assert.assertTrue(runtime.awaitClosed(deadline));
                runtime.closeAfterDrained();
            }
        });
    }

    @Test
    public void testUnmountedFiberRandomAccessThrows() throws Exception {
        assertMemoryLeak(() -> {
            try (SqlExecutionContextImpl executionContext = new SqlExecutionContextImpl(engine, 1)
                    .with(AllowAllSecurityContext.INSTANCE)) {
                Assert.assertNull(Fiber.current());
                Assert.assertFalse(Fiber.isMounted());

                final SuspensionScope.Mode previousMode = SuspensionScope.enter(SuspensionScope.Mode.FIBER);
                try {
                    final IllegalStateException asyncException = Assert.assertThrows(
                            IllegalStateException.class,
                            executionContext::getAsyncRandom
                    );
                    Assert.assertEquals("fiber async random requires a mounted fiber", asyncException.getMessage());

                    final IllegalStateException exception = Assert.assertThrows(
                            IllegalStateException.class,
                            executionContext::getRandom
                    );
                    Assert.assertEquals("fiber random requires a mounted fiber", exception.getMessage());
                } finally {
                    SuspensionScope.restore(previousMode);
                }
            }
        });
    }

    private static class RandomTask extends FiberTask {
        private final SqlExecutionContextImpl executionContext;
        private Rnd asyncRandom;
        private Rnd random;

        private RandomTask(SqlExecutionContextImpl executionContext) {
            this.executionContext = executionContext;
        }

        @Override
        protected boolean runStep() {
            asyncRandom = executionContext.getAsyncRandom();
            random = executionContext.getRandom();
            return true;
        }
    }
}
