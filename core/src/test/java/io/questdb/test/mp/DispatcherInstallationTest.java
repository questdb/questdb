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

package io.questdb.test.mp;

import io.questdb.MessageBus;
import io.questdb.cairo.sql.async.PageFrameReduceDispatcher;
import io.questdb.cairo.sql.async.QueryParallelFiberDispatcher;
import io.questdb.mp.WorkerPoolMode;
import io.questdb.mp.WorkerPoolUtils;
import io.questdb.mp.continuation.FiberRuntime;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;

public class DispatcherInstallationTest extends AbstractCairoTest {

    @Test
    public void testDefaultMessageBusDispatcherContract() throws Exception {
        assertMemoryLeak(() -> {
            final MessageBus messageBus = (MessageBus) Proxy.newProxyInstance(
                    MessageBus.class.getClassLoader(),
                    new Class[]{MessageBus.class},
                    InvocationHandler::invokeDefault
            );
            final FiberRuntime runtime = new FiberRuntime(1);
            try {
                try (
                        QueryParallelFiberDispatcher queryDispatcher = new QueryParallelFiberDispatcher(
                                engine,
                                engine.getMessageBus(),
                                runtime
                        );
                        PageFrameReduceDispatcher pageFrameDispatcher = new PageFrameReduceDispatcher(
                                engine,
                                engine.getMessageBus(),
                                runtime
                        )
                ) {
                    Assert.assertNull(messageBus.getQueryParallelFiberDispatcher());
                    Assert.assertNull(messageBus.getPageFrameReduceDispatcher());

                    messageBus.setQueryParallelFiberDispatcher(null);
                    messageBus.setPageFrameReduceDispatcher(null);

                    final UnsupportedOperationException queryFailure = Assert.assertThrows(
                            UnsupportedOperationException.class,
                            () -> messageBus.setQueryParallelFiberDispatcher(queryDispatcher)
                    );
                    Assert.assertEquals(
                            "query parallel fiber dispatcher is not supported",
                            queryFailure.getMessage()
                    );

                    final UnsupportedOperationException pageFrameFailure = Assert.assertThrows(
                            UnsupportedOperationException.class,
                            () -> messageBus.setPageFrameReduceDispatcher(pageFrameDispatcher)
                    );
                    Assert.assertEquals(
                            "page frame reduce dispatcher is not supported",
                            pageFrameFailure.getMessage()
                    );
                }
            } finally {
                closeRuntime(runtime);
            }
        });
    }

    @Test
    public void testPageFrameDispatcherRejectsDistinctSecondInstallation() throws Exception {
        assertMemoryLeak(() -> {
            Assert.assertTrue(
                    configuration.isSqlParallelFilterEnabled() || configuration.isSqlParallelGroupByEnabled()
            );
            final MessageBus messageBus = engine.getMessageBus();
            final FiberRuntime firstRuntime = new FiberRuntime(1);
            try {
                try (PageFrameReduceDispatcher firstDispatcher = new PageFrameReduceDispatcher(
                        engine,
                        messageBus,
                        firstRuntime
                )) {
                    messageBus.setPageFrameReduceDispatcher(firstDispatcher);
                    messageBus.setPageFrameReduceDispatcher(firstDispatcher);

                    final FiberRuntime poolRuntime;
                    try (TestWorkerPool pool = new TestWorkerPool(1, WorkerPoolMode.FIBER_HOST)) {
                        poolRuntime = pool.getFiberRuntime();
                        final IllegalStateException failure = Assert.assertThrows(
                                IllegalStateException.class,
                                () -> WorkerPoolUtils.setupQueryJobs(pool, engine, true)
                        );
                        Assert.assertEquals(
                                "page frame reduce dispatcher is already configured",
                                failure.getMessage()
                        );
                        Assert.assertSame(firstDispatcher, messageBus.getPageFrameReduceDispatcher());
                        Assert.assertNotNull(messageBus.getQueryParallelFiberDispatcher());
                        Assert.assertEquals(1, poolRuntime.getConfigurationListenerCountForTesting());
                        Assert.assertEquals(1, poolRuntime.getQuiesceListenerCountForTesting());
                    }

                    Assert.assertNull(messageBus.getQueryParallelFiberDispatcher());
                    Assert.assertEquals(0, poolRuntime.getConfigurationListenerCountForTesting());
                    Assert.assertEquals(0, poolRuntime.getQuiesceListenerCountForTesting());
                    Assert.assertSame(firstDispatcher, messageBus.getPageFrameReduceDispatcher());
                }
                Assert.assertNull(messageBus.getPageFrameReduceDispatcher());
            } finally {
                closeRuntime(firstRuntime);
            }
        });
    }

    @Test
    public void testQueryDispatcherRejectsDistinctSecondInstallation() throws Exception {
        assertMemoryLeak(() -> {
            final MessageBus messageBus = engine.getMessageBus();
            final FiberRuntime firstRuntime = new FiberRuntime(1);
            try {
                try (
                        QueryParallelFiberDispatcher firstDispatcher = new QueryParallelFiberDispatcher(
                                engine,
                                messageBus,
                                firstRuntime
                        );
                        TestWorkerPool pool = new TestWorkerPool(1, WorkerPoolMode.FIBER_HOST)
                ) {
                    messageBus.setQueryParallelFiberDispatcher(firstDispatcher);
                    messageBus.setQueryParallelFiberDispatcher(firstDispatcher);

                    final FiberRuntime poolRuntime = pool.getFiberRuntime();
                    final IllegalStateException failure = Assert.assertThrows(
                            IllegalStateException.class,
                            () -> WorkerPoolUtils.setupQueryJobs(pool, engine, true)
                    );
                    Assert.assertEquals(
                            "query parallel fiber dispatcher is already configured",
                            failure.getMessage()
                    );
                    Assert.assertSame(firstDispatcher, messageBus.getQueryParallelFiberDispatcher());
                    Assert.assertEquals(0, poolRuntime.getConfigurationListenerCountForTesting());
                    Assert.assertEquals(0, poolRuntime.getQuiesceListenerCountForTesting());
                }
                Assert.assertNull(messageBus.getQueryParallelFiberDispatcher());
            } finally {
                closeRuntime(firstRuntime);
            }
        });
    }

    private static void closeRuntime(FiberRuntime runtime) {
        runtime.beginQuiesce();
        final long deadline = System.nanoTime() + 5_000_000_000L;
        Assert.assertTrue(runtime.awaitClosed(deadline));
        runtime.closeAfterDrained();
    }
}
