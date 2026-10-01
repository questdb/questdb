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

package io.questdb.test.cairo.sql.async;

import io.questdb.cairo.sql.async.PageFrameReduceDispatcher;
import io.questdb.mp.continuation.FiberRuntime;
import io.questdb.mp.continuation.FiberRuntimeState;
import io.questdb.std.Misc;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class FiberTaskPoolTest extends AbstractCairoTest {

    @Test
    public void testCloseRefusesNewLeases() throws Exception {
        assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(1);
            final PageFrameReduceDispatcher dispatcher = new PageFrameReduceDispatcher(
                    engine,
                    engine.getMessageBus(),
                    runtime
            );
            try {
                Assert.assertTrue(dispatcher.tryLeaseTaskForTesting());
                dispatcher.releaseTaskLeaseForTesting();
                dispatcher.closeTaskPoolForTesting();
                assertLeaseRejected(dispatcher);
            } finally {
                close(runtime, dispatcher);
            }
        });
    }

    @Test
    public void testTryLeaseClampsAtCapacity() throws Exception {
        assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(1, 2);
            final PageFrameReduceDispatcher dispatcher = new PageFrameReduceDispatcher(
                    engine,
                    engine.getMessageBus(),
                    runtime
            );
            int leasedCount = 0;
            try {
                Assert.assertTrue(dispatcher.tryLeaseTaskForTesting());
                leasedCount++;
                Assert.assertTrue(dispatcher.tryLeaseTaskForTesting());
                leasedCount++;
                assertLeaseRejected(dispatcher);

                dispatcher.releaseTaskLeaseForTesting();
                leasedCount--;
                Assert.assertTrue(dispatcher.tryLeaseTaskForTesting());
                leasedCount++;
                assertLeaseRejected(dispatcher);
            } finally {
                try {
                    while (leasedCount > 0) {
                        dispatcher.releaseTaskLeaseForTesting();
                        leasedCount--;
                    }
                } finally {
                    close(runtime, dispatcher);
                }
            }
        });
    }

    @Test
    public void testUpdateLimitsTightensClamp() throws Exception {
        assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(1, 4);
            final PageFrameReduceDispatcher dispatcher = new PageFrameReduceDispatcher(
                    engine,
                    engine.getMessageBus(),
                    runtime
            );
            int leasedCount = 0;
            try {
                Assert.assertTrue(dispatcher.tryLeaseTaskForTesting());
                leasedCount++;
                Assert.assertTrue(dispatcher.tryLeaseTaskForTesting());
                leasedCount++;

                runtime.updateConfiguration(1, 1, 64);
                assertLeaseRejected(dispatcher);

                dispatcher.releaseTaskLeaseForTesting();
                leasedCount--;
                assertLeaseRejected(dispatcher);

                dispatcher.releaseTaskLeaseForTesting();
                leasedCount--;
                Assert.assertTrue(dispatcher.tryLeaseTaskForTesting());
                leasedCount++;
                assertLeaseRejected(dispatcher);
            } finally {
                try {
                    while (leasedCount > 0) {
                        dispatcher.releaseTaskLeaseForTesting();
                        leasedCount--;
                    }
                } finally {
                    close(runtime, dispatcher);
                }
            }
        });
    }

    private static void assertLeaseRejected(PageFrameReduceDispatcher dispatcher) {
        final boolean isLeased = dispatcher.tryLeaseTaskForTesting();
        if (isLeased) {
            dispatcher.releaseTaskLeaseForTesting();
        }
        Assert.assertFalse(isLeased);
    }

    private static void close(FiberRuntime runtime, PageFrameReduceDispatcher dispatcher) {
        try {
            runtime.beginQuiesce();
            final long deadline = System.nanoTime() + 5_000_000_000L;
            while (runtime.state() != FiberRuntimeState.CLOSED && System.nanoTime() < deadline) {
                runtime.drain(8);
            }
            Assert.assertTrue(
                    "fiber runtime did not close [state=" + runtime.state()
                            + ", created=" + runtime.getCreatedFiberCount()
                            + ", live=" + runtime.getLiveFiberCount()
                            + ", retained=" + runtime.getRetainedFiberCount()
                            + ", retired=" + runtime.getRetiredFiberCount()
                            + ", parked=" + runtime.getParkedFiberCount()
                            + ", mounted=" + runtime.getMountedCount()
                            + ", queued=" + runtime.getQueuedCount()
                            + ", outstanding=" + runtime.getOutstandingTaskCount()
                            + ", finalizers=" + runtime.getFinalizerCount()
                            + ']',
                    runtime.awaitClosed(deadline)
            );
            runtime.closeAfterDrained();
        } finally {
            Misc.free(dispatcher);
        }
    }
}
