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

package io.questdb.test.metrics;

import io.questdb.metrics.FiberMetrics;
import io.questdb.mp.continuation.Fiber;
import io.questdb.mp.continuation.FiberRuntime;
import io.questdb.mp.continuation.FiberRuntimeState;
import io.questdb.mp.continuation.FiberTask;
import io.questdb.mp.continuation.LaunchResult;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class FiberMetricsTest {

    @Test
    public void testScrapeClearAndUnregister() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final FiberRuntime runtime = new FiberRuntime(1);
            final FiberMetrics metrics = new FiberMetrics();
            metrics.register("test\"pool", runtime);

            final Fiber fiber = runtime.tryReserveFiber();
            Assert.assertNotNull(fiber);
            Assert.assertNull(runtime.tryReserveFiber());
            runtime.releaseReservedFiber(fiber, fiber.getReservationEpoch());
            final OneShotTask task = new OneShotTask();
            Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(task));
            Assert.assertEquals(LaunchResult.SATURATED, runtime.launch(new OneShotTask()));
            Assert.assertEquals(1, runtime.drain(1));

            try (DirectUtf8Sink sink = new DirectUtf8Sink(2048)) {
                metrics.scrapeIntoPrometheus(sink);
                final String value = sink.toString();
                TestUtils.assertContains(value, "questdb_worker_pool_fiber_live{worker_pool=\"test\\\"pool\"} 1\n");
                TestUtils.assertContains(value, "questdb_worker_pool_fiber_max_live{worker_pool=\"test\\\"pool\"} 1\n");
                TestUtils.assertContains(value, "questdb_worker_pool_fiber_outstanding{worker_pool=\"test\\\"pool\"} 0\n");
                TestUtils.assertContains(value, "questdb_worker_pool_fiber_queued{worker_pool=\"test\\\"pool\"} 0\n");
                TestUtils.assertContains(value, "questdb_worker_pool_fiber_mounted{worker_pool=\"test\\\"pool\"} 0\n");
                TestUtils.assertContains(value, "questdb_worker_pool_fiber_finalizing{worker_pool=\"test\\\"pool\"} 0\n");
                TestUtils.assertContains(value, "questdb_worker_pool_fiber_parked{worker_pool=\"test\\\"pool\"} 0\n");
                TestUtils.assertContains(value, "questdb_worker_pool_fiber_retained{worker_pool=\"test\\\"pool\"} 1\n");
                TestUtils.assertContains(value, "questdb_worker_pool_fiber_created_total{worker_pool=\"test\\\"pool\"} 1\n");
                TestUtils.assertContains(value, "questdb_worker_pool_fiber_retired_total{worker_pool=\"test\\\"pool\"} 0\n");
                TestUtils.assertContains(value, "questdb_worker_pool_fiber_mount_total{worker_pool=\"test\\\"pool\"} 1\n");
                TestUtils.assertContains(value, "questdb_worker_pool_fiber_saturation_total{worker_pool=\"test\\\"pool\"} 2\n");
                TestUtils.assertContains(
                        value,
                        "questdb_worker_pool_fiber_mount_budget_exhaustion_total{worker_pool=\"test\\\"pool\"} 0\n"
                );
                TestUtils.assertContains(
                        value,
                        "questdb_worker_pool_fiber_inline_suspend_violation_total{worker_pool=\"test\\\"pool\"} 0\n"
                );
                TestUtils.assertContains(
                        value,
                        "questdb_worker_pool_fiber_launch_total{worker_pool=\"test\\\"pool\",result=\"launched\"} 1\n"
                );
                TestUtils.assertContains(
                        value,
                        "questdb_worker_pool_fiber_launch_total{worker_pool=\"test\\\"pool\",result=\"saturated\"} 1\n"
                );

                metrics.clear();
                sink.clear();
                metrics.scrapeIntoPrometheus(sink);
                TestUtils.assertContains(
                        sink.toString(),
                        "questdb_worker_pool_fiber_saturation_total{worker_pool=\"test\\\"pool\"} 0\n"
                );
                TestUtils.assertContains(
                        sink.toString(),
                        "questdb_worker_pool_fiber_launch_total{worker_pool=\"test\\\"pool\",result=\"launched\"} 0\n"
                );

                metrics.unregister(runtime);
                sink.clear();
                metrics.scrapeIntoPrometheus(sink);
                Assert.assertEquals(0, sink.size());
            } finally {
                close(runtime);
            }
        });
    }

    private static void close(FiberRuntime runtime) {
        runtime.beginQuiesce();
        final long deadline = System.nanoTime() + 5_000_000_000L;
        while (runtime.state() != FiberRuntimeState.CLOSED && System.nanoTime() < deadline) {
            runtime.drain(64);
        }
        Assert.assertTrue(runtime.awaitClosed(deadline));
        runtime.closeAfterDrained();
    }

    private static class OneShotTask extends FiberTask {
        @Override
        protected boolean runStep() {
            return true;
        }
    }
}
