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

import io.questdb.Metrics;
import io.questdb.mp.DynamicFiberWorkerPoolConfiguration;
import io.questdb.mp.Job;
import io.questdb.mp.Worker;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.mp.WorkerPoolConfigurationWrapper;
import io.questdb.mp.WorkerPoolMode;
import io.questdb.mp.continuation.Fiber;
import io.questdb.mp.continuation.FiberDispatchController;
import io.questdb.mp.continuation.FiberDispatchRequest;
import io.questdb.mp.continuation.FiberDispatchSession;
import io.questdb.mp.continuation.FiberDispatchTicket;
import io.questdb.mp.continuation.FiberRuntime;
import io.questdb.mp.continuation.FiberRuntimeState;
import io.questdb.mp.continuation.FiberTask;
import io.questdb.mp.continuation.LaunchResult;
import io.questdb.std.CpuTopology;
import io.questdb.std.Os;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;

public class WorkerPoolGroupTest {

    @Test
    public void testConfigurationUpdateReachesEveryGroupRuntime() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final AtomicReference<DynamicFiberWorkerPoolConfiguration.FiberConfigurationListener> listener = new AtomicReference<>();
            final WorkerPool pool = new TestWorkerPool(new DynamicFiberWorkerPoolConfiguration() {
                @Override
                public FiberConfiguration getFiberConfiguration() {
                    return new FiberConfiguration(60, 12, 64);
                }

                @Override
                public int getGroupCount() {
                    return 2;
                }

                @Override
                public Metrics getMetrics() {
                    return Metrics.DISABLED;
                }

                @Override
                public String getPoolName() {
                    return "groups";
                }

                @Override
                public int getWorkerCount() {
                    return 6;
                }

                @Override
                public WorkerPoolMode getWorkerPoolMode() {
                    return WorkerPoolMode.FIBER_HOST;
                }

                @Override
                public void setFiberConfigurationListener(FiberConfigurationListener l) {
                    listener.set(l);
                }
            });
            try {
                for (int g = 0; g < 2; g++) {
                    Assert.assertEquals(30, pool.getFiberRuntime(g).getMaxLiveFiberCount());
                    Assert.assertEquals(6, pool.getFiberRuntime(g).getMaxRetainedFiberCount());
                }
                listener.get().onConfigurationChanged(90, 18, 16);
                for (int g = 0; g < 2; g++) {
                    final FiberRuntime runtime = pool.getFiberRuntime(g);
                    Assert.assertEquals(45, runtime.getMaxLiveFiberCount());
                    Assert.assertEquals(9, runtime.getMaxRetainedFiberCount());
                    Assert.assertEquals(16, runtime.getMountBudget());
                }
                Assert.assertEquals(90, pool.getFiberMaxLiveCount());
                Assert.assertEquals(18, pool.getFiberRetainedCount());
            } finally {
                pool.halt();
            }
            Assert.assertNull(listener.get());
        });
    }

    @Test
    public void testDefaultIsOneGroup() {
        final WorkerPool pool = new TestWorkerPool(4);
        try {
            Assert.assertEquals(1, pool.getGroupCount());
            for (int i = 0; i < 4; i++) {
                Assert.assertEquals(0, pool.getGroup(i));
            }
        } finally {
            pool.halt();
        }
    }

    @Test
    public void testFailedGroupRuntimeClosesEarlierRuntimes() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final List<FiberRuntime> opened = new ArrayList<>();
            final FiberDispatchController controller = runtime -> {
                if (!opened.isEmpty()) {
                    throw new IllegalStateException("second session refused");
                }
                opened.add(runtime);
                return new IdleSession();
            };
            try {
                new TestWorkerPool(fiberConfiguration(4, 2, 64, 16, controller));
                Assert.fail();
            } catch (IllegalStateException e) {
                TestUtils.assertContains(e.getMessage(), "second session refused");
            }
            Assert.assertEquals(1, opened.size());
            Assert.assertEquals(FiberRuntimeState.CLOSED, opened.get(0).state());
        });
    }

    @Test
    public void testFiberHostPoolHasOneRuntimePerGroup() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // 7 workers over 3 groups are 0-2, 3-4 and 5-6, and each group gets its share of the limits
            final WorkerPool pool = new TestWorkerPool(fiberConfiguration(7, 3, 70, 14, null));
            try {
                final int[] ownerWorkerCounts = {3, 2, 2};
                final int[] maxLiveCounts = {30, 20, 20};
                final int[] retainedCounts = {6, 4, 4};
                for (int g = 0; g < 3; g++) {
                    final FiberRuntime runtime = pool.getFiberRuntime(g);
                    Assert.assertEquals(ownerWorkerCounts[g], runtime.getOwnerWorkerCount());
                    Assert.assertEquals(maxLiveCounts[g], runtime.getMaxLiveFiberCount());
                    Assert.assertEquals(retainedCounts[g], runtime.getMaxRetainedFiberCount());
                    for (int h = 0; h < g; h++) {
                        Assert.assertNotSame(pool.getFiberRuntime(h), runtime);
                    }
                }
                Assert.assertEquals(70, pool.getFiberMaxLiveCount());
                Assert.assertEquals(14, pool.getFiberRetainedCount());
                try {
                    pool.getFiberRuntime();
                    Assert.fail();
                } catch (IllegalStateException e) {
                    TestUtils.assertContains(e.getMessage(), "worker pool has one fiber runtime per worker group");
                }
                try {
                    pool.getFiberRuntime(3);
                    Assert.fail();
                } catch (IllegalArgumentException e) {
                    TestUtils.assertContains(e.getMessage(), "worker group is out of range");
                }
            } finally {
                pool.halt();
            }
        });
    }

    @Test
    public void testFiberMetricsLabelEachGroup() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final Metrics metrics = Metrics.ENABLED;
            final WorkerPool pool = new TestWorkerPool(new WorkerPoolConfiguration() {
                @Override
                public int getGroupCount() {
                    return 2;
                }

                @Override
                public Metrics getMetrics() {
                    return metrics;
                }

                @Override
                public String getPoolName() {
                    return "labelled";
                }

                @Override
                public int getWorkerCount() {
                    return 4;
                }

                @Override
                public WorkerPoolMode getWorkerPoolMode() {
                    return WorkerPoolMode.FIBER_HOST;
                }
            });
            try (DirectUtf8Sink sink = new DirectUtf8Sink(64 * 1024)) {
                metrics.fiberMetrics().scrapeIntoPrometheus(sink);
                final String value = sink.toString();
                TestUtils.assertContains(value, "questdb_worker_pool_fiber_max_live{worker_pool=\"labelled\",group=\"0\"} 32\n");
                TestUtils.assertContains(value, "questdb_worker_pool_fiber_max_live{worker_pool=\"labelled\",group=\"1\"} 32\n");
                TestUtils.assertContains(value, "questdb_worker_pool_fiber_launch_total{worker_pool=\"labelled\",group=\"1\",result=\"launched\"} 0\n");
            } finally {
                pool.halt();
            }
            try (DirectUtf8Sink sink = new DirectUtf8Sink(64 * 1024)) {
                metrics.fiberMetrics().scrapeIntoPrometheus(sink);
                Assert.assertFalse(sink.toString().contains("worker_pool=\"labelled\""));
            }
        });
    }

    @Test
    public void testFibersRunOnlyOnTheirGroupWorkers() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final int tasksPerGroup = 64;
            final int yieldCount = 4;
            final WorkerPool pool = new TestWorkerPool(fiberConfiguration(6, 2, 256, 16, null));
            final AtomicInteger stepCount = new AtomicInteger();
            final AtomicReference<String> failure = new AtomicReference<>();
            final CountDownLatch done = new CountDownLatch(2 * tasksPerGroup);
            pool.start();
            try {
                for (int g = 0; g < 2; g++) {
                    final int group = g;
                    final FiberRuntime runtime = pool.getFiberRuntime(g);
                    for (int t = 0; t < tasksPerGroup; t++) {
                        Assert.assertEquals(LaunchResult.LAUNCHED, runtime.launch(new FiberTask() {
                            @Override
                            protected boolean runStep() {
                                // each yield lets another worker pick the fiber up, by stealing or
                                // from the run queues
                                for (int i = 0; i <= yieldCount; i++) {
                                    final Worker worker = Worker.current();
                                    if (worker == null || pool.getGroup(worker.getWorkerId()) != group) {
                                        failure.compareAndSet(null, "group " + group + " fiber ran on "
                                                + (worker == null ? "a non-worker" : "worker " + worker.getWorkerId()));
                                    }
                                    stepCount.incrementAndGet();
                                    if (i < yieldCount && !Fiber.yieldCooperatively()) {
                                        failure.compareAndSet(null, "fiber could not yield");
                                    }
                                }
                                done.countDown();
                                return true;
                            }
                        }));
                    }
                }
                Assert.assertTrue(done.await(30, TimeUnit.SECONDS));
                Assert.assertNull(failure.get());
                Assert.assertEquals(2 * tasksPerGroup * (yieldCount + 1), stepCount.get());
            } finally {
                pool.halt();
            }
        });
    }

    @Test
    public void testGroupsAreContiguousBlocks() {
        final WorkerPool pool = new TestWorkerPool(configuration(190, 2, null, null));
        try {
            Assert.assertEquals(2, pool.getGroupCount());
            for (int i = 0; i < 190; i++) {
                Assert.assertEquals("worker " + i, i < 95 ? 0 : 1, pool.getGroup(i));
            }
        } finally {
            pool.halt();
        }
        // uneven split: 7 workers over 3 groups are 0-2, 3-4, 5-6
        final int[] expected = {0, 0, 0, 1, 1, 2, 2};
        for (int i = 0; i < 7; i++) {
            Assert.assertEquals("worker " + i, expected[i], WorkerPool.getGroup(i, 3, 7));
        }
    }

    @Test
    public void testRejectsAffinityWithGroupCpus() {
        final int[] affinity = new int[4];
        Arrays.fill(affinity, -1);
        affinity[2] = 1;
        assertRejected(configuration(4, 2, new int[][]{{0}, {1}}, affinity), "worker affinity cannot be combined with group CPU sets");
    }

    @Test
    public void testRejectsBadGroupCount() {
        assertRejected(configuration(4, 0, null, null), "group count must be between 1 and the worker count");
        assertRejected(configuration(4, 5, null, null), "group count must be between 1 and the worker count");
    }

    @Test
    public void testRejectsGroupCpusOfWrongLength() {
        assertRejected(configuration(4, 2, new int[][]{{0}}, null), "group CPU sets do not match group count");
    }

    @Test
    public void testWorkersRunOnTheirGroupCpus() throws Exception {
        Assume.assumeTrue("thread CPU sets apply on Linux only", Os.isLinux());
        final int[] allowed = threadAllowedCpus();
        Assume.assumeTrue("needs at least 2 allowed CPUs", allowed.length > 1);
        // split the allowed CPUs into two halves, as two sockets would be
        final int half = allowed.length / 2;
        final int[][] groupCpus = {
                Arrays.copyOfRange(allowed, 0, half),
                Arrays.copyOfRange(allowed, half, allowed.length)
        };
        final int workerCount = 6;
        final AtomicReferenceArray<int[]> seen = new AtomicReferenceArray<>(workerCount);
        final CountDownLatch allSeen = new CountDownLatch(workerCount);
        final WorkerPool pool = new TestWorkerPool(configuration(workerCount, 2, groupCpus, null));
        try {
            for (int i = 0; i < workerCount; i++) {
                final int worker = i;
                pool.assign(i, new Job() {
                    @Override
                    public boolean run(@NotNull WorkerContext workerContext) {
                        if (seen.get(worker) == null) {
                            try {
                                seen.set(worker, threadAllowedCpus());
                            } catch (Exception e) {
                                seen.set(worker, new int[0]);
                            }
                            allSeen.countDown();
                        }
                        return false;
                    }
                });
            }
            pool.start();
            Assert.assertTrue(allSeen.await(30, TimeUnit.SECONDS));
        } finally {
            pool.halt();
        }
        for (int i = 0; i < workerCount; i++) {
            Assert.assertArrayEquals("worker " + i, groupCpus[i < 3 ? 0 : 1], seen.get(i));
        }
        // the restriction applies to the workers only
        Assert.assertArrayEquals(allowed, threadAllowedCpus());
    }

    @Test
    public void testWrapperDelegatesGroups() {
        final int[][] groupCpus = {{0, 1}, {2, 3}};
        final WorkerPoolConfigurationWrapper wrapper = new WorkerPoolConfigurationWrapper();
        wrapper.setDelegate(configuration(64, 2, groupCpus, null));
        Assert.assertEquals(2, wrapper.getGroupCount());
        Assert.assertSame(groupCpus, wrapper.getGroupCpus());
        wrapper.setDelegate(() -> 4);
        Assert.assertEquals(1, wrapper.getGroupCount());
        Assert.assertNull(wrapper.getGroupCpus());
    }

    private static void assertRejected(WorkerPoolConfiguration configuration, String message) {
        try {
            new TestWorkerPool(configuration);
            Assert.fail();
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().startsWith(message));
        }
    }

    private static WorkerPoolConfiguration configuration(int workerCount, int groupCount, int[][] groupCpus, int[] affinity) {
        return new WorkerPoolConfiguration() {
            @Override
            public int getGroupCount() {
                return groupCount;
            }

            @Override
            public int[][] getGroupCpus() {
                return groupCpus;
            }

            @Override
            public Metrics getMetrics() {
                return Metrics.DISABLED;
            }

            @Override
            public String getPoolName() {
                return "groups";
            }

            @Override
            public int[] getWorkerAffinity() {
                return affinity;
            }

            @Override
            public int getWorkerCount() {
                return workerCount;
            }
        };
    }

    private static WorkerPoolConfiguration fiberConfiguration(
            int workerCount,
            int groupCount,
            int maxLiveCount,
            int retainedCount,
            @Nullable FiberDispatchController controller
    ) {
        return new WorkerPoolConfiguration() {
            @Override
            public @Nullable FiberDispatchController getFiberDispatchController() {
                return controller;
            }

            @Override
            public int getFiberMaxLiveCount() {
                return maxLiveCount;
            }

            @Override
            public int getFiberRetainedCount() {
                return retainedCount;
            }

            @Override
            public int getGroupCount() {
                return groupCount;
            }

            @Override
            public Metrics getMetrics() {
                return Metrics.DISABLED;
            }

            @Override
            public String getPoolName() {
                return "groups";
            }

            @Override
            public int getWorkerCount() {
                return workerCount;
            }

            @Override
            public WorkerPoolMode getWorkerPoolMode() {
                return WorkerPoolMode.FIBER_HOST;
            }
        };
    }

    // the calling thread's allowed CPUs, as the kernel reports them
    private static int[] threadAllowedCpus() throws Exception {
        for (String line : Files.readAllLines(Path.of("/proc/thread-self/status"))) {
            if (line.startsWith("Cpus_allowed_list:")) {
                return CpuTopology.parseCpuList(line.substring("Cpus_allowed_list:".length()));
            }
        }
        throw new AssertionError("no Cpus_allowed_list in /proc/thread-self/status");
    }

    // a session that never receives a dispatch and is always quiesced
    private static class IdleSession implements FiberDispatchSession {
        @Override
        public void beginQuiesce() {
        }

        @Override
        public boolean isQuiesced() {
            return true;
        }

        @Override
        public void progressQuiesce() {
        }

        @Override
        public void requestDispatch(FiberDispatchRequest request) {
        }

        @Override
        public @Nullable FiberDispatchTicket tryDispatchDirect(FiberDispatchRequest request) {
            return null;
        }
    }
}
