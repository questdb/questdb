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
import io.questdb.mp.Job;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.mp.WorkerPoolConfigurationWrapper;
import io.questdb.std.CpuTopology;
import io.questdb.std.Os;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceArray;

public class WorkerPoolGroupTest {

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

    // the calling thread's allowed CPUs, as the kernel reports them
    private static int[] threadAllowedCpus() throws Exception {
        for (String line : Files.readAllLines(Path.of("/proc/thread-self/status"))) {
            if (line.startsWith("Cpus_allowed_list:")) {
                return CpuTopology.parseCpuList(line.substring("Cpus_allowed_list:".length()));
            }
        }
        throw new AssertionError("no Cpus_allowed_list in /proc/thread-self/status");
    }
}
