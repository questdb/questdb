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

package io.questdb.test;

import io.questdb.BuildInformationHolder;
import io.questdb.PropServerConfiguration;
import io.questdb.PropertyKey;
import io.questdb.ServerConfigurationException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.std.CpuTopology;
import io.questdb.std.Os;
import io.questdb.test.tools.CpuTopologyFixture;
import io.questdb.test.tools.LogCapture;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Properties;

public class NetworkSplitConfigurationTest {
    @ClassRule
    public static final TemporaryFolder temp = new TemporaryFolder();
    private static final String GROUPS_KEY = PropertyKey.SHARED_NETWORK_WORKER_GROUPS.getPropertyPath();
    private static final Log LOG = LogFactory.getLog(NetworkSplitConfigurationTest.class);
    private static final String TWO_SOCKETS_LAYOUT = " [layout=socket 0: CPUs 0-95 on node 0, socket 1: CPUs 96-191 on node 1]";
    private static String root;

    @AfterClass
    public static void afterClass() {
        TestUtils.removeTestPath(root);
    }

    @BeforeClass
    public static void setUp() throws IOException {
        final File root = new File(temp.getRoot(), "root");
        TestUtils.copyMimeTypes(root.getAbsolutePath());
        NetworkSplitConfigurationTest.root = root.getAbsolutePath();
    }

    @Test
    public void testAffinityKeepsGroupsUnpinned() throws Exception {
        final Properties properties = properties(16, 2);
        properties.setProperty(PropertyKey.SHARED_NETWORK_WORKER_AFFINITY.getPropertyPath(), "0,1,2,3,4,5,6,7,96,97,98,99,100,101,102,103");
        final WorkerPoolConfiguration network = create(
                twoSockets(96),
                properties,
                "network workers split into 2 groups without CPU pinning: "
                        + PropertyKey.SHARED_NETWORK_WORKER_AFFINITY.getPropertyPath() + " sets the workers' CPUs"
        );
        Assert.assertEquals(2, network.getGroupCount());
        Assert.assertNull(network.getGroupCpus());
        Assert.assertArrayEquals(new int[]{0, 1, 2, 3, 4, 5, 6, 7, 96, 97, 98, 99, 100, 101, 102, 103}, network.getWorkerAffinity());
    }

    @Test
    public void testDefaultIsNotSplit() throws Exception {
        final WorkerPoolConfiguration byDefault = new PropServerConfiguration(root, new Properties(), null, LOG, new BuildInformationHolder())
                .getSharedWorkerPoolNetworkConfiguration();
        Assert.assertEquals(1, byDefault.getGroupCount());
        Assert.assertNull(byDefault.getGroupCpus());
        final WorkerPoolConfiguration oneGroup = CpuTopologyFixture.configuration(root, properties(190, 1), twoSockets(96), LOG)
                .getSharedWorkerPoolNetworkConfiguration();
        Assert.assertEquals(1, oneGroup.getGroupCount());
        Assert.assertNull(oneGroup.getGroupCpus());
    }

    @Test
    public void testNotSplitForSmallPool() throws Exception {
        assertNotSplit(twoSockets(96), 15, 2, "network workers not split: 15 network workers cannot fill 2 groups of 8");
        assertNotSplit(singleSocket(), 15, 2, "network workers not split: 15 network workers cannot fill 2 groups of 8");
        assertNotSplit(singleSocket(), 15, 4, "network workers not split: 15 network workers cannot fill 2 groups of 8");
    }

    @Test
    public void testNotSplitWhereUnsupported() throws Exception {
        assertNotSplit(
                null,
                190,
                2,
                "network workers not split: " + GROUPS_KEY + " applies on Linux only"
        );
    }

    @Test
    public void testRealTopology() throws Exception {
        Assume.assumeTrue("the split reads the topology on Linux only", Os.isLinux());
        final Properties properties = new Properties();
        properties.setProperty(GROUPS_KEY, "2");
        final WorkerPoolConfiguration network = new PropServerConfiguration(root, properties, null, LOG, new BuildInformationHolder())
                .getSharedWorkerPoolNetworkConfiguration();
        final int workerCount = network.getWorkerCount();
        final CpuTopology.Layout layout = new CpuTopology().read();
        LOG.info().$("real topology [workers=").$(workerCount)
                .$(", accepted=").$(layout.isAccepted())
                .$(", refusal=").$(layout.getRefusal())
                .$(", layout=").$(layout.getDescription())
                .I$();
        final int[][] socketCpus = layout.getSocketCpus();
        if (workerCount < 2 * PropServerConfiguration.MIN_NETWORK_GROUP_WORKERS) {
            Assert.assertEquals(1, network.getGroupCount());
            Assert.assertNull(network.getGroupCpus());
        } else if (socketCpus == null || 2 % socketCpus.length != 0) {
            Assert.assertEquals(2, network.getGroupCount());
            Assert.assertNull(network.getGroupCpus());
        } else {
            Assert.assertEquals(2, network.getGroupCount());
            Assert.assertNotNull(network.getGroupCpus());
            for (int g = 0; g < 2; g++) {
                Assert.assertArrayEquals(socketCpus[g], network.getGroupCpus()[g]);
            }
        }
    }

    @Test
    public void testReducesGroupsToFitWorkers() throws Exception {
        final WorkerPoolConfiguration network = create(
                singleSocket(),
                properties(32, 6),
                GROUPS_KEY + "=6 reduced to 4: 32 network workers fill 4 groups of 8",
                "network workers split into 4 groups without CPU pinning: the allowed CPUs 0-31 are all on socket 0"
                        + " [layout=socket 0: CPUs 0-31 on node 0]"
        );
        Assert.assertEquals(4, network.getGroupCount());
        Assert.assertNull(network.getGroupCpus());
    }

    @Test
    public void testRejectsInvalidGroupCount() throws Exception {
        assertRejected("0", "invalid configuration value [key=" + GROUPS_KEY + ", value=0, expected 1 or more]");
        assertRejected("-1", "invalid configuration value [key=" + GROUPS_KEY + ", value=-1, expected 1 or more]");
        assertRejected("two", "invalid configuration value [key=" + GROUPS_KEY + ", value=two]");
    }

    @Test
    public void testSplitIgnoresAffinityOfMinusOne() throws Exception {
        final Properties properties = properties(64, 2);
        properties.setProperty(PropertyKey.SHARED_NETWORK_WORKER_AFFINITY.getPropertyPath(), "-1,".repeat(63) + "-1");
        final WorkerPoolConfiguration network = CpuTopologyFixture.configuration(root, properties, twoSockets(96), LOG).getSharedWorkerPoolNetworkConfiguration();
        Assert.assertEquals(2, network.getGroupCount());
        Assert.assertNotNull(network.getGroupCpus());
    }

    @Test
    public void testSplitsOnTwoSockets() throws Exception {
        final PropServerConfiguration configuration = createConfiguration(
                twoSockets(96),
                properties(190, 2),
                "network workers split by CPU socket: group 0 workers 0-94 on CPUs 0-95 (socket 0),"
                        + " group 1 workers 95-189 on CPUs 96-191 (socket 1)"
        );
        final WorkerPoolConfiguration network = configuration.getSharedWorkerPoolNetworkConfiguration();
        Assert.assertEquals(2, network.getGroupCount());
        final int[][] cpus = network.getGroupCpus();
        Assert.assertNotNull(cpus);
        Assert.assertEquals(2, cpus.length);
        Assert.assertArrayEquals(range(0, 95), cpus[0]);
        Assert.assertArrayEquals(range(96, 191), cpus[1]);
        // the split belongs to the network pool only
        final WorkerPoolConfiguration query = configuration.getSharedWorkerPoolQueryConfiguration();
        Assert.assertEquals(1, query.getGroupCount());
        Assert.assertNull(query.getGroupCpus());
    }

    @Test
    public void testSplitsSmallestPoolOnTwoSockets() throws Exception {
        final WorkerPoolConfiguration network = create(
                twoSockets(96),
                properties(16, 2),
                "network workers split by CPU socket: group 0 workers 0-7 on CPUs 0-95 (socket 0),"
                        + " group 1 workers 8-15 on CPUs 96-191 (socket 1)"
        );
        Assert.assertEquals(2, network.getGroupCount());
        final int[][] cpus = network.getGroupCpus();
        Assert.assertNotNull(cpus);
        Assert.assertArrayEquals(range(0, 95), cpus[0]);
        Assert.assertArrayEquals(range(96, 191), cpus[1]);
    }

    @Test
    public void testSplitsTwoGroupsPerSocket() throws Exception {
        final WorkerPoolConfiguration network = create(
                twoSockets(96),
                properties(32, 4),
                "network workers split by CPU socket: group 0 workers 0-7 on CPUs 0-95 (socket 0),"
                        + " group 1 workers 8-15 on CPUs 0-95 (socket 0), group 2 workers 16-23 on CPUs 96-191 (socket 1),"
                        + " group 3 workers 24-31 on CPUs 96-191 (socket 1)"
        );
        Assert.assertEquals(4, network.getGroupCount());
        final int[][] cpus = network.getGroupCpus();
        Assert.assertNotNull(cpus);
        Assert.assertArrayEquals(range(0, 95), cpus[0]);
        Assert.assertArrayEquals(range(0, 95), cpus[1]);
        Assert.assertArrayEquals(range(96, 191), cpus[2]);
        Assert.assertArrayEquals(range(96, 191), cpus[3]);
    }

    @Test
    public void testSplitsUnpinnedOnRefusedLayout() throws Exception {
        // two sockets on one NUMA node don't look like real hardware, so the groups aren't pinned
        final Path fixture = fixture("0-191");
        for (int cpu = 0; cpu < 192; cpu++) {
            CpuTopologyFixture.cpu(fixture, cpu, cpu / 96, 0);
        }
        assertSplitUnpinned(
                fixture,
                190,
                2,
                "network workers split into 2 groups without CPU pinning: NUMA node 0 holds CPUs of sockets 0 and 1"
                        + " [layout=socket 0: CPUs 0-95 on node 0, socket 1: CPUs 96-191 on node 0]"
        );
    }

    @Test
    public void testSplitsUnpinnedOnSingleSocket() throws Exception {
        final String layout = " [layout=socket 0: CPUs 0-31 on node 0]";
        assertSplitUnpinned(
                singleSocket(),
                16,
                2,
                "network workers split into 2 groups without CPU pinning: the allowed CPUs 0-31 are all on socket 0" + layout
        );
        assertSplitUnpinned(
                singleSocket(),
                64,
                2,
                "network workers split into 2 groups without CPU pinning: the allowed CPUs 0-31 are all on socket 0" + layout
        );
        assertSplitUnpinned(
                singleSocket(),
                24,
                3,
                "network workers split into 3 groups without CPU pinning: the allowed CPUs 0-31 are all on socket 0" + layout
        );
    }

    @Test
    public void testSplitsUnpinnedWhenGroupsDoNotDivideOverSockets() throws Exception {
        assertSplitUnpinned(
                twoSockets(96),
                24,
                3,
                "network workers split into 3 groups without CPU pinning: 3 groups don't divide evenly over 2 sockets"
                        + TWO_SOCKETS_LAYOUT
        );
        final Path threeSockets = fixture("0-23");
        for (int cpu = 0; cpu < 24; cpu++) {
            CpuTopologyFixture.cpu(threeSockets, cpu, cpu / 8, cpu / 8);
        }
        assertSplitUnpinned(
                threeSockets,
                16,
                2,
                "network workers split into 2 groups without CPU pinning: 2 groups don't divide evenly over 3 sockets"
                        + " [layout=socket 0: CPUs 0-7 on node 0, socket 1: CPUs 8-15 on node 1, socket 2: CPUs 16-23 on node 2]"
        );
        final WorkerPoolConfiguration network = create(
                threeSockets,
                properties(24, 3),
                "network workers split by CPU socket: group 0 workers 0-7 on CPUs 0-7 (socket 0),"
                        + " group 1 workers 8-15 on CPUs 8-15 (socket 1), group 2 workers 16-23 on CPUs 16-23 (socket 2)"
        );
        Assert.assertEquals(3, network.getGroupCount());
        final int[][] cpus = network.getGroupCpus();
        Assert.assertNotNull(cpus);
        Assert.assertArrayEquals(range(0, 7), cpus[0]);
        Assert.assertArrayEquals(range(8, 15), cpus[1]);
        Assert.assertArrayEquals(range(16, 23), cpus[2]);
    }

    private static void assertNotSplit(@Nullable Path fixture, int workerCount, int groupCount, String expectedLog) throws Exception {
        final WorkerPoolConfiguration network = create(fixture, properties(workerCount, groupCount), expectedLog);
        Assert.assertEquals(1, network.getGroupCount());
        Assert.assertNull(network.getGroupCpus());
    }

    private static void assertRejected(String groupCount, String expectedMessage) throws Exception {
        final Properties properties = new Properties();
        properties.setProperty(GROUPS_KEY, groupCount);
        try {
            CpuTopologyFixture.configuration(root, properties, twoSockets(96), LOG);
            Assert.fail();
        } catch (ServerConfigurationException e) {
            TestUtils.assertContains(e.getMessage(), expectedMessage);
        }
    }

    private static void assertSplitUnpinned(Path fixture, int workerCount, int groupCount, String expectedLog) throws Exception {
        final WorkerPoolConfiguration network = create(fixture, properties(workerCount, groupCount), expectedLog);
        Assert.assertEquals(groupCount, network.getGroupCount());
        Assert.assertNull(network.getGroupCpus());
    }

    private static WorkerPoolConfiguration create(@Nullable Path fixture, Properties properties, String... expectedLogs) throws Exception {
        return createConfiguration(fixture, properties, expectedLogs).getSharedWorkerPoolNetworkConfiguration();
    }

    // creates the configuration and waits for the log lines of the split's outcome
    private static PropServerConfiguration createConfiguration(
            @Nullable Path fixture,
            Properties properties,
            String... expectedLogs
    ) throws Exception {
        final LogCapture capture = new LogCapture();
        capture.start();
        try {
            final PropServerConfiguration configuration = CpuTopologyFixture.configuration(root, properties, fixture, LOG);
            for (String expectedLog : expectedLogs) {
                capture.waitFor(expectedLog, 10_000);
            }
            Assert.assertEquals(
                    properties.getProperty(PropertyKey.SHARED_NETWORK_WORKER_COUNT.getPropertyPath()),
                    Integer.toString(configuration.getSharedWorkerPoolNetworkConfiguration().getWorkerCount())
            );
            return configuration;
        } finally {
            capture.stop();
        }
    }

    // the allowed CPUs are all online
    private static Path fixture(String allowed) throws IOException {
        return CpuTopologyFixture.create(temp.newFolder().toPath(), allowed, allowed);
    }

    private static Properties properties(int workerCount, int groupCount) {
        final Properties properties = new Properties();
        properties.setProperty(PropertyKey.SHARED_NETWORK_WORKER_COUNT.getPropertyPath(), Integer.toString(workerCount));
        properties.setProperty(GROUPS_KEY, Integer.toString(groupCount));
        return properties;
    }

    private static int[] range(int first, int last) {
        final int[] cpus = new int[last - first + 1];
        for (int i = 0; i < cpus.length; i++) {
            cpus[i] = first + i;
        }
        return cpus;
    }

    private static Path singleSocket() throws IOException {
        final Path fixture = fixture("0-31");
        for (int cpu = 0; cpu < 32; cpu++) {
            CpuTopologyFixture.cpu(fixture, cpu, 0, 0);
        }
        return fixture;
    }

    private static Path twoSockets(int cpusPerSocket) throws IOException {
        return CpuTopologyFixture.twoSockets(temp.newFolder().toPath(), cpusPerSocket);
    }
}
