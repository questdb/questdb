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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

public class NetworkSplitConfigurationTest {
    @ClassRule
    public static final TemporaryFolder temp = new TemporaryFolder();
    private static final Log LOG = LogFactory.getLog(NetworkSplitConfigurationTest.class);
    private static final String SPLIT_KEY = PropertyKey.SHARED_NETWORK_WORKER_SPLIT_BY_CPU_SOCKET.getPropertyPath();
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
    public void testDefaultIsNotSplit() throws Exception {
        final WorkerPoolConfiguration network = new PropServerConfiguration(root, new Properties(), null, LOG, new BuildInformationHolder())
                .getSharedWorkerPoolNetworkConfiguration();
        Assert.assertEquals(1, network.getGroupCount());
        Assert.assertNull(network.getGroupCpus());
    }

    @Test
    public void testNotSplitForSmallPool() throws Exception {
        final WorkerPoolConfiguration network = assertNotSplit(
                twoSockets(96),
                16,
                "network workers not split: 16 network workers make groups smaller than 32 workers over 2 sockets"
                        + " [layout=socket 0: CPUs 0-95 on node 0, socket 1: CPUs 96-191 on node 1]"
        );
        Assert.assertEquals(16, network.getWorkerCount());
    }

    @Test
    public void testNotSplitOnSingleSocket() throws Exception {
        final Path fixture = fixture("0-31");
        for (int cpu = 0; cpu < 32; cpu++) {
            cpu(fixture, cpu, 0, 0);
        }
        assertNotSplit(
                fixture,
                64,
                "network workers not split: the allowed CPUs 0-31 are all on socket 0 [layout=socket 0: CPUs 0-31 on node 0]"
        );
    }

    @Test
    public void testNotSplitWhereUnsupported() throws Exception {
        assertNotSplit(
                null,
                190,
                "network workers not split: " + SPLIT_KEY + " applies on Linux only"
        );
    }

    @Test
    public void testRealTopology() throws Exception {
        Assume.assumeTrue("the split reads the topology on Linux only", Os.isLinux());
        final Properties properties = new Properties();
        properties.setProperty(SPLIT_KEY, "true");
        final WorkerPoolConfiguration network = new PropServerConfiguration(root, properties, null, LOG, new BuildInformationHolder())
                .getSharedWorkerPoolNetworkConfiguration();
        final CpuTopology.Layout layout = new CpuTopology().readLayout(network.getWorkerCount());
        LOG.info().$("real topology [workers=").$(network.getWorkerCount())
                .$(", accepted=").$(layout.isAccepted())
                .$(", refusal=").$(layout.getRefusal())
                .$(", layout=").$(layout.getDescription())
                .I$();
        final int[][] socketCpus = layout.getSocketCpus();
        if (socketCpus == null) {
            Assert.assertEquals(1, network.getGroupCount());
            Assert.assertNull(network.getGroupCpus());
        } else {
            Assert.assertEquals(socketCpus.length, network.getGroupCount());
            Assert.assertNotNull(network.getGroupCpus());
            for (int g = 0; g < socketCpus.length; g++) {
                Assert.assertArrayEquals(socketCpus[g], network.getGroupCpus()[g]);
            }
        }
    }

    @Test
    public void testRejectsAffinity() throws Exception {
        final Properties properties = new Properties();
        properties.setProperty(SPLIT_KEY, "true");
        properties.setProperty(PropertyKey.SHARED_NETWORK_WORKER_COUNT.getPropertyPath(), "2");
        properties.setProperty(PropertyKey.SHARED_NETWORK_WORKER_AFFINITY.getPropertyPath(), "-1,3");
        try {
            FixtureConfiguration.create(properties, twoSockets(96));
            Assert.fail();
        } catch (ServerConfigurationException e) {
            TestUtils.assertContains(e.getMessage(), SPLIT_KEY);
            TestUtils.assertContains(e.getMessage(), "cannot be combined with " + PropertyKey.SHARED_NETWORK_WORKER_AFFINITY.getPropertyPath());
        }
    }

    @Test
    public void testSplitIgnoresAffinityOfMinusOne() throws Exception {
        final Properties properties = new Properties();
        properties.setProperty(SPLIT_KEY, "true");
        properties.setProperty(PropertyKey.SHARED_NETWORK_WORKER_COUNT.getPropertyPath(), "64");
        properties.setProperty(PropertyKey.SHARED_NETWORK_WORKER_AFFINITY.getPropertyPath(), "-1,".repeat(63) + "-1");
        Assert.assertEquals(2, FixtureConfiguration.create(properties, twoSockets(96)).getSharedWorkerPoolNetworkConfiguration().getGroupCount());
    }

    @Test
    public void testSplitsOnTwoSockets() throws Exception {
        final Properties properties = new Properties();
        properties.setProperty(SPLIT_KEY, "true");
        properties.setProperty(PropertyKey.SHARED_NETWORK_WORKER_COUNT.getPropertyPath(), "190");
        final LogCapture capture = new LogCapture();
        capture.start();
        final WorkerPoolConfiguration network;
        final WorkerPoolConfiguration query;
        try {
            final PropServerConfiguration configuration = FixtureConfiguration.create(properties, twoSockets(96));
            network = configuration.getSharedWorkerPoolNetworkConfiguration();
            query = configuration.getSharedWorkerPoolQueryConfiguration();
            capture.waitFor("network workers split by CPU socket: group 0 workers 0-94 on CPUs 0-95 (socket 0),"
                    + " group 1 workers 95-189 on CPUs 96-191 (socket 1)", 10_000);
        } finally {
            capture.stop();
        }
        Assert.assertEquals(2, network.getGroupCount());
        final int[][] cpus = network.getGroupCpus();
        Assert.assertNotNull(cpus);
        Assert.assertEquals(2, cpus.length);
        Assert.assertEquals(96, cpus[0].length);
        Assert.assertEquals(0, cpus[0][0]);
        Assert.assertEquals(95, cpus[0][95]);
        Assert.assertEquals(96, cpus[1][0]);
        Assert.assertEquals(191, cpus[1][95]);
        // the split belongs to the network pool only
        Assert.assertEquals(1, query.getGroupCount());
        Assert.assertNull(query.getGroupCpus());
    }

    private static void cpu(Path fixture, int cpu, int socket, int node) throws IOException {
        final Path dir = fixture.resolve("sys/devices/system/cpu/cpu" + cpu);
        Files.createDirectories(dir.resolve("topology"));
        Files.writeString(dir.resolve("topology/physical_package_id"), socket + "\n");
        Files.createDirectories(dir.resolve("node" + node));
    }

    // the allowed CPUs are all online
    private static Path fixture(String allowed) throws IOException {
        final Path fixture = temp.newFolder().toPath();
        Files.createDirectories(fixture.resolve("proc/self"));
        Files.writeString(fixture.resolve("proc/self/status"), "Name:\tjava\nCpus_allowed_list:\t" + allowed + "\n");
        final Path cpuDir = fixture.resolve("sys/devices/system/cpu");
        Files.createDirectories(cpuDir);
        Files.writeString(cpuDir.resolve("online"), allowed + "\n");
        return fixture;
    }

    private static Path twoSockets(int cpusPerSocket) throws IOException {
        final Path fixture = fixture("0-" + (2 * cpusPerSocket - 1));
        for (int cpu = 0; cpu < 2 * cpusPerSocket; cpu++) {
            cpu(fixture, cpu, cpu / cpusPerSocket, cpu / cpusPerSocket);
        }
        return fixture;
    }

    private WorkerPoolConfiguration assertNotSplit(@Nullable Path fixture, int workerCount, String expectedLog) throws Exception {
        final Properties properties = new Properties();
        properties.setProperty(SPLIT_KEY, "true");
        properties.setProperty(PropertyKey.SHARED_NETWORK_WORKER_COUNT.getPropertyPath(), Integer.toString(workerCount));
        final LogCapture capture = new LogCapture();
        capture.start();
        final WorkerPoolConfiguration network;
        try {
            network = FixtureConfiguration.create(properties, fixture).getSharedWorkerPoolNetworkConfiguration();
            capture.waitFor(expectedLog, 10_000);
        } finally {
            capture.stop();
        }
        Assert.assertEquals(1, network.getGroupCount());
        Assert.assertNull(network.getGroupCpus());
        return network;
    }

    // Reads the CPU layout from a fixture /proc and /sys tree, or reports the split as
    // unsupported when there is none.
    private static class FixtureConfiguration extends PropServerConfiguration {
        // createCpuTopology() runs inside the super constructor, before this class's own fields
        // are set, so the fixture travels in a static field; the tests of a class run one at a time
        private static volatile Path fixture;

        private FixtureConfiguration(Properties properties) throws Exception {
            super(root, properties, null, LOG, new BuildInformationHolder());
        }

        static PropServerConfiguration create(Properties properties, @Nullable Path fixture) throws Exception {
            FixtureConfiguration.fixture = fixture;
            return new FixtureConfiguration(properties);
        }

        @Override
        protected @Nullable CpuTopology createCpuTopology() {
            final Path fixture = FixtureConfiguration.fixture;
            return fixture != null ? new CpuTopology(fixture.resolve("proc"), fixture.resolve("sys")) : null;
        }
    }
}
