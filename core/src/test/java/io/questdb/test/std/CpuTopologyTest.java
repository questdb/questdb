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

package io.questdb.test.std;

import io.questdb.std.CpuTopology;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

public class CpuTopologyTest {
    @Rule
    public final TemporaryFolder temp = new TemporaryFolder();

    @Test
    public void testAcceptsCpuCountsWithinSpread() throws IOException {
        // 48 and 40 CPUs are 16.7% apart
        assertAccepted(twoSockets(96).allowed("0-47,96-135").read(64), new int[][]{range(0, 47), range(96, 135)}, new int[]{0, 1});
    }

    @Test
    public void testAcceptsCpusetOnBothSockets() throws IOException {
        final Fixture fixture = twoSockets(96).allowed("0-15,96-111");
        final CpuTopology.Layout layout = fixture.read(64);
        assertAccepted(layout, new int[][]{range(0, 15), range(96, 111)}, new int[]{0, 1});
        Assert.assertEquals("socket 0: CPUs 0-15 on node 0, socket 1: CPUs 96-111 on node 1", layout.getDescription());
    }

    @Test
    public void testAcceptsInterleavedCpus() throws IOException {
        // even CPUs on socket 0, odd CPUs on socket 1
        final Fixture fixture = new Fixture(32).allowed("0-31");
        for (int cpu = 0; cpu < 32; cpu++) {
            fixture.cpu(cpu, cpu % 2, cpu % 2);
        }
        final int[] even = new int[16];
        final int[] odd = new int[16];
        for (int i = 0; i < 16; i++) {
            even[i] = 2 * i;
            odd[i] = 2 * i + 1;
        }
        assertAccepted(fixture.read(64), new int[][]{even, odd}, new int[]{0, 1});
    }

    @Test
    public void testAcceptsTwoSockets() throws IOException {
        final CpuTopology.Layout layout = twoSockets(96).read(190);
        assertAccepted(layout, new int[][]{range(0, 95), range(96, 191)}, new int[]{0, 1});
        Assert.assertEquals("socket 0: CPUs 0-95 on node 0, socket 1: CPUs 96-191 on node 1", layout.getDescription());
    }

    @Test
    public void testFormatCpuList() {
        Assert.assertEquals("", CpuTopology.formatCpuList(new int[0]));
        Assert.assertEquals("7", CpuTopology.formatCpuList(new int[]{7}));
        Assert.assertEquals("1,3-5,9", CpuTopology.formatCpuList(new int[]{1, 3, 4, 5, 9}));
        final int[] cpus = new int[160];
        for (int i = 0; i < 96; i++) {
            cpus[i] = i;
        }
        for (int i = 0; i < 64; i++) {
            cpus[96 + i] = 128 + i;
        }
        Assert.assertEquals("0-95,128-191", CpuTopology.formatCpuList(cpus));
    }

    @Test
    public void testIgnoresOfflineCpus() throws IOException {
        // 2 sockets of 16 cores whose SMT siblings, CPUs 32-63, are offline: the affinity mask still
        // names them, but the kernel removed their topology entries
        final Fixture fixture = new Fixture(64).allowed("0-63").online("0-31");
        for (int cpu = 0; cpu < 32; cpu++) {
            fixture.cpu(cpu, cpu / 16, cpu / 16);
        }
        final CpuTopology.Layout layout = fixture.read(64);
        assertAccepted(layout, new int[][]{range(0, 15), range(16, 31)}, new int[]{0, 1});
        Assert.assertEquals("socket 0: CPUs 0-15 on node 0, socket 1: CPUs 16-31 on node 1", layout.getDescription());
        // the same CPUs listed as online have no socket id
        assertRefused(fixture.online("0-63").read(64), "CPU 32 has no valid socket id", true);
    }

    @Test
    public void testOrdersSocketsByLowestCpu() throws IOException {
        // the kernel numbers the socket that holds CPU 0 as socket 1
        final Fixture fixture = new Fixture(32).allowed("0-31");
        for (int cpu = 0; cpu < 32; cpu++) {
            fixture.cpu(cpu, cpu < 16 ? 1 : 0, cpu < 16 ? 0 : 1);
        }
        final CpuTopology.Layout layout = fixture.read(64);
        assertAccepted(layout, new int[][]{range(0, 15), range(16, 31)}, new int[]{1, 0});
        Assert.assertEquals("socket 0: CPUs 16-31 on node 1, socket 1: CPUs 0-15 on node 0", layout.getDescription());
    }

    @Test
    public void testParseCpuList() {
        Assert.assertArrayEquals(new int[0], CpuTopology.parseCpuList(""));
        Assert.assertArrayEquals(new int[]{3}, CpuTopology.parseCpuList("3"));
        Assert.assertArrayEquals(new int[]{0, 1}, CpuTopology.parseCpuList(" 0-1 \n"));
        Assert.assertArrayEquals(new int[]{0, 1, 2, 4}, CpuTopology.parseCpuList("4,0-2,2"));
        final int[] cpus = CpuTopology.parseCpuList("0-95,128-191");
        Assert.assertEquals(160, cpus.length);
        Assert.assertEquals(95, cpus[95]);
        Assert.assertEquals(128, cpus[96]);
        Assert.assertEquals(191, cpus[159]);
        for (String malformed : new String[]{"5-3", "a", "1,,2", "-1", "1-"}) {
            try {
                CpuTopology.parseCpuList(malformed);
                Assert.fail("parsed " + malformed);
            } catch (IllegalArgumentException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().startsWith("malformed CPU list"));
            }
        }
    }

    @Test
    public void testRefusesLopsidedCpuset() throws IOException {
        // 48 and 32 CPUs are 33% apart
        assertRefused(
                twoSockets(96).allowed("0-47,96-127").read(64),
                "socket 0 has 48 allowed CPUs and socket 1 has 32, more than 25% apart",
                true
        );
    }

    @Test
    public void testRefusesManyOneCpuSockets() throws IOException {
        final Fixture fixture = new Fixture(32).allowed("0-31");
        for (int cpu = 0; cpu < 32; cpu++) {
            fixture.cpu(cpu, cpu, 0);
        }
        assertRefused(fixture.read(190), "the allowed CPUs report 32 sockets, more than 8", true);
    }

    @Test
    public void testRefusesMissingAllowedList() throws IOException {
        final Fixture fixture = twoSockets(8);
        fixture.writeStatus("Name:\tjava\n");
        final CpuTopology.Layout layout = fixture.read(64);
        Assert.assertFalse(layout.isAccepted());
        Assert.assertTrue(layout.getRefusal(), layout.getRefusal().startsWith("could not read the allowed CPUs: no Cpus_allowed_list: line"));
    }

    @Test
    public void testRefusesMissingNode() throws IOException {
        final Fixture fixture = twoSockets(96);
        fixture.removeNode(100);
        assertRefused(fixture.read(190), "CPU 100 has no NUMA node", true);
    }

    @Test
    public void testRefusesMissingOnlineList() throws IOException {
        final Fixture fixture = twoSockets(96);
        fixture.removeOnline();
        final CpuTopology.Layout layout = fixture.read(190);
        Assert.assertFalse(layout.isAccepted());
        Assert.assertTrue(layout.isRefusalWarning());
        Assert.assertTrue(layout.getRefusal(), layout.getRefusal().startsWith("could not read the online CPUs: "));
        Assert.assertNull(layout.getSocketCpus());
    }

    @Test
    public void testRefusesMissingSocketId() throws IOException {
        final Fixture fixture = twoSockets(96);
        fixture.removeSocket(5);
        assertRefused(fixture.read(190), "CPU 5 has no valid socket id", true);
    }

    @Test
    public void testRefusesMissingStatusFile() throws IOException {
        final CpuTopology.Layout layout = new CpuTopology(temp.newFolder("noproc").toPath(), temp.newFolder("nosys").toPath()).readLayout(64);
        Assert.assertFalse(layout.isAccepted());
        Assert.assertTrue(layout.isRefusalWarning());
        Assert.assertTrue(layout.getRefusal(), layout.getRefusal().startsWith("could not read the allowed CPUs"));
        Assert.assertNull(layout.getSocketCpus());
    }

    @Test
    public void testRefusesNegativeSocketId() throws IOException {
        final Fixture fixture = twoSockets(96);
        fixture.cpu(7, -1, 0);
        assertRefused(fixture.read(190), "CPU 7 has no valid socket id", true);
    }

    @Test
    public void testRefusesSharedNumaNode() throws IOException {
        final Fixture fixture = new Fixture(32).allowed("0-31");
        for (int cpu = 0; cpu < 32; cpu++) {
            fixture.cpu(cpu, cpu / 16, 0);
        }
        assertRefused(fixture.read(64), "NUMA node 0 holds CPUs of sockets 0 and 1", true);
    }

    @Test
    public void testRefusesSingleSocket() throws IOException {
        final Fixture fixture = new Fixture(32).allowed("0-31");
        for (int cpu = 0; cpu < 32; cpu++) {
            fixture.cpu(cpu, 0, 0);
        }
        // a single socket is a normal setup, so the refusal is not a warning
        assertRefused(fixture.read(64), "the allowed CPUs 0-31 are all on socket 0", false);
    }

    @Test
    public void testRefusesSingleSocketCpuset() throws IOException {
        assertRefused(twoSockets(96).allowed("96-191").read(64), "the allowed CPUs 96-191 are all on socket 1", false);
    }

    @Test
    public void testRefusesSmallPool() throws IOException {
        assertRefused(twoSockets(96).read(16), "16 network workers make groups smaller than 32 workers over 2 sockets", true);
        assertRefused(twoSockets(96).read(63), "63 network workers make groups smaller than 32 workers over 2 sockets", true);
        Assert.assertTrue(twoSockets(96).read(64).isAccepted());
    }

    @Test
    public void testRefusesSocketWithFewCpus() throws IOException {
        final Fixture fixture = new Fixture(32).allowed("0-31");
        for (int cpu = 0; cpu < 32; cpu++) {
            fixture.cpu(cpu, cpu / 4, cpu / 4);
        }
        assertRefused(fixture.read(256), "socket 0 has 4 allowed CPUs, fewer than 8", true);
    }

    @Test
    public void testRefusesWhenNoAllowedCpuIsOnline() throws IOException {
        assertRefused(twoSockets(32).allowed("32-63").online("0-31").read(64), "none of the allowed CPUs 32-63 is online", true);
    }

    @Test
    public void testSingleSocketWithoutNodes() throws IOException {
        // kernels built without NUMA support list no node entries
        final Fixture fixture = new Fixture(16).allowed("0-15");
        for (int cpu = 0; cpu < 16; cpu++) {
            fixture.cpu(cpu, 0, 0);
            fixture.removeNode(cpu);
        }
        final CpuTopology.Layout layout = fixture.read(64);
        assertRefused(layout, "the allowed CPUs 0-15 are all on socket 0", false);
        Assert.assertEquals("socket 0: CPUs 0-15", layout.getDescription());
    }

    private static void assertAccepted(CpuTopology.Layout layout, int[][] expectedCpus, int[] expectedIds) {
        Assert.assertTrue(layout.getRefusal(), layout.isAccepted());
        Assert.assertNull(layout.getRefusal());
        final int[][] cpus = layout.getSocketCpus();
        Assert.assertNotNull(cpus);
        Assert.assertEquals(expectedCpus.length, cpus.length);
        for (int i = 0; i < cpus.length; i++) {
            Assert.assertArrayEquals("socket index " + i, expectedCpus[i], cpus[i]);
        }
        Assert.assertArrayEquals(expectedIds, layout.getSocketIds());
    }

    private static void assertRefused(CpuTopology.Layout layout, String expectedRefusal, boolean isWarning) {
        Assert.assertFalse(layout.isAccepted());
        Assert.assertEquals(expectedRefusal, layout.getRefusal());
        Assert.assertEquals(isWarning, layout.isRefusalWarning());
        Assert.assertNull(layout.getSocketCpus());
        Assert.assertNull(layout.getSocketIds());
    }

    private static int[] range(int from, int to) {
        final int[] cpus = new int[to - from + 1];
        Arrays.setAll(cpus, i -> from + i);
        return cpus;
    }

    // CPUs 0 to 2 * cpusPerSocket - 1, the first half on socket 0 and node 0, the second half on
    // socket 1 and node 1, all allowed
    private Fixture twoSockets(int cpusPerSocket) throws IOException {
        final int cpuCount = 2 * cpusPerSocket;
        final Fixture fixture = new Fixture(cpuCount).allowed("0-" + (cpuCount - 1));
        for (int cpu = 0; cpu < cpuCount; cpu++) {
            fixture.cpu(cpu, cpu / cpusPerSocket, cpu / cpusPerSocket);
        }
        return fixture;
    }

    // A /proc/self/status and a /sys/devices/system/cpu tree in a temporary directory, with all
    // cpuCount CPUs online.
    private class Fixture {
        private final Path cpuDir;
        private final Path procRoot;
        private final Path sysRoot;

        Fixture(int cpuCount) throws IOException {
            final Path root = temp.newFolder().toPath();
            this.procRoot = root.resolve("proc");
            this.sysRoot = root.resolve("sys");
            this.cpuDir = sysRoot.resolve("devices/system/cpu");
            Files.createDirectories(procRoot.resolve("self"));
            for (int cpu = 0; cpu < cpuCount; cpu++) {
                Files.createDirectories(cpuDir.resolve("cpu" + cpu).resolve("topology"));
            }
            online("0-" + (cpuCount - 1));
        }

        Fixture allowed(String list) throws IOException {
            writeStatus("Name:\tjava\nCpus_allowed:\tff\nCpus_allowed_list:\t" + list + "\nMems_allowed_list:\t0-1\n");
            return this;
        }

        void cpu(int cpu, int socket, int node) throws IOException {
            final Path dir = cpuDir.resolve("cpu" + cpu);
            Files.writeString(dir.resolve("topology/physical_package_id"), socket + "\n");
            removeNodeEntries(dir);
            // sysfs has a symlink to the node's directory here; an empty directory reads the same
            Files.createDirectories(dir.resolve("node" + node));
        }

        Fixture online(String list) throws IOException {
            Files.writeString(cpuDir.resolve("online"), list + "\n");
            return this;
        }

        CpuTopology.Layout read(int workerCount) {
            return new CpuTopology(procRoot, sysRoot).readLayout(workerCount);
        }

        void removeNode(int cpu) throws IOException {
            removeNodeEntries(cpuDir.resolve("cpu" + cpu));
        }

        void removeOnline() throws IOException {
            Files.delete(cpuDir.resolve("online"));
        }

        void removeSocket(int cpu) throws IOException {
            Files.delete(cpuDir.resolve("cpu" + cpu).resolve("topology/physical_package_id"));
        }

        void writeStatus(String content) throws IOException {
            Files.writeString(procRoot.resolve("self/status"), content);
        }

        private void removeNodeEntries(Path dir) throws IOException {
            try (var entries = Files.newDirectoryStream(dir, "node*")) {
                for (Path entry : entries) {
                    Files.delete(entry);
                }
            }
        }
    }
}
