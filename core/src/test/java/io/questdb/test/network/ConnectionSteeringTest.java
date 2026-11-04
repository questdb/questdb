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


package io.questdb.test.network;

import io.questdb.log.Log;
import io.questdb.metrics.ConnectionSteeringMetrics;
import io.questdb.network.ConnectionSteering;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.network.ReceiveLayout;
import io.questdb.std.CpuTopology;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.ObjList;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.ArrayDeque;

import static io.questdb.network.ConnectionSteering.*;

public class ConnectionSteeringTest {
    private static final int ENOMEM = 12;
    private static final int ENOPROTOOPT = 92;
    @Rule
    public TemporaryFolder temp = new TemporaryFolder();
    private SysfsFixture fixture;
    private FakeFacade nf;

    @Before
    public void setUp() {
        fixture = new SysfsFixture(temp.getRoot().toPath());
        nf = new FakeFacade();
    }

    @Test
    public void testAttachFailureFallsBackToHashAndRetries() {
        twoSockets(8, 8);
        try (ConnectionSteering steering = create(2, 5_000, "6.8.0")) {
            listenAll(steering, 2);
            // the steered program fails, the out-of-range program attaches
            nf.attachResults.add(ENOMEM);
            steering.check();
            Assert.assertEquals(REASON_ATTACH_FAILING, steering.getReason());
            Assert.assertEquals(1, nf.lastProgram().size());
            Assert.assertEquals(-1, CbpfInterpreter.run(nf.lastProgram(), 0, 0));
            Assert.assertEquals(0, nf.detachCount);

            // the next check retries
            steering.check();
            Assert.assertEquals(REASON_NONE, steering.getReason());
            Assert.assertEquals(0, CbpfInterpreter.run(nf.lastProgram(), 0, 0));
        }
    }

    @Test
    public void testAttachFailuresDetach() {
        twoSockets(8, 8);
        try (ConnectionSteering steering = create(2, 5_000, "6.8.0")) {
            listenAll(steering, 2);
            nf.attachResults.add(ENOMEM);
            nf.attachResults.add(ENOMEM);
            steering.check();
            Assert.assertEquals(REASON_ATTACH_FAILING, steering.getReason());
            Assert.assertEquals(1, nf.detachCount);
        }
    }

    @Test
    public void testAttachFailuresKeepProgramBeforeLinux53() {
        twoSockets(8, 8);
        try (ConnectionSteering steering = create(2, 5_000, "5.2.21")) {
            listenAll(steering, 2);
            nf.attachResults.add(ENOMEM);
            nf.attachResults.add(ENOMEM);
            steering.check();
            Assert.assertEquals(REASON_ATTACH_FAILING, steering.getReason());
            Assert.assertEquals(0, nf.detachCount);
        }
    }

    @Test
    public void testClosedGroupRedirectsToAcceptingGroups() {
        threeSockets();
        try (ConnectionSteering steering = create(3, 5_000, "6.8.0")) {
            listenAll(steering, 3);
            steering.check();
            steering.closeListener(0, fd(0));
            // the order is [2, 1]; group 0's CPUs pick listener 0 or 1 at random
            assertOrder(steering, 2, 1);
            final LongList program = nf.lastProgram();
            Assert.assertEquals(0, CbpfInterpreter.run(program, 0, 0));
            Assert.assertEquals(1, CbpfInterpreter.run(program, 0, 1));
            Assert.assertEquals(1, CbpfInterpreter.run(program, 8, 0));
            Assert.assertEquals(0, CbpfInterpreter.run(program, 16, 0));
        }
    }

    @Test
    public void testCloseMovesTheLastListenerIntoTheFreedSlot() {
        threeSockets();
        try (ConnectionSteering steering = create(3, 5_000, "6.8.0")) {
            listenAll(steering, 3);
            assertOrder(steering, 0, 1, 2);
            steering.closeListener(0, fd(0));
            assertOrder(steering, 2, 1);
            Assert.assertEquals(0, steering.listen(0, fd(0) + 100, 16));
            assertOrder(steering, 2, 1, 0);
            steering.closeListener(1, fd(1));
            assertOrder(steering, 2, 0);
            steering.closeListener(2, fd(2));
            assertOrder(steering, 0);
        }
    }

    @Test
    public void testClosingStopsListensAndRebuilds() {
        twoSockets(8, 8);
        final ConnectionSteering steering = create(2, 5_000, "6.8.0");
        listenAll(steering, 2);
        steering.check();
        final int attaches = nf.attachCount;
        steering.close();
        Assert.assertEquals(LISTEN_CLOSING, steering.listen(0, 999, 16));
        steering.closeListener(1, fd(1));
        Assert.assertEquals(attaches, nf.attachCount);
        Assert.assertTrue(nf.isClosed(fd(1)));
        Assert.assertEquals(REASON_SHUT_DOWN, steering.getReason());
        // a second close does nothing
        steering.close();
    }

    @Test
    public void testExceedsGuard() {
        // the rule can't trip below 26 connections
        Assert.assertFalse(ConnectionSteering.exceedsGuard(25, 25, 2));
        Assert.assertTrue(ConnectionSteering.exceedsGuard(26, 26, 2));
        // 0.6 * 128 + 2 * sqrt(128) = 99.4
        Assert.assertFalse(ConnectionSteering.exceedsGuard(99, 128, 2));
        Assert.assertTrue(ConnectionSteering.exceedsGuard(100, 128, 2));
        // three groups: 0.4 * 300 + 2 * sqrt(300) = 154.6
        Assert.assertFalse(ConnectionSteering.exceedsGuard(154, 300, 3));
        Assert.assertTrue(ConnectionSteering.exceedsGuard(155, 300, 3));
    }

    @Test
    public void testFailedListenLeavesOrderUnchanged() {
        twoSockets(8, 8);
        try (ConnectionSteering steering = create(2, 5_000, "6.8.0")) {
            Assert.assertEquals(0, steering.listen(0, fd(0), 16));
            nf.listenResult = 98;
            Assert.assertEquals(98, steering.listen(1, fd(1), 16));
            assertOrder(steering, 0);
            nf.listenResult = 0;
            Assert.assertEquals(0, steering.listen(1, fd(1), 16));
            assertOrder(steering, 0, 1);
        }
    }

    @Test
    public void testFirstAttachRefusedMeansUnsupported() {
        twoSockets(8, 8);
        nf.attachResults.add(ENOPROTOOPT);
        try (ConnectionSteering steering = create(2, 5_000, "6.8.0")) {
            listenAll(steering, 2);
            Assert.assertEquals(REASON_UNSUPPORTED, steering.getReason());
            final int attaches = nf.attachCount;
            steering.check();
            steering.closeListener(0, fd(0));
            Assert.assertEquals(attaches, nf.attachCount);
            Assert.assertEquals(REASON_UNSUPPORTED, steering.getReason());
        }
    }

    @Test
    public void testGuardCountsOnlyConnectionsSinceSteeringTurnedOn() {
        twoSockets(8, 8);
        try (ConnectionSteering steering = create(2, 5_000, "6.8.0")) {
            listenAll(steering, 2);
            // accepted before the first check, while steering is off
            final int early = steering.onAccepted(0);
            steering.check();
            Assert.assertEquals(REASON_NONE, steering.getReason());
            Assert.assertEquals(0, steering.getGuardCount(0));
            Assert.assertEquals(1, steering.getOpenCount(0));
            final int stamp = steering.onAccepted(0);
            Assert.assertEquals(1, steering.getGuardCount(0));
            // the early connection's disconnect leaves the guard's count alone
            steering.onDisconnected(0, early);
            Assert.assertEquals(1, steering.getGuardCount(0));
            Assert.assertEquals(1, steering.getOpenCount(0));
            steering.onDisconnected(0, stamp);
            Assert.assertEquals(0, steering.getGuardCount(0));
            Assert.assertEquals(0, steering.getOpenCount(0));
        }
    }

    @Test
    public void testGuardTripLastsUntilTheLayoutChanges() {
        twoSockets(8, 8);
        try (ConnectionSteering steering = create(2, 5_000, "6.8.0")) {
            listenAll(steering, 2);
            steering.check();
            Assert.assertEquals(REASON_NONE, steering.getReason());
            // 0.6 * 120 + 2 * sqrt(120) = 93.9, so 90 of 120 doesn't trip the guard, and 100 does
            for (int i = 0; i < 90; i++) {
                steering.onAccepted(0);
            }
            final int[] stamps = new int[30];
            for (int i = 0; i < 30; i++) {
                stamps[i] = steering.onAccepted(1);
            }
            steering.check();
            Assert.assertEquals(REASON_NONE, steering.getReason());
            for (int i = 0; i < 10; i++) {
                steering.onAccepted(0);
                steering.onDisconnected(1, stamps[i]);
            }
            steering.check();
            Assert.assertEquals(REASON_GUARD_TRIPPED, steering.getReason());
            Assert.assertEquals(-1, CbpfInterpreter.run(nf.lastProgram(), 0, 0));

            // the share returns to even under the hash, but the layout hasn't changed
            for (int i = 0; i < 60; i++) {
                steering.onAccepted(1);
            }
            steering.check();
            Assert.assertEquals(REASON_GUARD_TRIPPED, steering.getReason());

            // an interrupt moving within its socket isn't a layout change
            fixture.effectiveAffinity(100, "3");
            steering.check();
            Assert.assertEquals(REASON_GUARD_TRIPPED, steering.getReason());

            // a different prediction is
            fixture.effectiveAffinity(100, "9");
            steering.check();
            Assert.assertEquals(REASON_NONE, steering.getReason());
            Assert.assertEquals(0, steering.getGuardCount(0));
            Assert.assertEquals(0, CbpfInterpreter.run(nf.lastProgram(), 0, 0));
        }
    }

    @Test
    public void testIsKernelAtLeast() {
        Assert.assertTrue(ConnectionSteering.isKernelAtLeast("4.6.0", 4, 6));
        Assert.assertTrue(ConnectionSteering.isKernelAtLeast("4.18.0-553.el8_10.x86_64", 4, 6));
        Assert.assertTrue(ConnectionSteering.isKernelAtLeast("6.10.14-linuxkit", 5, 3));
        Assert.assertFalse(ConnectionSteering.isKernelAtLeast("4.5.7", 4, 6));
        Assert.assertFalse(ConnectionSteering.isKernelAtLeast("3.10.0-1160.el7.x86_64", 4, 6));
        Assert.assertFalse(ConnectionSteering.isKernelAtLeast("5.2.21", 5, 3));
        Assert.assertFalse(ConnectionSteering.isKernelAtLeast(null, 4, 6));
        Assert.assertFalse(ConnectionSteering.isKernelAtLeast("garbage", 4, 6));
    }

    @Test
    public void testLayoutCheckFails() {
        twoSockets(15, 1);
        try (ConnectionSteering steering = create(2, 5_000, "6.8.0")) {
            listenAll(steering, 2);
            steering.check();
            Assert.assertEquals(REASON_LAYOUT_CHECK_FAILED, steering.getReason());
            Assert.assertEquals(-1, CbpfInterpreter.run(nf.lastProgram(), 0, 0));
            Assert.assertEquals(-1, CbpfInterpreter.run(nf.lastProgram(), 9, 0));
        }
    }

    @Test
    public void testLayoutCheckLimit() {
        // 9 and 7 is 56.25%, under the limit of 60%; 10 and 6 is 62.5%, over it
        twoSockets(9, 7);
        try (ConnectionSteering steering = create(2, 5_000, "6.8.0")) {
            listenAll(steering, 2);
            steering.check();
            Assert.assertEquals(REASON_NONE, steering.getReason());
            fixture.effectiveAffinity(109, "1");
            steering.check();
            Assert.assertEquals(REASON_LAYOUT_CHECK_FAILED, steering.getReason());
        }
    }

    @Test
    public void testLayoutUnreadable() {
        fixture.cpus(sockets(8, 8), 16);
        fixture.virtualNic("lo");
        try (ConnectionSteering steering = create(2, 5_000, "6.8.0")) {
            listenAll(steering, 2);
            steering.check();
            Assert.assertEquals(REASON_LAYOUT_UNREADABLE, steering.getReason());
        }
    }

    @Test
    public void testListenOrderAndProgram() {
        twoSockets(8, 8);
        try (ConnectionSteering steering = create(2, 5_000, "6.8.0")) {
            listenAll(steering, 2);
            Assert.assertEquals(REASON_NOT_CHECKED, steering.getReason());
            steering.check();
            Assert.assertEquals(REASON_NONE, steering.getReason());
            assertSteers(nf.lastProgram(), 0, 1);

            // group 0 closes at its limit: its CPUs go to the only listener left
            steering.closeListener(0, fd(0));
            assertOrder(steering, 1);
            Assert.assertEquals(0, CbpfInterpreter.run(nf.lastProgram(), 0, 0));
            Assert.assertEquals(0, CbpfInterpreter.run(nf.lastProgram(), 9, 0));

            // it reopens at the end of the order
            Assert.assertEquals(0, steering.listen(0, fd(0) + 100, 16));
            assertOrder(steering, 1, 0);
            assertSteers(nf.lastProgram(), 1, 0);
        }
    }

    @Test
    public void testMetrics() {
        twoSockets(8, 8);
        final ConnectionSteeringMetrics metrics = new ConnectionSteeringMetrics();
        final int[][] groupCpus = {{0, 1, 2, 3, 4, 5, 6, 7}, {8, 9, 10, 11, 12, 13, 14, 15}};
        try (DirectUtf8Sink sink = new DirectUtf8Sink(4096)) {
            try (ConnectionSteering steering = new ConnectionSteering("http-server", 2, groupCpus, nf, topology(), layout(), 0, 5_000, "6.8.0", metrics)) {
                listenAll(steering, 2);
                steering.check();
                steering.onAccepted(0);
                steering.onAccepted(0);
                steering.onAccepted(1);
                metrics.scrapeIntoPrometheus(sink);
                TestUtils.assertEquals(
                        """
                                # TYPE questdb_connection_steering_state gauge
                                questdb_connection_steering_state{server="http-server",state="attached"} 1
                                questdb_connection_steering_state{server="http-server",state="attach_failing"} 0
                                questdb_connection_steering_state{server="http-server",state="guard_tripped"} 0
                                questdb_connection_steering_state{server="http-server",state="layout_check_failed"} 0
                                questdb_connection_steering_state{server="http-server",state="layout_unreadable"} 0
                                questdb_connection_steering_state{server="http-server",state="not_checked"} 0
                                questdb_connection_steering_state{server="http-server",state="no_cpu_sets"} 0
                                questdb_connection_steering_state{server="http-server",state="program_too_large"} 0
                                questdb_connection_steering_state{server="http-server",state="queue_timeout_off"} 0
                                questdb_connection_steering_state{server="http-server",state="shut_down"} 0
                                questdb_connection_steering_state{server="http-server",state="unsupported"} 0

                                # TYPE questdb_connection_steering_attaches_total counter
                                questdb_connection_steering_attaches_total{server="http-server"} 3

                                # TYPE questdb_connection_steering_open_connections gauge
                                questdb_connection_steering_open_connections{server="http-server",group="0"} 2
                                questdb_connection_steering_open_connections{server="http-server",group="1"} 1

                                # TYPE questdb_connection_steering_guard_connections gauge
                                questdb_connection_steering_guard_connections{server="http-server",group="0"} 2
                                questdb_connection_steering_guard_connections{server="http-server",group="1"} 1

                                """,
                        sink.toString()
                );
            }
            // closing unregisters the port
            sink.clear();
            metrics.scrapeIntoPrometheus(sink);
            Assert.assertEquals(0, sink.size());
        }
    }

    @Test
    public void testNoCpuSets() {
        twoSockets(8, 8);
        try (ConnectionSteering steering = new ConnectionSteering("test", 2, null, nf, topology(), layout(), 0, 5_000, "6.8.0", null)) {
            listenAll(steering, 2);
            steering.check();
            Assert.assertEquals(REASON_NO_CPU_SETS, steering.getReason());
            Assert.assertEquals(0, nf.attachCount);
        }
    }

    @Test
    public void testOldKernel() {
        twoSockets(8, 8);
        try (ConnectionSteering steering = create(2, 5_000, "4.5.7")) {
            listenAll(steering, 2);
            steering.check();
            Assert.assertEquals(REASON_UNSUPPORTED, steering.getReason());
            Assert.assertEquals(0, nf.attachCount);
        }
    }

    @Test
    public void testOnlineCpusChangeRebuildsTheTable() {
        twoSockets(8, 8);
        try (ConnectionSteering steering = create(2, 5_000, "6.8.0")) {
            listenAll(steering, 2);
            steering.check();
            // CPU 16 doesn't exist yet: the hash places its SYNs
            Assert.assertEquals(-1, CbpfInterpreter.run(nf.lastProgram(), 16, 0));
            // CPU 16 comes online on socket 1
            final int[] sockets = new int[17];
            System.arraycopy(sockets(8, 8), 0, sockets, 0, 16);
            sockets[16] = 1;
            fixture.cpus(sockets, 17);
            steering.check();
            Assert.assertEquals(1, CbpfInterpreter.run(nf.lastProgram(), 16, 0));
        }
    }

    @Test
    public void testProgramTooLarge() {
        twoSockets(8, 8);
        fixture.optmemMax(300);
        try (ConnectionSteering steering = create(2, 5_000, "6.8.0")) {
            listenAll(steering, 2);
            steering.check();
            Assert.assertEquals(REASON_PROGRAM_TOO_LARGE, steering.getReason());
            Assert.assertEquals(-1, CbpfInterpreter.run(nf.lastProgram(), 0, 0));
        }
    }

    @Test
    public void testQueueTimeoutOff() {
        twoSockets(8, 8);
        try (ConnectionSteering steering = create(2, 0, "6.8.0")) {
            listenAll(steering, 2);
            steering.check();
            Assert.assertEquals(REASON_QUEUE_TIMEOUT_OFF, steering.getReason());
            Assert.assertEquals(0, nf.attachCount);
        }
    }

    @Test
    public void testRedirectAfterTheDelay() {
        twoSockets(8, 8);
        try (ConnectionSteering steering = create(2, 5_000, "6.8.0")) {
            listenAll(steering, 2);
            steering.check();
            final int attaches = nf.attachCount;

            // pauses shorter than the delay attach nothing
            for (int i = 0; i < 100; i++) {
                steering.onPause(0, 1_000 + i * 10);
                steering.tick(0, 1_000 + i * 10 + 5);
                steering.onResume(0, 1_000 + i * 10 + 6);
                steering.tick(0, 1_000 + i * 10 + 7);
            }
            Assert.assertEquals(attaches, nf.attachCount);

            steering.onPause(0, 10_000);
            steering.tick(0, 10_999);
            Assert.assertFalse(isRedirected(steering, 0));
            steering.tick(0, 11_000);
            Assert.assertTrue(isRedirected(steering, 0));
            Assert.assertEquals(attaches + 1, nf.attachCount);
            // group 0's CPUs go to group 1's listener
            Assert.assertEquals(1, CbpfInterpreter.run(nf.lastProgram(), 0, 0));
            Assert.assertEquals(1, CbpfInterpreter.run(nf.lastProgram(), 9, 0));

            // accepting again ends the redirect after the delay
            steering.onResume(0, 12_000);
            steering.tick(0, 12_500);
            Assert.assertTrue(isRedirected(steering, 0));
            steering.tick(0, 13_000);
            Assert.assertFalse(isRedirected(steering, 0));
            assertSteers(nf.lastProgram(), 0, 1);
        }
    }

    @Test
    public void testRedirectDelayIsHalfAShortQueueTimeout() {
        twoSockets(8, 8);
        try (ConnectionSteering steering = create(2, 400, "6.8.0")) {
            listenAll(steering, 2);
            steering.check();
            steering.onPause(0, 1_000);
            steering.tick(0, 1_199);
            Assert.assertFalse(isRedirected(steering, 0));
            steering.tick(0, 1_200);
            Assert.assertTrue(isRedirected(steering, 0));
        }
    }

    @Test
    public void testRedirectSurvivesAnotherGroupsReopen() {
        threeSockets();
        try (ConnectionSteering steering = create(3, 5_000, "6.8.0")) {
            listenAll(steering, 3);
            steering.check();
            steering.onPause(0, 0);
            steering.tick(0, 1_000);
            // group 1 closes and reopens: the order is [0, 2, 1]
            steering.closeListener(1, fd(1));
            Assert.assertEquals(0, steering.listen(1, fd(1) + 100, 16));
            assertOrder(steering, 0, 2, 1);
            final LongList program = nf.lastProgram();
            for (int random = 0; random < 10; random++) {
                final int index = CbpfInterpreter.run(program, 0, random);
                Assert.assertTrue("index " + index, index == 1 || index == 2);
            }
            Assert.assertEquals(2, CbpfInterpreter.run(program, 8, 0));
            Assert.assertEquals(1, CbpfInterpreter.run(program, 16, 0));
        }
    }

    @Test
    public void testRedirectWhenEveryGroupIsAtItsLimit() {
        twoSockets(8, 8);
        try (ConnectionSteering steering = create(2, 5_000, "6.8.0")) {
            listenAll(steering, 2);
            steering.check();
            steering.onPause(0, 0);
            steering.onPause(1, 0);
            steering.tick(0, 1_000);
            steering.tick(1, 1_000);
            Assert.assertTrue(isRedirected(steering, 0));
            Assert.assertTrue(isRedirected(steering, 1));
            // with nowhere to go, each group keeps its own listener
            assertSteers(nf.lastProgram(), 0, 1);
        }
    }

    @Test
    public void testStartRunsTheFirstCheck() {
        twoSockets(8, 8);
        final ConnectionSteering steering = create(2, 5_000, "6.8.0");
        try {
            listenAll(steering, 2);
            steering.start();
            Assert.assertEquals(REASON_NONE, steering.getReason());
        } finally {
            steering.close();
        }
    }

    private static void assertOrder(ConnectionSteering steering, int... groups) {
        final StringBuilder actual = new StringBuilder();
        final IntList order = steering.getOrder();
        for (int i = 0, n = order.size(); i < n; i++) {
            actual.append(order.getQuick(i)).append(' ');
        }
        final StringBuilder expected = new StringBuilder();
        for (int g : groups) {
            expected.append(g).append(' ');
        }
        Assert.assertEquals(expected.toString(), actual.toString());
    }

    // socket 0's CPUs reach listener index0, socket 1's index1
    private static void assertSteers(LongList program, int index0, int index1) {
        for (int cpu = 0; cpu < 8; cpu++) {
            Assert.assertEquals("cpu " + cpu, index0, CbpfInterpreter.run(program, cpu, 0));
        }
        for (int cpu = 8; cpu < 16; cpu++) {
            Assert.assertEquals("cpu " + cpu, index1, CbpfInterpreter.run(program, cpu, 0));
        }
    }

    private static long fd(int group) {
        return 1_000 + group;
    }

    private static boolean isRedirected(ConnectionSteering steering, int group) {
        return steering.isRedirected(group);
    }

    private static void listenAll(ConnectionSteering steering, int groupCount) {
        for (int g = 0; g < groupCount; g++) {
            Assert.assertEquals(0, steering.listen(g, fd(g), 16));
        }
    }

    private static int[] sockets(int socket0, int socket1) {
        final int[] sockets = new int[socket0 + socket1];
        for (int i = socket0; i < sockets.length; i++) {
            sockets[i] = 1;
        }
        return sockets;
    }

    private ConnectionSteering create(int groupCount, long queueTimeoutMs, String kernel) {
        final int[][] groupCpus = new int[groupCount][];
        for (int g = 0; g < groupCount; g++) {
            groupCpus[g] = new int[8];
            for (int i = 0; i < 8; i++) {
                groupCpus[g][i] = g * 8 + i;
            }
        }
        return new ConnectionSteering("test", groupCount, groupCpus, nf, topology(), layout(), 0, queueTimeoutMs, kernel, null);
    }

    private ReceiveLayout layout() {
        return new ReceiveLayout(fixture.getProcRoot(), fixture.getSysRoot(), _ -> null);
    }

    // three sockets of 8 CPUs, and a NIC with one interrupt on each
    private void threeSockets() {
        final int[] sockets = new int[24];
        for (int i = 0; i < 24; i++) {
            sockets[i] = i / 8;
        }
        fixture.cpus(sockets, 24);
        fixture.nic("eth0", 3).irq("eth0", 100, "0").irq("eth0", 101, "8").irq("eth0", 102, "16");
    }

    private CpuTopology topology() {
        return new CpuTopology(fixture.getProcRoot(), fixture.getSysRoot());
    }

    // two sockets of 8 CPUs, and a NIC with interrupts 100 onwards on CPU 0 for socket 0 and CPU 9
    // for socket 1
    private void twoSockets(int interruptsOnSocket0, int interruptsOnSocket1) {
        fixture.cpus(sockets(8, 8), 16);
        final int count = interruptsOnSocket0 + interruptsOnSocket1;
        fixture.nic("eth0", count);
        for (int i = 0; i < count; i++) {
            fixture.irq("eth0", 100 + i, i < interruptsOnSocket0 ? "0" : "9");
        }
    }

    private static class FakeFacade extends NetworkFacadeImpl {
        private final ArrayDeque<Integer> attachResults = new ArrayDeque<>();
        private final LongList closed = new LongList();
        private final ObjList<LongList> programs = new ObjList<>();
        private int attachCount;
        private int detachCount;
        private int listenResult;

        @Override
        public int attachReuseportProgram(long fd, LongList program) {
            final Integer result = attachResults.poll();
            final int rc = result != null ? result : 0;
            if (rc == 0) {
                attachCount++;
                programs.add(new LongList(program));
            }
            return rc;
        }

        @Override
        public void close(long fd, Log logger) {
            closed.add(fd);
        }

        @Override
        public int detachReuseportProgram(long fd) {
            detachCount++;
            return 0;
        }

        @Override
        public int listen(long serverFd, int backlog) {
            return listenResult;
        }

        boolean isClosed(long fd) {
            for (int i = 0, n = closed.size(); i < n; i++) {
                if (closed.getQuick(i) == fd) {
                    return true;
                }
            }
            return false;
        }

        LongList lastProgram() {
            return programs.getLast();
        }
    }
}
