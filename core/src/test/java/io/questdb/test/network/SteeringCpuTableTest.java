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

import io.questdb.network.SteeringCpuTable;
import io.questdb.std.CpuTopology;
import org.junit.Assert;
import org.junit.Test;

public class SteeringCpuTableTest {

    @Test
    public void testAlternatingNumbering() {
        final int[] cpus = new int[8];
        final int[] sockets = new int[8];
        for (int i = 0; i < 8; i++) {
            cpus[i] = i;
            sockets[i] = i % 2;
        }
        final SteeringCpuTable table = SteeringCpuTable.build(new int[][]{{0, 2}, {1, 3}}, online(cpus, sockets, 8));
        Assert.assertEquals("0 1 0 1 0 1 0 1", render(table));
        Assert.assertEquals(2, table.getGroupSetCount());
    }

    @Test
    public void testCpuInSeveralSets() {
        // CPU 2 is in both sets, and CPU 4, in neither, maps to both groups on its socket
        final SteeringCpuTable table = SteeringCpuTable.build(
                new int[][]{{0, 1, 2}, {2, 3}},
                online(new int[]{0, 1, 2, 3, 4}, new int[]{0, 0, 0, 0, 0}, 5)
        );
        Assert.assertEquals("0 0 0,1 1 0,1", render(table));
        Assert.assertEquals(3, table.getGroupSetCount());
    }

    @Test
    public void testCpuPastTheTable() {
        final SteeringCpuTable table = SteeringCpuTable.build(
                new int[][]{{0}, {1}},
                online(new int[]{0, 1}, new int[]{0, 1}, 2)
        );
        Assert.assertEquals(2, table.size());
        Assert.assertEquals(SteeringCpuTable.NO_GROUP, table.getGroupSet(2));
        Assert.assertEquals(SteeringCpuTable.NO_GROUP, table.getGroupSet(4000));
    }

    @Test
    public void testEmpty() {
        Assert.assertEquals(0, SteeringCpuTable.EMPTY.size());
        Assert.assertEquals(0, SteeringCpuTable.EMPTY.getGroupSetCount());
        Assert.assertEquals(SteeringCpuTable.NO_GROUP, SteeringCpuTable.EMPTY.getGroupSet(0));
    }

    @Test
    public void testExplicitSetsOnOneSocket() {
        // tests pin groups to CPUs of one socket; the socket's other CPUs map to both groups
        final SteeringCpuTable table = SteeringCpuTable.build(
                new int[][]{{0, 1}, {2, 3}},
                online(new int[]{0, 1, 2, 3, 4, 5}, new int[]{0, 0, 0, 0, 0, 0}, 8)
        );
        Assert.assertEquals("0 0 1 1 0,1 0,1 x x", render(table));
    }

    @Test
    public void testGroupsSharingAlternatingSockets() {
        // two groups per socket over CPUs numbered alternately across the sockets
        final int[] cpus = new int[16];
        final int[] sockets = new int[16];
        for (int i = 0; i < 16; i++) {
            cpus[i] = i;
            sockets[i] = i % 2;
        }
        final int[] socket0 = {0, 2, 4, 6, 8, 10, 12, 14};
        final int[] socket1 = {1, 3, 5, 7, 9, 11, 13, 15};
        final SteeringCpuTable table = SteeringCpuTable.build(
                new int[][]{socket0, socket0, socket1, socket1},
                online(cpus, sockets, 16)
        );
        Assert.assertEquals("0,1 2,3 0,1 2,3 0,1 2,3 0,1 2,3 0,1 2,3 0,1 2,3 0,1 2,3 0,1 2,3", render(table));
        Assert.assertEquals(2, table.getGroupSetCount());
    }

    @Test
    public void testGroupsSharingASocket() {
        // the split of 4 groups over 2 sockets gives the groups on each socket the socket's allowed
        // CPUs, 0-3 and 8-11; the socket's other CPUs take interrupts and map to the same groups
        final int[] socket0 = {0, 1, 2, 3};
        final int[] socket1 = {8, 9, 10, 11};
        final SteeringCpuTable table = SteeringCpuTable.build(
                new int[][]{socket0, socket0, socket1, socket1},
                online(
                        new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
                        new int[]{0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1},
                        16
                )
        );
        Assert.assertEquals("0,1 0,1 0,1 0,1 0,1 0,1 0,1 0,1 2,3 2,3 2,3 2,3 2,3 2,3 2,3 2,3", render(table));
        Assert.assertEquals(2, table.getGroupSetCount());
        Assert.assertEquals(0, table.getGroupSet(0));
        Assert.assertEquals(1, table.getGroupSet(15));
        Assert.assertArrayEquals(new int[]{0, 1}, table.getGroups(0));
        Assert.assertArrayEquals(new int[]{2, 3}, table.getGroups(1));
    }

    @Test
    public void testOfflineCpus() {
        // CPU 1 went offline after the split; CPUs 6 and 7 are possible but not online
        final SteeringCpuTable table = SteeringCpuTable.build(
                new int[][]{{0, 1, 2}, {3, 4, 5}},
                online(new int[]{0, 2, 3, 4, 5}, new int[]{0, 0, 1, 1, 1}, 8)
        );
        Assert.assertEquals("0 x 0 1 1 1 x x", render(table));
    }

    @Test
    public void testSocketWideMapping() {
        // the process may use CPUs 0-3 and 8-11; 4-7 on socket 0 and 12-15 on socket 1 take interrupts
        final SteeringCpuTable table = SteeringCpuTable.build(
                new int[][]{{0, 1, 2, 3}, {8, 9, 10, 11}},
                online(
                        new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
                        new int[]{0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1},
                        16
                )
        );
        Assert.assertEquals("0 0 0 0 0 0 0 0 1 1 1 1 1 1 1 1", render(table));
    }

    @Test
    public void testSocketWithoutGroup() {
        // socket 2 hosts no group, so its CPUs map to no group
        final SteeringCpuTable table = SteeringCpuTable.build(
                new int[][]{{0, 1}, {2, 3}},
                online(new int[]{0, 1, 2, 3, 4, 5}, new int[]{0, 0, 1, 1, 2, 2}, 6)
        );
        Assert.assertEquals("0 0 1 1 - -", render(table));
    }

    private static CpuTopology.OnlineCpus online(int[] cpus, int[] sockets, int possibleCount) {
        return new CpuTopology.OnlineCpus(cpus, sockets, possibleCount);
    }

    // each CPU's groups, such as 0,1, or - for no group and x for an offline CPU
    private static String render(SteeringCpuTable table) {
        final StringBuilder sb = new StringBuilder();
        for (int cpu = 0; cpu < table.size(); cpu++) {
            if (cpu > 0) {
                sb.append(' ');
            }
            final int groupSet = table.getGroupSet(cpu);
            if (groupSet == SteeringCpuTable.NO_GROUP) {
                sb.append('-');
            } else if (groupSet == SteeringCpuTable.OFFLINE) {
                sb.append('x');
            } else {
                final int[] groups = table.getGroups(groupSet);
                for (int i = 0; i < groups.length; i++) {
                    if (i > 0) {
                        sb.append(',');
                    }
                    sb.append(groups[i]);
                }
            }
        }
        return sb.toString();
    }
}
