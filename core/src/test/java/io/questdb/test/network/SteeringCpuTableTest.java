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

import static io.questdb.network.SteeringCpuTable.NO_GROUP;
import static io.questdb.network.SteeringCpuTable.OFFLINE;

public class SteeringCpuTableTest {

    @Test
    public void testAlternatingNumbering() {
        final int[] cpus = new int[8];
        final int[] sockets = new int[8];
        for (int i = 0; i < 8; i++) {
            cpus[i] = i;
            sockets[i] = i % 2;
        }
        final int[] table = SteeringCpuTable.build(new int[][]{{0, 2}, {1, 3}}, online(cpus, sockets, 8));
        Assert.assertArrayEquals(new int[]{0, 1, 0, 1, 0, 1, 0, 1}, table);
    }

    @Test
    public void testCpuInSeveralSetsMapsBySocket() {
        // CPU 2 is in both sets on socket 0, which then hosts both groups
        final int[] table = SteeringCpuTable.build(
                new int[][]{{0, 1, 2}, {2, 3}},
                online(new int[]{0, 1, 2, 3, 4}, new int[]{0, 0, 0, 0, 0}, 5)
        );
        Assert.assertArrayEquals(new int[]{0, 0, NO_GROUP, 1, NO_GROUP}, table);
    }

    @Test
    public void testExplicitSetsOnOneSocket() {
        // tests pin groups to CPUs of one socket; the socket's other CPUs map to no group
        final int[] table = SteeringCpuTable.build(
                new int[][]{{0, 1}, {2, 3}},
                online(new int[]{0, 1, 2, 3, 4, 5}, new int[]{0, 0, 0, 0, 0, 0}, 8)
        );
        Assert.assertArrayEquals(new int[]{0, 0, 1, 1, NO_GROUP, NO_GROUP, OFFLINE, OFFLINE}, table);
    }

    @Test
    public void testOfflineCpus() {
        // CPU 1 went offline after the split; CPUs 6 and 7 are possible but not online
        final int[] table = SteeringCpuTable.build(
                new int[][]{{0, 1, 2}, {3, 4, 5}},
                online(new int[]{0, 2, 3, 4, 5}, new int[]{0, 0, 1, 1, 1}, 8)
        );
        Assert.assertArrayEquals(new int[]{0, OFFLINE, 0, 1, 1, 1, OFFLINE, OFFLINE}, table);
    }

    @Test
    public void testSocketWideMapping() {
        // the process may use CPUs 0-3 and 8-11; 4-7 on socket 0 and 12-15 on socket 1 take interrupts
        final int[] table = SteeringCpuTable.build(
                new int[][]{{0, 1, 2, 3}, {8, 9, 10, 11}},
                online(
                        new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
                        new int[]{0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1},
                        16
                )
        );
        Assert.assertArrayEquals(new int[]{0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1}, table);
    }

    @Test
    public void testSocketWithoutGroup() {
        // socket 2 hosts no group, so its CPUs map to no group
        final int[] table = SteeringCpuTable.build(
                new int[][]{{0, 1}, {2, 3}},
                online(new int[]{0, 1, 2, 3, 4, 5}, new int[]{0, 0, 1, 1, 2, 2}, 6)
        );
        Assert.assertArrayEquals(new int[]{0, 0, 1, 1, NO_GROUP, NO_GROUP}, table);
    }

    private static CpuTopology.OnlineCpus online(int[] cpus, int[] sockets, int possibleCount) {
        return new CpuTopology.OnlineCpus(cpus, sockets, possibleCount);
    }
}
