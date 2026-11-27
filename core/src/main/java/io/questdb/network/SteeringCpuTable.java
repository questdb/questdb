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


package io.questdb.network;

import io.questdb.std.CpuTopology;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

import java.util.Arrays;

/**
 * Maps each CPU number to the group set whose listeners should take the connections whose SYN that
 * CPU processes. A group set is the ascending list of the worker groups among which the steering
 * program picks at random; CPUs that map to the same groups share one group set.
 * <ul>
 *     <li>A CPU in the CPU sets of one or more groups maps to those groups.</li>
 *     <li>Any other online CPU maps to the groups on its socket, where a group's sockets are those
 *     of its online CPUs. In production the set of every group on a socket is that socket's allowed
 *     CPUs, so this maps the CPUs of the socket that the process may not use, where interrupts and
 *     RPS can still run.</li>
 *     <li>Any other online CPU maps to {@link #NO_GROUP}, and an offline CPU to {@link #OFFLINE}.</li>
 * </ul>
 */
public final class SteeringCpuTable {
    public static final SteeringCpuTable EMPTY = new SteeringCpuTable(new int[0], new ObjList<>());
    public static final int NO_GROUP = -1;
    public static final int OFFLINE = -3;
    private final int[] cpuSets;
    private final ObjList<int[]> groupSets;

    private SteeringCpuTable(int[] cpuSets, ObjList<int[]> groupSets) {
        this.cpuSets = cpuSets;
        this.groupSets = groupSets;
    }

    /**
     * Builds the table over every possible CPU number and every CPU of the groups' sets.
     */
    public static SteeringCpuTable build(int[][] groupCpus, CpuTopology.OnlineCpus online) {
        final int[] onlineCpus = online.getCpus();
        final int[] onlineSockets = online.getSockets();
        final int groupCount = groupCpus.length;
        int size = online.getPossibleCount();
        for (int[] cpus : groupCpus) {
            for (int cpu : cpus) {
                size = Math.max(size, cpu + 1);
            }
        }

        // isInSet[cpu * groupCount + g]: group g's set holds CPU cpu
        final boolean[] isInSet = new boolean[size * groupCount];
        for (int g = 0; g < groupCount; g++) {
            for (int cpu : groupCpus[g]) {
                isInSet[cpu * groupCount + g] = true;
            }
        }

        // isOnSocket[socket * groupCount + g]: group g has an online CPU on the socket
        int maxSocket = -1;
        for (int socket : onlineSockets) {
            maxSocket = Math.max(maxSocket, socket);
        }
        final boolean[] isOnSocket = new boolean[(maxSocket + 1) * groupCount];
        for (int i = 0; i < onlineCpus.length; i++) {
            for (int g = 0; g < groupCount; g++) {
                if (isInSet[onlineCpus[i] * groupCount + g]) {
                    isOnSocket[onlineSockets[i] * groupCount + g] = true;
                }
            }
        }

        final int[] cpuSets = new int[size];
        Arrays.fill(cpuSets, OFFLINE);
        final ObjList<int[]> groupSets = new ObjList<>();
        final IntList groups = new IntList();
        for (int i = 0; i < onlineCpus.length; i++) {
            final int cpu = onlineCpus[i];
            groups.clear();
            for (int g = 0; g < groupCount; g++) {
                if (isInSet[cpu * groupCount + g]) {
                    groups.add(g);
                }
            }
            if (groups.size() == 0) {
                final int socket = onlineSockets[i];
                for (int g = 0; g < groupCount; g++) {
                    if (isOnSocket[socket * groupCount + g]) {
                        groups.add(g);
                    }
                }
            }
            cpuSets[cpu] = groups.size() > 0 ? intern(groupSets, groups) : NO_GROUP;
        }
        return new SteeringCpuTable(cpuSets, groupSets);
    }

    /**
     * The group set of CPU {@code cpu}, {@link #NO_GROUP} for a CPU without groups or past the
     * table, or {@link #OFFLINE}.
     */
    public int getGroupSet(int cpu) {
        return cpu < cpuSets.length ? cpuSets[cpu] : NO_GROUP;
    }

    public int getGroupSetCount() {
        return groupSets.size();
    }

    /**
     * The groups of group set {@code groupSet}, in ascending order.
     */
    public int[] getGroups(int groupSet) {
        return groupSets.getQuick(groupSet);
    }

    /**
     * The number of CPU numbers the table covers, from 0.
     */
    public int size() {
        return cpuSets.length;
    }

    // the index of the group set that holds exactly these groups, which it adds when there is none
    private static int intern(ObjList<int[]> groupSets, IntList groups) {
        for (int s = 0, n = groupSets.size(); s < n; s++) {
            final int[] groupSet = groupSets.getQuick(s);
            if (groupSet.length == groups.size()) {
                boolean isSame = true;
                for (int i = 0; i < groupSet.length; i++) {
                    if (groupSet[i] != groups.getQuick(i)) {
                        isSame = false;
                        break;
                    }
                }
                if (isSame) {
                    return s;
                }
            }
        }
        groupSets.add(groups.toArray());
        return groupSets.size() - 1;
    }
}
