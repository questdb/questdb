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

import java.util.Arrays;

/**
 * Maps each CPU number to the worker group whose listener should take the connections whose SYN
 * that CPU processes.
 * <ul>
 *     <li>A CPU in exactly one group's CPU set maps to that group.</li>
 *     <li>Any other online CPU maps to the group on its socket, when its socket hosts exactly one
 *     group. A group's sockets are those of its online CPUs. In production each group's set is its
 *     socket's allowed CPUs, so this maps the CPUs of the socket that the process may not use,
 *     where interrupts and RPS can still run.</li>
 *     <li>Any other online CPU maps to {@link #NO_GROUP}, and an offline CPU to {@link #OFFLINE}.</li>
 * </ul>
 */
public final class SteeringCpuTable {
    public static final int NO_GROUP = -1;
    public static final int OFFLINE = -3;

    private SteeringCpuTable() {
    }

    /**
     * Builds the table, indexed by CPU number, over every possible CPU number and every CPU of the
     * groups' sets.
     */
    public static int[] build(int[][] groupCpus, CpuTopology.OnlineCpus online) {
        final int[] onlineCpus = online.getCpus();
        final int[] onlineSockets = online.getSockets();
        int size = online.getPossibleCount();
        for (int[] cpus : groupCpus) {
            for (int cpu : cpus) {
                size = Math.max(size, cpu + 1);
            }
        }
        final int[] table = new int[size];
        Arrays.fill(table, OFFLINE);

        // the group of each CPU in exactly one group's set, or -2 for a CPU in several sets
        final int[] owner = new int[size];
        Arrays.fill(owner, NO_GROUP);
        for (int g = 0; g < groupCpus.length; g++) {
            for (int cpu : groupCpus[g]) {
                owner[cpu] = owner[cpu] == NO_GROUP || owner[cpu] == g ? g : -2;
            }
        }

        // the group each socket hosts, or -2 when it hosts several
        int maxSocket = -1;
        for (int socket : onlineSockets) {
            maxSocket = Math.max(maxSocket, socket);
        }
        final int[] socketGroup = new int[maxSocket + 1];
        Arrays.fill(socketGroup, NO_GROUP);
        for (int i = 0; i < onlineCpus.length; i++) {
            final int group = owner[onlineCpus[i]];
            final int socket = onlineSockets[i];
            if (group != NO_GROUP) {
                socketGroup[socket] = socketGroup[socket] == NO_GROUP || socketGroup[socket] == group ? group : -2;
            }
        }

        for (int i = 0; i < onlineCpus.length; i++) {
            final int cpu = onlineCpus[i];
            if (owner[cpu] >= 0) {
                table[cpu] = owner[cpu];
            } else {
                final int group = socketGroup[onlineSockets[i]];
                table[cpu] = group >= 0 ? group : NO_GROUP;
            }
        }
        return table;
    }
}
