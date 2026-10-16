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

package io.questdb.std;

import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.TreeMap;

/**
 * The CPU sockets among the CPUs this process may use, read from the Linux {@code /proc} and
 * {@code /sys} trees, and the checks that decide whether the layout looks real enough to split the
 * shared network pool by socket.
 * <p>
 * The allowed CPUs come from {@code Cpus_allowed_list} in {@code /proc/self/status}, which reflects
 * {@code taskset}, cgroup cpusets and {@code docker --cpuset-cpus}. That list can name offline CPUs,
 * which have no topology entries, so only the allowed CPUs that {@code /sys/devices/system/cpu/online}
 * lists count. For each of them, its socket is
 * {@code /sys/devices/system/cpu/cpu<N>/topology/physical_package_id} and its NUMA node is the
 * {@code node<K>} entry of {@code /sys/devices/system/cpu/cpu<N>/}.
 * <p>
 * The limits below are judgement calls, not measured values. Virtual machines may report each vCPU
 * as a socket of its own, or invent sockets on a single memory node, and the checks refuse such
 * layouts.
 */
public final class CpuTopology {
    // the largest difference between the sockets' allowed CPU counts, in percent of the largest count
    public static final int MAX_CPU_COUNT_SPREAD_PERCENT = 25;
    public static final int MAX_SOCKETS = 8;
    public static final int MIN_CPUS_PER_SOCKET = 8;
    public static final int MIN_WORKERS_PER_GROUP = 32;
    private static final String ALLOWED_LIST_PREFIX = "Cpus_allowed_list:";
    private final Path procRoot;
    private final Path sysRoot;

    public CpuTopology() {
        this(Path.of("/proc"), Path.of("/sys"));
    }

    /**
     * Reads {@code <procRoot>/self/status} and {@code <sysRoot>/devices/system/cpu}, so that tests
     * can point both at fixture directories.
     */
    public CpuTopology(Path procRoot, Path sysRoot) {
        this.procRoot = procRoot;
        this.sysRoot = sysRoot;
    }

    /**
     * Formats a sorted list of CPUs as ranges, for example {@code 0-95,128-191}.
     */
    public static String formatCpuList(int[] cpus) {
        final StringBuilder sb = new StringBuilder();
        int i = 0;
        while (i < cpus.length) {
            int j = i;
            while (j + 1 < cpus.length && cpus[j + 1] == cpus[j] + 1) {
                j++;
            }
            if (!sb.isEmpty()) {
                sb.append(',');
            }
            sb.append(cpus[i]);
            if (j > i) {
                sb.append('-').append(cpus[j]);
            }
            i = j + 1;
        }
        return sb.toString();
    }

    /**
     * Parses a kernel CPU list such as {@code 0-95,128-191} into a sorted array without duplicates.
     *
     * @throws IllegalArgumentException if the list is malformed
     */
    public static int[] parseCpuList(CharSequence list) {
        final IntList cpus = new IntList();
        final String text = list.toString().trim();
        if (text.isEmpty()) {
            return new int[0];
        }
        for (String part : text.split(",")) {
            final int dash = part.indexOf('-');
            final int from;
            final int to;
            try {
                if (dash < 0) {
                    from = to = Integer.parseInt(part.trim());
                } else {
                    from = Integer.parseInt(part.substring(0, dash).trim());
                    to = Integer.parseInt(part.substring(dash + 1).trim());
                }
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("malformed CPU list: " + text);
            }
            if (from < 0 || to < from) {
                throw new IllegalArgumentException("malformed CPU list: " + text);
            }
            for (int cpu = from; cpu <= to; cpu++) {
                cpus.add(cpu);
            }
        }
        final int[] result = new int[cpus.size()];
        for (int i = 0, n = cpus.size(); i < n; i++) {
            result[i] = cpus.getQuick(i);
        }
        Arrays.sort(result);
        int unique = 0;
        for (int i = 0; i < result.length; i++) {
            if (i == 0 || result[i] != result[i - 1]) {
                result[unique++] = result[i];
            }
        }
        return unique == result.length ? result : Arrays.copyOf(result, unique);
    }

    /**
     * Reads the topology and runs the layout checks for a network pool of {@code workerCount}
     * workers, returning the per-socket CPU lists or the reason for refusing them.
     */
    public Layout readLayout(int workerCount) {
        final int[] allowed;
        try {
            allowed = readAllowedCpus();
        } catch (IOException | IllegalArgumentException e) {
            return Layout.refused("could not read the allowed CPUs: " + e.getMessage(), true, null);
        }
        if (allowed.length == 0) {
            return Layout.refused("no allowed CPUs listed in " + procRoot.resolve("self/status"), true, null);
        }
        final int[] online;
        try {
            online = readOnlineCpus();
        } catch (IOException | IllegalArgumentException e) {
            return Layout.refused("could not read the online CPUs: " + e.getMessage(), true, null);
        }
        // the affinity mask behind Cpus_allowed_list can name offline CPUs, which have no topology entries
        final int[] cpus = intersect(allowed, online);
        if (cpus.length == 0) {
            return Layout.refused("none of the allowed CPUs " + formatCpuList(allowed) + " is online", true, null);
        }
        final int[] sockets = new int[cpus.length];
        final int[] nodes = new int[cpus.length];
        final Path cpuDir = sysRoot.resolve("devices/system/cpu");
        for (int i = 0; i < cpus.length; i++) {
            final Path dir = cpuDir.resolve("cpu" + cpus[i]);
            sockets[i] = readSocket(dir);
            if (sockets[i] < 0) {
                return Layout.refused("CPU " + cpus[i] + " has no valid socket id", true, null);
            }
            // -1 when the kernel lists no node, which matters only once there are several sockets
            nodes[i] = readNode(dir);
        }
        return checkLayout(cpus, sockets, nodes, workerCount);
    }

    // sockets[i] and nodes[i] are the socket and the NUMA node of allowed CPU allowed[i], with
    // node -1 when the kernel lists none
    private static Layout checkLayout(int[] allowed, int[] sockets, int[] nodes, int workerCount) {
        // socket id -> its allowed CPUs, in CPU order
        final TreeMap<Integer, IntList> bySocket = new TreeMap<>();
        // NUMA node -> the first socket seen on it
        final TreeMap<Integer, Integer> nodeSocket = new TreeMap<>();
        // the first problem with the NUMA nodes, reported only for a layout with several sockets
        String nodeProblem = null;
        for (int i = 0; i < allowed.length; i++) {
            bySocket.computeIfAbsent(sockets[i], _ -> new IntList()).add(allowed[i]);
            if (nodeProblem == null) {
                if (nodes[i] < 0) {
                    nodeProblem = "CPU " + allowed[i] + " has no NUMA node";
                } else {
                    final Integer other = nodeSocket.putIfAbsent(nodes[i], sockets[i]);
                    if (other != null && other != sockets[i]) {
                        nodeProblem = "NUMA node " + nodes[i] + " holds CPUs of sockets " + other + " and " + sockets[i];
                    }
                }
            }
        }
        // order the sockets by their lowest CPU
        final int socketCount = bySocket.size();
        final int[][] socketCpus = new int[socketCount][];
        final int[] socketIds = new int[socketCount];
        int s = 0;
        for (var entry : bySocket.entrySet()) {
            final IntList cpus = entry.getValue();
            socketCpus[s] = new int[cpus.size()];
            for (int i = 0, n = cpus.size(); i < n; i++) {
                socketCpus[s][i] = cpus.getQuick(i);
            }
            socketIds[s] = entry.getKey();
            s++;
        }
        sortByLowestCpu(socketCpus, socketIds);
        final String description = describe(bySocket, allowed, sockets, nodes);
        if (socketCount < 2) {
            return Layout.refused("the allowed CPUs " + formatCpuList(allowed) + " are all on socket " + socketIds[0], false, description);
        }
        if (socketCount > MAX_SOCKETS) {
            return Layout.refused("the allowed CPUs report " + socketCount + " sockets, more than " + MAX_SOCKETS, true, description);
        }
        int minCpus = Integer.MAX_VALUE;
        int maxCpus = 0;
        int minSocket = -1;
        int maxSocket = -1;
        for (int i = 0; i < socketCount; i++) {
            final int n = socketCpus[i].length;
            if (n < MIN_CPUS_PER_SOCKET) {
                return Layout.refused("socket " + socketIds[i] + " has " + n + " allowed CPUs, fewer than " + MIN_CPUS_PER_SOCKET, true, description);
            }
            if (n < minCpus) {
                minCpus = n;
                minSocket = socketIds[i];
            }
            if (n > maxCpus) {
                maxCpus = n;
                maxSocket = socketIds[i];
            }
        }
        if (nodeProblem != null) {
            return Layout.refused(nodeProblem, true, description);
        }
        if ((long) (maxCpus - minCpus) * 100 > (long) maxCpus * MAX_CPU_COUNT_SPREAD_PERCENT) {
            return Layout.refused(
                    "socket " + maxSocket + " has " + maxCpus + " allowed CPUs and socket " + minSocket + " has " + minCpus
                            + ", more than " + MAX_CPU_COUNT_SPREAD_PERCENT + "% apart",
                    true,
                    description
            );
        }
        if (workerCount / socketCount < MIN_WORKERS_PER_GROUP) {
            return Layout.refused(
                    workerCount + " network workers make groups smaller than " + MIN_WORKERS_PER_GROUP + " workers over " + socketCount + " sockets",
                    true,
                    description
            );
        }
        return new Layout(socketCpus, socketIds, null, false, description);
    }

    // For example "socket 0: CPUs 0-95 on node 0, socket 1: CPUs 96-191 on node 1", without the
    // nodes when the kernel lists none.
    private static String describe(TreeMap<Integer, IntList> bySocket, int[] allowed, int[] sockets, int[] nodes) {
        final StringBuilder sb = new StringBuilder();
        for (var entry : bySocket.entrySet()) {
            final int socket = entry.getKey();
            final IntList cpus = entry.getValue();
            final int[] list = new int[cpus.size()];
            for (int i = 0; i < list.length; i++) {
                list[i] = cpus.getQuick(i);
            }
            final TreeMap<Integer, Boolean> socketNodes = new TreeMap<>();
            for (int i = 0; i < allowed.length; i++) {
                if (sockets[i] == socket && nodes[i] >= 0) {
                    socketNodes.put(nodes[i], Boolean.TRUE);
                }
            }
            if (!sb.isEmpty()) {
                sb.append(", ");
            }
            sb.append("socket ").append(socket).append(": CPUs ").append(formatCpuList(list));
            if (!socketNodes.isEmpty()) {
                sb.append(socketNodes.size() == 1 ? " on node " : " on nodes ");
                boolean first = true;
                for (Integer node : socketNodes.keySet()) {
                    if (!first) {
                        sb.append(',');
                    }
                    sb.append(node);
                    first = false;
                }
            }
        }
        return sb.toString();
    }

    // The CPUs on both sorted lists without duplicates.
    private static int[] intersect(int[] a, int[] b) {
        final int[] result = new int[Math.min(a.length, b.length)];
        int n = 0;
        int i = 0;
        int j = 0;
        while (i < a.length && j < b.length) {
            if (a[i] < b[j]) {
                i++;
            } else if (a[i] > b[j]) {
                j++;
            } else {
                result[n++] = a[i];
                i++;
                j++;
            }
        }
        return n == result.length ? result : Arrays.copyOf(result, n);
    }

    // The kernel's NUMA node of the CPU, from the node<K> entry in its directory, or -1.
    private static int readNode(Path cpuDir) {
        try (DirectoryStream<Path> entries = java.nio.file.Files.newDirectoryStream(cpuDir, "node*")) {
            for (Path entry : entries) {
                final String name = entry.getFileName().toString();
                if (name.length() > 4 && name.substring(4).chars().allMatch(Character::isDigit)) {
                    return Integer.parseInt(name.substring(4));
                }
            }
        } catch (IOException | NumberFormatException e) {
            return -1;
        }
        return -1;
    }

    // The kernel's physical package (socket) id of the CPU, or -1.
    private static int readSocket(Path cpuDir) {
        try {
            return Integer.parseInt(java.nio.file.Files.readString(cpuDir.resolve("topology/physical_package_id")).trim());
        } catch (IOException | NumberFormatException e) {
            return -1;
        }
    }

    private static void sortByLowestCpu(int[][] socketCpus, int[] socketIds) {
        // insertion sort: there are at most a few sockets
        for (int i = 1; i < socketCpus.length; i++) {
            final int[] cpus = socketCpus[i];
            final int id = socketIds[i];
            int j = i - 1;
            while (j >= 0 && socketCpus[j][0] > cpus[0]) {
                socketCpus[j + 1] = socketCpus[j];
                socketIds[j + 1] = socketIds[j];
                j--;
            }
            socketCpus[j + 1] = cpus;
            socketIds[j + 1] = id;
        }
    }

    private int[] readAllowedCpus() throws IOException {
        final Path status = procRoot.resolve("self/status");
        for (String line : java.nio.file.Files.readAllLines(status)) {
            if (line.startsWith(ALLOWED_LIST_PREFIX)) {
                return parseCpuList(line.substring(ALLOWED_LIST_PREFIX.length()));
            }
        }
        throw new IOException("no " + ALLOWED_LIST_PREFIX + " line in " + status);
    }

    private int[] readOnlineCpus() throws IOException {
        return parseCpuList(java.nio.file.Files.readString(sysRoot.resolve("devices/system/cpu/online")));
    }

    /**
     * The outcome of the layout checks: either the allowed CPUs of each socket, with the sockets
     * ordered by their lowest CPU, or the reason the layout was refused.
     */
    public static final class Layout {
        private final String description;
        private final boolean isRefusalWarning;
        private final @Nullable String refusal;
        private final @Nullable int[][] socketCpus;
        private final @Nullable int[] socketIds;

        private Layout(
                @Nullable int[][] socketCpus,
                @Nullable int[] socketIds,
                @Nullable String refusal,
                boolean isRefusalWarning,
                @Nullable String description
        ) {
            this.socketCpus = socketCpus;
            this.socketIds = socketIds;
            this.refusal = refusal;
            this.isRefusalWarning = isRefusalWarning;
            this.description = description != null ? description : "unknown";
        }

        /**
         * The detected sockets, their allowed CPUs and their NUMA nodes, for logs.
         */
        public String getDescription() {
            return description;
        }

        /**
         * Why the layout was refused, or null if it was accepted.
         */
        public @Nullable String getRefusal() {
            return refusal;
        }

        /**
         * Per socket, its allowed CPUs in ascending order, or null if the layout was refused.
         */
        public @Nullable int[][] getSocketCpus() {
            return socketCpus;
        }

        /**
         * The kernel's id of each socket of {@link #getSocketCpus()}, or null if the layout was
         * refused.
         */
        public @Nullable int[] getSocketIds() {
            return socketIds;
        }

        public boolean isAccepted() {
            return refusal == null;
        }

        /**
         * True when the refusal points at a layout that doesn't look real, and false for a
         * single-socket machine or cpuset, which is a normal setup.
         */
        public boolean isRefusalWarning() {
            return isRefusalWarning;
        }

        private static Layout refused(String refusal, boolean isWarning, @Nullable String description) {
            return new Layout(null, null, refusal, isWarning, description);
        }
    }
}
