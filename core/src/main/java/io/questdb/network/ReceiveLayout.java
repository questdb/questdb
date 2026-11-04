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
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.function.IntFunction;

/**
 * Reads where the kernel processes the SYNs that reach a port, from {@code /sys} and {@code /proc},
 * and predicts each worker group's share of the port's new connections.
 * <p>
 * The interfaces are the one that owns the port's bind address, or, for {@code 0.0.0.0}, every
 * interface that is up and has a device behind it ({@code /sys/class/net/<if>/device}). Loopback
 * and virtual interfaces have no device. For each interface:
 * <ul>
 *     <li>with RPS on every receive queue ({@code rps_cpus} not zero), each queue is a unit of
 *     equal traffic that RPS spreads evenly over its CPUs;</li>
 *     <li>without RPS, each requested interrupt vector of the device ({@code msi_irqs/}, of the
 *     direct parent for a virtio device) is a unit, processed on the CPU that takes the interrupt
 *     ({@code /proc/irq/<n>/effective_affinity_list}, or {@code smp_affinity_list} where that
 *     doesn't exist).</li>
 * </ul>
 * The layout is unreadable when no interface qualifies, a device has no MSI-X vectors, RPS covers
 * some queues but not others, a device has more requested vectors than twice its receive queues plus
 * two, threaded NAPI or busy polling moves receive processing between CPUs, or a file can't be read.
 */
public final class ReceiveLayout {
    private final IntFunction<String> interfaceOfAddress;
    private final Path procRoot;
    private final Path sysRoot;

    public ReceiveLayout(Path procRoot, Path sysRoot) {
        this(procRoot, sysRoot, ReceiveLayout::findInterface);
    }

    /**
     * @param interfaceOfAddress the name of the interface that owns an IPv4 address, or null
     */
    public ReceiveLayout(Path procRoot, Path sysRoot, IntFunction<String> interfaceOfAddress) {
        this.procRoot = procRoot;
        this.sysRoot = sysRoot;
        this.interfaceOfAddress = interfaceOfAddress;
    }

    /**
     * Predicts each group's share of new connections from a snapshot, through the CPU-to-group
     * table. A unit processed on a CPU without a group spreads evenly over all groups, as the
     * kernel's hash does.
     */
    public static Prediction predict(Snapshot snapshot, int[] cpuTable, int groupCount) {
        if (snapshot.unreadable != null) {
            return Prediction.unreadable(snapshot.unreadable);
        }
        final ObjList<double[]> shares = new ObjList<>();
        for (int i = 0, n = snapshot.nics.size(); i < n; i++) {
            final Nic nic = snapshot.nics.getQuick(i);
            final double[] nicShares = new double[groupCount];
            for (int u = 0, m = nic.units.size(); u < m; u++) {
                final int[] cpus = nic.units.getQuick(u);
                if (nic.isRps) {
                    for (int cpu : cpus) {
                        addShare(nicShares, groupOf(cpuTable, cpu), 1.0 / cpus.length);
                    }
                } else {
                    final int group = groupOf(cpuTable, cpus[0]);
                    for (int cpu : cpus) {
                        if (groupOf(cpuTable, cpu) != group) {
                            return Prediction.unreadable("the CPUs " + CpuTopology.formatCpuList(cpus) + " of interrupt "
                                    + nic.labels.getQuick(u) + " of " + nic.name + " map to different groups");
                        }
                    }
                    addShare(nicShares, group, 1.0);
                }
            }
            for (int g = 0; g < groupCount; g++) {
                nicShares[g] /= nic.units.size();
            }
            shares.add(nicShares);
        }
        return new Prediction(snapshot, shares);
    }

    /**
     * Reads the layout of the interfaces that {@code bindIPv4} covers, 0 meaning every interface.
     */
    public Snapshot read(int bindIPv4) {
        try {
            final long busyPoll = readLong(procRoot.resolve("sys/net/core/busy_poll"), 0);
            if (busyPoll > 0) {
                return Snapshot.unreadable("busy polling is on (net.core.busy_poll=" + busyPoll + ')');
            }
            final ObjList<String> names = new ObjList<>();
            if (bindIPv4 == 0) {
                final Path classNet = sysRoot.resolve("class/net");
                try (DirectoryStream<Path> entries = Files.newDirectoryStream(classNet)) {
                    for (Path entry : entries) {
                        final String name = entry.getFileName().toString();
                        if (Files.exists(entry.resolve("device")) && (readHex(entry.resolve("flags")) & 1) != 0) {
                            names.add(name);
                        }
                    }
                }
                if (names.size() == 0) {
                    return Snapshot.unreadable("no interface that is up has a device behind it");
                }
                names.sort(String::compareTo);
            } else {
                final String name = interfaceOfAddress.apply(bindIPv4);
                if (name == null) {
                    return Snapshot.unreadable("no interface owns the bind address " + formatIPv4(bindIPv4));
                }
                if (!Files.exists(sysRoot.resolve("class/net").resolve(name).resolve("device"))) {
                    return Snapshot.unreadable("interface " + name + " of the bind address has no device behind it");
                }
                names.add(name);
            }
            final ObjList<Nic> nics = new ObjList<>();
            for (int i = 0, n = names.size(); i < n; i++) {
                final Nic nic = new Nic(names.getQuick(i));
                final String problem = readNic(nic);
                if (problem != null) {
                    return Snapshot.unreadable(problem);
                }
                nics.add(nic);
            }
            return new Snapshot(nics, null);
        } catch (IOException | RuntimeException e) {
            return Snapshot.unreadable("could not read the receive layout: " + e.getMessage());
        }
    }

    private static void addShare(double[] shares, int group, double share) {
        if (group >= 0) {
            shares[group] += share;
        } else {
            for (int g = 0; g < shares.length; g++) {
                shares[g] += share / shares.length;
            }
        }
    }

    private static @Nullable String findInterface(int ipv4) {
        try {
            final InetAddress address = InetAddress.getByAddress(new byte[]{
                    (byte) (ipv4 >>> 24), (byte) (ipv4 >>> 16), (byte) (ipv4 >>> 8), (byte) ipv4
            });
            final NetworkInterface nif = NetworkInterface.getByInetAddress(address);
            return nif != null ? nif.getName() : null;
        } catch (IOException e) {
            return null;
        }
    }

    private static String formatIPv4(int ipv4) {
        return (ipv4 >>> 24) + "." + ((ipv4 >>> 16) & 0xFF) + '.' + ((ipv4 >>> 8) & 0xFF) + '.' + (ipv4 & 0xFF);
    }

    private static int groupOf(int[] cpuTable, int cpu) {
        return cpu < cpuTable.length ? cpuTable[cpu] : SteeringCpuTable.NO_GROUP;
    }

    // a kernel CPU mask such as 00000000,0000ff00, most significant word first
    private static int[] parseCpuMask(String mask) {
        final IntList cpus = new IntList();
        final String digits = mask.trim().replace(",", "");
        for (int i = digits.length() - 1, bit = 0; i >= 0; i--, bit += 4) {
            final int value = Character.digit(digits.charAt(i), 16);
            if (value < 0) {
                throw new IllegalArgumentException("malformed CPU mask: " + mask);
            }
            for (int b = 0; b < 4; b++) {
                if ((value & 1 << b) != 0) {
                    cpus.add(bit + b);
                }
            }
        }
        final int[] result = new int[cpus.size()];
        for (int i = 0; i < result.length; i++) {
            result[i] = cpus.getQuick(i);
        }
        return result;
    }

    private static long readHex(Path file) throws IOException {
        String text = Files.readString(file).trim();
        if (text.startsWith("0x")) {
            text = text.substring(2);
        }
        return Long.parseLong(text, 16);
    }

    private static long readLong(Path file, long missing) throws IOException {
        if (!Files.exists(file)) {
            return missing;
        }
        return Long.parseLong(Files.readString(file).trim());
    }

    private int[] readInterruptCpus(Path irqDir) throws IOException {
        final Path effective = irqDir.resolve("effective_affinity_list");
        final Path file = Files.exists(effective) ? effective : irqDir.resolve("smp_affinity_list");
        return CpuTopology.parseCpuList(Files.readString(file));
    }

    // fills the NIC's units, or returns why its layout is unreadable
    private @Nullable String readNic(Nic nic) throws IOException {
        final Path netDir = sysRoot.resolve("class/net").resolve(nic.name);
        final Path threaded = netDir.resolve("threaded");
        if (Files.exists(threaded) && readLong(threaded, 0) != 0) {
            return "interface " + nic.name + " runs threaded NAPI, so receive processing moves between CPUs";
        }

        final ObjList<int[]> rpsUnits = new ObjList<>();
        int rxQueues = 0;
        try (DirectoryStream<Path> queues = Files.newDirectoryStream(netDir.resolve("queues"), "rx-*")) {
            for (Path queue : queues) {
                rxQueues++;
                final Path rps = queue.resolve("rps_cpus");
                final int[] cpus = Files.exists(rps) ? parseCpuMask(Files.readString(rps)) : new int[0];
                if (cpus.length > 0) {
                    rpsUnits.add(cpus);
                }
            }
        }
        if (rxQueues == 0) {
            return "interface " + nic.name + " lists no receive queues";
        }
        if (rpsUnits.size() > 0) {
            if (rpsUnits.size() < rxQueues) {
                return "interface " + nic.name + " has RPS on " + rpsUnits.size() + " of its " + rxQueues + " receive queues";
            }
            nic.isRps = true;
            for (int i = 0, n = rpsUnits.size(); i < n; i++) {
                nic.units.add(rpsUnits.getQuick(i));
                nic.labels.add("rx-" + i);
            }
            return null;
        }

        Path device = netDir.resolve("device").toRealPath();
        final Path subsystem = device.resolve("subsystem");
        if (Files.isSymbolicLink(subsystem) && "virtio".equals(Files.readSymbolicLink(subsystem).getFileName().toString())) {
            // virtio-net's interrupts belong to the virtio PCI function, the device's direct parent
            device = device.getParent();
        }
        final Path msiDir = device.resolve("msi_irqs");
        if (!Files.isDirectory(msiDir)) {
            return "interface " + nic.name + " has no MSI-X interrupts (" + msiDir + ')';
        }
        final IntList vectors = new IntList();
        try (DirectoryStream<Path> entries = Files.newDirectoryStream(msiDir)) {
            for (Path entry : entries) {
                final int irq = Integer.parseInt(entry.getFileName().toString());
                // the driver requested the vector when /proc/irq/<n>/ exists
                if (Files.isDirectory(procRoot.resolve("irq").resolve(Integer.toString(irq)))) {
                    vectors.add(irq);
                }
            }
        }
        if (vectors.size() == 0) {
            return "interface " + nic.name + " has no requested interrupt vectors";
        }
        if (vectors.size() > 2 * rxQueues + 2) {
            return "interface " + nic.name + " has " + vectors.size() + " interrupt vectors for " + rxQueues
                    + " receive queues, so the vectors don't show where its queues interrupt";
        }
        final int[] sorted = new int[vectors.size()];
        for (int i = 0; i < sorted.length; i++) {
            sorted[i] = vectors.getQuick(i);
        }
        Arrays.sort(sorted);
        for (int irq : sorted) {
            final int[] cpus = readInterruptCpus(procRoot.resolve("irq").resolve(Integer.toString(irq)));
            if (cpus.length == 0) {
                return "interrupt " + irq + " of " + nic.name + " lists no CPUs";
            }
            nic.units.add(cpus);
            nic.labels.add(Integer.toString(irq));
        }
        return null;
    }

    // a network interface and its units of receive traffic: RPS queues or interrupt vectors
    public static final class Nic {
        private boolean isRps;
        private final ObjList<String> labels = new ObjList<>();
        private final String name;
        private final ObjList<int[]> units = new ObjList<>();

        private Nic(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public ObjList<int[]> getUnits() {
            return units;
        }

        public boolean isRps() {
            return isRps;
        }
    }

    /**
     * The predicted share of each group per interface, or why the layout can't be predicted. The
     * signature identifies the prediction, so that a different signature means a layout change.
     */
    public static final class Prediction {
        private final double maxShare;
        private final ObjList<double[]> shares;
        private final String signature;
        private final @Nullable Snapshot snapshot;
        private final @Nullable String unreadable;

        private Prediction(@Nullable Snapshot snapshot, ObjList<double[]> shares, @Nullable String unreadable) {
            this.snapshot = snapshot;
            this.shares = shares;
            this.unreadable = unreadable;
            double max = 0;
            final StringBuilder sb = new StringBuilder();
            if (unreadable != null) {
                sb.append("unreadable: ").append(unreadable);
            } else {
                for (int i = 0, n = shares.size(); i < n; i++) {
                    final double[] nicShares = shares.getQuick(i);
                    if (i > 0) {
                        sb.append(", ");
                    }
                    assert snapshot != null;
                    sb.append(snapshot.nics.getQuick(i).name).append(' ');
                    for (int g = 0; g < nicShares.length; g++) {
                        max = Math.max(max, nicShares[g]);
                        if (g > 0) {
                            sb.append('/');
                        }
                        final long permille = Math.round(nicShares[g] * 1000);
                        sb.append(permille / 10).append('.').append(permille % 10).append('%');
                    }
                }
            }
            this.maxShare = max;
            this.signature = sb.toString();
        }

        private Prediction(Snapshot snapshot, ObjList<double[]> shares) {
            this(snapshot, shares, null);
        }

        public double getMaxShare() {
            return maxShare;
        }

        /**
         * Per interface, each group's predicted share.
         */
        public ObjList<double[]> getShares() {
            return shares;
        }

        /**
         * The shares per interface as text, such as {@code eth0 50.0%/50.0%}, or why the layout is
         * unreadable.
         */
        public String getSignature() {
            return signature;
        }

        public @Nullable String getUnreadable() {
            return unreadable;
        }

        private static Prediction unreadable(String reason) {
            return new Prediction(null, new ObjList<>(), reason);
        }
    }

    /**
     * The interfaces and their receive units, or why they couldn't be read.
     */
    public static final class Snapshot {
        private final ObjList<Nic> nics;
        private final @Nullable String unreadable;

        private Snapshot(ObjList<Nic> nics, @Nullable String unreadable) {
            this.nics = nics;
            this.unreadable = unreadable;
        }

        public ObjList<Nic> getNics() {
            return nics;
        }

        public @Nullable String getUnreadable() {
            return unreadable;
        }

        private static Snapshot unreadable(String reason) {
            return new Snapshot(new ObjList<>(), reason);
        }
    }
}
