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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * A fake {@code /proc} and {@code /sys} tree with CPUs, network interfaces, their receive queues and
 * their interrupt vectors, for connection steering's layout check.
 */
public final class SysfsFixture {
    private final Path procRoot;
    private final Path sysRoot;

    public SysfsFixture(Path root) {
        this.procRoot = root.resolve("proc");
        this.sysRoot = root.resolve("sys");
        mkdirs(procRoot.resolve("irq"));
        mkdirs(sysRoot.resolve("class/net"));
        mkdirs(sysRoot.resolve("bus/virtio"));
    }

    public SysfsFixture busyPoll(int value) {
        write(procRoot.resolve("sys/net/core/busy_poll"), Integer.toString(value));
        return this;
    }

    /**
     * Online CPUs 0 to {@code sockets.length - 1}, CPU i on socket {@code sockets[i]}, with
     * {@code possibleCount} possible CPU numbers.
     */
    public SysfsFixture cpus(int[] sockets, int possibleCount) {
        final Path cpuDir = sysRoot.resolve("devices/system/cpu");
        write(cpuDir.resolve("online"), sockets.length == 1 ? "0" : "0-" + (sockets.length - 1));
        write(cpuDir.resolve("possible"), "0-" + (possibleCount - 1));
        for (int cpu = 0; cpu < sockets.length; cpu++) {
            write(cpuDir.resolve("cpu" + cpu + "/topology/physical_package_id"), Integer.toString(sockets[cpu]));
        }
        return this;
    }

    public SysfsFixture down(String name) {
        write(sysRoot.resolve("class/net").resolve(name).resolve("flags"), "0x1002");
        return this;
    }

    public SysfsFixture effectiveAffinity(int irq, String cpuList) {
        write(procRoot.resolve("irq").resolve(Integer.toString(irq)).resolve("effective_affinity_list"), cpuList);
        return this;
    }

    public Path getProcRoot() {
        return procRoot;
    }

    public Path getSysRoot() {
        return sysRoot;
    }

    /**
     * A requested interrupt vector of the interface's device, taken on {@code cpuList}.
     */
    public SysfsFixture irq(String name, int irq, String cpuList) {
        vector(name, irq);
        write(procRoot.resolve("irq").resolve(Integer.toString(irq)).resolve("smp_affinity_list"), "0-1023");
        return effectiveAffinity(irq, cpuList);
    }

    /**
     * A physical interface with {@code rxQueues} receive queues, RPS off, and a PCI device.
     */
    public SysfsFixture nic(String name, int rxQueues) {
        final Path device = sysRoot.resolve("devices/pci0000:00").resolve(name + "-pci");
        mkdirs(device.resolve("msi_irqs"));
        return nicOn(name, rxQueues, device);
    }

    public SysfsFixture optmemMax(long value) {
        write(procRoot.resolve("sys/net/core/optmem_max"), Long.toString(value));
        return this;
    }

    public SysfsFixture removeEffectiveAffinity(int irq) {
        try {
            Files.delete(procRoot.resolve("irq").resolve(Integer.toString(irq)).resolve("effective_affinity_list"));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return this;
    }

    public SysfsFixture rps(String name, int queue, String mask) {
        write(sysRoot.resolve("class/net").resolve(name).resolve("queues/rx-" + queue + "/rps_cpus"), mask);
        return this;
    }

    public SysfsFixture smpAffinity(int irq, String cpuList) {
        write(procRoot.resolve("irq").resolve(Integer.toString(irq)).resolve("smp_affinity_list"), cpuList);
        return this;
    }

    public SysfsFixture threaded(String name) {
        write(sysRoot.resolve("class/net").resolve(name).resolve("threaded"), "1");
        return this;
    }

    /**
     * A vector the driver allocated but never requested, so it has no {@code /proc/irq/<n>/}.
     */
    public SysfsFixture unrequestedVector(String name, int irq) {
        return vector(name, irq);
    }

    /**
     * A virtio-net interface, whose interrupt vectors belong to the virtio PCI function, the
     * device's direct parent.
     */
    public SysfsFixture virtioNic(String name, int rxQueues) {
        final Path function = sysRoot.resolve("devices/pci0000:00").resolve(name + "-virtio-pci");
        final Path device = function.resolve("virtio0");
        mkdirs(function.resolve("msi_irqs"));
        mkdirs(device);
        try {
            Files.createSymbolicLink(device.resolve("subsystem"), sysRoot.resolve("bus/virtio"));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return nicOn(name, rxQueues, device);
    }

    /**
     * An interface without a device, such as a bridge, a bond or a veth pair.
     */
    public SysfsFixture virtualNic(String name) {
        final Path dir = sysRoot.resolve("class/net").resolve(name);
        write(dir.resolve("flags"), "0x1003");
        mkdirs(dir.resolve("queues/rx-0"));
        return this;
    }

    private static void mkdirs(Path dir) {
        try {
            Files.createDirectories(dir);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void write(Path file, String content) {
        try {
            Files.createDirectories(file.getParent());
            Files.writeString(file, content + "\n");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    // the directory that holds the interface's msi_irqs: the device, or a virtio device's parent
    private Path msiDir(String name) {
        try {
            Path device = sysRoot.resolve("class/net").resolve(name).resolve("device").toRealPath();
            if (Files.isSymbolicLink(device.resolve("subsystem"))) {
                device = device.getParent();
            }
            return device.resolve("msi_irqs");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private SysfsFixture nicOn(String name, int rxQueues, Path device) {
        final Path dir = sysRoot.resolve("class/net").resolve(name);
        mkdirs(dir);
        try {
            Files.createSymbolicLink(dir.resolve("device"), device);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        write(dir.resolve("flags"), "0x1003");
        for (int q = 0; q < rxQueues; q++) {
            write(dir.resolve("queues/rx-" + q + "/rps_cpus"), "00000000,00000000");
        }
        return this;
    }

    private SysfsFixture vector(String name, int irq) {
        write(msiDir(name).resolve(Integer.toString(irq)), "msix");
        return this;
    }
}
