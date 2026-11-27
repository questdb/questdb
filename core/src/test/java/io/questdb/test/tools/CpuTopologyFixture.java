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

package io.questdb.test.tools;

import io.questdb.BuildInformationHolder;
import io.questdb.PropServerConfiguration;
import io.questdb.log.Log;
import io.questdb.std.CpuTopology;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

/**
 * Fixture {@code /proc} and {@code /sys} trees for {@link CpuTopology}, and a server configuration
 * that reads its CPU layout from one.
 */
public final class CpuTopologyFixture {

    private CpuTopologyFixture() {
    }

    /**
     * Sets the CPUs the process may use to the kernel CPU list {@code list}.
     */
    public static Path allowed(Path fixture, String list) throws IOException {
        Files.writeString(fixture.resolve("proc/self/status"), "Name:\tjava\nCpus_allowed_list:\t" + list + "\n");
        return fixture;
    }

    /**
     * Creates a server configuration that reads the CPU layout from {@code fixture}, or reports
     * CPU socket placement as unsupported when it is null.
     */
    public static PropServerConfiguration configuration(
            String root,
            Properties properties,
            @Nullable Path fixture,
            Log log
    ) throws Exception {
        return FixtureConfiguration.create(root, properties, fixture, log);
    }

    /**
     * Adds CPU {@code cpu} on CPU socket {@code socket} and NUMA node {@code node}.
     */
    public static void cpu(Path fixture, int cpu, int socket, int node) throws IOException {
        final Path dir = fixture.resolve("sys/devices/system/cpu/cpu" + cpu);
        Files.createDirectories(dir.resolve("topology"));
        Files.writeString(dir.resolve("topology/physical_package_id"), socket + "\n");
        // sysfs has a symlink to the node's directory here; an empty directory reads the same
        Files.createDirectories(dir.resolve("node" + node));
    }

    /**
     * Starts a fixture in the empty directory {@code dir} with the given allowed and online CPU
     * lists and no CPUs yet.
     */
    public static Path create(Path dir, String allowed, String online) throws IOException {
        Files.createDirectories(dir.resolve("proc/self"));
        Files.createDirectories(dir.resolve("sys/devices/system/cpu"));
        allowed(dir, allowed);
        return online(dir, online);
    }

    /**
     * Sets the online CPUs to the kernel CPU list {@code list}.
     */
    public static Path online(Path fixture, String list) throws IOException {
        Files.writeString(fixture.resolve("sys/devices/system/cpu/online"), list + "\n");
        return fixture;
    }

    /**
     * A {@link CpuTopology} that reads {@code fixture}.
     */
    public static CpuTopology topology(Path fixture) {
        return new CpuTopology(fixture.resolve("proc"), fixture.resolve("sys"));
    }

    /**
     * A fixture of two CPU sockets of {@code cpusPerSocket} CPUs, all allowed and online: CPU socket
     * 0 and NUMA node 0 hold the first half, CPU socket 1 and NUMA node 1 the second.
     */
    public static Path twoSockets(Path dir, int cpusPerSocket) throws IOException {
        final int cpuCount = 2 * cpusPerSocket;
        final String all = "0-" + (cpuCount - 1);
        create(dir, all, all);
        for (int cpu = 0; cpu < cpuCount; cpu++) {
            cpu(dir, cpu, cpu / cpusPerSocket, cpu / cpusPerSocket);
        }
        return dir;
    }

    private static class FixtureConfiguration extends PropServerConfiguration {
        // createCpuTopology() runs inside the super constructor, before this class's own fields
        // are set, so the fixture travels in a static field; a test creates one configuration at a
        // time
        private static volatile Path fixture;

        private FixtureConfiguration(String root, Properties properties, Log log) throws Exception {
            super(root, properties, null, log, new BuildInformationHolder());
        }

        static PropServerConfiguration create(String root, Properties properties, @Nullable Path fixture, Log log) throws Exception {
            FixtureConfiguration.fixture = fixture;
            return new FixtureConfiguration(root, properties, log);
        }

        @Override
        protected @Nullable CpuTopology createCpuTopology() {
            final Path fixture = FixtureConfiguration.fixture;
            return fixture != null ? topology(fixture) : null;
        }
    }
}
