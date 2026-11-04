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

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import org.jetbrains.annotations.Nullable;

public class IODispatchers {
    private static final Log LOG = LogFactory.getLog(IODispatchers.class);

    private IODispatchers() {
    }

    /**
     * Binds a plain socket, without SO_REUSEPORT, to the port and closes it. Another process that
     * listens on the port then fails the server the same way it fails a single listener, rather
     * than sharing the port with the server's SO_REUSEPORT listeners.
     *
     * @throws NetworkError "could not bind socket" if the port is taken
     */
    public static void checkPortFree(NetworkFacade nf, int bindIPv4, int port, String logName) {
        final long fd = nf.socketTcp(false);
        if (fd < 0) {
            throw NetworkError.instance(nf.errno()).put("could not create socket [who=").put(logName).put(']');
        }
        try {
            if (!nf.bindTcp(fd, bindIPv4, port)) {
                throw NetworkError.instance(nf.errno()).couldNotBindSocket(logName, bindIPv4, port);
            }
        } finally {
            nf.close(fd, LOG);
        }
    }

    public static <C extends IOContext<C>> IODispatcher<C> create(
            IODispatcherConfiguration configuration,
            IOContextFactory<C> ioContextFactory
    ) {
        return create(configuration, ioContextFactory, false);
    }

    /**
     * Creates a dispatcher whose listener, when {@code reusePort} is set, joins the SO_REUSEPORT
     * group of its port.
     */
    public static <C extends IOContext<C>> IODispatcher<C> create(
            IODispatcherConfiguration configuration,
            IOContextFactory<C> ioContextFactory,
            boolean reusePort
    ) {
        return create(configuration, ioContextFactory, reusePort, null, 0);
    }

    /**
     * Creates the dispatcher of worker group {@code group}, whose listener, when {@code reusePort}
     * is set, joins the SO_REUSEPORT group of its port, and opens and closes through the port's
     * {@code steering} when there is one.
     */
    public static <C extends IOContext<C>> IODispatcher<C> create(
            IODispatcherConfiguration configuration,
            IOContextFactory<C> ioContextFactory,
            boolean reusePort,
            @Nullable ConnectionSteering steering,
            int group
    ) {
        return switch (Os.type) {
            case Os.LINUX -> new IODispatcherLinux<>(configuration, ioContextFactory, reusePort, steering, group);
            case Os.DARWIN, Os.FREEBSD -> new IODispatcherOsx<>(configuration, ioContextFactory, reusePort, steering, group);
            case Os.WINDOWS -> new IODispatcherWindows<>(configuration, ioContextFactory, reusePort, steering, group);
            default -> throw new RuntimeException();
        };
    }

    /**
     * Creates one dispatcher per group into {@code dispatchers}, as
     * {@link #createGroups(IODispatcherConfiguration, IOContextFactory, int, int[][], ObjList)} does,
     * without the groups' CPU sets, so without connection steering.
     */
    public static <C extends IOContext<C>> void createGroups(
            IODispatcherConfiguration configuration,
            IOContextFactory<C> ioContextFactory,
            int groupCount,
            ObjList<IODispatcher<C>> dispatchers
    ) {
        createGroups(configuration, ioContextFactory, groupCount, null, dispatchers);
    }

    /**
     * Creates one dispatcher per group into {@code dispatchers}, as
     * {@link #createGroups(IODispatcherConfiguration, IOContextFactory, int, int[][], ObjList)} does,
     * with the CPU sets of {@code pool}'s groups when the server has one dispatcher per group of
     * the pool.
     */
    public static <C extends IOContext<C>> void createGroups(
            IODispatcherConfiguration configuration,
            IOContextFactory<C> ioContextFactory,
            WorkerPool pool,
            int groupCount,
            ObjList<IODispatcher<C>> dispatchers
    ) {
        int[][] groupCpus = null;
        if (groupCount > 1 && groupCount == pool.getGroupCount()) {
            groupCpus = new int[groupCount][];
            for (int g = 0; g < groupCount; g++) {
                groupCpus[g] = pool.getGroupCpus(g);
                if (groupCpus[g] == null) {
                    groupCpus = null;
                    break;
                }
            }
        }
        createGroups(configuration, ioContextFactory, groupCount, groupCpus, dispatchers);
    }

    /**
     * Creates one dispatcher per group into {@code dispatchers}, which must be empty. With more than
     * one group, it first checks that no other process holds the port, then gives every listener
     * SO_REUSEPORT, and, when the configuration enables connection steering, steers each new
     * connection by the groups' CPU sets. If a dispatcher fails, it frees the ones it created,
     * leaves the list empty, and rethrows.
     */
    public static <C extends IOContext<C>> void createGroups(
            IODispatcherConfiguration configuration,
            IOContextFactory<C> ioContextFactory,
            int groupCount,
            int @Nullable [][] groupCpus,
            ObjList<IODispatcher<C>> dispatchers
    ) {
        assert dispatchers.size() == 0;
        ConnectionSteering steering = null;
        try {
            if (groupCount > 1) {
                checkPortFree(
                        configuration.getNetworkFacade(),
                        configuration.getBindIPv4Address(),
                        configuration.getBindPort(),
                        configuration.getDispatcherLogName()
                );
            }
            steering = ConnectionSteering.create(configuration, groupCount, groupCpus);
            for (int g = 0; g < groupCount; g++) {
                dispatchers.add(create(configuration, ioContextFactory, groupCount > 1, steering, g));
            }
            if (steering != null) {
                steering.start();
            }
        } catch (Throwable th) {
            for (int g = 0, n = dispatchers.size(); g < n; g++) {
                Misc.free(dispatchers.getQuick(g), th);
            }
            dispatchers.clear();
            // the dispatchers close the steering too, unless none was created
            Misc.free(steering, th);
            throw th;
        }
    }

    /**
     * The number of dispatchers a server on {@code pool} creates: one per group of the pool, or one
     * when the server binds to port 0, because each group's listener would get an ephemeral port of
     * its own.
     */
    public static int getGroupCount(IODispatcherConfiguration configuration, WorkerPool pool) {
        final int groupCount = pool.getGroupCount();
        if (groupCount > 1 && configuration.getBindPort() == 0) {
            LOG.advisory().$("not split by CPU socket, the server binds to port 0 [who=")
                    .$(configuration.getDispatcherLogName())
                    .$(", groups=").$(groupCount)
                    .I$();
            return 1;
        }
        return groupCount;
    }
}
