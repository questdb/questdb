/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.metrics;

import io.questdb.std.IntHashSet;
import io.questdb.std.IntObjHashMap;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class DatabaseActivityRegistry {
    private static final DatabaseActivityRegistry INSTANCE = new DatabaseActivityRegistry();
    private final AtomicLong serverStartTime = new AtomicLong(Os.currentTimeMicros());
    private final AtomicInteger nextConnectionId = new AtomicInteger(1);
    private final IntObjHashMap<ConnectionInfo> connections = new IntObjHashMap<>();
    private final AtomicLong totalConnections = new AtomicLong(0);
    private final AtomicLong totalQueries = new AtomicLong(0);
    private final AtomicLong totalTransactionCommits = new AtomicLong(0);
    private final AtomicLong totalTransactionRollbacks = new AtomicLong(0);

    private DatabaseActivityRegistry() {
    }

    public static DatabaseActivityRegistry getInstance() {
        return INSTANCE;
    }

    public int registerConnection(String remoteAddress, String username, String database) {
        int connectionId = nextConnectionId.getAndIncrement();
        ConnectionInfo info = new ConnectionInfo(connectionId, remoteAddress, username, database, Os.currentTimeMicros());
        synchronized (connections) {
            connections.put(connectionId, info);
        }
        totalConnections.incrementAndGet();
        return connectionId;
    }

    public void unregisterConnection(int connectionId) {
        synchronized (connections) {
            connections.remove(connectionId);
        }
    }

    public void updateQuery(int connectionId, String query, long queryStartTime) {
        synchronized (connections) {
            ConnectionInfo info = connections.get(connectionId);
            if (info != null) {
                info.currentQuery = query;
                info.queryStartTime = queryStartTime;
                info.state = "active";
            }
        }
        totalQueries.incrementAndGet();
    }

    public void clearQuery(int connectionId) {
        synchronized (connections) {
            ConnectionInfo info = connections.get(connectionId);
            if (info != null) {
                info.currentQuery = null;
                info.queryStartTime = 0;
                info.state = "idle";
            }
        }
    }

    public void incrementCommits() {
        totalTransactionCommits.incrementAndGet();
    }

    public void incrementRollbacks() {
        totalTransactionRollbacks.incrementAndGet();
    }

    public ConnectionInfo[] getActiveConnections() {
        synchronized (connections) {
            ConnectionInfo[] result = new ConnectionInfo[connections.size()];
            int i = 0;
            for (int j = 0, n = connections.capacity(); j < n; j++) {
                if (connections.keyAt(j) != IntHashSet.NO_ENTRY_KEY) {
                    result[i++] = connections.valueAt(j);
                }
            }
            return result;
        }
    }

    public long getServerStartTime() {
        return serverStartTime.get();
    }

    public int getConnectionCount() {
        synchronized (connections) {
            return connections.size();
        }
    }

    public long getTotalConnections() {
        return totalConnections.get();
    }

    public long getTotalQueries() {
        return totalQueries.get();
    }

    public long getTotalCommits() {
        return totalTransactionCommits.get();
    }

    public long getTotalRollbacks() {
        return totalTransactionRollbacks.get();
    }

    public static class ConnectionInfo {
        public final int connectionId;
        public final String remoteAddress;
        public final String username;
        public final String database;
        public final long connectionStartTime;
        public volatile String currentQuery;
        public volatile long queryStartTime;
        public volatile String state = "idle";

        public ConnectionInfo(int connectionId, String remoteAddress, String username, String database, long connectionStartTime) {
            this.connectionId = connectionId;
            this.remoteAddress = remoteAddress;
            this.username = username;
            this.database = database;
            this.connectionStartTime = connectionStartTime;
        }
    }
}