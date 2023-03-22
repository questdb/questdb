/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
package io.questdb.cutlass.pgwire;


import java.net.URL;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import io.questdb.cairo.*;
import io.questdb.client.Sender;
import io.questdb.cutlass.line.tcp.*;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.mp.TestWorkerPool;
import io.questdb.mp.WorkerPool;
import io.questdb.network.DefaultIODispatcherConfiguration;
import io.questdb.network.IODispatcherConfiguration;
import io.questdb.network.NetworkFacade;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.std.FilesFacade;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.std.str.Path;
import org.junit.*;

public class ILPAndPGTest extends BasePGTest {

    static final AtomicBoolean O_EXIT = new AtomicBoolean(false);
    static final String[] O_IDS = genStrings(16, "generated-id-");
    static final String[] O_LINKED_IDS = genLinkedIds(O_IDS);
    static final Lock O_LOCK = new ReentrantLock();
    static final Rnd O_RAND = new Rnd();
    static final String[] O_STR1_VALS = genStrings(8, "STR1-");
    static final String[] O_STR2_VALS = genStrings(4, "STR1-");
    static final String O_TABLE = "table";
    private final static Log LOG = LogFactory.getLog(ILPAndPGTest.class);
    protected final int bindPort = 9002;
    protected final WorkerPool sharedWorkerPool = new TestWorkerPool(1, metrics);
    private final IODispatcherConfiguration ioDispatcherConfiguration = new DefaultIODispatcherConfiguration() {
        @Override
        public int getBindIPv4Address() {
            return 0;
        }

        @Override
        public int getBindPort() {
            return bindPort;
        }

        @Override
        public long getHeartbeatInterval() {
            return 15;
        }
    };

    protected String authKeyId = null;
    protected boolean autoCreateNewColumns = true;
    protected long commitIntervalDefault = 2000;
    protected double commitIntervalFraction = 0.5;
    protected boolean disconnectOnError = false;
    protected long maintenanceInterval = 25;
    protected int maxMeasurementSize = 256;
    protected long minIdleMsBeforeWriterRelease = 30000;
    protected int msgBufferSize = 256 * 1024;
    protected NetworkFacade nf = NetworkFacadeImpl.INSTANCE;
    protected int partitionByDefault = PartitionBy.DAY;
    protected boolean symbolAsFieldSupported;
    protected final LineTcpReceiverConfiguration lineConfiguration = new DefaultLineTcpReceiverConfiguration() {
        @Override
        public String getAuthDbPath() {
            if (null == authKeyId) {
                return null;
            }
            URL u = getClass().getResource("authDb.txt");
            assert u != null;
            return u.getFile();
        }

        @Override
        public boolean getAutoCreateNewColumns() {
            return autoCreateNewColumns;
        }

        @Override
        public long getCommitInterval() {
            return LineTcpReceiverConfigurationHelper.calcCommitInterval(
                    configuration.getO3MinLag(),
                    getCommitIntervalFraction(),
                    getCommitIntervalDefault()
            );
        }

        @Override
        public long getCommitIntervalDefault() {
            return commitIntervalDefault;
        }

        @Override
        public double getCommitIntervalFraction() {
            return commitIntervalFraction;
        }

        @Override
        public int getDefaultPartitionBy() {
            return partitionByDefault;
        }

        @Override
        public boolean getDisconnectOnError() {
            return disconnectOnError;
        }

        @Override
        public IODispatcherConfiguration getDispatcherConfiguration() {
            return ioDispatcherConfiguration;
        }

        @Override
        public long getMaintenanceInterval() {
            return maintenanceInterval;
        }

        @Override
        public int getMaxMeasurementSize() {
            return maxMeasurementSize;
        }

        @Override
        public MicrosecondClock getMicrosecondClock() {
            return testMicrosClock;
        }

        @Override
        public int getNetMsgBufferSize() {
            return msgBufferSize;
        }

        @Override
        public NetworkFacade getNetworkFacade() {
            return nf;
        }

        @Override
        public long getWriterIdleTimeout() {
            return minIdleMsBeforeWriterRelease;
        }

        @Override
        public int getWriterQueueCapacity() {
            return 4;
        }

        @Override
        public boolean isSymbolAsFieldSupported() {
            return symbolAsFieldSupported;
        }
    };
    long oMinTimeMs = 0L;

    @Test
    public void testQueryWhileInsertingDataReturnsSameCountsRegardlessOfOrdering() throws Exception {
        partitionByDefault = PartitionBy.HOUR;
        runInContext(r -> {
            final SOCountDownLatch ilpProducerHalted = new SOCountDownLatch(1);

            insertData(ilpProducerHalted);
            performQueries();
            O_EXIT.set(true);
            ilpProducerHalted.await();
        });
    }

    private static LineTcpReceiver createLineTcpReceiver(
            LineTcpReceiverConfiguration configuration,
            CairoEngine cairoEngine,
            WorkerPool workerPool
    ) {
        return new LineTcpReceiver(configuration, cairoEngine, workerPool, workerPool);
    }

    private static int nextInt(int lo, int hi) {
        return lo + O_RAND.nextInt(hi - lo);
    }

    private long nextLong(long lo, long hi) {
        return lo + O_RAND.nextLong(hi - lo);
    }

    static String[] genLinkedIds(String[] idsBase) {
        String[] out = new String[idsBase.length];
        for (int j = 0; j < idsBase.length; j++) {
            out[j] = "linkedId-" + idsBase[j];
        }
        return out;
    }

    static String[] genStrings(int n, String base) {
        String[] out = new String[n];
        for (int j = 0; j < n; j++) {
            out[j] = base + j;
        }
        return out;
    }

    static int getCountStar(String query, Connection conn) throws Exception {
        int count = -1;
        try (PreparedStatement stmt = conn.prepareStatement(query)) {
            LOG.info().$(" run query ").$(stmt).$();
            try (ResultSet result = stmt.executeQuery()) {
                if (result.next()) {
                    count = result.getInt(1);
                }
            }
            LOG.info().$(" query completed [stmt=").$(stmt).$(",count=").$(count).I$();
        }
        return count;
    }

    static int getRowCount(String query, Connection conn) throws Exception {
        int count = 0;
        try (PreparedStatement stmt = conn.prepareStatement(query)) {
            stmt.setFetchSize(100_000);
            LOG.info().$(" run query [stmt=").$(stmt).$();
            try (ResultSet result = stmt.executeQuery()) {
                while (result.next()) {
                    count++;
                }
            }
            LOG.info().$(" query completed [stmt=").$(stmt).$(",count=").$(count).I$();
        }
        return count;
    }

    static String randString(int minLen, int maxLen) {
        StringBuilder buf = new StringBuilder();
        int len = nextInt(minLen, maxLen + 1);
        for (int j = 0; j < len; j++) {
            if (minLen > 10 && O_RAND.nextDouble() > 0.8) {
                // sometimes append a UTF-8 character
                buf.append("ü");
            } else {
                // A-Z, a-z
                buf.append((char) (O_RAND.nextBoolean() ? nextInt(0x41, 0x5B) : nextInt(0x61, 0x7b)));
            }
        }
        return buf.toString();
    }

    static <T> T randValue(T[] arr) {
        return arr[nextInt(0, arr.length)];
    }

    void insertData(SOCountDownLatch ilpProducerHalted) {
        final Set<Long> allTimestamps = new HashSet<>();
        final int nRows = 4999;

        //insert basic data set
        try (Sender ilp = Sender.builder().address("127.0.0.1").port(bindPort).build()) {
            LOG.info().$("insert [").$(nRows).$("] rows").$();
            for (int j = 1; j <= nRows; j++) {
                insertRow(j, ilp, allTimestamps);
            }
            ilp.flush();
            LOG.info().$("insert [").$(nRows).$("] rows - DONE [").$(allTimestamps.size()).$("] unique timestamps").$();
        }

        // start background inserter thread
        Runnable backgroundInsert = () -> {
            int bgInsertCount = 0;
            try (Sender ilp = Sender.builder().address("127.0.0.1").port(bindPort).build()) {
                while (!O_EXIT.get()) {
                    O_LOCK.lock();
                    try {
                        insertRow(nextInt(1, 1000), ilp, new HashSet<>());
                        ++bgInsertCount;
                        if (bgInsertCount % 200 == 0) {
                            ilp.flush();
                            LOG.info().$("background-insert totals: [").$(bgInsertCount).I$();
                        }
                    } finally {
                        O_LOCK.unlock();
                    }
                }
            } catch (Exception e) {
                LOG.info().$("ERROR - backgroundInsert() failed").$();
                e.printStackTrace();
            }

            ilpProducerHalted.countDown();
        };

        Thread bgTask = new Thread(backgroundInsert, "Background ILP Insert");
        bgTask.setDaemon(true);
        bgTask.start();
    }

    void insertRow(int row, Sender ilp, Set<Long> allTimestamps) {
        String id = randValue(O_IDS);
        String linkedId = randValue(O_LINKED_IDS);
        String desc = randString(32, 256);
        String str1 = randValue(O_STR1_VALS);
        String str2 = randValue(O_STR2_VALS);
        boolean active = O_RAND.nextBoolean();
        long whenStartMs = System.currentTimeMillis() + nextInt(1, Math.max(2, row / 2));
        if (row > 2000) {
            whenStartMs += 300L;
        }
        if (oMinTimeMs == 0L || whenStartMs < oMinTimeMs) {
            oMinTimeMs = whenStartMs;
        }
        // mostly null
        Long whenEndMs = O_RAND.nextDouble() > 0.9 ? whenStartMs + nextLong(1000L, 2000L) : null;
        long tsNanos = whenStartMs * 1_000_000L;
        allTimestamps.add(tsNanos);
        ilp = ilp.table(O_TABLE)
                .symbol("id", id)
                .symbol("linkedId", linkedId)
                .stringColumn("description", desc)
                .stringColumn("stringCol1", str1)
                .stringColumn("stringCol2", str2)
                .boolColumn("active", active)
                .longColumn("whenStartMs", whenStartMs);
        if (whenEndMs != null) {
            ilp = ilp.longColumn("whenEndMs", whenEndMs);
        }
        ilp.at(tsNanos);
    }

    void performQueries() throws Exception {
        O_LOCK.lock();
        LOG.info().$("%n performing queries ...%n%n").$();
        try {
            try (
                    final PGWireServer server = createPGServer(2);
                    final WorkerPool workerPool = server.getWorkerPool()
            ) {
                workerPool.start(LOG);
                try (final Connection conn = getConnection(server.getPort(), false, true)) {
                    conn.setAutoCommit(false);
                    Thread.sleep(1000L);
                    DateTimeFormatter timeFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

                    String queryTimeFilter = timeFormat.format(Instant.ofEpochMilli(System.currentTimeMillis() - (86_400_000L / 24L))
                            .atOffset(ZoneOffset.UTC));

                    String queryBase = "select * from " + O_TABLE + ""
                            + " WHERE timestamp >= '" + queryTimeFilter
                            + "' ORDER BY " + "timestamp" + " ";

                    int countStar = getCountStar("SELECT COUNT(*) FROM '" + O_TABLE + "'", conn);
                    int ascCount = getRowCount(queryBase + "ASC", conn);
                    int ascLimitCount = getRowCount(queryBase + "ASC LIMIT 100000", conn);
                    int descCount = getRowCount(queryBase + "DESC", conn);
                    int descLimitCount = getRowCount(queryBase + "DESC LIMIT 100000", conn);

                    String message =
                            String.format("%n -- QUERY RESULTS -- %n"
                                    + "  count(*)       = [%d]%n"
                                    + "  descCount      = [%d]%n"
                                    + "  descLimitCount = [%d]%n"
                                    + "  ascCount       = [%d]%n"
                                    + "  ascLimitCount  = [%d]%n"
                                    + " -----------------%n", countStar, descCount, descLimitCount, ascCount, ascLimitCount);
                    boolean allEqual = countStar == descCount
                            && descCount == descLimitCount && descCount == ascCount && descCount == ascLimitCount;

                    Assert.assertTrue(message, allEqual);
                }
            }
        } finally {
            O_LOCK.unlock();
        }
    }

    protected void runInContext(AbstractLineTcpReceiverTest.LineTcpServerAwareContext r) throws Exception {
        runInContext(r, false, 250);
    }

    protected void runInContext(AbstractLineTcpReceiverTest.LineTcpServerAwareContext r, boolean needMaintenanceJob, long minIdleMsBeforeWriterRelease) throws Exception {
        runInContext(AbstractCairoTest.ff, r, needMaintenanceJob, minIdleMsBeforeWriterRelease);
    }

    protected void runInContext(FilesFacade ff, AbstractLineTcpReceiverTest.LineTcpServerAwareContext r, boolean needMaintenanceJob, long minIdleMsBeforeWriterRelease) throws Exception {
        this.minIdleMsBeforeWriterRelease = minIdleMsBeforeWriterRelease;
        assertMemoryLeak(ff, () -> {
            try (LineTcpReceiver receiver = createLineTcpReceiver(lineConfiguration, engine, sharedWorkerPool)) {
                O3Utils.setupWorkerPool(sharedWorkerPool, engine, null, null);
                if (needMaintenanceJob) {
                    sharedWorkerPool.assign(engine.getEngineMaintenanceJob());
                }
                sharedWorkerPool.start(LOG);
                try {
                    r.run(receiver);
                } catch (Throwable err) {
                    LOG.error().$("Stopping ILP worker pool because of an error").$(err).$();
                    throw err;
                } finally {
                    sharedWorkerPool.halt();
                    Path.clearThreadLocals();
                }
            } catch (Throwable err) {
                LOG.error().$("Stopping ILP receiver because of an error").$(err).$();
                throw err;
            }
        });
    }
}
