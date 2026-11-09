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

package io.questdb.test.cutlass.qwp;

import io.questdb.Bootstrap;
import io.questdb.DefaultBootstrapConfiguration;
import io.questdb.PropertyKey;
import io.questdb.cairo.ColumnType;
import io.questdb.client.cutlass.qwp.client.QwpBindSetter;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;
import io.questdb.client.cutlass.qwp.client.QwpQueryClient;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.cutlass.qwp.server.egress.QwpBackwardReadAhead;
import io.questdb.cutlass.qwp.server.egress.QwpEgressMetrics;
import io.questdb.cutlass.qwp.server.egress.QwpEgressUpgradeProcessor;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.Misc;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.TestServerMain;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * QWP egress over plans that scan the designated timestamp backward, which the server
 * streams from a backward page-frame cursor: frames arrive newest first, and each frame
 * stores its rows oldest first. Each test checks the streamed rows against two oracles
 * that do not share the code under test: the in-process engine's result for the same SQL,
 * and the same query run forward over QWP, reversed.
 */
public class QwpEgressBackwardScanTest extends AbstractReusedServerQwpEgressTest {

    // Every column type QWP egress ships; the narrow DECIMALs, GEOHASH and the var-size
    // types take the per-row fallback of the page-frame append, the rest the columnar path.
    static final String ALL_TYPES_COLUMNS = """
            x LONG,
            b BOOLEAN,
            bt BYTE,
            sh SHORT,
            ch CHAR,
            i INT,
            l LONG,
            f FLOAT,
            d DOUBLE,
            dt DATE,
            tn TIMESTAMP_NS,
            ip IPV4,
            sy SYMBOL,
            vc VARCHAR,
            st STRING,
            bn BINARY,
            u UUID,
            l256 LONG256,
            g1 GEOHASH(3b),
            g2 GEOHASH(2c),
            g4 GEOHASH(5c),
            g8 GEOHASH(10c),
            d8 DECIMAL(2, 1),
            d16 DECIMAL(4, 2),
            d32 DECIMAL(9, 3),
            d64 DECIMAL(18, 4),
            d128 DECIMAL(38, 6),
            d256 DECIMAL(76, 10),
            arr DOUBLE[],
            ts TIMESTAMP""";
    // Columns added after the first rows exist, so earlier rows read them through a column top.
    static final String COLUMN_TOP_COLUMNS = """
            tl LONG,
            td DOUBLE,
            ti INT,
            tip IPV4,
            tf FLOAT,
            tsh SHORT,
            tch CHAR,
            tbt BYTE,
            tb BOOLEAN,
            tsy SYMBOL,
            tvc VARCHAR,
            tt TIMESTAMP,
            td64 DECIMAL(18, 2)""";
    private static final Log LOG = LogFactory.getLog(QwpEgressBackwardScanTest.class);
    // the frame of the read-ahead tests: one 200,000-row partition of LONG and DOUBLE
    private static final long READ_AHEAD_FRAME_ROWS = 200_000;
    private static final String[] SMALL_PAGE_FRAMES = {
            PropertyKey.CAIRO_SQL_PAGE_FRAME_MIN_ROWS.getEnvVarName(), "64",
            PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS.getEnvVarName(), "128"
    };

    // How the servers startServer boots read the frames of a backward stream ahead: through
    // MADV_WILLNEED windows or by reading each frame at once, drawn once per class, so every
    // test here runs either way on every platform.
    private static boolean isReadAheadWillNeed;

    @BeforeClass
    public static void setUpReadAhead() {
        isReadAheadWillNeed = TestUtils.generateRandom(LOG).nextBoolean();
        LOG.info().$("backward read-ahead [willNeed=").$(isReadAheadWillNeed).$(']').$();
    }

    @Test
    public void testAllTypesDefaultPageFrames() throws Exception {
        // Default page frames hold each partition in one frame, so a 3,000-row partition
        // streams as several 1,024-row slices taken from the top of the frame down.
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startServer()) {
                createAllTypesTable(serverMain, "t", 3_000, 2, true);
                try (QwpQueryClient client = connect("")) {
                    assertBackwardScan(serverMain, client, "t", "");
                    assertBackwardScan(serverMain, client, "t", "WHERE ts >= '1970-01-01T13:00:00.000000Z'");
                }
            }
        });
    }

    @Test
    public void testAllTypesSmallPageFrames() throws Exception {
        // 128-row frames split every partition into many frames, and the column tops
        // split the frames again where the added columns start.
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startServer(SMALL_PAGE_FRAMES)) {
                createAllTypesTable(serverMain, "t", 1_000, 3, true);
                try (QwpQueryClient client = connect("")) {
                    assertBackwardScan(serverMain, client, "t", "");
                }
            }
        });
    }

    @Test
    public void testBackwardScanMapsPartitionsWithoutSequentialHint() throws Exception {
        // MADV_SEQUENTIAL points readahead forward, the wrong way for a backward scan, so
        // only forward streams open their partitions under the sequential scan profile,
        // which maps them with it. Only Linux has the hint: elsewhere POSIX_MADV_SEQUENTIAL
        // is -1, the "no advice" value every unhinted mapping passes to madvise() too.
        final boolean hasSequentialHint = Files.POSIX_MADV_SEQUENTIAL != -1;
        final AtomicInteger sequentialHints = new AtomicInteger();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public void madvise(long address, long len, int advise) {
                if (hasSequentialHint && advise == Files.POSIX_MADV_SEQUENTIAL) {
                    sequentialHints.incrementAndGet();
                }
                super.madvise(address, len, advise);
            }
        };
        // the partitions of t a query opens, each with the scan profile it opens under
        final Queue<String> partitionOpens = new ConcurrentLinkedQueue<>();
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startServerWithRetry(() -> {
                final TestServerMain server = new TestServerMain(new Bootstrap(new DefaultBootstrapConfiguration() {
                    @Override
                    public FilesFacade getFilesFacade() {
                        return ff;
                    }
                }, Bootstrap.getServerMainArgs(root)));
                try {
                    // set before start(), so every worker that creates a reader sees it
                    server.getEngine().setReaderListener((reader, partitionIndex) -> {
                        if ("t".equals(reader.getTableToken().getTableName())) {
                            partitionOpens.add(partitionIndex + ":" + reader.getScanProfile());
                        }
                    });
                    server.start();
                    return server;
                } catch (Throwable th) {
                    Misc.free(server, th);
                    throw th;
                }
            })) {
                serverMain.execute("CREATE TABLE t (x LONG, v VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("INSERT INTO t SELECT x, 'v' || x, (x * 3_600_000_000L)::TIMESTAMP FROM long_sequence(100)");
                serverMain.awaitTable("t");
                try (QwpQueryClient client = connect("")) {
                    // a released reader opens and maps its partitions afresh on the next query
                    serverMain.getEngine().releaseAllReaders();
                    partitionOpens.clear();
                    sequentialHints.set(0);
                    Assert.assertEquals(descendingLongs(100, 1), column(qwpRows(client, "SELECT x, v FROM t ORDER BY ts DESC"), 0));
                    // ts runs from 1970-01-01T01:00 to 1970-01-05T04:00: daily partitions 0 to 4
                    Assert.assertEquals(
                            "[4:DEFAULT, 3:DEFAULT, 2:DEFAULT, 1:DEFAULT, 0:DEFAULT]",
                            partitionOpens.toString()
                    );
                    Assert.assertEquals(0, sequentialHints.get());

                    serverMain.getEngine().releaseAllReaders();
                    partitionOpens.clear();
                    Assert.assertEquals(100, qwpRows(client, "SELECT x, v FROM t ORDER BY ts").size());
                    Assert.assertEquals(
                            "[0:SEQUENTIAL_CACHED, 1:SEQUENTIAL_CACHED, 2:SEQUENTIAL_CACHED, 3:SEQUENTIAL_CACHED, 4:SEQUENTIAL_CACHED]",
                            partitionOpens.toString()
                    );
                    if (hasSequentialHint) {
                        Assert.assertTrue("hints=" + sequentialHints.get(), sequentialHints.get() > 0);
                    }
                }
            }
        });
    }

    @Test
    public void testBackwardScanDoesNotAskForFramesInMemory() throws Exception {
        // Past its first batch, a backward scan asks the OS for no frame that is in memory: it
        // checks the frames, and trusts small ones after a streak, until its thread reads from
        // storage or the stream moves to another thread. Every query starts afresh, however it
        // parks: the frames of its first batch go out unchecked.
        TestUtils.assertMemoryLeak(() -> {
            final ReadAheadFilesFacade ff = new ReadAheadFilesFacade(true) {
                @Override
                public long getThreadMajorFaults() {
                    return 0;
                }

                @Override
                public boolean isPageCacheResident(long address, long len) {
                    // counts the check
                    super.isPageCacheResident(address, len);
                    return true;
                }
            };
            try (TestServerMain serverMain = startServer(ff, PropertyKey.METRICS_ENABLED.getEnvVarName(), "true")) {
                serverMain.execute("CREATE TABLE t (x LONG, d DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                // 60 days of 1,000 rows, a small frame a day
                serverMain.execute("INSERT INTO t SELECT x, x * 0.5, ((x - 1) * 86_400_000L)::TIMESTAMP FROM long_sequence(60_000)");
                serverMain.awaitTable("t");
                final QwpEgressMetrics metrics = serverMain.getEngine().getMetrics().qwpEgressMetrics();
                final String desc = "SELECT x, d FROM t ORDER BY ts DESC";
                // the 17 days whose top rows go out before row 16,384, at 16 bytes a row
                final long firstBatchBytes = 17_000 * 16L;
                try (QwpQueryClient client = connect("")) {
                    for (int run = 0; run < 2; run++) {
                        final long checks = ff.residencyCheckCount();
                        assertReadAhead(ff, metrics, true, firstBatchBytes, () -> Assert.assertEquals(descendingLongs(60_000, 1), column(qwpRows(client, desc), 0)));
                        // a column at a time, at least the streak before the stream trusted, at
                        // most every frame after the first batch
                        final long streamChecks = ff.residencyCheckCount() - checks;
                        Assert.assertTrue("checks=" + streamChecks, streamChecks >= 2L * QwpBackwardReadAhead.TRUST_STREAK && streamChecks <= 2L * 43);
                    }
                }
                // parked after every 7-row batch, the stream resumes on either worker thread
                try (QwpQueryClient client = QwpQueryClient.fromConfig("ws::addr=127.0.0.1:" + HTTP_PORT + ";max_batch_rows=7;")
                        .withInitialCredit(1)) {
                    client.connect();
                    assertReadAhead(ff, metrics, true, firstBatchBytes, () -> Assert.assertEquals(descendingLongs(60_000, 1), column(qwpRows(client, desc), 0)));
                }
            }
        });
    }

    @Test
    public void testBackwardScanReadsEachFrameAheadOnce() throws Exception {
        // A backward scan over data not in memory reads every native frame ahead exactly once,
        // whatever the batches and the parks in between, through MADV_WILLNEED windows and by
        // reading each frame at once alike; a forward scan and parquet frames read nothing ahead.
        for (boolean isWillNeed : new boolean[]{true, false}) {
            TestUtils.assertMemoryLeak(() -> {
                final ReadAheadFilesFacade ff = new ReadAheadFilesFacade(isWillNeed, true);
                final String table = isWillNeed ? "t_advised" : "t_read";
                try (TestServerMain serverMain = startServer(
                        ff,
                        PropertyKey.METRICS_ENABLED.getEnvVarName(), "true",
                        PropertyKey.CAIRO_SQL_PAGE_FRAME_MIN_ROWS.getEnvVarName(), "64",
                        PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS.getEnvVarName(), "128"
                )) {
                    serverMain.execute("CREATE TABLE " + table + " (x LONG, d DOUBLE, s SYMBOL, v VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                    // 400 rows a day over 3 days
                    serverMain.execute("INSERT INTO " + table + " SELECT x, x * 0.5, 's' || (x % 7), 'v' || x, ((x - 1) * 216_000_000L)::TIMESTAMP " +
                            "FROM long_sequence(1_200)");
                    serverMain.awaitTable(table);
                    final QwpEgressMetrics metrics = serverMain.getEngine().getMetrics().qwpEgressMetrics();
                    // LONG + DOUBLE + SYMBOL key: 20 bytes a row
                    final String desc = "SELECT x, d, s FROM " + table + " ORDER BY ts DESC";
                    try (QwpQueryClient client = connect("")) {
                        assertReadAhead(ff, metrics, isWillNeed, 1_200 * 20L, () -> Assert.assertEquals(descendingLongs(1_200, 1), column(qwpRows(client, desc), 0)));
                        assertReadAhead(ff, metrics, isWillNeed, 0, () -> Assert.assertEquals(1_200, qwpRows(client, "SELECT x, d, s FROM " + table + " ORDER BY ts").size()));
                        // an interval reads the frames of its own rows only: the second day
                        assertReadAhead(ff, metrics, isWillNeed, 400 * 20L, () -> Assert.assertEquals(
                                descendingLongs(800, 401),
                                column(qwpRows(client, "SELECT x, d, s FROM " + table + " WHERE ts IN '1970-01-02' ORDER BY ts DESC"), 0)
                        ));
                    }
                    // 7-row batches, each parked until the client grants credit: frames resume
                    // mid-way many times and are still read ahead once
                    try (QwpQueryClient client = QwpQueryClient.fromConfig("ws::addr=127.0.0.1:" + HTTP_PORT + ";max_batch_rows=7;")
                            .withInitialCredit(1)) {
                        client.connect();
                        assertReadAhead(ff, metrics, isWillNeed, 1_200 * 20L, () -> Assert.assertEquals(descendingLongs(1_200, 1), column(qwpRows(client, desc), 0)));
                    }
                    serverMain.execute("ALTER TABLE " + table + " CONVERT PARTITION TO PARQUET WHERE ts < '1970-01-02'");
                    serverMain.awaitTable(table);
                    try (QwpQueryClient client = connect("")) {
                        // the first day's 400 rows now come from parquet
                        assertReadAhead(ff, metrics, isWillNeed, 800 * 20L, () -> Assert.assertEquals(descendingLongs(1_200, 1), column(qwpRows(client, desc), 0)));
                    }
                }
            });
        }
    }

    @Test
    public void testBackwardStreamsReadAheadTheirOwnFramesConcurrently() throws Exception {
        // The read-ahead keeps its window per connection: streams that interleave their
        // batches on one server each read their own frames, every row once, over data that
        // is not in memory.
        TestUtils.assertMemoryLeak(() -> {
            final ReadAheadFilesFacade ff = new ReadAheadFilesFacade(true, true);
            try (TestServerMain serverMain = startReadAheadServer(ff)) {
                final QwpEgressMetrics metrics = serverMain.getEngine().getMetrics().qwpEgressMetrics();
                final int streams = 3;
                final int runs = 2;
                final long before = metrics.readAheadBytesCount();
                final QwpQueryClient[] clients = new QwpQueryClient[streams];
                final Thread[] threads = new Thread[streams];
                final Throwable[] errors = new Throwable[streams];
                try {
                    for (int i = 0; i < streams; i++) {
                        // credit for one batch at a time: each stream parks after every batch,
                        // and the streams take turns on the server's workers
                        clients[i] = QwpQueryClient.fromConfig("ws::addr=127.0.0.1:" + HTTP_PORT + ";failover=off;").withInitialCredit(1);
                        clients[i].connect();
                    }
                    for (int i = 0; i < streams; i++) {
                        final int index = i;
                        threads[i] = new Thread(() -> {
                            try {
                                for (int r = 0; r < runs; r++) {
                                    Assert.assertEquals(
                                            descendingLongs(READ_AHEAD_FRAME_ROWS, 1),
                                            column(qwpRows(clients[index], "SELECT x, d FROM t ORDER BY ts DESC"), 0)
                                    );
                                }
                            } catch (Throwable th) {
                                errors[index] = th;
                            }
                        });
                        threads[i].start();
                    }
                    for (Thread thread : threads) {
                        thread.join();
                    }
                } finally {
                    for (QwpQueryClient client : clients) {
                        if (client != null) {
                            client.close();
                        }
                    }
                }
                for (Throwable error : errors) {
                    if (error != null) {
                        throw new AssertionError(error);
                    }
                }
                Assert.assertEquals(streams * runs * READ_AHEAD_FRAME_ROWS * 16, metrics.readAheadBytesCount() - before);
                Assert.assertEquals(streams * runs * READ_AHEAD_FRAME_ROWS * 16, ff.advisedBytes());
            }
        });
    }

    @Test
    public void testBatchBoundariesInsideFrame() throws Exception {
        // Small client batches end inside a frame, so every batch resumes the frame
        // where the previous one stopped; one partition with a 1,500-row frame and one
        // with 1-row frames' worth of rows exercise both slice sizes.
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startServer()) {
                createAllTypesTable(serverMain, "t", 1_500, 2, false);
                for (int maxBatchRows : new int[]{1, 7, 100, 1_023, 1_025}) {
                    try (QwpQueryClient client = connect("max_batch_rows=" + maxBatchRows + ";")) {
                        assertBackwardScan(serverMain, client, "t", "");
                    }
                }
            }
        });
    }

    @Test
    public void testCachedFactoryAfterNewRows() throws Exception {
        // The second run reuses the factory the first one cached; rows inserted after it
        // must come back first, and the repeat must return the same rows.
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startServer()) {
                serverMain.execute("CREATE TABLE t (x LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("INSERT INTO t SELECT x, (x * 3_600_000_000L)::TIMESTAMP FROM long_sequence(30)");
                serverMain.awaitTable("t");
                try (QwpQueryClient client = connect("")) {
                    final String sql = "SELECT x FROM t ORDER BY ts DESC";
                    Assert.assertEquals(descendingLongs(30, 1), qwpRows(client, sql));
                    Assert.assertEquals(descendingLongs(30, 1), qwpRows(client, sql));
                    serverMain.execute("INSERT INTO t SELECT x + 30, ((x + 30) * 3_600_000_000L)::TIMESTAMP FROM long_sequence(20)");
                    serverMain.awaitTable("t");
                    Assert.assertEquals(descendingLongs(50, 1), qwpRows(client, sql));
                    serverMain.execute("TRUNCATE TABLE t");
                    serverMain.awaitTable("t");
                    Assert.assertEquals(List.of(), qwpRows(client, sql));
                    serverMain.execute("INSERT INTO t SELECT x, (x * 3_600_000_000L)::TIMESTAMP FROM long_sequence(3)");
                    serverMain.awaitTable("t");
                    Assert.assertEquals(descendingLongs(3, 1), qwpRows(client, sql));
                }
            }
        });
    }

    @Test
    public void testCancelDuringBackwardScan() throws Exception {
        // A credit of 1 byte parks the stream after every batch, so the cancel lands while
        // the backward cursor is mid-frame; the next query on the connection must start clean.
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startServer()) {
                serverMain.execute("CREATE TABLE t (x LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("INSERT INTO t SELECT x, (x * 1_000_000L)::TIMESTAMP FROM long_sequence(10_000)");
                serverMain.awaitTable("t");
                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                                "ws::addr=127.0.0.1:" + HTTP_PORT + ";failover=off;max_batch_rows=100;")
                        .withInitialCredit(1)) {
                    client.connect();
                    final List<String> firstBatch = new ArrayList<>();
                    final byte[] status = {0};
                    final boolean[] isEndSeen = {false};
                    client.execute("SELECT x FROM t ORDER BY ts DESC", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            if (firstBatch.isEmpty()) {
                                for (int r = 0; r < batch.getRowCount(); r++) {
                                    firstBatch.add(Long.toString(batch.getLongValue(0, r)));
                                }
                                client.cancel();
                            }
                        }

                        @Override
                        public void onEnd(long totalRows) {
                            isEndSeen[0] = true;
                        }

                        @Override
                        public void onError(byte s, String message) {
                            status[0] = s;
                        }
                    });
                    Assert.assertEquals(descendingLongs(10_000, 9_901), firstBatch);
                    Assert.assertFalse(isEndSeen[0]);
                    Assert.assertEquals(QwpConstants.STATUS_CANCELLED, status[0]);
                    Assert.assertEquals(descendingLongs(10_000, 1), qwpRows(client, "SELECT x FROM t ORDER BY ts DESC"));
                    Assert.assertEquals(descendingLongs(3, 1), qwpRows(client, "SELECT x FROM t WHERE ts <= 3_000_000 ORDER BY ts DESC"));
                }
            }
        });
    }

    @Test
    public void testFirstBatchWaitsForItsOwnRowsOnly() throws Exception {
        // A descending stream over data not in memory asks for the rows of its first batch,
        // and of the next window, and sends the batch: by then MADV_WILLNEED windows have asked for the top 16,384
        // rows of the 200,000-row frame and, from the middle of them on, for the next 32,768.
        // Reading each frame at once reads the whole frame before the first row goes out.
        // Credit for one batch parks the stream after the batch, so the count the client
        // sees in its first batch is all the stream asked for before it.
        for (boolean isWillNeed : new boolean[]{true, false}) {
            TestUtils.assertMemoryLeak(() -> {
                final ReadAheadFilesFacade ff = new ReadAheadFilesFacade(isWillNeed, true);
                try (TestServerMain serverMain = startReadAheadServer(ff)) {
                    final QwpEgressMetrics metrics = serverMain.getEngine().getMetrics().qwpEgressMetrics();
                    try (QwpQueryClient client = QwpQueryClient.fromConfig("ws::addr=127.0.0.1:" + HTTP_PORT + ";failover=off;")
                            .withInitialCredit(1)) {
                        client.connect();
                        final long before = metrics.readAheadBytesCount();
                        // the bytes read ahead, and the bytes asked of the OS, when the first batch arrives
                        final long[] atFirstBatch = {-1, -1};
                        final List<String> firstBatch = new ArrayList<>();
                        final byte[] status = {0};
                        client.execute("SELECT x, d FROM t ORDER BY ts DESC", new QwpColumnBatchHandler() {
                            @Override
                            public void onBatch(QwpColumnBatch batch) {
                                if (firstBatch.isEmpty()) {
                                    atFirstBatch[0] = metrics.readAheadBytesCount() - before;
                                    atFirstBatch[1] = ff.advisedBytes();
                                    for (int r = 0; r < batch.getRowCount(); r++) {
                                        firstBatch.add(Long.toString(batch.getLongValue(0, r)));
                                    }
                                    client.cancel();
                                }
                            }

                            @Override
                            public void onEnd(long totalRows) {
                            }

                            @Override
                            public void onError(byte s, String message) {
                                status[0] = s;
                            }
                        });
                        Assert.assertEquals(descendingLongs(READ_AHEAD_FRAME_ROWS, READ_AHEAD_FRAME_ROWS - 16_383), firstBatch);
                        Assert.assertEquals(QwpConstants.STATUS_CANCELLED, status[0]);
                        // LONG + DOUBLE: 16 bytes a row
                        Assert.assertEquals(
                                isWillNeed ? 3 * QwpBackwardReadAhead.INITIAL_WINDOW_ROWS * 16 : READ_AHEAD_FRAME_ROWS * 16,
                                atFirstBatch[0]
                        );
                        Assert.assertEquals(isWillNeed ? atFirstBatch[0] : 0, atFirstBatch[1]);
                        // The client grants credit for the first batch before the server reads its
                        // cancel, so the stream sends a second batch, whose first slice moves into
                        // the second window and asks for the third, 65,536 rows. The frame is read
                        // in full by then only when it is read at once.
                        final long readAhead = metrics.readAheadBytesCount() - before;
                        Assert.assertEquals(
                                isWillNeed ? 7 * QwpBackwardReadAhead.INITIAL_WINDOW_ROWS * 16 : READ_AHEAD_FRAME_ROWS * 16,
                                readAhead
                        );
                        Assert.assertEquals(isWillNeed ? readAhead : 0, ff.advisedBytes());
                        // the connection then streams the query whole, every row read ahead once
                        assertReadAhead(ff, metrics, isWillNeed, READ_AHEAD_FRAME_ROWS * 16, () -> Assert.assertEquals(
                                descendingLongs(READ_AHEAD_FRAME_ROWS, 1),
                                column(qwpRows(client, "SELECT x, d FROM t ORDER BY ts DESC"), 0)
                        ));
                    }
                }
            });
        }
    }

    @Test
    public void testCreditParksBackwardScanMidFrame() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startServer()) {
                createAllTypesTable(serverMain, "t", 2_000, 2, true);
                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                                "ws::addr=127.0.0.1:" + HTTP_PORT + ";max_batch_rows=150;")
                        .withInitialCredit(1)) {
                    client.connect();
                    assertBackwardScan(serverMain, client, "t", "");
                }
            }
        });
    }

    @Test
    public void testEmptySingleRowAndAllNull() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startServer()) {
                serverMain.execute("CREATE TABLE empty (x LONG, v VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("CREATE TABLE one (x LONG, v VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("INSERT INTO one VALUES (42, 'only', '2024-02-29T12:00:00.000000Z')");
                serverMain.execute("CREATE TABLE nulls (x LONG, l LONG, d DOUBLE, v VARCHAR, s SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("INSERT INTO nulls SELECT x, NULL, NULL, NULL, NULL, (x * 7_200_000_000L)::TIMESTAMP FROM long_sequence(50)");
                serverMain.awaitTable("empty");
                serverMain.awaitTable("one");
                serverMain.awaitTable("nulls");
                try (QwpQueryClient client = connect("")) {
                    final List<String> columns = new ArrayList<>();
                    final long[] totalRows = {-1};
                    client.execute("SELECT x, v FROM empty ORDER BY ts DESC", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            Assert.assertEquals(0, batch.getRowCount());
                            for (int c = 0; c < batch.getColumnCount(); c++) {
                                columns.add(batch.getColumnName(c));
                            }
                        }

                        @Override
                        public void onEnd(long rows) {
                            totalRows[0] = rows;
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail(message);
                        }
                    });
                    Assert.assertEquals(List.of("x", "v"), columns);
                    Assert.assertEquals(0, totalRows[0]);
                    Assert.assertEquals(List.of("42|'only'"), qwpRows(client, "SELECT x, v FROM one ORDER BY ts DESC"));
                    assertBackwardScan(serverMain, client, "one", "");
                    assertBackwardScan(serverMain, client, "nulls", "");
                }
            }
        });
    }

    @Test
    public void testIntervalScans() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startServer(SMALL_PAGE_FRAMES)) {
                createAllTypesTable(serverMain, "t", 700, 3, false);
                try (QwpQueryClient client = connect("")) {
                    assertBackwardScan(serverMain, client, "t", "WHERE ts IN '1970-01-02'");
                    assertBackwardScan(serverMain, client, "t", "WHERE ts >= '1970-01-01T11:00:00.000000Z' AND ts < '1970-01-03T05:00:00.000000Z'");
                    assertBackwardScan(serverMain, client, "t", "WHERE ts BETWEEN '1970-01-02T01:00:00.000000Z' AND '1970-01-02T01:30:00.000000Z'");
                    assertBackwardScan(serverMain, client, "t", "WHERE ts > '1970-01-03T23:00:00.000000Z'");
                    assertBackwardScan(serverMain, client, "t", "WHERE ts IN '1980'");
                    // several intervals, walked newest first, two of them in one partition
                    assertBackwardScan(serverMain, client, "t", "WHERE ts IN '1970-01-01' OR ts IN '1970-01-03T02' OR ts IN '1970-01-03T20'");
                    assertBackwardScan(serverMain, client, "t", "WHERE ts NOT IN '1970-01-02T00;6h'");
                }
            }
        });
    }

    @Test
    public void testNonPartitionedAndNonWalTables() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startServer()) {
                serverMain.getSqlExecutionContext().setRandom(TestUtils.generateRandom(LOG));
                serverMain.execute("CREATE TABLE np (" + ALL_TYPES_COLUMNS + ") TIMESTAMP(ts) PARTITION BY NONE");
                serverMain.execute("INSERT INTO np " + allTypesSelect(1, 2_500, 0, 1_000_000));
                serverMain.execute("CREATE TABLE nw (" + ALL_TYPES_COLUMNS + ") TIMESTAMP(ts) PARTITION BY HOUR BYPASS WAL");
                serverMain.execute("INSERT INTO nw " + allTypesSelect(1, 2_500, 0, 5_000_000));
                try (QwpQueryClient client = connect("")) {
                    assertBackwardScan(serverMain, client, "np", "");
                    assertBackwardScan(serverMain, client, "nw", "");
                }
            }
        });
    }

    @Test
    public void testOrderByDesignatedTimestampDesc() throws Exception {
        // The report: one row per hour, one page frame per day, frames newest first.
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startServer()) {
                serverMain.execute("CREATE TABLE tb (x LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("""
                        INSERT INTO tb VALUES
                            (1, '1970-01-01T00:00:00Z'),
                            (2, '1970-01-01T01:00:00Z'),
                            (3, '1970-01-01T02:00:00Z'),
                            (4, '1970-01-02T00:00:00Z'),
                            (5, '1970-01-02T01:00:00Z'),
                            (6, '1970-01-02T02:00:00Z')
                        """);
                serverMain.awaitTable("tb");
                try (QwpQueryClient client = connect("")) {
                    Assert.assertEquals(descendingLongs(6, 1), qwpRows(client, "SELECT x FROM tb ORDER BY ts DESC"));
                    // the bare backward page frame, timestamps in micros
                    Assert.assertEquals(
                            List.of(
                                    "93600000000|6", "90000000000|5", "86400000000|4",
                                    "7200000000|3", "3600000000|2", "0|1"
                            ),
                            qwpRows(client, "SELECT ts, x FROM tb ORDER BY ts DESC")
                    );
                }
            }
        });
    }

    @Test
    public void testParquetPartitions() throws Exception {
        // Parquet frames take the per-row path; mix them with native partitions and with
        // a partition whose rows predate the added columns.
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startServer()) {
                createAllTypesTable(serverMain, "t", 800, 4, true);
                serverMain.execute("ALTER TABLE t CONVERT PARTITION TO PARQUET WHERE ts < '1970-01-03'");
                serverMain.awaitTable("t");
                TestUtils.assertEquals(
                        "count\n2\n",
                        TestUtils.printSqlToString(serverMain.getEngine(), serverMain.getSqlExecutionContext(), "SELECT count() FROM table_partitions('t') WHERE isParquet", new StringSink())
                );
                try (QwpQueryClient client = connect("")) {
                    assertBackwardScan(serverMain, client, "t", "");
                    assertBackwardScan(serverMain, client, "t", "WHERE ts >= '1970-01-01T20:00:00.000000Z'");
                }
            }
        });
    }

    @Test
    public void testPartialEmitOfBackwardBatches() throws Exception {
        // A small send buffer splits each batch, so the unsent suffix of a backward batch
        // ships first in the next frame.
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startServer(PropertyKey.HTTP_SEND_BUFFER_SIZE.getEnvVarName(), "256K")) {
                createAllTypesTable(serverMain, "t", 3_000, 2, true);
                try (QwpQueryClient client = connect("")) {
                    final List<String> desc = qwpRows(client, "SELECT * FROM t ORDER BY ts DESC");
                    final int[] batches = {0};
                    client.execute("SELECT * FROM t ORDER BY ts DESC", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            batches[0]++;
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail(message);
                        }
                    });
                    // 6,000 rows fit one 16,384-row batch, so more than one frame means the
                    // buffer split them.
                    Assert.assertTrue("batches=" + batches[0], batches[0] > 1);
                    Assert.assertTrue(6_000 < QwpEgressUpgradeProcessor.MAX_ROWS_PER_BATCH);
                    assertBackwardScan(serverMain, client, "t", "");
                    Assert.assertEquals(desc, qwpRows(client, "SELECT * FROM t ORDER BY ts DESC"));
                }
            }
        });
    }

    @Test
    public void testPlansStreamFromBackwardPageFrames() throws Exception {
        // Pins the plans the other tests rely on: these shapes reach the backward
        // page-frame path rather than a sort or a limit.
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startServer()) {
                serverMain.execute("CREATE TABLE t (x LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                for (String sql : new String[]{
                        "SELECT x FROM t ORDER BY ts DESC",
                        "SELECT ts, x FROM t ORDER BY ts DESC",
                        "t ORDER BY ts DESC",
                        "SELECT x FROM (SELECT * FROM t ORDER BY ts DESC)",
                        "SELECT x FROM t WHERE ts IN '1970-01-02' ORDER BY ts DESC"
                }) {
                    assertQuery(serverMain, sql).assertsPlanContaining("Row backward scan");
                    assertQuery(serverMain, sql).assertsPlanNotContaining("Sort", "Limit");
                }
            }
        });
    }

    @Test
    public void testSqlContexts() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startServer(SMALL_PAGE_FRAMES)) {
                createAllTypesTable(serverMain, "t", 500, 3, true);
                serverMain.execute("CREATE VIEW v AS (SELECT * FROM t)");
                try (QwpQueryClient client = connect("")) {
                    // each query with the position of x in its projection
                    final Object[][] queries = {
                            {"SELECT x FROM t ORDER BY ts DESC", 0},
                            {"SELECT ts, x, x, vc, tl FROM t ORDER BY ts DESC", 1},
                            {"SELECT tl, sy, x FROM t ORDER BY ts DESC", 2},
                            {"t ORDER BY ts DESC", 0},
                            {"SELECT x, vc FROM (SELECT * FROM t ORDER BY ts DESC)", 0},
                            {"WITH c AS (SELECT * FROM t ORDER BY ts DESC) SELECT d, sy, x FROM c", 2},
                            {"SELECT i, tl, x FROM v ORDER BY ts DESC", 2},
                            {"SELECT x FROM t WHERE ts IN '1970-01-02' ORDER BY ts DESC", 0},
                            {"SELECT x, l FROM t ORDER BY ts DESC LIMIT 7", 0},
                            {"SELECT x, l FROM t ORDER BY ts DESC LIMIT -7", 0},
                            {"SELECT x, l FROM t WHERE x % 3 = 0 ORDER BY ts DESC", 0},
                            {"SELECT x, l FROM t ORDER BY x DESC", 0}
                    };
                    for (Object[] query : queries) {
                        final String sql = (String) query[0];
                        Assert.assertEquals(sql, inProcessRows(serverMain, sql, "x"), column(qwpRows(client, sql), (Integer) query[1]));
                    }
                    // bind variables in the interval: one compiled factory, three bind sets
                    final String bindSql = "SELECT x, vc FROM t WHERE ts >= $1 ORDER BY ts DESC";
                    final long tsStep = 86_400_000_000L / 500;
                    for (long lo : new long[]{0, 100_000_000_000L, 200_000_000_000L}) {
                        final QwpBindSetter binds = b -> b.setTimestampMicros(0, lo);
                        // row x has ts (x - 1) * tsStep, see createAllTypesTable
                        final long firstX = (lo + tsStep - 1) / tsStep + 1;
                        final List<String> rows = column(qwpRows(client, bindSql, binds), 0);
                        Assert.assertEquals(bindSql + " lo=" + lo, descendingLongs(1_500, firstX), rows);
                        Assert.assertEquals(
                                bindSql + " lo=" + lo,
                                inProcessRows(serverMain, "SELECT x, vc FROM t WHERE ts >= " + lo + "::TIMESTAMP ORDER BY ts DESC", "x"),
                                rows
                        );
                    }
                }
            }
        });
    }

    @Test
    public void testSymbolDictionarySplitsBackwardBatches() throws Exception {
        // Thousands of distinct long symbols overflow the per-batch dictionary budget,
        // which cuts batches early in the middle of a frame.
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = startServer(PropertyKey.HTTP_SEND_BUFFER_SIZE.getEnvVarName(), "256K")) {
                serverMain.execute("CREATE TABLE t (x LONG, s SYMBOL, s2 SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("""
                        INSERT INTO t SELECT
                            x,
                            'symbol_value_with_a_long_prefix_' || x,
                            CASE WHEN x % 4 = 0 THEN NULL ELSE 'k' || (x % 11) END,
                            (x * 10_000_000L)::TIMESTAMP
                        FROM long_sequence(20_000)""");
                serverMain.awaitTable("t");
                try (QwpQueryClient client = connect("")) {
                    assertBackwardScan(serverMain, client, "t", "");
                }
            }
        });
    }

    @Test
    public void testWideRandomPartitionsAgainstEngine() throws Exception {
        // Random partition sizes, from empty days to multi-frame ones, checked row for
        // row against the in-process result.
        TestUtils.assertMemoryLeak(() -> {
            final Rnd rnd = TestUtils.generateRandom(LOG);
            try (TestServerMain serverMain = startServer(SMALL_PAGE_FRAMES)) {
                serverMain.execute("CREATE TABLE t (x LONG, v VARCHAR, s SYMBOL, d DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                long x = 0;
                for (int day = 0; day < 12; day++) {
                    final int rows = rnd.nextInt(4) == 0 ? 0 : 1 + rnd.nextInt(400);
                    if (rows > 0) {
                        serverMain.execute("INSERT INTO t SELECT x + " + x + ", "
                                + "CASE WHEN x % 9 = 0 THEN NULL ELSE 'v' || (x + " + x + ") END, "
                                + "CASE WHEN x % 5 = 0 THEN NULL ELSE 's' || (x % 13) END, "
                                + "CASE WHEN x % 7 = 0 THEN NULL ELSE x * 0.25 END, "
                                + "(" + day + " * 86_400_000_000L + x * 60_000_000L)::TIMESTAMP "
                                + "FROM long_sequence(" + rows + ")");
                        x += rows;
                    }
                }
                serverMain.awaitTable("t");
                try (QwpQueryClient client = connect("")) {
                    final String sql = "SELECT x, v, s, d FROM t ORDER BY ts DESC";
                    Assert.assertEquals(inProcessRows(serverMain, sql), qwpRowsAsText(client, sql));
                    assertBackwardScan(serverMain, client, "t", "");
                }
            }
        });
    }

    /**
     * Rows {@code firstX .. firstX + count - 1}; row {@code firstX + k} gets timestamp
     * {@code firstTs + k * tsStep}.
     */
    static String allTypesSelect(long firstX, long count, long firstTs, long tsStep) {
        final String ts = "(" + firstTs + " + (x - 1) * " + tsStep + ")";
        return "SELECT "
                + "x + " + (firstX - 1) + ", "
                + "rnd_boolean(), "
                + "rnd_byte(), "
                + "rnd_short(), "
                + "rnd_char(), "
                + "CASE WHEN x % 7 = 0 THEN NULL WHEN x % 7 = 1 THEN -2_147_483_647 WHEN x % 7 = 2 THEN 2_147_483_647 ELSE rnd_int() END, "
                + "CASE WHEN x % 5 = 0 THEN NULL WHEN x % 5 = 1 THEN -9_223_372_036_854_775_807L WHEN x % 5 = 2 THEN 9_223_372_036_854_775_807L ELSE rnd_long() END, "
                + "CASE WHEN x % 6 = 0 THEN NULL WHEN x % 6 = 1 THEN -0.0::FLOAT ELSE rnd_float() END, "
                + "CASE WHEN x % 6 = 0 THEN NULL WHEN x % 6 = 1 THEN -0.0 ELSE rnd_double() END, "
                + "rnd_date(to_date('1960-01-01', 'yyyy-MM-dd'), to_date('2100-01-01', 'yyyy-MM-dd'), 4), "
                + "CASE WHEN x % 8 = 0 THEN NULL ELSE (" + ts + " * 1_000L + x % 1_000)::TIMESTAMP_NS END, "
                + "CASE WHEN x % 9 = 0 THEN NULL ELSE rnd_ipv4() END, "
                + "rnd_symbol(12, 1, 6, 4), "
                + "CASE WHEN x % 13 = 0 THEN '' ELSE rnd_varchar(1, 24, 4) END, "
                + "CASE WHEN x % 13 = 1 THEN '' ELSE rnd_str(1, 24, 4) END, "
                + "rnd_bin(1, 24, 4), "
                + "CASE WHEN x % 10 = 0 THEN NULL ELSE rnd_uuid4() END, "
                + "CASE WHEN x % 10 = 1 THEN NULL ELSE rnd_long256() END, "
                + "CASE WHEN x % 11 = 0 THEN NULL ELSE rnd_geohash(3) END, "
                + "CASE WHEN x % 11 = 1 THEN NULL ELSE rnd_geohash(10) END, "
                + "CASE WHEN x % 11 = 2 THEN NULL ELSE rnd_geohash(25) END, "
                + "CASE WHEN x % 11 = 3 THEN NULL ELSE rnd_geohash(50) END, "
                + "rnd_decimal(2, 1, 4), "
                + "rnd_decimal(4, 2, 4), "
                + "rnd_decimal(9, 3, 4), "
                + "rnd_decimal(18, 4, 4), "
                + "rnd_decimal(38, 6, 4), "
                + "rnd_decimal(76, 10, 4), "
                + "CASE WHEN x % 12 = 0 THEN NULL ELSE rnd_double_array(1, 5, 4) END, "
                + ts + "::TIMESTAMP "
                + "FROM long_sequence(" + count + ")";
    }

    static String columnTopSelect(long count) {
        return ", "
                + "CASE WHEN x % 5 = 0 THEN NULL ELSE x * 31 END, "
                + "CASE WHEN x % 6 = 0 THEN NULL ELSE x * 1.25 END, "
                + "CASE WHEN x % 7 = 0 THEN NULL ELSE x::INT END, "
                + "CASE WHEN x % 8 = 0 THEN NULL ELSE rnd_ipv4() END, "
                + "CASE WHEN x % 9 = 0 THEN NULL ELSE (x * 0.5)::FLOAT END, "
                + "(x % 30_000)::SHORT, "
                + "rnd_char(), "
                + "(x % 100)::BYTE, "
                + "x % 2 = 0, "
                + "CASE WHEN x % 4 = 0 THEN NULL ELSE 'ts' || (x % 9) END, "
                + "CASE WHEN x % 3 = 0 THEN NULL ELSE 'top_' || x END, "
                + "CASE WHEN x % 10 = 0 THEN NULL ELSE (x * 1_000_000L)::TIMESTAMP END, "
                + "rnd_decimal(18, 2, 4) "
                + "FROM long_sequence(" + count + ")";
    }

    private static List<String> column(List<String> rows, int index) {
        // qwpRows renders a row as "cell|cell|..."; no rendered cell contains '|' in the
        // columns the callers pick (LONG)
        final List<String> values = new ArrayList<>(rows.size());
        for (String row : rows) {
            values.add(row.split("\\|", -1)[index]);
        }
        return values;
    }

    private static List<String> descendingLongs(long from, long to) {
        final List<String> values = new ArrayList<>();
        for (long v = from; v >= to; v--) {
            values.add(Long.toString(v));
        }
        return values;
    }

    private static List<String> inProcessRows(TestServerMain serverMain, String sql, String column) throws Exception {
        // Project the one column over the query, which keeps its row order, so that cells
        // the printer spreads over several lines (BINARY) stay out of the text.
        final String text = TestUtils.printSqlToString(
                serverMain.getEngine(),
                serverMain.getSqlExecutionContext(),
                "SELECT " + column + " FROM (" + sql + ")",
                new StringSink()
        );
        final String[] lines = text.split("\n");
        Assert.assertEquals(sql, column, lines[0]);
        return new ArrayList<>(Arrays.asList(lines).subList(1, lines.length));
    }

    private static String inProcessRows(TestServerMain serverMain, String sql) throws Exception {
        return TestUtils.printSqlToString(serverMain.getEngine(), serverMain.getSqlExecutionContext(), sql, new StringSink());
    }

    private static List<String> qwpRows(QwpQueryClient client, String sql) {
        return qwpRows(client, sql, null);
    }

    private static List<String> qwpRows(QwpQueryClient client, String sql, QwpBindSetter binds) {
        final List<String> rows = new ArrayList<>();
        final long[] totalRows = {-1};
        final StringBuilder row = new StringBuilder();
        final QwpColumnBatchHandler handler = new QwpColumnBatchHandler() {
            @Override
            public void onBatch(QwpColumnBatch batch) {
                for (int r = 0; r < batch.getRowCount(); r++) {
                    row.setLength(0);
                    for (int c = 0; c < batch.getColumnCount(); c++) {
                        if (c > 0) {
                            row.append('|');
                        }
                        renderCell(row, batch, c, r);
                    }
                    rows.add(row.toString());
                }
            }

            @Override
            public void onEnd(long total) {
                totalRows[0] = total;
            }

            @Override
            public void onError(byte status, String message) {
                Assert.fail("[" + sql + "] " + message);
            }
        };
        if (binds == null) {
            client.execute(sql, handler);
        } else {
            client.execute(sql, binds, handler);
        }
        Assert.assertEquals(sql, rows.size(), totalRows[0]);
        return rows;
    }

    /**
     * Renders the stream in the in-process printer's text shape for the few types the
     * callers project (LONG, DOUBLE, VARCHAR, SYMBOL, TIMESTAMP as micros), so the two can be
     * compared as text.
     */
    private static String qwpRowsAsText(QwpQueryClient client, String sql) {
        final StringBuilder text = new StringBuilder();
        final boolean[] isHeaderDone = {false};
        final long[] totalRows = {-1};
        final long[] rows = {0};
        client.execute(sql, new QwpColumnBatchHandler() {
            @Override
            public void onBatch(QwpColumnBatch batch) {
                if (!isHeaderDone[0]) {
                    for (int c = 0; c < batch.getColumnCount(); c++) {
                        text.append(c > 0 ? "\t" : "").append(batch.getColumnName(c));
                    }
                    text.append('\n');
                    isHeaderDone[0] = true;
                }
                for (int r = 0; r < batch.getRowCount(); r++, rows[0]++) {
                    for (int c = 0; c < batch.getColumnCount(); c++) {
                        if (c > 0) {
                            text.append('\t');
                        }
                        if (batch.isNull(c, r)) {
                            text.append(batch.getColumnWireType(c) == QwpConstants.TYPE_VARCHAR
                                    || batch.getColumnWireType(c) == QwpConstants.TYPE_SYMBOL ? "" : "null");
                            continue;
                        }
                        switch (batch.getColumnWireType(c)) {
                            case QwpConstants.TYPE_LONG -> text.append(batch.getLongValue(c, r));
                            case QwpConstants.TYPE_DOUBLE -> text.append(batch.getDoubleValue(c, r));
                            case QwpConstants.TYPE_VARCHAR, QwpConstants.TYPE_SYMBOL ->
                                    text.append(batch.getString(c, r));
                            case QwpConstants.TYPE_TIMESTAMP -> {
                                final StringSink sink = new StringSink();
                                ColumnType.getTimestampDriver(ColumnType.TIMESTAMP).append(sink, batch.getLongValue(c, r));
                                text.append(sink);
                            }
                            default -> throw new AssertionError("unexpected wire type " + batch.getColumnWireType(c));
                        }
                    }
                    text.append('\n');
                }
            }

            @Override
            public void onEnd(long total) {
                totalRows[0] = total;
            }

            @Override
            public void onError(byte status, String message) {
                Assert.fail("[" + sql + "] " + message);
            }
        });
        Assert.assertEquals(sql, rows[0], totalRows[0]);
        return text.toString();
    }

    /**
     * Renders one cell exactly: floating point as raw bits, so -0.0 and NaN payloads
     * would show, and every other type through its widest accessor.
     */
    private static void renderCell(StringBuilder sink, QwpColumnBatch batch, int col, int row) {
        if (batch.isNull(col, row)) {
            sink.append("null");
            return;
        }
        final byte wireType = batch.getColumnWireType(col);
        switch (wireType) {
            case QwpConstants.TYPE_BOOLEAN -> sink.append(batch.getBoolValue(col, row));
            case QwpConstants.TYPE_BYTE -> sink.append(batch.getByteValue(col, row));
            case QwpConstants.TYPE_SHORT -> sink.append(batch.getShortValue(col, row));
            case QwpConstants.TYPE_CHAR -> sink.append("c").append((int) batch.getCharValue(col, row));
            case QwpConstants.TYPE_INT, QwpConstants.TYPE_IPv4 -> sink.append(batch.getIntValue(col, row));
            case QwpConstants.TYPE_LONG, QwpConstants.TYPE_DATE, QwpConstants.TYPE_TIMESTAMP,
                 QwpConstants.TYPE_TIMESTAMP_NANOS -> sink.append(batch.getLongValue(col, row));
            case QwpConstants.TYPE_DECIMAL64 ->
                    sink.append(batch.getLongValue(col, row)).append('e').append(batch.getDecimalScale(col));
            case QwpConstants.TYPE_FLOAT ->
                    sink.append("f").append(Float.floatToRawIntBits(batch.getFloatValue(col, row)));
            case QwpConstants.TYPE_DOUBLE ->
                    sink.append("d").append(Double.doubleToRawLongBits(batch.getDoubleValue(col, row)));
            case QwpConstants.TYPE_VARCHAR, QwpConstants.TYPE_SYMBOL ->
                    sink.append('\'').append(batch.getString(col, row)).append('\'');
            case QwpConstants.TYPE_BINARY -> sink.append(Arrays.toString(batch.getBinary(col, row)));
            case QwpConstants.TYPE_UUID ->
                    sink.append(batch.getUuidHi(col, row)).append('/').append(batch.getUuidLo(col, row));
            case QwpConstants.TYPE_LONG256, QwpConstants.TYPE_DECIMAL256 -> {
                for (int w = 3; w >= 0; w--) {
                    sink.append(batch.getLong256Word(col, row, w)).append(w > 0 ? "/" : "");
                }
                if (wireType == QwpConstants.TYPE_DECIMAL256) {
                    sink.append('e').append(batch.getDecimalScale(col));
                }
            }
            case QwpConstants.TYPE_DECIMAL128 -> sink.append(batch.getDecimal128High(col, row)).append('/')
                    .append(batch.getDecimal128Low(col, row)).append('e').append(batch.getDecimalScale(col));
            case QwpConstants.TYPE_GEOHASH ->
                    sink.append('#').append(batch.getGeohashValue(col, row)).append('/').append(batch.getGeohashPrecisionBits(col));
            case QwpConstants.TYPE_DOUBLE_ARRAY -> {
                sink.append(batch.getArrayNDims(col, row)).append(':');
                for (double v : batch.getDoubleArrayElements(col, row)) {
                    sink.append(Double.doubleToRawLongBits(v)).append(',');
                }
            }
            default -> throw new AssertionError("unexpected wire type " + wireType);
        }
    }

    private static List<String> reversed(List<String> rows) {
        final List<String> copy = new ArrayList<>(rows);
        Collections.reverse(copy);
        return copy;
    }

    /**
     * Boots a server with this class's fragmentation chunks plus {@code extraEnv}, retrying
     * while another server still holds the fixed test ports. The send chunk is floored at 64
     * bytes: HttpResponseSink parks after every chunk, and these tests stream megabytes.
     */
    private static TestServerMain startServer(String... extraEnv) {
        return startServer(new ReadAheadFilesFacade(isReadAheadWillNeed), extraEnv);
    }

    // as startServer, on a files facade that picks how backward streams read their frames ahead
    private static TestServerMain startServer(ReadAheadFilesFacade ff, String... extraEnv) {
        final String[] env = new String[extraEnv.length + 4];
        env[0] = PropertyKey.DEBUG_HTTP_FORCE_RECV_FRAGMENTATION_CHUNK_SIZE.getEnvVarName();
        env[1] = Integer.toString(recvChunk);
        env[2] = PropertyKey.DEBUG_HTTP_FORCE_SEND_FRAGMENTATION_CHUNK_SIZE.getEnvVarName();
        env[3] = Integer.toString(Math.max(64, sendChunk));
        System.arraycopy(extraEnv, 0, env, 4, extraEnv.length);
        return startServerWithRetry(() -> startWithEnvVariables(ff, env));
    }

    /**
     * Boots a server with the read-ahead frame: table t holds one 200,000-row partition of
     * LONG x, DOUBLE d and the timestamp, in one page frame.
     */
    private static TestServerMain startReadAheadServer(ReadAheadFilesFacade ff) {
        final TestServerMain serverMain = startServer(
                ff,
                PropertyKey.METRICS_ENABLED.getEnvVarName(), "true",
                PropertyKey.CAIRO_SQL_PAGE_FRAME_MIN_ROWS.getEnvVarName(), "1000000",
                PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS.getEnvVarName(), "1000000"
        );
        try {
            // a test that boots a server per read-ahead way finds the table of the previous one
            serverMain.execute("DROP TABLE IF EXISTS t");
            serverMain.execute("CREATE TABLE t (x LONG, d DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            // 0.4 s apart: 80,000 s, one day
            serverMain.execute("INSERT INTO t SELECT x, x * 0.5, ((x - 1) * 400_000L)::TIMESTAMP FROM long_sequence(" + READ_AHEAD_FRAME_ROWS + ")");
            serverMain.awaitTable("t");
            return serverMain;
        } catch (Throwable th) {
            Misc.free(serverMain, th);
            throw th;
        }
    }

    // runs a query and checks how many bytes it read ahead, and that only MADV_WILLNEED
    // windows ask the OS for them
    private static void assertReadAhead(ReadAheadFilesFacade ff, QwpEgressMetrics metrics, boolean isWillNeed, long expectedBytes, ThrowingRunnable query) throws Exception {
        final long before = metrics.readAheadBytesCount();
        final long advisedBefore = ff.advisedBytes();
        query.run();
        Assert.assertEquals(ff.toString(), expectedBytes, metrics.readAheadBytesCount() - before);
        Assert.assertEquals(ff.toString(), isWillNeed ? expectedBytes : 0, ff.advisedBytes() - advisedBefore);
    }

    private void assertBackwardScan(TestServerMain serverMain, QwpQueryClient client, String table, String where) throws Exception {
        final String desc = "SELECT * FROM " + table + " " + where + " ORDER BY ts DESC";
        final String asc = "SELECT * FROM " + table + " " + where + " ORDER BY ts";
        assertQuery(serverMain, desc).assertsPlanContaining("Row backward scan");
        assertQuery(serverMain, asc).assertsPlanContaining("Row forward scan");
        final List<String> descRows = qwpRows(client, desc);
        // oracle 1: the forward stream, reversed
        Assert.assertEquals(desc, reversed(qwpRows(client, asc)), descRows);
        // oracle 2: the engine's own cursor; x is the first column of every table here
        Assert.assertEquals(desc, inProcessRows(serverMain, desc, "x"), column(descRows, 0));
    }

    private QwpQueryClient connect(String options) {
        final QwpQueryClient client = QwpQueryClient.fromConfig("ws::addr=127.0.0.1:" + HTTP_PORT + ";" + options);
        client.connect();
        return client;
    }

    /**
     * Creates {@code table} with {@code partitions} DAY partitions of
     * {@code rowsPerPartition} rows each, with random values seeded from the test's Rnd.
     * With {@code withColumnTops}, the second half of the rows also fills columns added
     * halfway through, so the first half reads them through a column top - in the middle
     * of a partition when the partition count is odd.
     */
    private void createAllTypesTable(TestServerMain serverMain, String table, int rowsPerPartition, int partitions, boolean withColumnTops) {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        serverMain.getSqlExecutionContext().setRandom(rnd);
        serverMain.execute("CREATE TABLE " + table + " (" + ALL_TYPES_COLUMNS + ") TIMESTAMP(ts) PARTITION BY DAY WAL");
        final long totalRows = (long) rowsPerPartition * partitions;
        final long tsStep = 86_400_000_000L / rowsPerPartition;
        if (!withColumnTops) {
            serverMain.execute("INSERT INTO " + table + " " + allTypesSelect(1, totalRows, 0, tsStep));
            serverMain.awaitTable(table);
            return;
        }
        final long firstHalf = totalRows / 2;
        serverMain.execute("INSERT INTO " + table + " " + allTypesSelect(1, firstHalf, 0, tsStep));
        serverMain.awaitTable(table);
        // WAL tables take one added column per statement; the list has one column per line
        for (String column : COLUMN_TOP_COLUMNS.split(",\n")) {
            serverMain.execute("ALTER TABLE " + table + " ADD COLUMN " + column.trim());
        }
        serverMain.awaitTable(table);
        final String select = allTypesSelect(firstHalf + 1, totalRows - firstHalf, firstHalf * tsStep, tsStep);
        // allTypesSelect ends with "... ts FROM long_sequence(n)"; splice the added
        // columns in after ts
        final int from = select.lastIndexOf(" FROM long_sequence(");
        serverMain.execute("INSERT INTO " + table + " " + select.substring(0, from) + columnTopSelect(totalRows - firstHalf));
        serverMain.awaitTable(table);
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }
}
