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

package io.questdb.test.cairo.mv;

import io.questdb.Bootstrap;
import io.questdb.PropBootstrapConfiguration;
import io.questdb.PropertyKey;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.NanosTimestampDriver;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.mv.MatViewDefinition;
import io.questdb.cairo.mv.MatViewRefreshJob;
import io.questdb.cairo.mv.MatViewState;
import io.questdb.cairo.mv.MatViewTimerJob;
import io.questdb.cairo.wal.WalPurgeJob;
import io.questdb.client.Sender;
import io.questdb.cutlass.line.LineSenderException;
import io.questdb.cutlass.line.LineUdpSender;
import io.questdb.mp.WorkerPool;
import io.questdb.network.Net;
import io.questdb.network.NetworkFacadeImpl;
import io.questdb.std.LongList;
import io.questdb.std.datetime.MicrosecondClock;
import io.questdb.std.datetime.NanosecondClock;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.TestTimestampType;
import io.questdb.test.cutlass.http.SendAndReceiveRequestBuilder;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static io.questdb.cairo.wal.WalUtils.WAL_NAME_BASE;
import static io.questdb.test.cairo.mv.MatViewTest.parseFloorPartialTimestamp;
import static io.questdb.test.tools.TestUtils.assertContains;

@RunWith(Parameterized.class)
public class MatViewReloadOnRestartTest extends AbstractBootstrapTest {
    private final TestTimestampType timestampType;

    public MatViewReloadOnRestartTest(TestTimestampType timestampType) {
        this.timestampType = timestampType;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> testParams() {
        return Arrays.asList(new Object[][]{
                {TestTimestampType.MICRO}, {TestTimestampType.NANO}
        });
    }

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
    }

    @Test
    public void testMatViewsCheckUpdates() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain main1 = startWithEnvVariables0(
                    PropertyKey.DEV_MODE_ENABLED.getEnvVarName(), "true",
                    PropertyKey.HTTP_MIN_ENABLED.getEnvVarName(), "false",
                    PropertyKey.PG_ENABLED.getEnvVarName(), "false"
            )) {
                executeWithRewriteTimestamp(
                        main1,
                        "create table base_price (" +
                                "sym varchar, price double, ts #TIMESTAMP" +
                                ") timestamp(ts) partition by DAY WAL"
                );

                createMatView(main1);

                execute(
                        main1,
                        "insert into base_price values('gbpusd', 1.320, '2024-09-10T12:01')" +
                                ",('gbpusd', 1.323, '2024-09-10T12:02')" +
                                ",('jpyusd', 103.21, '2024-09-10T12:02')" +
                                ",('gbpusd', 1.321, '2024-09-10T13:02')"
                );
                drainWalQueue(main1.getEngine());

                try (MatViewRefreshJob refreshJob = createMatViewRefreshJob(main1.getEngine())) {
                    drainWalAndMatViewQueues(refreshJob, main1.getEngine());

                    assertSql(
                            main1,
                            replaceExpectedTimestamp(
                                    "sym\tprice\tts\n" +
                                            "gbpusd\t1.323\t2024-09-10T12:00:00.000000Z\n" +
                                            "jpyusd\t103.21\t2024-09-10T12:00:00.000000Z\n" +
                                            "gbpusd\t1.321\t2024-09-10T13:00:00.000000Z\n"),
                            "price_1h order by ts, sym"
                    );

                    execute(
                            main1, "insert into base_price values('gbpusd', 1.319, '2024-09-10T12:05')" +
                                    ",('gbpusd', 1.325, '2024-09-10T13:03')"
                    );

                    drainWalAndMatViewQueues(refreshJob, main1.getEngine());

                    String expected = "sym\tprice\tts\n" +
                            "gbpusd\t1.319\t2024-09-10T12:00:00.000000Z\n" +
                            "jpyusd\t103.21\t2024-09-10T12:00:00.000000Z\n" +
                            "gbpusd\t1.325\t2024-09-10T13:00:00.000000Z\n";

                    assertSql(main1, replaceExpectedTimestamp(expected), "select sym, last(price) as price, ts from base_price sample by 1h order by ts, sym");
                    assertSql(main1, replaceExpectedTimestamp(expected), "price_1h order by ts, sym");

                    assertLineError(Transport.HTTP, main1, refreshJob, replaceExpectedTimestamp(expected));
                    assertLineError(Transport.UDP, main1, refreshJob, replaceExpectedTimestamp(expected));
                    assertLineError(Transport.TCP, main1, refreshJob, replaceExpectedTimestamp(expected));
                }

                new SendAndReceiveRequestBuilder().withPort(HTTP_PORT).execute(
                        "POST /imp?name=price_1h HTTP/1.1\r\n" +
                                "Host: localhost:9010\r\n" +
                                "User-Agent: curl/7.71.1\r\n" +
                                "Accept: */*\r\n" +
                                "Content-Length: 243\r\n" +
                                "Content-Type: multipart/form-data; boundary=----WebKitFormBoundaryOsOAD9cPKyHuxyBV\r\n" +
                                "\r\n" +
                                "------WebKitFormBoundaryOsOAD9cPKyHuxyBV\r\n" +
                                "Content-Disposition: form-data; name=\"data\"\r\n" +
                                "\r\n" +
                                "col_a,ts\r\n" +
                                "1000,1000\r\n" +
                                "2000,2000\r\n" +
                                "3000,3000\r\n" +
                                "\r\n" +
                                "------WebKitFormBoundaryOsOAD9cPKyHuxyBV--",
                        "HTTP/1.1 200 OK\r\n" +
                                "Server: questDB/1.0\r\n" +
                                "Date: Thu, 1 Jan 1970 00:00:00 GMT\r\n" +
                                "Transfer-Encoding: chunked\r\n" +
                                "Content-Type: text/plain; charset=utf-8\r\n" +
                                "\r\n" +
                                "2f\r\n" +
                                "cannot modify materialized view [view=price_1h]\r\n" +
                                "00\r\n"
                );
            }
        });
    }

    @Test
    public void testMatViewsRefreshIntervalsReloadOnServerStart() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final long now = parseFloorPartialTimestamp("2024-01-01T01:01:01.000001Z");
            final TestMicroClock testClock = new TestMicroClock(now);

            final LongList expectedIntervals = new LongList();
            final TimestampDriver timestampDriver = timestampType.getDriver();
            expectedIntervals.add(timestampDriver.parseFloorLiteral("2024-09-11T12:01"), timestampDriver.parseFloorLiteral("2024-09-11T12:02"));
            expectedIntervals.add(timestampDriver.parseFloorLiteral("2024-09-12T03:01"), timestampDriver.parseFloorLiteral("2024-09-12T23:02"));

            try (final TestServerMain main1 = startMainPortsDisabled(
                    testClock,
                    PropertyKey.CAIRO_MAT_VIEW_REFRESH_INTERVALS_UPDATE_PERIOD.getEnvVarName(), "10s"
            )) {
                executeWithRewriteTimestamp(
                        main1,
                        "create table base_price (" +
                                "sym varchar, price double, ts #TIMESTAMP" +
                                ") timestamp(ts) partition by DAY WAL"
                );

                execute(
                        main1,
                        "create materialized view price_1h refresh manual as " +
                                "select sym, last(price) as price, ts from base_price sample by 1h;"
                );

                execute(
                        main1,
                        "insert into base_price(sym, price, ts) values('gbpusd', 1.320, '2024-09-10T12:01')" +
                                ",('gbpusd', 1.323, '2024-09-10T12:02')" +
                                ",('jpyusd', 103.21, '2024-09-10T12:02')" +
                                ",('gbpusd', 1.321, '2024-09-10T13:02')"
                );

                final MatViewTimerJob timerJob = new MatViewTimerJob(main1.getEngine());
                drainMatViewTimerQueue(timerJob);

                final TableToken viewToken = main1.getEngine().getTableTokenIfExists("price_1h");
                Assert.assertNotNull(viewToken);
                final MatViewState viewState = main1.getEngine().getMatViewStateStore().getViewState(viewToken);
                Assert.assertNotNull(viewState);

                try (var refreshJob = createMatViewRefreshJob(main1.getEngine())) {
                    drainWalAndMatViewQueues(refreshJob, main1.getEngine());

                    // nothing is cached after a successful refresh
                    Assert.assertEquals(-1, viewState.getRefreshIntervalsBaseTxn());
                    Assert.assertEquals(0, viewState.getRefreshIntervals().size());

                    assertSql(
                            main1,
                            replaceExpectedTimestamp("sym\tprice\tts\n" +
                                    "gbpusd\t1.323\t2024-09-10T12:00:00.000000Z\n" +
                                    "jpyusd\t103.21\t2024-09-10T12:00:00.000000Z\n" +
                                    "gbpusd\t1.321\t2024-09-10T13:00:00.000000Z\n"),
                            "price_1h order by ts, sym"
                    );

                    execute(
                            main1,
                            "insert into base_price(sym, price, ts) values('gbpusd', 1.320, '2024-09-11T12:01')" +
                                    ",('jpyusd', 103.21, '2024-09-11T12:02')"
                    );
                    execute(
                            main1,
                            "insert into base_price(sym, price, ts) values('gbpusd', 1.320, '2024-09-12T13:01')" +
                                    ",('jpyusd', 103.21, '2024-09-12T13:02')"
                    );
                    execute(
                            main1,
                            "insert into base_price(sym, price, ts) values('gbpusd', 1.320, '2024-09-12T03:01')" +
                                    ",('jpyusd', 103.21, '2024-09-12T23:02')"
                    );
                    // tick timer job to enqueue caching task
                    testClock.micros.addAndGet(Micros.MINUTE_MICROS);
                    drainMatViewTimerQueue(timerJob);
                    drainWalAndMatViewQueues(refreshJob, main1.getEngine());

                    // the newly inserted intervals should be in the cache
                    Assert.assertEquals(4, viewState.getRefreshIntervalsBaseTxn());
                    TestUtils.assertEquals(expectedIntervals, viewState.getRefreshIntervals());

                    // range refresh shouldn't mess up txn intervals
                    execute(main1, "refresh materialized view price_1h range from '2024-09-11' to '2024-09-12';");
                    drainWalAndMatViewQueues(refreshJob, main1.getEngine());
                }
            }

            try (final TestServerMain main2 = startMainPortsDisabled(testClock)) {
                final TableToken viewToken = main2.getEngine().getTableTokenIfExists("price_1h");
                Assert.assertNotNull(viewToken);
                final MatViewState viewState = main2.getEngine().getMatViewStateStore().getViewState(viewToken);
                Assert.assertNotNull(viewState);

                // the txn intervals should stay as they are
                Assert.assertEquals(4, viewState.getRefreshIntervalsBaseTxn());
                TestUtils.assertEquals(expectedIntervals, viewState.getRefreshIntervals());

                execute(main2, "refresh materialized view price_1h incremental;");

                drainWalAndMatViewQueues(main2.getEngine());

                final String expected = "sym\tprice\tts\n" +
                        "gbpusd\t1.323\t2024-09-10T12:00:00.000000Z\n" +
                        "jpyusd\t103.21\t2024-09-10T12:00:00.000000Z\n" +
                        "gbpusd\t1.321\t2024-09-10T13:00:00.000000Z\n" +
                        "gbpusd\t1.32\t2024-09-11T12:00:00.000000Z\n" +
                        "jpyusd\t103.21\t2024-09-11T12:00:00.000000Z\n" +
                        "gbpusd\t1.32\t2024-09-12T03:00:00.000000Z\n" +
                        "gbpusd\t1.32\t2024-09-12T13:00:00.000000Z\n" +
                        "jpyusd\t103.21\t2024-09-12T13:00:00.000000Z\n" +
                        "jpyusd\t103.21\t2024-09-12T23:00:00.000000Z\n";

                assertSql(main2, replaceExpectedTimestamp(expected), "select sym, last(price) as price, ts from base_price sample by 1h order by ts, sym");
                assertSql(main2, replaceExpectedTimestamp(expected), "price_1h order by ts, sym");
            }
        });
    }

    @Test
    public void testMatViewsReloadOnServerStart() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain main1 = startMainPortsDisabled()) {
                executeWithRewriteTimestamp(
                        main1,
                        "create table base_price (" +
                                "sym varchar, price double, ts #TIMESTAMP" +
                                ") timestamp(ts) partition by DAY WAL"
                );

                createMatView(main1);

                execute(
                        main1,
                        "insert into base_price values('gbpusd', 1.320, '2024-09-10T12:01')" +
                                ",('gbpusd', 1.323, '2024-09-10T12:02')" +
                                ",('jpyusd', 103.21, '2024-09-10T12:02')" +
                                ",('gbpusd', 1.321, '2024-09-10T13:02')"
                );
                try (var refreshJob = createMatViewRefreshJob(main1.getEngine())) {
                    drainWalAndMatViewQueues(refreshJob, main1.getEngine());

                    assertSql(
                            main1,
                            replaceExpectedTimestamp("sym\tprice\tts\n" +
                                    "gbpusd\t1.323\t2024-09-10T12:00:00.000000Z\n" +
                                    "jpyusd\t103.21\t2024-09-10T12:00:00.000000Z\n" +
                                    "gbpusd\t1.321\t2024-09-10T13:00:00.000000Z\n"),
                            "price_1h order by ts, sym"
                    );

                    execute(
                            main1, "insert into base_price values('gbpusd', 1.319, '2024-09-10T12:05')" +
                                    ",('gbpusd', 1.325, '2024-09-10T13:03')"
                    );
                    drainWalAndMatViewQueues(refreshJob, main1.getEngine());
                }

                String expected = "sym\tprice\tts\n" +
                        "gbpusd\t1.319\t2024-09-10T12:00:00.000000Z\n" +
                        "jpyusd\t103.21\t2024-09-10T12:00:00.000000Z\n" +
                        "gbpusd\t1.325\t2024-09-10T13:00:00.000000Z\n";

                assertSql(main1, replaceExpectedTimestamp(expected), "select sym, last(price) as price, ts from base_price sample by 1h order by ts, sym");
                assertSql(main1, replaceExpectedTimestamp(expected), "price_1h order by ts, sym");
            }

            try (final TestServerMain main2 = startMainPortsDisabled()) {
                String expected = "sym\tprice\tts\n" +
                        "gbpusd\t1.319\t2024-09-10T12:00:00.000000Z\n" +
                        "jpyusd\t103.21\t2024-09-10T12:00:00.000000Z\n" +
                        "gbpusd\t1.325\t2024-09-10T13:00:00.000000Z\n";

                assertSql(main2, replaceExpectedTimestamp(expected), "select sym, last(price) as price, ts from base_price sample by 1h order by ts, sym");
                assertSql(main2, replaceExpectedTimestamp(expected), "price_1h order by ts, sym");

                execute(
                        main2,
                        "insert into base_price values('gbpusd', 1.320, '2024-09-10T12:10')" +
                                ",('gbpusd', 1.327, '2024-09-10T13:03')"
                );

                drainWalAndMatViewQueues(main2.getEngine());

                expected = "sym\tprice\tts\n" +
                        "gbpusd\t1.32\t2024-09-10T12:00:00.000000Z\n" +
                        "jpyusd\t103.21\t2024-09-10T12:00:00.000000Z\n" +
                        "gbpusd\t1.327\t2024-09-10T13:00:00.000000Z\n";

                assertSql(main2, replaceExpectedTimestamp(expected), "select sym, last(price) as price, ts from base_price sample by 1h order by ts, sym");
                assertSql(main2, replaceExpectedTimestamp(expected), "price_1h order by ts, sym");
            }
        });
    }

    @Test
    public void testMatViewsReloadOnServerStartAppliedAllWal() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain main1 = startMainPortsDisabled()) {
                executeWithRewriteTimestamp(
                        main1,
                        "create table base_price (" +
                                "sym varchar, price double, ts #TIMESTAMP" +
                                ") timestamp(ts) partition by DAY WAL"
                );

                createMatView(main1);

                execute(
                        main1,
                        "insert into base_price values('gbpusd', 1.320, '2024-09-10T12:01')" +
                                ",('gbpusd', 1.323, '2024-09-10T12:02')" +
                                ",('jpyusd', 103.21, '2024-09-10T12:02')" +
                                ",('gbpusd', 1.321, '2024-09-10T13:02')"
                );

                try (MatViewRefreshJob refreshJob = createMatViewRefreshJob(main1.getEngine())) {
                    drainWalAndMatViewQueues(refreshJob, main1.getEngine());

                    assertSql(
                            main1,
                            replaceExpectedTimestamp("sym\tprice\tts\n" +
                                    "gbpusd\t1.323\t2024-09-10T12:00:00.000000Z\n" +
                                    "jpyusd\t103.21\t2024-09-10T12:00:00.000000Z\n" +
                                    "gbpusd\t1.321\t2024-09-10T13:00:00.000000Z\n"),
                            "price_1h order by ts, sym"
                    );

                    execute(
                            main1, "insert into base_price values('gbpusd', 1.319, '2024-09-10T12:05')" +
                                    ",('gbpusd', 1.325, '2024-09-10T13:03')"
                    );
                    drainWalAndMatViewQueues(refreshJob, main1.getEngine());
                }

                // apply WAL and remove WAL files
                drainWalQueue(main1.getEngine());
                main1.getEngine().clear();
                try (WalPurgeJob purgeJob = new WalPurgeJob(main1.getEngine())) {
                    purgeJob.drain(0);
                }
                // assert that WAL files are removed
                TableToken token = main1.getEngine().getTableTokenIfExists("price_1h");
                try (Path path = new Path()) {
                    CairoConfiguration configuration = main1.getEngine().getConfiguration();
                    path.of(configuration.getDbRoot()).concat(token).concat(WAL_NAME_BASE).put(1);
                    Assert.assertFalse(configuration.getFilesFacade().exists(path.$()));
                }
            }

            try (final TestServerMain main2 = startMainPortsDisabled()) {
                TableToken token = main2.getEngine().getTableTokenIfExists("price_1h");
                MatViewState state = main2.getEngine().getMatViewStateStore().getViewState(token);
                Assert.assertNotNull(state);
                long refreshTxn = state.getLastRefreshBaseTxn();
                Assert.assertEquals(2, refreshTxn); // two inserts into base table

                String expected = "sym\tprice\tts\n" +
                        "gbpusd\t1.319\t2024-09-10T12:00:00.000000Z\n" +
                        "jpyusd\t103.21\t2024-09-10T12:00:00.000000Z\n" +
                        "gbpusd\t1.325\t2024-09-10T13:00:00.000000Z\n";

                assertSql(main2, replaceExpectedTimestamp(expected), "select sym, last(price) as price, ts from base_price sample by 1h order by ts, sym");
                assertSql(main2, replaceExpectedTimestamp(expected), "price_1h order by ts, sym");

                execute(
                        main2,
                        "insert into base_price values('gbpusd', 1.320, '2024-09-10T12:10')" +
                                ",('gbpusd', 1.327, '2024-09-10T13:03')"
                );

                drainWalAndMatViewQueues(main2.getEngine());

                expected = "sym\tprice\tts\n" +
                        "gbpusd\t1.32\t2024-09-10T12:00:00.000000Z\n" +
                        "jpyusd\t103.21\t2024-09-10T12:00:00.000000Z\n" +
                        "gbpusd\t1.327\t2024-09-10T13:00:00.000000Z\n";

                assertSql(main2, replaceExpectedTimestamp(expected), "select sym, last(price) as price, ts from base_price sample by 1h order by ts, sym");
                assertSql(main2, replaceExpectedTimestamp(expected), "price_1h order by ts, sym");
            }
        });
    }

    @Test
    public void testMatViewsReloadOnServerStartCorruptedDefinitionFile() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String viewDirName;
            try (final TestServerMain main1 = startMainPortsDisabled()) {
                executeWithRewriteTimestamp(
                        main1,
                        "create table base_price (" +
                                "sym varchar, price double, ts #TIMESTAMP" +
                                ") timestamp(ts) partition by DAY WAL"
                );

                createMatView(main1);

                execute(
                        main1,
                        "insert into base_price values('gbpusd', 1.320, '2024-09-10T12:01')" +
                                ",('gbpusd', 1.323, '2024-09-10T12:02')" +
                                ",('jpyusd', 103.21, '2024-09-10T12:02')" +
                                ",('gbpusd', 1.321, '2024-09-10T13:02')"
                );
                drainWalAndMatViewQueues(main1.getEngine());
                assertSql(
                        main1,
                        replaceExpectedTimestamp("sym\tprice\tts\n" +
                                "gbpusd\t1.323\t2024-09-10T12:00:00.000000Z\n" +
                                "jpyusd\t103.21\t2024-09-10T12:00:00.000000Z\n" +
                                "gbpusd\t1.321\t2024-09-10T13:00:00.000000Z\n"),
                        "price_1h order by ts, sym"
                );

                TableToken tableToken = main1.getEngine().getTableTokenIfExists("price_1h");
                viewDirName = tableToken.getDirName();
            }

            // Delete _mv file.
            TestFilesFacadeImpl.INSTANCE.remove(dbPath.trimTo(dbPathLen).concat(viewDirName).concat(MatViewDefinition.MAT_VIEW_DEFINITION_FILE_NAME).$());

            // The mat view should be skipped on server start.
            try (final TestServerMain main2 = startMainPortsDisabled()) {
                drainMatViewQueue(main2.getEngine());
                assertSql(
                        main2,
                        "count()\n" +
                                "0\n",
                        "select count() from materialized_views();"
                );
            }
        });
    }

    @Test
    public void testMatViewsReloadOnServerStartInvalidState() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain main1 = startMainPortsDisabled()) {
                executeWithRewriteTimestamp(
                        main1,
                        "create table base_price (" +
                                "sym varchar, price double, ts #TIMESTAMP" +
                                ") timestamp(ts) partition by DAY WAL"
                );

                createMatView(main1);

                execute(
                        main1,
                        "insert into base_price values('gbpusd', 1.320, '2024-09-10T12:01')" +
                                ",('gbpusd', 1.323, '2024-09-10T12:02')" +
                                ",('jpyusd', 103.21, '2024-09-10T12:02')" +
                                ",('gbpusd', 1.321, '2024-09-10T13:02')"
                );
                try (MatViewRefreshJob refreshJob = createMatViewRefreshJob(main1.getEngine())) {
                    drainWalAndMatViewQueues(refreshJob, main1.getEngine());

                    assertSql(
                            main1,
                            replaceExpectedTimestamp(
                                    "sym\tprice\tts\n" +
                                            "gbpusd\t1.323\t2024-09-10T12:00:00.000000Z\n" +
                                            "jpyusd\t103.21\t2024-09-10T12:00:00.000000Z\n" +
                                            "gbpusd\t1.321\t2024-09-10T13:00:00.000000Z\n"),
                            "price_1h order by ts, sym"
                    );

                    execute(
                            main1,
                            "insert into base_price values('gbpusd', 1.319, '2024-09-10T12:05')" +
                                    ",('gbpusd', 1.325, '2024-09-10T13:03')"
                    );

                    drainWalAndMatViewQueues(refreshJob, main1.getEngine());

                    String expected = "sym\tprice\tts\n" +
                            "gbpusd\t1.319\t2024-09-10T12:00:00.000000Z\n" +
                            "jpyusd\t103.21\t2024-09-10T12:00:00.000000Z\n" +
                            "gbpusd\t1.325\t2024-09-10T13:00:00.000000Z\n";

                    assertSql(main1, replaceExpectedTimestamp(expected), "select sym, last(price) as price, ts from base_price sample by 1h order by ts, sym");
                    assertSql(main1, replaceExpectedTimestamp(expected), "price_1h order by ts, sym");

                    execute(main1, "truncate table base_price");
                    drainWalAndMatViewQueues(refreshJob, main1.getEngine());
                    assertSql(
                            main1,
                            "view_name\tview_status\tinvalidation_reason\n" +
                                    "price_1h\tinvalid\ttruncate operation\n",
                            "select view_name, view_status, invalidation_reason from materialized_views()"
                    );
                }
            }

            try (final TestServerMain main2 = startMainPortsDisabled()) {
                assertSql(
                        main2,
                        "view_name\tview_status\tinvalidation_reason\n" +
                                "price_1h\tinvalid\ttruncate operation\n",
                        "select view_name, view_status, invalidation_reason from materialized_views()"
                );

                execute(main2, "refresh materialized view price_1h full;");
                drainWalAndMatViewQueues(main2.getEngine());

                assertSql(
                        main2,
                        "view_name\tview_status\tinvalidation_reason\n" +
                                "price_1h\tvalid\t\n",
                        "select view_name, view_status, invalidation_reason from materialized_views()"
                );
            }

            try (final TestServerMain main3 = startMainPortsDisabled()) {
                assertSql(
                        main3,
                        "view_name\tview_status\tinvalidation_reason\n" +
                                "price_1h\tvalid\t\n",
                        "select view_name, view_status, invalidation_reason from materialized_views()"
                );
            }
        });
    }

    @Test
    public void testMatViewsReloadOnServerStartMissingBaseTable() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain main1 = startMainPortsDisabled()) {
                executeWithRewriteTimestamp(
                        main1,
                        "create table base_price (" +
                                "sym varchar, price double, ts #TIMESTAMP" +
                                ") timestamp(ts) partition by DAY WAL"
                );

                createMatView(main1);

                execute(
                        main1,
                        "insert into base_price values('gbpusd', 1.320, '2024-09-10T12:01')" +
                                ",('gbpusd', 1.323, '2024-09-10T12:02')" +
                                ",('jpyusd', 103.21, '2024-09-10T12:02')" +
                                ",('gbpusd', 1.321, '2024-09-10T13:02')"
                );
                drainWalAndMatViewQueues(main1.getEngine());
                assertSql(
                        main1,
                        replaceExpectedTimestamp(
                                "sym\tprice\tts\n" +
                                        "gbpusd\t1.323\t2024-09-10T12:00:00.000000Z\n" +
                                        "jpyusd\t103.21\t2024-09-10T12:00:00.000000Z\n" +
                                        "gbpusd\t1.321\t2024-09-10T13:00:00.000000Z\n"),
                        "price_1h order by ts, sym"
                );

                // Drop base table.
                execute(main1, "drop table base_price");
            }

            // The mat view should be skipped on server start.
            try (final TestServerMain main2 = startMainPortsDisabled()) {
                drainWalAndMatViewQueues(main2.getEngine());

                // The mat view should be loaded, but left in invalid state.
                assertSql(
                        main2,
                        "view_name\tview_status\n" +
                                "price_1h\tinvalid\n",
                        "select view_name, view_status from materialized_views();"
                );
            }
        });
    }

    @Test
    public void testMatViewsReloadOnServerStartNonWalBaseTable() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain main1 = startMainPortsDisabled()) {
                executeWithRewriteTimestamp(
                        main1,
                        "create table base_price (" +
                                "sym varchar, price double, ts #TIMESTAMP" +
                                ") timestamp(ts) partition by DAY WAL"
                );

                createMatView(main1);

                execute(
                        main1,
                        "insert into base_price values('gbpusd', 1.320, '2024-09-10T12:01')" +
                                ",('gbpusd', 1.323, '2024-09-10T12:02')" +
                                ",('jpyusd', 103.21, '2024-09-10T12:02')" +
                                ",('gbpusd', 1.321, '2024-09-10T13:02')"
                );
                drainWalAndMatViewQueues(main1.getEngine());
                assertSql(
                        main1,
                        replaceExpectedTimestamp(
                                "sym\tprice\tts\n" +
                                        "gbpusd\t1.323\t2024-09-10T12:00:00.000000Z\n" +
                                        "jpyusd\t103.21\t2024-09-10T12:00:00.000000Z\n" +
                                        "gbpusd\t1.321\t2024-09-10T13:00:00.000000Z\n"),
                        "price_1h order by ts, sym"
                );

                // Convert base table to non-WAL.
                execute(main1, "alter table base_price set type bypass wal");
            }

            // The mat view should be skipped on server start.
            try (final TestServerMain main2 = startMainPortsDisabled()) {
                drainWalAndMatViewQueues(main2.getEngine());

                // The mat view should be loaded, but marked as invalid after refresh.
                assertSql(
                        main2,
                        "view_name\tview_status\n" +
                                "price_1h\tinvalid\n",
                        "select view_name, view_status from materialized_views();"
                );
            }
        });
    }

    @Test
    public void testPeriodMatViewsReloadOnServerStart() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final long start = MicrosFormatUtils.parseUTCTimestamp("2024-12-12T00:00:00.000000Z");
            final TestMicroClock testClock = new TestMicroClock(start);

            final String firstExpected = "sym\tprice\tts\n" +
                    "gbpusd\t1.323\t2024-09-10T12:00:00.000000Z\n" +
                    "jpyusd\t103.21\t2024-09-10T12:00:00.000000Z\n" +
                    "gbpusd\t1.321\t2024-09-10T13:00:00.000000Z\n";

            try (final TestServerMain main1 = startMainPortsDisabled(testClock)) {
                executeWithRewriteTimestamp(
                        main1,
                        "create table base_price (" +
                                "sym varchar, price double, ts #TIMESTAMP" +
                                ") timestamp(ts) partition by DAY WAL"
                );

                execute(
                        main1,
                        "create materialized view price_1h refresh incremental period(length 1d) as " +
                                "select sym, last(price) as price, ts from base_price sample by 1h;"
                );

                execute(
                        main1,
                        "insert into base_price values('gbpusd', 1.320, '2024-09-10T12:01')" +
                                ",('gbpusd', 1.323, '2024-09-10T12:02')" +
                                ",('jpyusd', 103.21, '2024-09-10T12:02')" +
                                ",('gbpusd', 1.321, '2024-09-10T13:02')"
                );

                try (var refreshJob = createMatViewRefreshJob(main1.getEngine())) {
                    final MatViewTimerJob timerJob = new MatViewTimerJob(main1.getEngine());
                    drainMatViewTimerQueue(timerJob);
                    drainWalAndMatViewQueues(refreshJob, main1.getEngine());

                    assertSql(
                            main1,
                            replaceExpectedTimestamp(firstExpected),
                            "price_1h order by ts, sym"
                    );
                }
            }

            try (final TestServerMain main2 = startMainPortsDisabled(testClock)) {
                assertSql(main2, replaceExpectedTimestamp(firstExpected), "select sym, last(price) as price, ts from base_price sample by 1h order by ts, sym");
                assertSql(main2, replaceExpectedTimestamp(firstExpected), "price_1h order by ts, sym");

                assertSql(
                        main2,
                        "view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_status\tinvalidation_reason\trefresh_period_hi\trefresh_base_table_txn\ttimer_time_zone\ttimer_start\ttimer_interval\ttimer_interval_unit\tperiod_length\tperiod_length_unit\tperiod_delay\tperiod_delay_unit\n" +
                                "price_1h\timmediate\tbase_price\t\t2024-12-12T00:00:00.000000Z\tvalid\t\t2024-12-12T00:00:00.000000Z\t1\t\t2024-12-12T00:00:00.000000Z\t0\t\t1\tDAY\t0\t\n",
                        "select view_name, refresh_type, base_table_name, last_refresh_start_timestamp, last_refresh_finish_timestamp, " +
                                "view_status, invalidation_reason, refresh_period_hi, refresh_base_table_txn, " +
                                "timer_time_zone, timer_start, timer_interval, timer_interval_unit, " +
                                "period_length, period_length_unit, period_delay, period_delay_unit " +
                                "from materialized_views();"
                );
            }
        });
    }

    @Test
    public void testTimerMatViewsReloadOnServerStart1() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String startStr = "2024-12-12T00:00:00.000000Z";
            final long start = MicrosFormatUtils.parseUTCTimestamp(startStr);
            final TestMicroClock testClock = new TestMicroClock(start);

            final String firstExpected = "sym\tprice\tts\n" +
                    "gbpusd\t1.323\t2024-09-10T12:00:00.000000Z\n" +
                    "jpyusd\t103.21\t2024-09-10T12:00:00.000000Z\n" +
                    "gbpusd\t1.321\t2024-09-10T13:00:00.000000Z\n";

            try (final TestServerMain main1 = startMainPortsDisabled(testClock)) {
                executeWithRewriteTimestamp(
                        main1,
                        "create table base_price (" +
                                "sym varchar, price double, ts #TIMESTAMP" +
                                ") timestamp(ts) partition by DAY WAL"
                );

                execute(
                        main1,
                        "create materialized view price_1h as " +
                                "select sym, last(price) as price, ts from base_price sample by 1h"
                );

                execute(
                        main1,
                        "create materialized view price_1h_t refresh every 1h start '" + startStr + "' as " +
                                "select sym, last(price) as price, ts from base_price sample by 1h"
                );

                execute(
                        main1,
                        "insert into base_price values('gbpusd', 1.320, '2024-09-10T12:01')" +
                                ",('gbpusd', 1.323, '2024-09-10T12:02')" +
                                ",('jpyusd', 103.21, '2024-09-10T12:02')" +
                                ",('gbpusd', 1.321, '2024-09-10T13:02')"
                );

                try (var refreshJob = createMatViewRefreshJob(main1.getEngine())) {
                    drainWalAndMatViewQueues(refreshJob, main1.getEngine());

                    assertSql(
                            main1,
                            replaceExpectedTimestamp(firstExpected),
                            "price_1h order by ts, sym"
                    );

                    final MatViewTimerJob timerJob = new MatViewTimerJob(main1.getEngine());
                    drainMatViewTimerQueue(timerJob);
                    drainWalAndMatViewQueues(refreshJob, main1.getEngine());

                    assertSql(
                            main1,
                            replaceExpectedTimestamp(firstExpected),
                            "price_1h_t order by ts, sym"
                    );
                }
            }

            testClock.micros.addAndGet(Micros.HOUR_MICROS);
            try (final TestServerMain main2 = startMainPortsDisabled(testClock)) {
                assertSql(
                        main2,
                        "view_name\trefresh_type\tbase_table_name\tlast_refresh_start_timestamp\tlast_refresh_finish_timestamp\tview_status\tinvalidation_reason\trefresh_period_hi\trefresh_base_table_txn\ttimer_time_zone\ttimer_start\ttimer_interval\ttimer_interval_unit\tperiod_length\tperiod_length_unit\tperiod_delay\tperiod_delay_unit\n" +
                                "price_1h\timmediate\tbase_price\t\t2024-12-12T00:00:00.000000Z\tvalid\t\t\t1\t\t\t0\t\t0\t\t0\t\n" +
                                "price_1h_t\ttimer\tbase_price\t\t2024-12-12T00:00:00.000000Z\tvalid\t\t\t1\t\t2024-12-12T00:00:00.000000Z\t1\tHOUR\t0\t\t0\t\n",
                        "select view_name, refresh_type, base_table_name, last_refresh_start_timestamp, last_refresh_finish_timestamp, " +
                                "view_status, invalidation_reason, refresh_period_hi, refresh_base_table_txn, " +
                                "timer_time_zone, timer_start, timer_interval, timer_interval_unit, " +
                                "period_length, period_length_unit, period_delay, period_delay_unit " +
                                "from materialized_views() " +
                                "order by view_name;"
                );

                assertSql(main2, replaceExpectedTimestamp(firstExpected), "select sym, last(price) as price, ts from base_price sample by 1h order by ts, sym");
                assertSql(main2, replaceExpectedTimestamp(firstExpected), "price_1h order by ts, sym");
                assertSql(main2, replaceExpectedTimestamp(firstExpected), "price_1h_t order by ts, sym");

                final String secondExpected = "sym\tprice\tts\n" +
                        "gbpusd\t1.333\t2024-09-10T12:00:00.000000Z\n" +
                        "jpyusd\t103.21\t2024-09-10T12:00:00.000000Z\n" +
                        "gbpusd\t1.444\t2024-09-10T13:00:00.000000Z\n";

                execute(
                        main2,
                        "insert into base_price values('gbpusd', 1.333, '2024-09-10T12:10')" +
                                ",('gbpusd', 1.444, '2024-09-10T13:03')"
                );

                drainWalAndMatViewQueues(main2.getEngine());

                assertSql(main2, replaceExpectedTimestamp(secondExpected), "price_1h order by ts, sym");

                final MatViewTimerJob timerJob = new MatViewTimerJob(main2.getEngine());
                drainMatViewTimerQueue(timerJob);
                drainWalAndMatViewQueues(main2.getEngine());

                assertSql(main2, replaceExpectedTimestamp(secondExpected), "select sym, last(price) as price, ts from base_price sample by 1h order by ts, sym");
                assertSql(main2, replaceExpectedTimestamp(secondExpected), "price_1h_t order by ts, sym");
            }
        });
    }

    @Test
    public void testTimerMatViewsReloadOnServerStart2() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String startStr = "2024-12-12T00:00:00.000000Z";
            final long start = MicrosFormatUtils.parseUTCTimestamp(startStr);
            // Set the clock to an earlier timestamp.
            final TestMicroClock testClock = new TestMicroClock(start - Micros.DAY_MICROS);

            // Timer refresh should not kick in during the first server start.
            final String firstExpected = "sym\tprice\tts\n";

            try (final TestServerMain main1 = startMainPortsDisabled(testClock)) {
                executeWithRewriteTimestamp(
                        main1,
                        "create table base_price (" +
                                "sym varchar, price double, ts #TIMESTAMP" +
                                ") timestamp(ts) partition by DAY WAL"
                );

                execute(
                        main1,
                        "create materialized view price_1h refresh every 1h deferred start '" + startStr + "' as (" +
                                "select sym, last(price) as price, ts from base_price sample by 1h" +
                                ") partition by DAY"
                );

                execute(
                        main1,
                        "insert into base_price values('gbpusd', 1.320, '2024-09-10T12:01')" +
                                ",('gbpusd', 1.323, '2024-09-10T12:02')" +
                                ",('jpyusd', 103.21, '2024-09-10T12:02')" +
                                ",('gbpusd', 1.321, '2024-09-10T13:02')"
                );

                try (var refreshJob = createMatViewRefreshJob(main1.getEngine())) {
                    final MatViewTimerJob timerJob = new MatViewTimerJob(main1.getEngine());
                    drainMatViewTimerQueue(timerJob);
                    drainWalAndMatViewQueues(refreshJob, main1.getEngine());

                    assertSql(
                            main1,
                            firstExpected,
                            "price_1h order by ts, sym"
                    );
                }
            }

            // Now the timer refresh should happen.
            testClock.micros.set(start);
            try (final TestServerMain main2 = startMainPortsDisabled(testClock)) {
                assertSql(main2, firstExpected, "price_1h order by ts, sym");

                final MatViewTimerJob timerJob = new MatViewTimerJob(main2.getEngine());
                drainMatViewTimerQueue(timerJob);
                drainWalAndMatViewQueues(main2.getEngine());

                final String secondExpected = "sym\tprice\tts\n" +
                        "gbpusd\t1.323\t2024-09-10T12:00:00.000000Z\n" +
                        "jpyusd\t103.21\t2024-09-10T12:00:00.000000Z\n" +
                        "gbpusd\t1.321\t2024-09-10T13:00:00.000000Z\n";

                assertSql(main2, replaceExpectedTimestamp(secondExpected), "select sym, last(price) as price, ts from base_price sample by 1h order by ts, sym");
                assertSql(main2, replaceExpectedTimestamp(secondExpected), "price_1h order by ts, sym");
            }
        });
    }

    private static void assertSql(TestServerMain serverMain, final String expected, final String sql) {
        serverMain.assertSql(sql, expected);
    }

    private static void createMatView(TestServerMain serverMain) {
        execute(serverMain, "create materialized view price_1h as (select sym, last(price) as price, ts from base_price sample by 1h) partition by DAY");
    }

    private static void execute(TestServerMain serverMain, final String sql) {
        serverMain.execute(sql);
    }

    private static Bootstrap newBootstrapWithClock(MicrosecondClock microsecondClock, Map<String, String> envs) {
        Map<String, String> env = new HashMap<>(System.getenv());
        env.putAll(envs);
        return new Bootstrap(
                new PropBootstrapConfiguration() {
                    @Override
                    public Map<String, String> getEnv() {
                        return env;
                    }
                },
                getServerMainArgs()
        ) {
            @Override
            public MicrosecondClock getMicrosecondClock() {
                return microsecondClock != null ? microsecondClock : super.getMicrosecondClock();
            }
        };
    }

    @NotNull
    private static TestServerMain startMainPortsDisabled() {
        return startWithEnvVariables0(
                PropertyKey.DEV_MODE_ENABLED.getEnvVarName(), "true",
                PropertyKey.LINE_TCP_ENABLED.getEnvVarName(), "false",
                PropertyKey.LINE_UDP_ENABLED.getEnvVarName(), "false",
                PropertyKey.HTTP_MIN_ENABLED.getEnvVarName(), "false",
                PropertyKey.HTTP_ENABLED.getEnvVarName(), "false",
                PropertyKey.PG_ENABLED.getEnvVarName(), "false"
        );
    }

    @NotNull
    private static TestServerMain startMainPortsDisabled(MicrosecondClock microsecondClock, String... extraEnvs) {
        assert extraEnvs.length % 2 == 0;
        ((MicrosTimestampDriver) (MicrosTimestampDriver.INSTANCE)).setTicker(microsecondClock);
        ((NanosTimestampDriver) (NanosTimestampDriver.INSTANCE)).setTicker((NanosecondClock) () -> NanosTimestampDriver.INSTANCE.fromMicros(microsecondClock.getTicks()));

        final String[] disablePortsEnvs = new String[]{
                PropertyKey.DEV_MODE_ENABLED.getEnvVarName(), "true",
                PropertyKey.LINE_TCP_ENABLED.getEnvVarName(), "false",
                PropertyKey.LINE_UDP_ENABLED.getEnvVarName(), "false",
                PropertyKey.HTTP_MIN_ENABLED.getEnvVarName(), "false",
                PropertyKey.HTTP_ENABLED.getEnvVarName(), "false",
                PropertyKey.PG_ENABLED.getEnvVarName(), "false"
        };

        final String[] envs = new String[disablePortsEnvs.length + extraEnvs.length];
        for (int i = 0; i < extraEnvs.length; i += 2) {
            envs[i] = extraEnvs[i];
            envs[i + 1] = extraEnvs[i + 1];
        }
        for (int i = disablePortsEnvs.length; i < envs.length; i += 2) {
            envs[i] = disablePortsEnvs[i - disablePortsEnvs.length];
            envs[i + 1] = disablePortsEnvs[i + 1 - disablePortsEnvs.length];
        }

        return startWithEnvVariables0(microsecondClock, envs);
    }

    private static TestServerMain startWithEnvVariables0(String... envs) {
        return startWithEnvVariables0(null, envs);
    }

    private static TestServerMain startWithEnvVariables0(MicrosecondClock microsecondClock, String... envs) {
        assert envs.length % 2 == 0;

        Map<String, String> envMap = new HashMap<>();
        for (int i = 0; i < envs.length; i += 2) {
            envMap.put(envs[i], envs[i + 1]);
        }
        TestServerMain serverMain = new TestServerMain(newBootstrapWithClock(microsecondClock, envMap)) {
            @Override
            protected void setupMatViewJobs(
                    WorkerPool mvWorkerPool,
                    CairoEngine engine,
                    int sharedQueryWorkerCount
            ) {
            }

            @Override
            protected void setupWalApplyJob(
                    WorkerPool workerPool,
                    CairoEngine engine,
                    int sharedQueryWorkerCount
            ) {
            }
        };
        serverMain.start();
        return serverMain;
    }

    private void assertLineError(Transport transport, TestServerMain main, MatViewRefreshJob refreshJob, String expected) {
        try (Sender sender = buildLineSender(transport)) {
            sender.table("price_1h").stringColumn("sym", "gbpusd")
                    .doubleColumn("price", 1.330)
                    .atNow();
            sender.flush();
        } catch (LineSenderException e) {
            // TODO(eugene): how to check the error for TCP/UDP?
            assertContains(e.getMessage(), "cannot modify materialized view");
        }

        refreshJob.run(0);
        drainWalQueue(main.getEngine());
        assertSql(main, expected, "price_1h order by ts, sym");
    }

    private Sender buildLineSender(Transport transport) {
        switch (transport) {
            case UDP:
                return new LineUdpSender(
                        NetworkFacadeImpl.INSTANCE,
                        0,
                        Net.parseIPv4("127.0.0.1"),
                        ILP_PORT, 200, 1
                );
            case TCP:
                return Sender.builder(Sender.Transport.TCP)
                        .address("localhost")
                        .port(ILP_PORT)
                        .build();
            default:
                return Sender.builder(Sender.Transport.HTTP)
                        .address("localhost")
                        .port(HTTP_PORT)
                        .build();
        }
    }

    private void executeWithRewriteTimestamp(TestServerMain serverMain, String sql) {
        sql = sql.replaceAll("#TIMESTAMP", timestampType.getTypeName());
        execute(serverMain, sql);
    }

    private String replaceExpectedTimestamp(String expected) {
        return ColumnType.isTimestampMicro(timestampType.getTimestampType()) ? expected : expected.replaceAll(".000000Z", ".000000000Z");
    }

    enum Transport {
        HTTP,
        UDP,
        TCP
    }
}
