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

import io.questdb.TelemetryJob;
import io.questdb.cairo.mv.MatViewRefreshJob;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.std.NumericException;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static io.questdb.PropertyKey.*;
import static io.questdb.griffin.model.IntervalUtils.parseFloorPartialTimestamp;
import static org.junit.Assert.assertNull;

public class MatViewTelemetryTest extends AbstractCairoTest {

    @BeforeClass
    public static void setUpStatic() throws Exception {
        // needed for static engine instance
        setProperty(CAIRO_MAT_VIEW_ENABLED, "true");
        AbstractCairoTest.setUpStatic();
    }

    @Before
    public void setUp() {
        setProperty(CAIRO_MAT_VIEW_ENABLED, "true");
        setProperty(DEV_MODE_ENABLED, "true");
        setProperty(CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 10);
        super.setUp();
    }

    @Test
    public void testMatViewDrop() throws Exception {
        assertMemoryLeak(() -> {
            try (final TelemetryJob telemetryJob = new TelemetryJob(engine)) {
                createBaseTable("2024-10-24T17:00:00.000000Z");
                createMatView("2024-10-24T17:00:15.000000Z", telemetryJob);

                try (MatViewRefreshJob refreshJob = new MatViewRefreshJob(0, engine)) {
                    execute("2024-10-24T17:00:25.000000Z", refreshJob, telemetryJob,
                            "insert into base_price values('gbpusd', 1.320, '2024-09-10T12:01')" +
                                    ",('gbpusd', 1.323, '2024-09-10T12:02')" +
                                    ",('jpyusd', 103.21, '2024-09-10T12:02')" +
                                    ",('gbpusd', 1.321, '2024-09-10T13:02')"
                    );
                }

                assertSql(
                        "sym\tprice\tts\n" +
                                "gbpusd\t1.323\t2024-09-10T12:00:00.000000Z\n" +
                                "jpyusd\t103.21\t2024-09-10T12:00:00.000000Z\n" +
                                "gbpusd\t1.321\t2024-09-10T13:00:00.000000Z\n",
                        "price_1h order by ts, sym"
                );

                dropMatView("2024-10-24T17:00:33.000000Z", telemetryJob);

                assertSql(
                        "created\tevent\tview_table_id\tbase_table_txn\tinvalidation_reason\tlatency\n" +
                                "2024-10-24T17:00:15.000000Z\t200\t6\tnull\t\t0.0000\n" +
                                "2024-10-24T17:00:25.000000Z\t204\t6\t1\t\t10000.0000\n" +
                                "2024-10-24T17:00:33.000000Z\t201\t6\tnull\t\t0.0000\n",
                        "sys.telemetry_mat_view"
                );
            }
        });
    }

    @Test
    public void testMatViewInvalidate() throws Exception {
        assertMemoryLeak(() -> {
            try (final TelemetryJob telemetryJob = new TelemetryJob(engine)) {
                createBaseTable("2024-10-24T17:00:00.000000Z");
                createMatView("2024-10-24T17:00:15.000000Z", telemetryJob);

                try (MatViewRefreshJob refreshJob = new MatViewRefreshJob(0, engine)) {

                    execute("2024-10-24T17:00:25.000000Z", refreshJob, telemetryJob,
                            "insert into base_price values('gbpusd', 1.320, '2024-09-10T12:01')" +
                                    ",('gbpusd', 1.323, '2024-09-10T12:02')" +
                                    ",('jpyusd', 103.21, '2024-09-10T12:02')" +
                                    ",('gbpusd', 1.321, '2024-09-10T13:02')"
                    );

                    assertSql(
                            "sym\tprice\tts\n" +
                                    "gbpusd\t1.323\t2024-09-10T12:00:00.000000Z\n" +
                                    "jpyusd\t103.21\t2024-09-10T12:00:00.000000Z\n" +
                                    "gbpusd\t1.321\t2024-09-10T13:00:00.000000Z\n",
                            "price_1h order by ts, sym"
                    );

                    execute("2024-10-24T17:00:41.000000Z", refreshJob, telemetryJob,
                            "truncate table base_price"
                    );
                }

                assertSql(
                        "created\tevent\tview_table_id\tbase_table_txn\tinvalidation_reason\tlatency\n" +
                                "2024-10-24T17:00:15.000000Z\t200\t6\tnull\t\t0.0000\n" +
                                "2024-10-24T17:00:25.000000Z\t204\t6\t1\t\t10000.0000\n" +
                                "2024-10-24T17:00:41.000000Z\t202\t6\tnull\ttruncate operation\t0.0000\n",
                        "sys.telemetry_mat_view"
                );
            }
        });
    }

    @Test
    public void testMatViewRefreshFails() throws Exception {
        assertMemoryLeak(() -> {
            try (final TelemetryJob telemetryJob = new TelemetryJob(engine)) {
                createBaseTable("2024-10-24T17:00:00.000000Z");
                createMatView("2024-10-24T17:00:15.000000Z", telemetryJob);

                try (MatViewRefreshJob refreshJob = new MatViewRefreshJob(0, engine)) {
                    execute("2024-10-24T17:00:25.000000Z", refreshJob, telemetryJob,
                            "insert into base_price values('gbpusd', 1.320, '2024-09-10T12:01')" +
                                    ",('gbpusd', 1.323, '2024-09-10T12:02')" +
                                    ",('jpyusd', 103.21, '2024-09-10T12:02')" +
                                    ",('gbpusd', 1.321, '2024-09-10T13:02')"
                    );

                    assertSql(
                            "sym\tprice\tts\n" +
                                    "gbpusd\t1.323\t2024-09-10T12:00:00.000000Z\n" +
                                    "jpyusd\t103.21\t2024-09-10T12:00:00.000000Z\n" +
                                    "gbpusd\t1.321\t2024-09-10T13:00:00.000000Z\n",
                            "price_1h order by ts, sym"
                    );

                    execute("2024-10-24T17:00:33.000000Z", refreshJob, telemetryJob,
                            "rename table base_price to base_price2",
                            "refresh materialized view 'price_1h' full;"
                    );
                }

                assertSql(
                        "created\tevent\tview_table_id\tbase_table_txn\tinvalidation_reason\tlatency\n" +
                                "2024-10-24T17:00:15.000000Z\t200\t6\tnull\t\t0.0000\n" +
                                "2024-10-24T17:00:25.000000Z\t204\t6\t1\t\t10000.0000\n" +
                                "2024-10-24T17:00:33.000000Z\t202\t6\tnull\ttable does not exist [table=base_price]\t0.0000\n" +
                                "2024-10-24T17:00:33.000000Z\t203\t6\tnull\ttable does not exist [table=base_price]\t0.0000\n",
                        "sys.telemetry_mat_view"
                );
            }
        });
    }

    @Test
    public void testMatViewRefreshSuccessful() throws Exception {
        assertMemoryLeak(() -> {
            try (final TelemetryJob telemetryJob = new TelemetryJob(engine)) {
                createBaseTable("2024-10-24T17:00:10.000000Z");
                createMatView("2024-10-24T17:00:20.000000Z", telemetryJob);

                try (MatViewRefreshJob refreshJob = new MatViewRefreshJob(0, engine)) {
                    execute("2024-10-24T17:01:00.000000Z", refreshJob, telemetryJob,
                            "insert into base_price " +
                                    "select 'gbpusd', 1.320 + x / 1000.0, timestamp_sequence('2024-09-10T12:02', 1000000*60*5) " +
                                    "from long_sequence(24 * 20 * 5)"
                    );

                    assertSql(
                            "sequencerTxn\tminTimestamp\tmaxTimestamp\n" +
                                    "1\t2024-09-10T12:00:00.000000Z\t2024-09-18T19:00:00.000000Z\n",
                            "select sequencerTxn, minTimestamp, maxTimestamp from wal_transactions('price_1h')"
                    );

                    execute("2024-10-24T17:01:30.000000Z", refreshJob, telemetryJob,
                            "insert into base_price values('gbpusd', 1.319, '2024-09-10T12:05')" +
                                    ",('gbpusd', 1.325, '2024-09-10T13:03')"
                    );
                }

                assertSql(
                        "sequencerTxn\tminTimestamp\tmaxTimestamp\n" +
                                "1\t2024-09-10T12:00:00.000000Z\t2024-09-18T19:00:00.000000Z\n" +
                                "2\t2024-09-10T12:00:00.000000Z\t2024-09-10T13:00:00.000000Z\n",
                        "select sequencerTxn, minTimestamp, maxTimestamp from wal_transactions('price_1h')"
                );

                assertViewMatchesSqlOverBaseTable();

                assertSql(
                        "created\tevent\tview_table_id\tbase_table_txn\tinvalidation_reason\tlatency\n" +
                                "2024-10-24T17:00:20.000000Z\t200\t6\tnull\t\t0.0000\n" +
                                "2024-10-24T17:01:00.000000Z\t204\t6\t1\t\t40000.0000\n" +
                                "2024-10-24T17:01:30.000000Z\t204\t6\t2\t\t30000.0000\n",
                        "sys.telemetry_mat_view"
                );
            }
        });
    }

    private static void assertViewMatchesSqlOverBaseTable() throws SqlException {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            TestUtils.assertEquals(
                    compiler,
                    sqlExecutionContext,
                    "select sym, last(price) as price, ts from base_price sample by 1h order by ts, sym",
                    "price_1h order by ts, sym"
            );
        }
    }

    private static void createBaseTable(String currentTime) throws SqlException, NumericException {
        currentMicros = parseFloorPartialTimestamp(currentTime);
        execute("create table " + "base_price" + " (" +
                "sym varchar, price double, ts timestamp" +
                ") timestamp(ts) partition by DAY WAL"
        );
        engine.verifyTableName("base_price");
    }

    private static void createMatView(String currentTime, TelemetryJob telemetryJob) throws NumericException, SqlException {
        currentMicros = parseFloorPartialTimestamp(currentTime);
        execute("create materialized view " + "price_1h" + " as ("
                + "select sym, last(price) as price, ts from " + "base_price" + " sample by 1h"
                + ") partition by DAY");
        engine.verifyTableName("price_1h");
        telemetryJob.runSerially();
    }

    private static void dropMatView(String currentTime, TelemetryJob telemetryJob) throws NumericException, SqlException {
        currentMicros = parseFloorPartialTimestamp(currentTime);
        execute("drop materialized view price_1h");
        assertNull(engine.getTableTokenIfExists("price_1h"));
        telemetryJob.runSerially();
    }

    private void execute(String currentTime, MatViewRefreshJob refreshJob, TelemetryJob telemetryJob, String... sqls) throws SqlException, NumericException {
        for (int i = 0, n = sqls.length; i < n; i++) {
            execute(sqls[i]);
        }
        drainWalQueue();
        currentMicros = parseFloorPartialTimestamp(currentTime);
        refreshJob.run(0);
        drainWalQueue();
        telemetryJob.runSerially();
    }
}
