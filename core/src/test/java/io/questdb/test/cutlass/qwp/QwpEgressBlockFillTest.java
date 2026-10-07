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

import io.questdb.PropertyKey;
import io.questdb.cutlass.qwp.codec.QwpEgressMsgKind;
import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.cutlass.qwp.protocol.QwpVarint;
import io.questdb.cutlass.qwp.server.egress.QwpEgressMetrics;
import io.questdb.cutlass.qwp.server.egress.QwpEgressUpgradeProcessor;
import io.questdb.cutlass.qwp.websocket.WebSocketOpcode;
import io.questdb.std.Chars;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Cursor results that offer {@link io.questdb.cairo.sql.RecordBlock}s are filled column by column
 * ({@code QwpResultBatchBuffer.appendBlock}). The client must receive exactly the bytes the row
 * by row fill ({@code appendRow}) sends: every test runs the same exchange twice on a fresh
 * connection, once with the block fill switched off and once on, and compares every frame the
 * server sent, byte for byte. Each also checks that the block fill did run, so that no comparison
 * is between two row fills.
 */
public class QwpEgressBlockFillTest extends AbstractBootstrapTest {
    private static final String ALL_TYPES_DDL = "create table at (" +
            "b boolean, by byte, sh short, ch char, i int, ip ipv4, l long, d date, ts timestamp, tn timestamp_ns, " +
            "f float, db double, s symbol, s2 symbol, u uuid, l256 long256, g1 geohash(1c), g3 geohash(3c), g6 geohash(6c), " +
            "g12 geohash(12c), dc64 decimal(12,2), dc128 decimal(30,4), dc256 decimal(60,6), v varchar, st string, bn binary, " +
            "arr double[]) timestamp(ts) partition by DAY BYPASS WAL";
    private static final String ALL_TYPES_SELECT = "select x % 3 = 0, (x % 120)::byte, (x * 7 % 30000)::short, rnd_char(), " +
            "case when x % 11 = 0 then null else x::int end, case when x % 13 = 0 then null else rnd_ipv4() end, " +
            "case when x % 5 = 0 then null else x * 1000003 end, case when x % 9 = 0 then null else (x * 86400000)::date end, " +
            "(x * 30000000)::timestamp, case when x % 6 = 0 then null else (x * 1000)::timestamp_ns end, " +
            "case when x % 4 = 0 then null else (x / 3.0)::float end, case when x % 7 = 0 then null else x * 1.5 end, " +
            "case when x % 17 = 0 then null else 'k' || (x % 50) end, 'z' || (x % 7), rnd_uuid4(), rnd_long256(), " +
            "rnd_geohash(5), rnd_geohash(15), rnd_geohash(30), rnd_geohash(60), rnd_decimal(12,2,5), rnd_decimal(30,4,5), " +
            "rnd_decimal(60,6,5), case when x % 8 = 0 then null else 'v' || x end, case when x % 10 = 0 then null else 's' || x end, " +
            "rnd_bin(1, 8, 3), rnd_double_array(1, 1)";
    // idx 50's shape, with passthrough columns of every fixed-size type a task chain holds
    private static final String WINDOW_SQL = "select sym, ex, b, by, sh, ch, i, ip, l, d, f, db, u, g6, dc64, ts, " +
            "avg((bsize * bid + asize * ask) / (bsize + asize)) over (partition by sym rows between 4 preceding and current row) mid, " +
            "avg(asize + bsize) over (partition by sym rows between 4 preceding and current row) size " +
            "from q where sym in ('A', 'B', 'C1', 'C2', 'C3', 'C5', 'C8', 'D') order by sym";

    // idx 3, 9, 18 and 19's shapes over every type: JIT and Java filters, LIMITs, a selective one
    private static final String[] FILTER_SQLS = {
            "select * from at where l > 5000",
            "select * from at where 0 < l and 0 < db and i < 2147483647",
            "select s, ts, db, f, i, s2, b from at where s in ('k1', 'k7', 'k9') and 20 < i + by",
            "select * from at where v like '%3%'",
            "select * from at where l % 97 = 0",
            "select * from at where l > 5000 limit 3000",
            "select * from at where l > 5000 limit 13, 7013",
            "select * from at where l < 0",
    };

    // whether every query of an exchange must fail, rather than none
    private boolean errorExpected;
    private QwpEgressMetrics metrics;

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @After
    public void resetBlockFill() {
        QwpEgressUpgradeProcessor.DEBUG_DISABLE_BLOCK_FILL = false;
        errorExpected = false;
    }

    @Test
    public void testAllTypesAcrossManyBatches() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = start(smallFrames())) {
                createAllTypes(serverMain, 20_000);
                // LIMIT over a table scan is a cursor result: the scan offers its frames
                final String[] sqls = {
                        "select * from at limit 1000000",
                        "select * from at limit 13, 7013",
                        "select * from at limit -2500",
                        "select ts, s, db, l, s2, b from at limit 15000",
                };
                assertBlockFillMatchesRowFill(sqls, "?qwp_max_batch_rows=1000", 0, -1);
                assertBlockFillMatchesRowFill(sqls, "", 0, -1);
            }
        });
    }

    @Test
    public void testAsyncFilter() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = start(filterEnv())) {
                createAllTypes(serverMain, 20_000);
                assertPlanContains(serverMain, "select * from at where l > 5000", "Async JIT Filter");
                assertPlanContains(serverMain, "select * from at where v like '%3%'", "Async Filter");
                assertBlockFillMatchesRowFill(FILTER_SQLS, "?qwp_max_batch_rows=1000", 0, -1);
                assertBlockFillMatchesRowFill(FILTER_SQLS, "", 0, -1);
            }
        });
    }

    @Test
    public void testAsyncFilterOverParquet() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = start(filterEnv())) {
                createAllTypes(serverMain, 20_000);
                // Parquet partitions, the filter's columns decoded first and the rest for the selected
                // rows only (late materialization), and a native partition left between them
                serverMain.execute("alter table at convert partition to parquet where ts < '1970-01-04' or (ts >= '1970-01-05' and ts < '1970-01-07')");
                assertBlockFillMatchesRowFill(FILTER_SQLS, "?qwp_max_batch_rows=1000", 0, -1);
                assertBlockFillMatchesRowFill(FILTER_SQLS, "", 0, -1);
            }
        });
    }

    @Test
    public void testAsyncWindow() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = start(windowEnv())) {
                createWindowTable(serverMain);
                // alone first: only the Async Window offers blocks here, so the block fill ran on its tasks
                assertBlockFillMatchesRowFill(new String[]{WINDOW_SQL}, "", 0, -1);
                final String[] sqls = {
                        WINDOW_SQL,
                        "select * from (" + WINDOW_SQL + ") limit 7, 20007",
                        // twice on one connection: the second query reuses the connection's symbols
                        WINDOW_SQL,
                };
                assertBlockFillMatchesRowFill(sqls, "?qwp_max_batch_rows=777", 0, -1);
                assertBlockFillMatchesRowFill(sqls, "", 0, -1);
            }
        });
    }

    @Test
    public void testBudgetStopOnLastRowOfRowByRowWindow() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // two SYMBOL columns of all-new keys, padded so that with the smallest send buffer the
            // dictionary budget stops the row fill on the 512th row of a batch: the last row of a
            // 64-row row-by-row window of the block fill
            try (TestServerMain serverMain = start(
                    "QDB_HTTP_SEND_BUFFER_SIZE", "163840",
                    PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS.getEnvVarName(), "100000")) {
                serverMain.execute("create table sy2 (a symbol capacity 262144, b symbol capacity 262144, n long, ts timestamp) " +
                        "timestamp(ts) partition by YEAR BYPASS WAL");
                serverMain.execute("insert into sy2 select rpad('a' || x, 95, '.'), rpad('b' || x, 95, '-'), x, " +
                        "(x * 1000000)::timestamp from long_sequence(60000)");
                final long splits = metrics.batchOverflowSplitCount();
                assertBlockFillMatchesRowFill(new String[]{"select a, b, n from sy2 where n % 3 <> 1", "select a, b, n + 1 from sy2"}, "", 0, -1);
                Assert.assertTrue("the dictionary budget must have split batches", metrics.batchOverflowSplitCount() > splits);
            }
        });
    }

    @Test
    public void testCancelMidStream() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = start(windowEnv())) {
                createWindowTable(serverMain);
                createAllTypes(serverMain, 5_000);
                // credit of 1 byte: the server parks after the first batch, the CANCEL lands, the
                // next CREDIT resumes it into the cancellation; then a full query on the same connection
                assertBlockFillMatchesRowFill(new String[]{WINDOW_SQL, WINDOW_SQL}, "?qwp_max_batch_rows=500", 1, 0);
                assertBlockFillMatchesRowFill(new String[]{"select * from at limit 4000", "select * from at limit 4000"},
                        "?qwp_max_batch_rows=500", 1, 0);
                // a filter and a projection over it, cancelled while workers hold frames
                final String filtered = "select ts, (db + l) / 2 mid, s, l from at where l > 50";
                assertBlockFillMatchesRowFill(new String[]{filtered, filtered}, "?qwp_max_batch_rows=500", 1, 0);
                // an index scan, its rows read ahead of the cursor when the cancel lands
                assertBlockFillMatchesRowFill(new String[]{"select * from q where sym = 'A'", "select * from q where sym = 'A'"},
                        "?qwp_max_batch_rows=300", 1, 0);
            }
        });
    }

    @Test
    public void testColumnTopsOnSymbolAndNoNullTypes() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = start(smallFrames())) {
                // the first 1000 rows are column tops of the SYMBOL column and of the types with
                // a columnar fill, among them those with no NULL on the wire
                serverMain.execute("create table ct (x long, ts timestamp) timestamp(ts) partition by DAY BYPASS WAL");
                serverMain.execute("insert into ct select x, (x * 30000000)::timestamp from long_sequence(1000)");
                serverMain.execute("alter table ct add column s symbol, b boolean, by byte, sh short, ch char, i int, ip ipv4, f float");
                serverMain.execute("insert into ct select x, ((x + 1000) * 30000000)::timestamp, " +
                        "case when x % 7 = 0 then null else 's' || (x % 13) end, x % 2 = 0, (x % 100)::byte, (x % 999)::short, " +
                        "rnd_char(), x::int, rnd_ipv4(), (x / 3.0)::float from long_sequence(3000)");
                final String[] sqls = {
                        "select * from ct limit 1000000",
                        "select s, x from ct limit 500, 3500",
                        "select * from ct limit -2000",
                };
                assertBlockFillMatchesRowFill(sqls, "?qwp_max_batch_rows=333", 0, -1);
                assertBlockFillMatchesRowFill(sqls, "", 0, -1);
            }
        });
    }

    @Test
    public void testCreditFlow() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = start(windowEnv())) {
                createWindowTable(serverMain);
                createAllTypes(serverMain, 5_000);
                final long suspensions = metrics.creditSuspensionsCount();
                // a small initial credit, topped up by each batch's size as the client reads it
                assertBlockFillMatchesRowFill(new String[]{WINDOW_SQL, "select * from at limit 5000",
                        "select * from at where l > 50", "select * from q where sym = 'B'"}, "?qwp_max_batch_rows=300", 4096, -1);
                Assert.assertTrue("the streams must have parked on credit", metrics.creditSuspensionsCount() > suspensions);
            }
        });
    }

    @Test
    public void testCursorsWithoutBlocksStreamRowByRow() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = start(smallFrames())) {
                createAllTypes(serverMain, 5_000);
                // none of these cursors offers blocks: a backward scan, filtered or not, a sort, a
                // filter's negative LIMIT and a projection computing a SYMBOL; they must stream as
                // before, row by row
                final String[] sqls = {
                        "select * from at where l > 5000 order by ts desc",
                        "select * from at order by ts desc limit 4000",
                        "select s, l from at order by l limit -3000",
                        "select * from at where l > 5000 limit -1000",
                        "select v::symbol vs, l from at where l > 100",
                };
                assertBlockFillMatchesRowFill(sqls, "?qwp_max_batch_rows=700", 0, -1, false);
            }
        });
    }

    @Test
    public void testHashJoinLightAndWindowMinMaxFilter() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = start(filterEnv())) {
                serverMain.execute("create table t as (select timestamp_sequence(0, 10000000) ts, rnd_symbol('A', 'B', 'C', null) ex, " +
                        "rnd_symbol(40, 1, 3, 5) sym, rnd_varchar('p', 'q', 'r', null) v, " +
                        "case when x % 37 = 0 then null else ((x * 7919) % 13)::float / 4 end size, " +
                        "case when x % 41 = 0 then null else ((x * 31) % 17) / 3.0 end price, x " +
                        "from long_sequence(20000)) timestamp(ts) partition by day BYPASS WAL");
                final String c = "ts, ex, sym, v, size, price, x";
                // idx 59 and 70: Manual Opt's semi-joins, and the plain window forms
                final String mo59 = "select t.ts, t.ex, t.sym, t.v, t.size, t.price, t.x from t t " +
                        "join (select ex mex, sym msym, min(size) min_size from t) m on t.ex = m.mex and t.sym = m.msym and t.size = m.min_size";
                final String mo70 = "select " + c + " from t join (select sym msym, min(price) min_price from t) m on t.sym = m.msym where price = min_price";
                final String slave = "select t.ts, m.msym, t.price, m.min_price, t.x from t join (select sym msym, min(price) min_price from t) m " +
                        "on t.sym = m.msym where price = min_price";
                final String w59 = "select " + c + " from (select " + c + " from (select " + c + ", min(size) over (partition by ex, sym) min_size from t) where size = min_size)";
                final String w70 = "select " + c + " from (select " + c + " from (select " + c + ", min(price) over (partition by sym) min_price from t) where price = min_price)";
                final String windowColumns = "select " + c + ", mn, mx from (select " + c + ", min(price) over (partition by sym, v) mn, " +
                        "max(price) over (partition by sym, v) mx from t) where price = mn or price = mx";
                for (String sql : new String[]{mo59, mo70, slave}) {
                    assertPlanContains(serverMain, sql, "Async Hash Join Light");
                }
                for (String sql : new String[]{w59, w70, windowColumns}) {
                    assertPlanContains(serverMain, sql, "Async Window Min/Max Filter");
                }
                final String[] sqls = {mo59, mo70, slave, w59, w70, windowColumns, mo59 + " limit 5, 700"};
                assertBlockFillMatchesRowFill(sqls, "?qwp_max_batch_rows=99", 0, -1);
                assertBlockFillMatchesRowFill(sqls, "", 0, -1);
            }
        });
    }

    @Test
    public void testIndexFilterErrorAfterDictionarySplits() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = start(
                    "QDB_HTTP_SEND_BUFFER_SIZE", "163840",
                    PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS.getEnvVarName(), "100000")) {
                for (String indexType : new String[]{"posting", "bitmap"}) {
                    serverMain.execute("drop table if exists fe");
                    serverMain.execute("create table fe (s symbol index type " + indexType + ", s2 symbol capacity 65536, v varchar, " +
                            "x long, ts timestamp) timestamp(ts) partition by YEAR BYPASS WAL");
                    // a new long SYMBOL value per row: the dictionary budget splits the batches; the
                    // filter's implicit cast fails on x = 9001 alone
                    serverMain.execute("insert into fe select 'k' || (x % 3), rpad('z' || x, 150, '.'), " +
                            "case when x = 9001 then 'bad' else '1970-01-01' end, x, (x * 1000000)::timestamp from long_sequence(12000)");
                    final String[] sqls = {"select s, s2, x from fe where s = 'k1' and ts > v"};
                    assertPlanContains(serverMain, sqls[0], "Index forward scan on: s");
                    // the row fill ships the batches before the failing row, then the error; the block
                    // fill must not read ahead into the error before them
                    errorExpected = true;
                    final long splits = metrics.batchOverflowSplitCount();
                    assertBlockFillMatchesRowFill(sqls, "", 0, -1);
                    Assert.assertTrue("the dictionary budget must have split batches", metrics.batchOverflowSplitCount() > splits);
                    errorExpected = false;
                }
            }
        });
    }

    @Test
    public void testIndexScans() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = start(smallFrames())) {
                createIndexedTable(serverMain);
                final String[] sqls = {
                        // idx 5, 6, 10, 16's shapes
                        "select * from ix where s = 'k7' and ts in '1970-01-03'",
                        "select * from ix where s = 'k7'",
                        "select ts, (d + l) / 2 mid from ix where s = 'k3'",
                        "select s, ts, d, f, i, s2, b from ix where s = 'k5'",
                        "select * from ix where s in ('k1', 'k7', 'k11')",
                        "select * from ix where s = 'k7' limit 17, 120",
                        "select * from ix where s = null",
                };
                assertPlanContains(serverMain, sqls[1], "Index forward scan on: s");
                assertBlockFillMatchesRowFill(sqls, "?qwp_max_batch_rows=50", 0, -1);
                assertBlockFillMatchesRowFill(sqls, "", 0, -1);
            }
        });
    }

    @Test
    public void testLatestOnIndexedScans() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = start(smallFrames())) {
                for (String indexType : new String[]{"posting", "bitmap"}) {
                    serverMain.execute("drop table if exists lb");
                    serverMain.execute("create table lb (s symbol index type " + indexType + ", s2 symbol, i int, d double, ts timestamp) " +
                            "timestamp(ts) partition by DAY BYPASS WAL");
                    serverMain.execute("insert into lb select case when x % 17 = 0 then null else 'k' || (x % 13) end, 'z' || (x % 5), " +
                            "x::int, x * 1.5, (x * 900000000)::timestamp from long_sequence(1500)");
                    final String[] sqls = {
                            // the row cursor of one value's latest row ends the scan with NoMoreFramesException
                            "select * from lb where s = 'k7' latest on ts partition by s",
                            "select ts, s, d + i, s2 from lb where s = 'k7' latest on ts partition by s",
                            "select * from lb latest by s where s = 'k3'",
                            "select * from lb where s = 'k99' latest on ts partition by s",
                            "select * from lb where s = 'k7' and i > 10 latest on ts partition by s",
                            "select * from lb where s in ('k1', 'k7', 'k11') latest on ts partition by s",
                            "select * from lb latest on ts partition by s",
                            "select * from lb latest on ts partition by s, s2",
                            // and an index scan that offers blocks, on the same connection
                            "select * from lb where s = 'k7'",
                    };
                    assertPlanContains(serverMain, sqls[0], "Index backward scan on: s");
                    assertBlockFillMatchesRowFill(sqls, "?qwp_max_batch_rows=50", 0, -1);
                    assertBlockFillMatchesRowFill(sqls, "", 0, -1);
                }
            }
        });
    }

    @Test
    public void testNewSymbolsSplitBatchesOnTheDictionaryBudget() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // the smallest send buffer the handshake accepts: the dictionary budget is 60% of it
            try (TestServerMain serverMain = start(
                    "QDB_HTTP_SEND_BUFFER_SIZE", "163840",
                    PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS.getEnvVarName(), "1000")) {
                // two SYMBOL columns of long values, new ones arriving all through the result
                serverMain.execute("create table sy (a symbol capacity 65536, n long, b symbol capacity 65536, d double, ts timestamp) " +
                        "timestamp(ts) partition by DAY BYPASS WAL");
                serverMain.execute("insert into sy select " +
                        "case when x % 31 = 0 then null else rpad('a' || (x % 20000), 150, '.') end, x, " +
                        "case when x % 3 = 0 then rpad('b' || (x % 9000), 120, '-') else 'b' end, x * 0.25, " +
                        "(x * 1000000)::timestamp from long_sequence(40000)");
                final long splits = metrics.batchOverflowSplitCount();
                final String[] sqls = {
                        "select * from sy limit 100000",
                        "select b, n, a from sy limit 100, 35000",
                        "select * from sy limit 100000",
                };
                assertBlockFillMatchesRowFill(sqls, "", 0, -1);
                Assert.assertTrue("the dictionary budget must have split batches", metrics.batchOverflowSplitCount() > splits);
                // one SYMBOL column, on a fresh connection so that its values are new: its own loop
                final long splits2 = metrics.batchOverflowSplitCount();
                assertBlockFillMatchesRowFill(new String[]{"select n, a, d from sy limit 100000"}, "", 0, -1);
                Assert.assertTrue("the dictionary budget must have split batches", metrics.batchOverflowSplitCount() > splits2);
                // the same over a filter's gathered rows, and a projection over it
                final long splits3 = metrics.batchOverflowSplitCount();
                assertBlockFillMatchesRowFill(new String[]{"select * from sy where n % 3 <> 1", "select b, n + 1, a from sy where d > 100"}, "", 0, -1);
                Assert.assertTrue("the dictionary budget must have split batches", metrics.batchOverflowSplitCount() > splits3);
                final long splits4 = metrics.batchOverflowSplitCount();
                assertBlockFillMatchesRowFill(new String[]{"select n, a, d * 2 from sy where n % 5 <> 0"}, "", 0, -1);
                Assert.assertTrue("the dictionary budget must have split batches", metrics.batchOverflowSplitCount() > splits4);
            }
        });
    }

    @Test
    public void testProjections() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = start(filterEnv())) {
                createAllTypes(serverMain, 20_000);
                final String[] sqls = {
                        // idx 11 and 13's shapes: an expression over a filter, and over a selection
                        "select ts, (db + l) / 2 mid from at where s = 'k7'",
                        "select ts, (db + f) / 2 mid from (select * from at where s = 'k7')",
                        // idx 17's: a selection over a filter
                        "select s, ts, db, f, i, s2, ch, ip from at where l > 50",
                        // memoized aliases, computed columns of other types, a plain scan below
                        "select l + 1 a, a * 2 a2, a - 3 a3, s, ts from at where l > 100",
                        "select a, a * 2 a2, a - 3 a3, s from (select l + i a, s, ts from at where l > 100)",
                        "select s, s2, l256, v, i * 2, g6, dc128 from at where l > 100",
                        "select case when b then s else s2 end cs, l from at where l > 100",
                        "select l + 1, s, ts, d from at",
                        "select ts, (db + l) / 2 mid, s from at where l > 5000 limit 10, 4000",
                };
                assertBlockFillMatchesRowFill(sqls, "?qwp_max_batch_rows=1000", 0, -1);
                assertBlockFillMatchesRowFill(sqls, "", 0, -1);
            }
        });
    }

    @Test
    public void testWindowNegativeLimits() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = start(windowEnv())) {
                createWindowTable(serverMain);
                final String[] sqls = {
                        "select * from (" + WINDOW_SQL + ") limit -20000",
                        "select * from (" + WINDOW_SQL + ") limit -30000, -333",
                        "select * from (select * from (" + WINDOW_SQL + ") limit 20000) limit 100, 19000",
                        "select * from (" + WINDOW_SQL + ") limit 1",
                        "select * from (" + WINDOW_SQL + ") limit 0",
                };
                assertBlockFillMatchesRowFill(sqls, "?qwp_max_batch_rows=999", 0, -1);
            }
        });
    }

    @Test
    public void testWindowRunningCarry() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = start(windowEnv())) {
                createWindowTable(serverMain);
                // running windows: a task that continues a key gets the key's running values
                // combined into its rows in place before it is emitted, and the blocks expose
                // those rows. Without ORDER BY in OVER: with it, this plans as the cached window
                final String sql = "select sym, ts, l, i, " +
                        "sum(l) over (partition by sym rows between unbounded preceding and current row) s, " +
                        "count(*) over (partition by sym rows between unbounded preceding and current row) c, " +
                        "row_number() over (partition by sym) rn, " +
                        "max(i) over (partition by sym rows between unbounded preceding and current row) mx " +
                        "from q where sym in ('A', 'B', 'C1', 'C2', 'C3', 'C5', 'C8', 'D') order by sym";
                assertPlanContains(serverMain, sql, "Async Window", "keySplit: running carry");
                assertBlockFillMatchesRowFill(new String[]{sql, "select * from (" + sql + ") limit 333, 14444"},
                        "?qwp_max_batch_rows=555", 0, -1);
                assertBlockFillMatchesRowFill(new String[]{sql}, "", 0, -1);
            }
        });
    }

    @Test
    public void testWindowSingleSymbolColumn() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = start(windowEnv())) {
                createWindowTable(serverMain);
                // idx 50's case: one SYMBOL column, so its own loop, over the window's task
                // chains, whose stride is the chain's row width, not 4
                final String sql = "select sym, b, l, d, f, db, ts, " +
                        "avg((bsize * bid + asize * ask) / (bsize + asize)) over (partition by sym rows between 4 preceding and current row) mid " +
                        "from q where sym in ('A', 'B', 'C1', 'C2', 'C3', 'C5', 'C8', 'D') order by sym";
                assertPlanContains(serverMain, sql, "Async Window");
                // twice on one connection: the second run finds its symbols in the dictionary
                assertBlockFillMatchesRowFill(new String[]{sql, sql}, "?qwp_max_batch_rows=777", 0, -1);
                assertBlockFillMatchesRowFill(new String[]{sql}, "", 0, -1);
            }
        });
    }

    @Test
    public void testWindowSingleSymbolDictionaryBudget() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain serverMain = start(
                    "QDB_HTTP_SEND_BUFFER_SIZE", "163840",
                    PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS.getEnvVarName(), "128",
                    PropertyKey.CAIRO_SQL_PAGE_FRAME_MIN_ROWS.getEnvVarName(), "64",
                    PropertyKey.SHARED_QUERY_WORKER_COUNT.getEnvVarName(), "2",
                    PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_TASK_ROWS.getEnvVarName(), "500",
                    PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_MAX_KEY_ROWS.getEnvVarName(), "4000",
                    PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_MIN_ROWS.getEnvVarName(), "1000",
                    PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_ROUND_ROWS.getEnvVarName(), "3000")) {
                // 60 keys of 2,000 bytes, 25 rows each: a task holds many keys, so new dictionary
                // entries arrive inside the window's blocks, and the single-column loop stops on
                // the budget in the middle of a block, not only at a task's first row, which
                // hasNext() returns. The index and the IN list make the scan key-major, which
                // the Async Window needs
                serverMain.execute("create table kk (sym symbol capacity 1024 index type posting, v double, ts timestamp) " +
                        "timestamp(ts) partition by DAY BYPASS WAL");
                serverMain.execute("insert into kk select rpad('k' || (x % 60), 2000, '.'), x * 0.5, (x * 1000000)::timestamp " +
                        "from long_sequence(1500)");
                final StringBuilder keys = new StringBuilder();
                for (int k = 0; k < 60; k++) {
                    final StringBuilder key = new StringBuilder("k").append(k);
                    while (key.length() < 2000) {
                        key.append('.');
                    }
                    keys.append(k > 0 ? ",'" : "'").append(key).append('\'');
                }
                final String sql = "select sym, ts, avg(v) over (partition by sym rows between 2 preceding and current row) a " +
                        "from kk where sym in (" + keys + ") order by sym";
                assertPlanContains(serverMain, sql, "Async Window");
                final long splits = metrics.batchOverflowSplitCount();
                // twice on one connection: the second run adds no entries
                assertBlockFillMatchesRowFill(new String[]{sql, sql}, "", 0, -1);
                Assert.assertTrue("the dictionary budget must have split batches", metrics.batchOverflowSplitCount() > splits);
            }
        });
    }

    private static byte[] buildCancel(long requestId) {
        final byte[] p = new byte[9];
        p[0] = QwpEgressMsgKind.CANCEL;
        for (int s = 0; s < 8; s++) {
            p[1 + s] = (byte) (requestId >>> (8 * s));
        }
        return p;
    }

    private static byte[] buildQueryRequest(long requestId, String sql, long initialCredit) {
        final byte[] sqlBytes = sql.getBytes(StandardCharsets.UTF_8);
        final byte[] p = new byte[1 + 8 + 10 + sqlBytes.length + 10 + 1];
        int i = 0;
        p[i++] = QwpEgressMsgKind.QUERY_REQUEST;
        for (int s = 0; s < 8; s++) {
            p[i++] = (byte) (requestId >>> (8 * s));
        }
        i = QwpVarint.encode(p, i, sqlBytes.length);
        System.arraycopy(sqlBytes, 0, p, i, sqlBytes.length);
        i += sqlBytes.length;
        i = QwpVarint.encode(p, i, initialCredit);
        p[i++] = 0; // bind_count
        return Arrays.copyOf(p, i);
    }

    private static void createAllTypes(TestServerMain serverMain, int rows) {
        // the first rows have column tops for the columns added later
        serverMain.execute(ALL_TYPES_DDL.replace(", u uuid, l256 long256, g1 geohash(1c), g3 geohash(3c), g6 geohash(6c), " +
                "g12 geohash(12c), dc64 decimal(12,2), dc128 decimal(30,4), dc256 decimal(60,6), v varchar, st string, bn binary, " +
                "arr double[]", ""));
        serverMain.execute("insert into at select x % 3 = 0, (x % 120)::byte, (x * 7 % 30000)::short, rnd_char(), x::int, rnd_ipv4(), x, " +
                "(x * 86400000)::date, (x * 30000000)::timestamp, (x * 1000)::timestamp_ns, (x / 3.0)::float, x * 1.5, " +
                "'k' || (x % 50), 'z' || (x % 7) from long_sequence(1000)");
        serverMain.execute("alter table at add column u uuid, l256 long256, g1 geohash(1c), g3 geohash(3c), g6 geohash(6c), " +
                "g12 geohash(12c), dc64 decimal(12,2), dc128 decimal(30,4), dc256 decimal(60,6), v varchar, st string, bn binary, arr double[]");
        serverMain.execute("insert into at " + ALL_TYPES_SELECT.replace("(x * 30000000)::timestamp", "((x + 1000) * 30000000)::timestamp")
                + " from long_sequence(" + rows + ")");
    }

    private static void createIndexedTable(TestServerMain serverMain) {
        serverMain.execute("create table ix (ts timestamp, x long) timestamp(ts) partition by DAY BYPASS WAL");
        serverMain.execute("insert into ix select (x * 30000000)::timestamp, x from long_sequence(1000)");
        // the first rows are column tops of the columns added here
        serverMain.execute("alter table ix add column s symbol index type posting, b boolean, sh short, i int, " +
                "f float, d double, l long, s2 symbol, u uuid, v varchar");
        serverMain.execute("insert into ix select ((x + 1000) * 30000000)::timestamp, x, " +
                "case when x % 17 = 0 then null else 'k' || (x % 13) end, x % 3 = 0, (x % 999)::short, " +
                "case when x % 11 = 0 then null else x::int end, case when x % 4 = 0 then null else (x / 3.0)::float end, " +
                "case when x % 7 = 0 then null else x * 1.5 end, case when x % 5 = 0 then null else x * 1000003 end, " +
                "'z' || (x % 7), rnd_uuid4(), 'v' || x from long_sequence(15000)");
    }

    private static void createWindowTable(TestServerMain serverMain) {
        // keys A and B above max.key.rows, the C keys small, NULL keys too
        serverMain.execute("create table q (sym symbol index type posting, ex symbol, b boolean, by byte, sh short, ch char, " +
                "i int, ip ipv4, l long, d date, f float, db double, u uuid, g6 geohash(6c), dc64 decimal(12,2), " +
                "bid float, bsize int, ask float, asize int, ts timestamp) timestamp(ts) partition by DAY BYPASS WAL");
        serverMain.execute("insert into q select " +
                "case when x % 3 = 0 then 'A' when x % 5 = 0 then 'B' when x % 23 = 0 then null when x % 29 = 0 then 'D' else 'C' || (x % 9) end, " +
                "rnd_symbol('N', 'P', 'Q', null), x % 2 = 0, (x % 100)::byte, (x % 999)::short, rnd_char(), " +
                "case when x % 11 = 0 then null else x::int end, rnd_ipv4(), case when x % 13 = 0 then null else x end, " +
                "(x * 1000)::date, case when x % 4 = 0 then null else (x / 7.0)::float end, x * 0.5, rnd_uuid4(), rnd_geohash(30), " +
                "rnd_decimal(12,2,5), case when x % 7 = 0 then null else rnd_float() * 100 end, rnd_int(1, 100, 0), " +
                "rnd_float() * 100, rnd_int(1, 100, 0), (x * 3000000)::timestamp from long_sequence(60000)");
    }

    private static String describe(List<byte[]> frames, int index) {
        final byte[] f = frames.get(index);
        return "frame " + index + " of " + frames.size() + ", kind 0x" + Integer.toHexString(f[QwpConstants.HEADER_SIZE] & 0xFF) + ", " + f.length + " bytes";
    }

    private static String[] filterEnv() {
        return new String[]{
                PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS.getEnvVarName(), "1000",
                PropertyKey.CAIRO_SQL_PAGE_FRAME_MIN_ROWS.getEnvVarName(), "64",
                PropertyKey.SHARED_QUERY_WORKER_COUNT.getEnvVarName(), "2",
        };
    }

    private static String[] smallFrames() {
        return new String[]{
                PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS.getEnvVarName(), "128",
                PropertyKey.CAIRO_SQL_PAGE_FRAME_MIN_ROWS.getEnvVarName(), "64",
        };
    }

    private static String[] windowEnv() {
        return new String[]{
                PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS.getEnvVarName(), "128",
                PropertyKey.CAIRO_SQL_PAGE_FRAME_MIN_ROWS.getEnvVarName(), "64",
                PropertyKey.SHARED_QUERY_WORKER_COUNT.getEnvVarName(), "2",
                PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_TASK_ROWS.getEnvVarName(), "500",
                PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_MAX_KEY_ROWS.getEnvVarName(), "4000",
                PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_MIN_ROWS.getEnvVarName(), "1000",
                PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_ROUND_ROWS.getEnvVarName(), "3000",
        };
    }

    private static String[] withMetrics(String... env) {
        final String[] all = Arrays.copyOf(env, env.length + 2);
        all[env.length] = "QDB_METRICS_ENABLED";
        all[env.length + 1] = "true";
        return all;
    }

    /**
     * Runs the queries one after another on one connection, with the block fill off and then on,
     * and asserts both runs received the same frames.
     *
     * @param initialCredit      0 for an unbounded stream; else the query's initial credit,
     *                           topped up by each batch's size as it arrives
     * @param cancelAfterBatches -1 for none; else the batches after which the first query is
     *                           cancelled (with a 1-byte credit, the server parks there)
     */
    private void assertBlockFillMatchesRowFill(String[] sqls, String urlQuery, long initialCredit, int cancelAfterBatches) throws Exception {
        assertBlockFillMatchesRowFill(sqls, urlQuery, initialCredit, cancelAfterBatches, true);
    }

    /**
     * @param expectBlocks whether the block fill must run; false for results whose cursor offers
     *                     no blocks, which must then stream row by row
     */
    private void assertBlockFillMatchesRowFill(
            String[] sqls,
            String urlQuery,
            long initialCredit,
            int cancelAfterBatches,
            boolean expectBlocks
    ) throws Exception {
        QwpEgressUpgradeProcessor.DEBUG_DISABLE_BLOCK_FILL = true;
        final long blockRowsBefore = metrics.blockFillRowsCount();
        final List<byte[]> rowFill = exchange(sqls, urlQuery, initialCredit, cancelAfterBatches);
        Assert.assertEquals("the row fill must not use blocks", blockRowsBefore, metrics.blockFillRowsCount());
        QwpEgressUpgradeProcessor.DEBUG_DISABLE_BLOCK_FILL = false;
        final List<byte[]> blockFill = exchange(sqls, urlQuery, initialCredit, cancelAfterBatches);
        if (expectBlocks) {
            Assert.assertTrue("the block fill must have run", metrics.blockFillRowsCount() > blockRowsBefore);
        } else {
            Assert.assertEquals("no block may be offered", blockRowsBefore, metrics.blockFillRowsCount());
        }
        Assert.assertTrue(rowFill.size() > sqls.length);
        for (int i = 0, n = Math.min(rowFill.size(), blockFill.size()); i < n; i++) {
            if (!Arrays.equals(rowFill.get(i), blockFill.get(i))) {
                Assert.fail("block fill differs at " + describe(blockFill, i) + "; row fill: " + describe(rowFill, i)
                        + ", first differing byte " + Arrays.mismatch(rowFill.get(i), blockFill.get(i)));
            }
        }
        Assert.assertEquals("frame count", rowFill.size(), blockFill.size());
    }

    private void assertPlanContains(TestServerMain serverMain, String query, String... fragments) throws Exception {
        final StringSink sink = new StringSink();
        TestUtils.printSql(serverMain.getEngine(), serverMain.getSqlExecutionContext(), "explain " + query, sink);
        for (String fragment : fragments) {
            Assert.assertTrue("plan must contain [" + fragment + "]: " + sink, Chars.contains(sink, fragment));
        }
    }

    /**
     * @return every frame the server sent for the queries, SERVER_INFO excluded
     */
    private List<byte[]> exchange(String[] sqls, String urlQuery, long initialCredit, int cancelAfterBatches) throws Exception {
        final List<byte[]> frames = new ArrayList<>();
        // the same request ids in both runs: they are in every frame
        long requestIdSeq = 1;
        try (Socket socket = new Socket("127.0.0.1", HTTP_PORT)) {
            socket.setSoTimeout(120_000);
            QwpWireTestFixtures.performReadHandshake(socket, urlQuery);
            final OutputStream out = socket.getOutputStream();
            final InputStream in = new BufferedInputStream(socket.getInputStream(), 1 << 16);
            for (int q = 0; q < sqls.length; q++) {
                final long requestId = requestIdSeq++;
                final boolean cancel = q == 0 && cancelAfterBatches >= 0;
                out.write(QwpWireTestFixtures.maskedFrame(WebSocketOpcode.BINARY, buildQueryRequest(requestId, sqls[q], initialCredit)));
                out.flush();
                int batches = 0;
                while (true) {
                    final byte[] frame = QwpWireTestFixtures.readServerFrame(in);
                    final byte kind = frame[QwpConstants.HEADER_SIZE];
                    if (kind == QwpEgressMsgKind.SERVER_INFO) {
                        continue;
                    }
                    frames.add(frame);
                    if (kind == QwpEgressMsgKind.RESULT_END || kind == QwpEgressMsgKind.QUERY_ERROR) {
                        Assert.assertEquals("only a cancelled query may fail", cancel || errorExpected, kind == QwpEgressMsgKind.QUERY_ERROR);
                        break;
                    }
                    if (kind == QwpEgressMsgKind.RESULT_BATCH) {
                        batches++;
                        if (cancel && batches == cancelAfterBatches + 1) {
                            out.write(QwpWireTestFixtures.maskedFrame(WebSocketOpcode.BINARY, buildCancel(requestId)));
                        }
                        if (initialCredit > 0) {
                            out.write(QwpWireTestFixtures.maskedFrame(WebSocketOpcode.BINARY,
                                    QwpWireTestFixtures.buildCreditFrame(requestId, cancel ? 1 : frame.length)));
                        }
                        out.flush();
                    }
                }
            }
        }
        return frames;
    }

    private TestServerMain start(String... env) {
        final TestServerMain serverMain = startWithEnvVariables(withMetrics(env));
        metrics = serverMain.getEngine().getMetrics().qwpEgressMetrics();
        return serverMain;
    }
}
