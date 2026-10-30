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
import io.questdb.cairo.sql.RecordBlock;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cutlass.qwp.codec.QwpEgressColumnDef;
import io.questdb.cutlass.qwp.codec.QwpEgressConnSymbolDict;
import io.questdb.cutlass.qwp.codec.QwpResultBatchBuffer;
import io.questdb.cutlass.qwp.codec.QwpSymbolKeyMap;
import io.questdb.cutlass.qwp.server.egress.QwpEgressProcessorState;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * The egress block fill ({@link QwpResultBatchBuffer#appendBlock}) over real cursors, without a
 * server: each query is streamed into batches twice, as the egress loop streams it, once row by
 * row and once peeking blocks before every row, and the delta section and table block of every
 * batch must be byte-identical. The batch caps and dictionary budgets are random and the same in
 * both runs; the block sizes asked for are random in the block run only. The {@code rnd_*}
 * functions are re-seeded before each run, so stateful projections draw the same values in both
 * runs only if they are evaluated in the same order. Every random choice derives from the seeds
 * {@link TestUtils#generateRandom} logs.
 */
public class QwpCursorBlockFillTest extends AbstractCairoTest {
    private static final int WIRE_BUFFER_SIZE = 1 << 23;

    @Test
    public void testBudgetStopOnLastRowOfRowByRowWindow() throws Exception {
        assertMemoryLeak(() -> {
            sqlExecutionContext.changePageFrameSizes(1, 100_000);
            // two SYMBOL columns, every key new: every row adds two dictionary entries, so every
            // row goes row by row, in windows of 64 rows after the first new key
            execute("create table nk as (select 'a' || x a, 'b' || x b, x n, timestamp_sequence(0, 1000000) ts " +
                    "from long_sequence(300)) timestamp(ts) partition by year");
            execute("alter table nk alter column a type symbol");
            execute("alter table nk alter column b type symbol");
            final String query = "select a, b, n from nk";
            final StringSink mismatches = new StringSink();
            int windowEnds = 0;
            try (RecordCursorFactory factory = select(query)) {
                final ObjList<QwpEgressColumnDef> defs = columnDefs(factory.getMetadata());
                for (int budget = 3; budget < 4000; budget++) {
                    final int rowFill;
                    try (
                            RecordCursor cursor = factory.getCursor(sqlExecutionContext);
                            QwpResultBatchBuffer buffer = new QwpResultBatchBuffer();
                            QwpEgressConnSymbolDict dict = new QwpEgressConnSymbolDict()
                    ) {
                        buffer.beginBatch(defs, cursor, dict);
                        int rows = 0;
                        while (cursor.hasNext()) {
                            buffer.appendRow(cursor.getRecord());
                            rows++;
                            if (buffer.currentBatchDeltaWireBytes() > budget) {
                                break;
                            }
                        }
                        rowFill = rows;
                    }
                    final int blockFill;
                    try (
                            RecordCursor cursor = factory.getCursor(sqlExecutionContext);
                            QwpResultBatchBuffer buffer = new QwpResultBatchBuffer();
                            QwpEgressConnSymbolDict dict = new QwpEgressConnSymbolDict()
                    ) {
                        buffer.beginBatch(defs, cursor, dict);
                        // a plain scan offers no block before its first row
                        Assert.assertTrue(cursor.hasNext());
                        buffer.appendRow(cursor.getRecord());
                        if (buffer.currentBatchDeltaWireBytes() > budget) {
                            blockFill = 1;
                        } else {
                            final RecordBlock block = cursor.peekRecordBlock(1000);
                            Assert.assertNotNull(block);
                            blockFill = 1 + buffer.appendBlock(block, budget);
                        }
                    }
                    // the block's rows 63 and 127: the last of a 64-row row-by-row window
                    if (rowFill == 65 || rowFill == 129) {
                        windowEnds++;
                    }
                    if (blockFill != rowFill) {
                        mismatches.put("budget=").put(budget).put(" rowFill=").put(rowFill).put(" blockFill=").put(blockFill).put('\n');
                    }
                }
            }
            Assert.assertTrue("no budget stopped on a window's last row", windowEnds > 0);
            TestUtils.assertEquals("", mismatches);
        });
    }

    @Test
    public void testIndexJoinAndLatestOnShapes() throws Exception {
        assertMemoryLeak(() -> {
            final Rnd driver = TestUtils.generateRandom(LOG);
            sqlExecutionContext.changePageFrameSizes(1, 64);
            sqlExecutionContext.setParallelHashJoinProbeEnabled(true);
            // an index scan keeps the small page frames too, as before cairo.sql.index.scan.page.frame.max.rows
            setProperty(PropertyKey.CAIRO_SQL_INDEX_SCAN_PAGE_FRAME_MAX_ROWS, 1);
            createTable(driver);
            for (String indexType : new String[]{"posting", "bitmap"}) {
                execute("drop table if exists ix");
                execute("create table ix (ts timestamp, x long) timestamp(ts) partition by DAY");
                execute("insert into ix select (x * 900000000)::timestamp, x from long_sequence(150)");
                execute("alter table ix add column s symbol index type " + indexType + ", i int, d double, s2 symbol");
                execute("insert into ix select ((x + 150) * 900000000)::timestamp, x, " +
                        "case when x % 17 = 0 then null else 'k' || (x % 13) end, " +
                        "case when x % 11 = 0 then null else x::int end, x * 1.5, rnd_symbol(400, 3, 8, 3) from long_sequence(5000)");
                final String[] queries = {
                        "select t.ts, m.msym, t.s2, t.l, m.mn from t join (select s msym, min(l) mn from t) m on t.s = m.msym where l = mn",
                        "select t.ts, t.s, t.s2, t.l from t join (select s msym, s2 ms2, min(l) mn from t) m on t.s = m.msym and t.s2 = m.ms2 and t.l = m.mn",
                        "select * from ix where s = 'k7'",
                        "select * from ix where s = 'k7' order by ts desc",
                        "select s2, i * 2, d + i, s from ix where s in ('k1', 'k7', 'k11')",
                        "select * from ix where s = 'k7' limit 5, 300",
                        "select * from ix where s = 'k7' and i > 100",
                        "select * from ix where s = 'k7' latest on ts partition by s",
                        "select s2, s, d from ix where s = 'k7' latest on ts partition by s",
                        "select * from ix where s = 'k99' latest on ts partition by s",
                        "select * from ix where s in ('k1', 'k7') latest on ts partition by s",
                        "select * from ix latest on ts partition by s",
                        "select * from ix latest on ts partition by s2",
                };
                for (String query : queries) {
                    assertBytesMatch(driver, query, 8, !query.contains("latest on") && !query.contains("desc"));
                }
            }
        });
    }

    @Test
    public void testManySymbolColumnsRandomBudgets() throws Exception {
        assertMemoryLeak(() -> {
            final Rnd driver = TestUtils.generateRandom(LOG);
            for (int round = 0; round < 3; round++) {
                sqlExecutionContext.changePageFrameSizes(1, 32 + driver.nextInt(2000));
                sqlExecutionContext.setRandom(new Rnd(driver.nextLong(), driver.nextLong()));
                // 2 to 7 SYMBOL columns, from a few keys to every key new, short to long values,
                // with or without NULLs, and new keys arriving late in some of them, so that the
                // dictionary budget stops at varying offsets of the row-by-row windows
                final int symbolCount = 2 + driver.nextInt(6);
                final int rows = 3_000 + driver.nextInt(20_000);
                final StringSink ddl = new StringSink();
                final StringSink projection = new StringSink();
                ddl.put("create table m as (select ");
                for (int c = 0; c < symbolCount; c++) {
                    final int length = 1 + driver.nextInt(40);
                    final int nullRate = driver.nextInt(3) == 0 ? 0 : 1 + driver.nextInt(20);
                    switch (driver.nextInt(4)) {
                        case 0 ->
                                ddl.put("rnd_symbol(").put(2 + driver.nextInt(20)).put(", 1, ").put(length).put(", ").put(nullRate).put(')');
                        case 1 ->
                                ddl.put("rnd_symbol(").put(100 + driver.nextInt(5000)).put(", 1, ").put(length).put(", ").put(nullRate).put(')');
                        case 2 -> ddl.put("rpad('n' || x, ").put(length + 5).put(", '.')::symbol");
                        default ->
                                ddl.put("case when x % ").put(50 + driver.nextInt(1000)).put(" = 0 then 'late' || x else rnd_symbol('p', 'q', 'r') end::symbol");
                    }
                    ddl.put(" s").put(c).put(", ");
                    projection.put(c > 0 ? ", " : "").put('s').put(symbolCount - 1 - c);
                }
                ddl.put("x l, x * 0.5 d, timestamp_sequence(0, 1000000) ts from long_sequence(").put(rows).put(")) timestamp(ts) partition by hour");
                execute("drop table if exists m");
                execute(ddl.toString());
                if (driver.nextBoolean()) {
                    execute("alter table m alter column s0 add index");
                }
                final String[] queries = {
                        "select * from m",
                        "select * from m where l % 7 <> 3",
                        "select " + projection + ", l from m where l > 10",
                        "select s1, s1 x1, s0, s0 x0, l, d * 2 from m where d > 3",
                        "select s1, l + 1, s0 from m where l % 2 = 0 limit 3, " + (rows - 100),
                        "select * from m where s0 is not null and l % 5 <> 0",
                        // over the index, when s0 has one, else a filter; it may select no row
                        "select * from m where s0 in ('p', 'q', 'n7')",
                };
                for (String query : queries) {
                    assertBytesMatch(driver, query, 6, !query.contains(" in ("));
                }
            }
        });
    }

    @Test
    public void testSymbolDenseKeyMapMatchesHashMap() throws Exception {
        assertMemoryLeak(() -> {
            final Rnd driver = TestUtils.generateRandom(LOG);
            for (int round = 0; round < 3; round++) {
                sqlExecutionContext.changePageFrameSizes(1, 32 + driver.nextInt(2000));
                sqlExecutionContext.setRandom(new Rnd(driver.nextLong(), driver.nextLong()));
                final int rows = 3_000 + driver.nextInt(20_000);
                final int keys = 50 + driver.nextInt(3000);
                // s0: keys 0..keys-1 in order, then scattered, so that a large key arrives before
                // smaller new ones; s1: a high-cardinality column in random order; s2: a few keys
                execute("drop table if exists m");
                execute("create table m as (select " +
                        "case when x <= " + keys + " then 'k' || x else 'k' || ((x * 104729) % " + keys + " + 1) end::symbol s0, " +
                        "rnd_symbol(" + (100 + driver.nextInt(5000)) + ", 1, " + (1 + driver.nextInt(30)) + ", " + driver.nextInt(10) + ") s1, " +
                        "rnd_symbol('p', 'q', null) s2, x l, timestamp_sequence(0, 1000000) ts from long_sequence(" + rows + ")) timestamp(ts) partition by hour");
                final String[] queries = {
                        "select * from m",
                        "select s0, l from m where l > " + keys,
                        "select s1, l from m",
                        "select s2, s0, s1 from m where l % 3 <> 1",
                        "select s0, s1, l from m order by l desc",
                };
                for (String query : queries) {
                    for (int it = 0; it < 4; it++) {
                        final long a = driver.nextLong();
                        final long b = driver.nextLong();
                        final long blockSeed = driver.nextLong();
                        // the hash map for every key, row by row
                        final List<byte[]> reference = stream(query, a, b, false, blockSeed, null, 0);
                        final int cap = switch (driver.nextInt(4)) {
                            case 0 -> 1 + driver.nextInt(64);
                            case 1 -> 1 + driver.nextInt(keys + 10);
                            case 2 -> 0;
                            default -> QwpResultBatchBuffer.DEFAULT_SYMBOL_DENSE_MAX_KEYS;
                        };
                        for (boolean blocks : new boolean[]{false, true}) {
                            final List<byte[]> batches = stream(query, a, b, blocks, blockSeed, null, cap);
                            for (int i = 0, n = Math.min(reference.size(), batches.size()); i < n; i++) {
                                if (!Arrays.equals(reference.get(i), batches.get(i))) {
                                    Assert.fail(query + ": cap " + cap + ", blocks " + blocks + ": batch " + i + " of " + reference.size()
                                            + " differs at byte " + Arrays.mismatch(reference.get(i), batches.get(i)));
                                }
                            }
                            Assert.assertEquals(query + ": cap " + cap + ", blocks " + blocks + ": batch count", reference.size(), batches.size());
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testSymbolDenseKeyMapReleasedAcrossQueriesAndConnections() throws Exception {
        assertMemoryLeak(() -> {
            // 270 000 distinct symbols: 'k' || x has the key x - 1
            execute("create table big as (select ('k' || x)::symbol s, x l, timestamp_sequence(0, 1000) ts " +
                    "from long_sequence(270000)) timestamp(ts) partition by day");
            for (boolean blocks : new boolean[]{false, true}) {
                try (QwpEgressProcessorState state = new QwpEgressProcessorState(configuration)) {
                    final QwpResultBatchBuffer buffer = state.getBatchBuffer();
                    // one row with a rare large key grows the column's array to the cap: 1 MiB
                    fillOneQuery(buffer, state.getConnSymbolDict(), "select s, l from big where l = 262001", blocks);
                    Assert.assertEquals(QwpResultBatchBuffer.DEFAULT_SYMBOL_DENSE_MAX_KEYS, buffer.getSymbolDenseCapacity(0));
                    // the next query on the connection does not keep it
                    buffer.resetForNewQuery();
                    Assert.assertEquals(0, buffer.getSymbolDenseCapacity(0));
                    // an empty query in between keeps nothing either
                    fillOneQuery(buffer, state.getConnSymbolDict(), "select s, l from big where l < 0", blocks);
                    buffer.resetForNewQuery();
                    Assert.assertEquals(0, buffer.getSymbolDenseCapacity(0));
                    // small keys: a small array, kept from one query to the next
                    fillOneQuery(buffer, state.getConnSymbolDict(), "select s, l from big where l <= 100", blocks);
                    Assert.assertEquals(128, buffer.getSymbolDenseCapacity(0));
                    buffer.resetForNewQuery();
                    Assert.assertEquals(128, buffer.getSymbolDenseCapacity(0));
                    Assert.assertTrue(buffer.getSymbolDenseCapacity(0) <= QwpSymbolKeyMap.RETAINED_DENSE_CAPACITY);
                    // the large key again, then the connection ends: the pooled state keeps no array
                    fillOneQuery(buffer, state.getConnSymbolDict(), "select s, l from big where l = 262001", blocks);
                    Assert.assertEquals(QwpResultBatchBuffer.DEFAULT_SYMBOL_DENSE_MAX_KEYS, buffer.getSymbolDenseCapacity(0));
                    state.clear();
                    Assert.assertEquals(0, buffer.getSymbolDenseCapacity(0));
                    // the next connection on the pooled state streams as before
                    fillOneQuery(buffer, state.getConnSymbolDict(), "select s, l from big where l in (5, 262001)", blocks);
                    Assert.assertEquals(QwpResultBatchBuffer.DEFAULT_SYMBOL_DENSE_MAX_KEYS, buffer.getSymbolDenseCapacity(0));
                }
            }
        });
    }

    @Test
    public void testAsyncFilterGatheredColumns() throws Exception {
        // a block consumer from the cursor's start: the filtering threads gather the selected rows'
        // fixed-size columns (SYMBOL keys too), which the blocks offer as dense columns; the wire
        // must be the row fill's, byte for byte, over NULLs, column tops and both filter kinds
        assertMemoryLeak(() -> {
            final Rnd driver = TestUtils.generateRandom(LOG);
            sqlExecutionContext.changePageFrameSizes(1, 64);
            sqlExecutionContext.setRandom(new Rnd(driver.nextLong(), driver.nextLong()));
            execute("create table g (b boolean, by byte, sh short, ch char, i int, ip ipv4, l long, dt date, " +
                    "ts timestamp, f float, d double, s symbol, s2 symbol) timestamp(ts) partition by DAY");
            execute("insert into g select x % 3 = 0, (x % 120)::byte, (x * 7 % 30000)::short, rnd_char(), " +
                    "case when x % 11 = 0 then null else x::int end, case when x % 13 = 0 then null else rnd_ipv4() end, " +
                    "case when x % 5 = 0 then null else x * 1000003 end, case when x % 9 = 0 then null else (x * 86400000)::date end, " +
                    "(x * 90000000)::timestamp, case when x % 4 = 0 then null else (x / 3.0)::float end, " +
                    "case when x % 7 = 0 then null else x * 1.5 end, case when x % 17 = 0 then null else 'k' || (x % 13) end, " +
                    "rnd_symbol(300, 3, 9, 5) from long_sequence(400)");
            // the first rows have column tops for these
            execute("alter table g add column tn timestamp_ns, s3 symbol, i2 int, v varchar");
            execute("insert into g select x % 3 = 0, (x % 120)::byte, (x * 7 % 30000)::short, rnd_char(), " +
                    "case when x % 11 = 0 then null else x::int end, case when x % 13 = 0 then null else rnd_ipv4() end, " +
                    "case when x % 5 = 0 then null else x * 1000003 end, case when x % 9 = 0 then null else (x * 86400000)::date end, " +
                    "((x + 400) * 90000000)::timestamp, case when x % 4 = 0 then null else (x / 3.0)::float end, " +
                    "case when x % 7 = 0 then null else x * 1.5 end, case when x % 17 = 0 then null else 'k' || (x % 13) end, " +
                    "rnd_symbol(300, 3, 9, 5), case when x % 6 = 0 then null else (x * 1000)::timestamp_ns end, " +
                    "rnd_symbol(40, 2, 5, 3), case when x % 8 = 0 then null else x::int end, 'v' || (x % 31) from long_sequence(6000)");
            final String[] queries = {
                    // JIT filters
                    "select * from g where s = 'k7'",
                    "select s, ts, bid, s2, i from (select s, ts, d bid, s2, i from g where s = 'k7')",
                    "select * from g where l % 7 = 0 and s2 <> 'z1'",
                    "select s3, i2, tn, s, b from g where i2 > 10",
                    "select * from g where s = 'k7' limit 3, 700",
                    // a Java filter
                    "select * from g where v like '%3%'",
                    "select s, d * 2, s2, l + i from g where s in ('k1', 'k7')",
            };
            for (String query : queries) {
                assertBytesMatch(driver, query, 6, true);
            }
        });
    }

    @Test
    public void testMaterializedSortOffersSortedRows() throws Exception {
        // a materialized encoded sort of fixed-size, non-SYMBOL columns offers its sorted rows as
        // blocks gathered from its chain: the wire must be the row fill's, byte for byte
        assertMemoryLeak(() -> {
            final Rnd driver = TestUtils.generateRandom(LOG);
            sqlExecutionContext.changePageFrameSizes(1, 64);
            createTable(driver);
            execute("create table st as (select x % 3 = 0 b, (x % 120)::byte by, (x * 7 % 30000)::short sh, rnd_char() ch, " +
                    "case when x % 11 = 0 then null else x::int end i, case when x % 5 = 0 then null else x * 1000003 end l, " +
                    "case when x % 9 = 0 then null else (x * 86400000)::date end dt, (x * 90000000)::timestamp ts, " +
                    "case when x % 6 = 0 then null else (x * 1000)::timestamp_ns end tn, " +
                    "case when x % 4 = 0 then null else (x / 3.0)::float end f, case when x % 7 = 0 then null else rnd_double() end d, " +
                    "rnd_symbol(30, 2, 5, 3) s from long_sequence(7000)) timestamp(ts) partition by DAY");
            final String[] queries = {
                    "select ts, d from (select ts, (d + f) / 2 d from st where s <> 'zz' union all select ts, d from st where 1 = 0) order by d desc",
                    "select b, by, sh, ch, i, l, dt, ts, tn, f, d from (select b, by, sh, ch, i, l, dt, ts, tn, f, d from st union all select b, by, sh, ch, i, l, dt, ts, tn, f, d from st where 1 = 0) order by f, l desc",
                    "select l, d from (select l, d from t union all select l, d from t where 1 = 0) order by d desc limit 5000",
            };
            for (String query : queries) {
                assertBytesMatch(driver, query, 6, true);
            }
        });
    }

    @Test
    public void testStatefulProjectionsKeepRowOrder() throws Exception {
        assertMemoryLeak(() -> {
            final Rnd driver = TestUtils.generateRandom(LOG);
            sqlExecutionContext.changePageFrameSizes(1, 64);
            createTable(driver);
            final String[] queries = {
                    // stateful functions on the record path, between pass-through SYMBOLs
                    "select rnd_int() a, s, rnd_double() b, l + 1 c, s2, rnd_long() d from t where l > 5",
                    "select rnd_int() a, rnd_int() b, l from t",
                    "select rnd_int(1, 100, 0) r, r + 1 r1, r * 2 r2, s from t where l > 5",
                    "select s, rnd_str(3, 5, 0) rs, s2, rnd_varchar(3, 5, 0) rv from t where l % 3 <> 0",
            };
            for (String query : queries) {
                assertBytesMatch(driver, query, 12, true);
            }
        });
    }

    private static ObjList<QwpEgressColumnDef> columnDefs(RecordMetadata metadata) {
        final ObjList<QwpEgressColumnDef> defs = new ObjList<>();
        for (int c = 0, n = metadata.getColumnCount(); c < n; c++) {
            final QwpEgressColumnDef def = new QwpEgressColumnDef();
            def.of(metadata.getColumnName(c), metadata.getColumnType(c));
            defs.add(def);
        }
        return defs;
    }

    // streams one query's rows into the buffer, a single batch, as the egress loop does
    private static void fillOneQuery(QwpResultBatchBuffer buffer, QwpEgressConnSymbolDict dict, String query, boolean blocks) throws Exception {
        try (RecordCursorFactory factory = select(query)) {
            final ObjList<QwpEgressColumnDef> defs = columnDefs(factory.getMetadata());
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                buffer.beginBatch(defs, cursor, dict);
                while (true) {
                    final RecordBlock block = blocks && cursor.supportsRecordBlocks() ? cursor.peekRecordBlock(1000) : null;
                    if (block != null) {
                        cursor.skipRecordBlock(buffer.appendBlock(block, Integer.MAX_VALUE));
                    } else if (cursor.hasNext()) {
                        buffer.appendRow(cursor.getRecord());
                    } else {
                        break;
                    }
                }
                buffer.reset();
            }
        }
    }

    private static List<byte[]> stream(
            String query,
            long s0,
            long s1,
            boolean blocks,
            long blockSeed,
            long[] blockRowsOut
    ) throws Exception {
        return stream(query, s0, s1, blocks, blockSeed, blockRowsOut, QwpResultBatchBuffer.DEFAULT_SYMBOL_DENSE_MAX_KEYS);
    }

    private static List<byte[]> stream(
            String query,
            long s0,
            long s1,
            boolean blocks,
            long blockSeed,
            long[] blockRowsOut,
            int symbolDenseMaxKeys
    ) throws Exception {
        final List<byte[]> batches = new ArrayList<>();
        // the batch caps and budgets come from the same seed in both runs
        final Rnd shape = new Rnd(s0, s1);
        // the block sizes asked for, in the block run only: they must not change the bytes
        final Rnd blockRnd = new Rnd(blockSeed, ~blockSeed);
        final long wire = Unsafe.malloc(WIRE_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
        try (
                RecordCursorFactory factory = select(query);
                QwpResultBatchBuffer buffer = new QwpResultBatchBuffer();
                QwpEgressConnSymbolDict dict = new QwpEgressConnSymbolDict()
        ) {
            buffer.setSymbolDenseMaxKeys(symbolDenseMaxKeys);
            final ObjList<QwpEgressColumnDef> defs = columnDefs(factory.getMetadata());
            sqlExecutionContext.setRandom(new Rnd(s0 ^ 0x5DEECE66DL, s1 ^ 0xB));
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                final boolean supportsBlocks = cursor.supportsRecordBlocks();
                boolean hasMore = true;
                boolean first = true;
                while (hasMore) {
                    buffer.beginBatch(defs, cursor, dict);
                    int rowsToAdd = 1 + shape.nextInt(3000);
                    final int budget = switch (shape.nextInt(4)) {
                        case 0 -> 2 + shape.nextInt(200);
                        case 1 -> 500 + shape.nextInt(4000);
                        default -> Integer.MAX_VALUE;
                    };
                    if (blocks && supportsBlocks) {
                        // the egress loop: a peek before every row
                        while (rowsToAdd > 0) {
                            final int ask = blockRnd.nextBoolean() ? rowsToAdd : Math.min(rowsToAdd, 1 + blockRnd.nextInt(50));
                            final RecordBlock block = cursor.peekRecordBlock(ask);
                            if (block != null) {
                                Assert.assertTrue(block.getRowCount() > 0 && block.getRowCount() <= ask);
                                final int taken = buffer.appendBlock(block, budget);
                                cursor.skipRecordBlock(taken);
                                rowsToAdd -= taken;
                                if (blockRowsOut != null) {
                                    blockRowsOut[0] += taken;
                                }
                            } else if (hasMore = cursor.hasNext()) {
                                buffer.appendRow(cursor.getRecord());
                                rowsToAdd--;
                            } else {
                                break;
                            }
                            if (buffer.currentBatchDeltaWireBytes() > budget) {
                                break;
                            }
                        }
                    } else {
                        while (rowsToAdd > 0 && (hasMore = cursor.hasNext())) {
                            buffer.appendRow(cursor.getRecord());
                            rowsToAdd--;
                            if (buffer.currentBatchDeltaWireBytes() > budget) {
                                break;
                            }
                        }
                    }
                    final int rows = buffer.getRowCount();
                    if (rows == 0 && !first) {
                        break;
                    }
                    final int deltaBytes = buffer.emitDeltaSection(wire, wire + WIRE_BUFFER_SIZE);
                    final int tableBytes = buffer.emitTableBlockPrefix(wire + deltaBytes, wire + WIRE_BUFFER_SIZE, rows, first);
                    final byte[] bytes = new byte[deltaBytes + tableBytes + 4];
                    for (int i = 0, n = deltaBytes + tableBytes; i < n; i++) {
                        bytes[i] = Unsafe.getByte(wire + i);
                    }
                    // and the batch's row count
                    bytes[deltaBytes + tableBytes] = (byte) rows;
                    bytes[deltaBytes + tableBytes + 1] = (byte) (rows >>> 8);
                    bytes[deltaBytes + tableBytes + 2] = (byte) (rows >>> 16);
                    batches.add(bytes);
                    buffer.advanceStartRow(rows);
                    first = false;
                }
            }
        } finally {
            Unsafe.free(wire, WIRE_BUFFER_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
        return batches;
    }

    /**
     * @param expectBlocks whether the block run must have taken rows from blocks
     */
    private void assertBytesMatch(Rnd driver, String query, int iterations, boolean expectBlocks) throws Exception {
        long blockRows = 0;
        for (int it = 0; it < iterations; it++) {
            final long s0 = driver.nextLong();
            final long s1 = driver.nextLong();
            final long blockSeed = driver.nextLong();
            final List<byte[]> rowFill = stream(query, s0, s1, false, blockSeed, null);
            final long[] blockRowsOut = new long[1];
            final List<byte[]> blockFill = stream(query, s0, s1, true, blockSeed, blockRowsOut);
            blockRows += blockRowsOut[0];
            for (int i = 0, n = Math.min(rowFill.size(), blockFill.size()); i < n; i++) {
                if (!Arrays.equals(rowFill.get(i), blockFill.get(i))) {
                    Assert.fail(query + ": batch " + i + " of " + rowFill.size() + " differs at byte " + Arrays.mismatch(rowFill.get(i), blockFill.get(i))
                            + " (" + rowFill.get(i).length + " vs " + blockFill.get(i).length + " bytes), iteration " + it);
                }
            }
            Assert.assertEquals(query + ": batch count, iteration " + it, rowFill.size(), blockFill.size());
        }
        if (expectBlocks) {
            Assert.assertTrue(query + ": the block fill must have run", blockRows > 0);
        }
    }

    private void createTable(Rnd driver) throws Exception {
        sqlExecutionContext.setRandom(new Rnd(driver.nextLong(), driver.nextLong()));
        execute("create table t as (select rnd_symbol(300, 3, 9, 5) s, rnd_symbol(40, 2, 5, 3) s2, x l, x * 1.25 d, " +
                "timestamp_sequence(0, 10000000) ts from long_sequence(20000)) timestamp(ts) partition by day");
    }
}
