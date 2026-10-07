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

import io.questdb.cairo.sql.RecordBlock;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cutlass.qwp.codec.QwpEgressColumnDef;
import io.questdb.cutlass.qwp.codec.QwpEgressConnSymbolDict;
import io.questdb.cutlass.qwp.codec.QwpResultBatchBuffer;
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

    private static List<byte[]> stream(
            String query,
            long s0,
            long s1,
            boolean blocks,
            long blockSeed,
            long[] blockRowsOut
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
                                blockRowsOut[0] += taken;
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
                    buffer.advanceDeltaStart();
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
