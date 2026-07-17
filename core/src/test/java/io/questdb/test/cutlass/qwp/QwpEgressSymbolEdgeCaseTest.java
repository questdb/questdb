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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;
import io.questdb.client.cutlass.qwp.client.QwpQueryClient;
import io.questdb.client.std.str.DirectUtf8Sequence;
import io.questdb.cutlass.qwp.codec.QwpEgressColumnDef;
import io.questdb.cutlass.qwp.codec.QwpEgressConnSymbolDict;
import io.questdb.cutlass.qwp.codec.QwpResultBatchBuffer;
import io.questdb.cutlass.qwp.protocol.QwpConstants;
import io.questdb.cutlass.qwp.server.egress.QwpEgressUpgradeProcessor;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.std.ObjList;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Edge-case coverage for the SYMBOL wire path on egress. Complements the basic
 * {@code testSymbol} in {@code QwpEgressTypesExhaustiveTest} and the delta-dict
 * behaviour in {@code QwpEgressDeltaSymbolDictTest}. Focuses on cases the
 * native-heap + delta-dict refactors can corrupt if not handled precisely:
 * <ul>
 *   <li>multi-byte UTF-8 round-trip (server's {@code encodeUtf8} path and
 *       client's memcpy into the connection dict heap);</li>
 *   <li>4-byte UTF-8 via surrogate pairs (emoji / supplementary plane);</li>
 *   <li>long symbol values (dict heap growth);</li>
 *   <li>all-NULL / single-value columns (bitmap edge cases + tiny dict);</li>
 *   <li>native-key and dynamic-text symbol-table paths;</li>
 *   <li>multi-batch streaming: schema reference + delta section coexistence;</li>
 *   <li>fresh connection gets a fresh dict (server state isolation).</li>
 * </ul>
 */
public class QwpEgressSymbolEdgeCaseTest extends AbstractQwpBootstrapTest {

    /**
     * Row count sized off the live server cap so the test reliably spans
     * multiple batches. With the current cap of 16_384 this produces three
     * batches; a future cap bump still leaves at least two. Kept as a multiple
     * of 4 so the per-bucket even-distribution assertion doesn't need reshuffling.
     */
    private static final int MULTI_BATCH_ROWS = 2 * QwpEgressUpgradeProcessor.MAX_ROWS_PER_BATCH + 4;

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testAllRowsNullColumn() throws Exception {
        // Column is SYMBOL, every row NULL. Dict stays empty (size=0), null
        // bitmap has every bit set, zero non-null varint ids are emitted.
        // Catches off-by-one errors in the "SYMBOL column with empty dict"
        // path and any accidental attempt to index into an empty dict.
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startFragmented()) {
                serverMain.execute("CREATE TABLE allnull(s SYMBOL, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("INSERT INTO allnull VALUES (NULL, 1::TIMESTAMP), (NULL, 2::TIMESTAMP), "
                        + "(NULL, 3::TIMESTAMP), (NULL, 4::TIMESTAMP), (NULL, 5::TIMESTAMP)");
                serverMain.awaitTable("allnull");

                final int[] nullCount = {0};
                final int[] rowCount = {0};
                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    client.execute("SELECT s FROM allnull", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            for (int r = 0; r < batch.getRowCount(); r++) {
                                if (batch.isNull(0, r)) nullCount[0]++;
                                else Assert.fail("expected NULL at row " + r);
                                rowCount[0]++;
                            }
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("egress error: " + message);
                        }
                    });
                }
                Assert.assertEquals(5, rowCount[0]);
                Assert.assertEquals(5, nullCount[0]);
            }
        });
    }

    @Test
    public void testDynamicSymbolUsesTextWithoutMaterializingKeys() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            ObjList<QwpEgressColumnDef> cols = new ObjList<>();
            QwpEgressColumnDef def = new QwpEgressColumnDef();
            def.of("s", ColumnType.SYMBOL);
            cols.add(def);

            final SymbolTable dynamicSymbolTable = new SymbolTable() {
                @Override
                public CharSequence valueBOf(int key) {
                    throw new AssertionError("dynamic symbol table key path must not be used");
                }

                @Override
                public CharSequence valueOf(int key) {
                    throw new AssertionError("dynamic symbol table key path must not be used");
                }
            };
            final SymbolTableSource symbolTableSource = new SymbolTableSource() {
                @Override
                public SymbolTable getSymbolTable(int columnIndex) {
                    return dynamicSymbolTable;
                }

                @Override
                public SymbolTable newSymbolTable(int columnIndex) {
                    return dynamicSymbolTable;
                }
            };
            final Record record = new Record() {
                @Override
                public int getInt(int col) {
                    throw new AssertionError("dynamic symbol must be read through getSymA");
                }

                @Override
                public CharSequence getSymA(int col) {
                    return "dynamic_value";
                }
            };

            try (QwpResultBatchBuffer batch = new QwpResultBatchBuffer();
                 QwpEgressConnSymbolDict dict = new QwpEgressConnSymbolDict()) {
                batch.beginBatch(cols, symbolTableSource, dict);
                batch.appendRow(record);
                batch.appendRow(record);
                Assert.assertEquals(2, batch.getRowCount());
                Assert.assertEquals("the connection dictionary still deduplicates text values", 1, dict.size());
            }
        });
    }

    @Test
    public void testEmoji4ByteUtf8() throws Exception {
        // 4-byte UTF-8 sequences (supplementary plane / surrogate pairs). The
        // server's encodeUtf8 helper emits them as 0xF0-prefixed 4-byte runs;
        // the client memcpys the same bytes into its connection dict heap.
        // Catches bugs in the surrogate-pair branch of either encoder.
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startFragmented()) {
                serverMain.execute("CREATE TABLE emojis(s SYMBOL, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                // Rocket: U+1F680 (0xF0 0x9F 0x9A 0x80), thumbs up: U+1F44D.
                serverMain.execute("""
                        INSERT INTO emojis VALUES
                            ('\uD83D\uDE80', 1::TIMESTAMP),
                            ('\uD83D\uDC4D', 2::TIMESTAMP),
                            ('\uD83D\uDE80', 3::TIMESTAMP),
                            ('plain',        4::TIMESTAMP),
                            ('\uD83D\uDC4D', 5::TIMESTAMP)
                        """);
                serverMain.awaitTable("emojis");

                // Two unique emojis (4 bytes each) + 'plain' (5 bytes) + 2
                // repeats of the emojis = 3 unique, 5 total rows.
                final String[] expected = {
                        "\uD83D\uDE80", "\uD83D\uDC4D", "\uD83D\uDE80", "plain", "\uD83D\uDC4D"
                };
                final int[] rowIdx = {0};
                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    client.execute("SELECT s FROM emojis", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            for (int r = 0; r < batch.getRowCount(); r++) {
                                DirectUtf8Sequence v = batch.getStrA(0, r);
                                Assert.assertNotNull("row " + rowIdx[0], v);
                                // Compare via Java String (forced allocation here is
                                // fine -- only 5 rows total).
                                Assert.assertEquals(
                                        "row " + rowIdx[0],
                                        expected[rowIdx[0]],
                                        v.toString()
                                );
                                rowIdx[0]++;
                            }
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("egress error: " + message);
                        }
                    });
                }
                Assert.assertEquals(5, rowIdx[0]);
            }
        });
    }

    @Test
    public void testFreshConnectionGetsFreshDict() throws Exception {
        // Client #1 connects, queries, populates the server's connection dict,
        // disconnects. Client #2 opens a fresh connection and runs the same
        // query: the server must ship a fresh dict (deltaStart=0) because the
        // previous client's state was tied to the previous socket. Catches
        // leaks of connection dict state across connections.
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startFragmented()) {
                serverMain.execute("CREATE TABLE shared(s SYMBOL, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("INSERT INTO shared VALUES ('red', 1::TIMESTAMP), ('green', 2::TIMESTAMP), "
                        + "('blue', 3::TIMESTAMP), ('red', 4::TIMESTAMP)");
                serverMain.awaitTable("shared");

                final long[] c1Bytes = {0};
                final long[] c2Bytes = {0};
                final int[] c1Rows = {0};
                final int[] c2Rows = {0};

                try (QwpQueryClient c1 = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    c1.connect();
                    c1.execute("SELECT s FROM shared", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            c1Bytes[0] += batch.payloadLimit() - batch.payloadAddr();
                            c1Rows[0] += batch.getRowCount();
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("egress error on c1: " + message);
                        }
                    });
                }

                try (QwpQueryClient c2 = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    c2.connect();
                    c2.execute("SELECT s FROM shared", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            c2Bytes[0] += batch.payloadLimit() - batch.payloadAddr();
                            c2Rows[0] += batch.getRowCount();
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("egress error on c2: " + message);
                        }
                    });
                }
                Assert.assertEquals(4, c1Rows[0]);
                Assert.assertEquals(4, c2Rows[0]);
                // Bytes-wise: c2 is a fresh connection so its delta is non-empty
                // (red/green/blue all get transmitted), meaning its payload size
                // should be within 1-2 bytes of c1's. If the server accidentally
                // reused the previous connection's dict state, c2 would emit an
                // empty delta and come back noticeably smaller.
                long delta = Math.abs(c1Bytes[0] - c2Bytes[0]);
                Assert.assertTrue(
                        "fresh connections must transmit equal-sized payloads (off by "
                                + delta + "; c1=" + c1Bytes[0] + ", c2=" + c2Bytes[0] + ')',
                        delta <= 2
                );
            }
        });
    }

    @Test
    public void testLongSymbolValue() throws Exception {
        // Long (but well-formed) symbol value exercises the dict heap's growth
        // paths on both server (symbolDictHeap) and client (connDictHeap). A
        // 200-char ASCII value fits in the initial 4 KiB heap but verifies
        // length varints can be > 1 byte when emitted.
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startFragmented()) {
                serverMain.execute("CREATE TABLE longsym(s SYMBOL, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                // 200 chars 'x' repeated -- distinct from any tiny test value.
                String longValue = "x".repeat(200);
                serverMain.execute(
                        "INSERT INTO longsym VALUES ('" + longValue + "', 1::TIMESTAMP), "
                                + "('short', 2::TIMESTAMP), ('" + longValue + "', 3::TIMESTAMP)"
                );
                serverMain.awaitTable("longsym");

                final int[] longCount = {0};
                final int[] shortCount = {0};
                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    client.execute("SELECT s FROM longsym", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            for (int r = 0; r < batch.getRowCount(); r++) {
                                DirectUtf8Sequence v = batch.getStrA(0, r);
                                Assert.assertNotNull(v);
                                if (v.size() == 200) {
                                    // Spot-check contents: all bytes must be 'x'.
                                    for (int i = 0; i < 200; i++) {
                                        Assert.assertEquals((byte) 'x', v.byteAt(i));
                                    }
                                    longCount[0]++;
                                } else if (v.size() == 5) {
                                    shortCount[0]++;
                                } else {
                                    Assert.fail("unexpected size " + v.size());
                                }
                            }
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("egress error: " + message);
                        }
                    });
                }
                Assert.assertEquals(2, longCount[0]);
                Assert.assertEquals(1, shortCount[0]);
            }
        });
    }

    @Test
    public void testMultiBatchWithDelta() throws Exception {
        // MULTI_BATCH_ROWS rows span 3 batches (4096 rows/batch max). Batch 1
        // ships the full schema + dict delta; batches 2 and 3 carry rows only
        // (schema known from batch 1) AND a (possibly empty) delta section.
        // Catches bugs where a continuation batch skips or corrupts the delta
        // section read.
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startFragmented()) {
                serverMain.execute("CREATE TABLE multi(s SYMBOL, x LONG, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                // 4 unique symbols cycling; all unique values appear in batch 1,
                // so batches 2 and 3 have empty delta sections.
                serverMain.execute(String.format("""
                        INSERT INTO multi
                        SELECT CASE WHEN (x %% 4) = 0 THEN 'A'
                                    WHEN (x %% 4) = 1 THEN 'B'
                                    WHEN (x %% 4) = 2 THEN 'C'
                                    ELSE 'D' END, x, x::TIMESTAMP
                        FROM long_sequence(%d)
                        """, MULTI_BATCH_ROWS));
                serverMain.awaitTable("multi");

                final int[] counts = new int[4];  // A, B, C, D
                final int[] totalRows = {0};
                final int[] batchCount = {0};
                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    client.execute("SELECT s, x FROM multi", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            batchCount[0]++;
                            for (int r = 0; r < batch.getRowCount(); r++) {
                                DirectUtf8Sequence v = batch.getStrA(0, r);
                                Assert.assertNotNull(v);
                                Assert.assertEquals(1, v.size());
                                int bucket = v.byteAt(0) - 'A';
                                Assert.assertTrue(bucket >= 0 && bucket < 4);
                                counts[bucket]++;
                                totalRows[0]++;
                            }
                        }

                        @Override
                        public void onEnd(long trs) {
                            Assert.assertEquals(MULTI_BATCH_ROWS, trs);
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("egress error: " + message);
                        }
                    });
                }
                Assert.assertEquals(MULTI_BATCH_ROWS, totalRows[0]);
                Assert.assertTrue(
                        "expected >= 2 batches, got " + batchCount[0],
                        batchCount[0] >= 2
                );
                // Even distribution: each symbol gets MULTI_BATCH_ROWS / 4 rows.
                int perBucket = MULTI_BATCH_ROWS / 4;
                for (int i = 0; i < 4; i++) {
                    Assert.assertEquals("bucket " + (char) ('A' + i), perBucket, counts[i]);
                }
            }
        });
    }

    @Test
    public void testNullAndNonNullInterleavedMultiBatch() throws Exception {
        // Alternating NULL and non-null across multiple batches. The server
        // bitmap grows batch-by-batch; each batch's non-null count drives how
        // many varint ids get emitted. Off-by-one in the null bitmap OR in the
        // nonNullCount-based emit loop would produce garbage here.
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startFragmented()) {
                serverMain.execute("CREATE TABLE mix(s SYMBOL, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute(String.format("""
                        INSERT INTO mix
                        SELECT CASE WHEN x %% 3 = 0 THEN CAST(NULL AS SYMBOL)
                                    WHEN x %% 3 = 1 THEN 'odd'
                                    ELSE 'even' END,
                               x::TIMESTAMP
                        FROM long_sequence(%d)
                        """, MULTI_BATCH_ROWS));
                serverMain.awaitTable("mix");

                final int[] nullCount = {0};
                final int[] oddCount = {0};
                final int[] evenCount = {0};
                final int[] rowCount = {0};
                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    client.execute("SELECT s FROM mix", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            for (int r = 0; r < batch.getRowCount(); r++) {
                                if (batch.isNull(0, r)) {
                                    nullCount[0]++;
                                } else {
                                    DirectUtf8Sequence v = batch.getStrA(0, r);
                                    Assert.assertNotNull(v);
                                    // 'odd' is 3 bytes starting 'o', 'even' is 4 bytes starting 'e'.
                                    if (v.size() == 3 && v.byteAt(0) == (byte) 'o') oddCount[0]++;
                                    else if (v.size() == 4 && v.byteAt(0) == (byte) 'e') evenCount[0]++;
                                    else Assert.fail("unexpected symbol at row " + r);
                                }
                                rowCount[0]++;
                            }
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("egress error: " + message);
                        }
                    });
                }
                Assert.assertEquals(MULTI_BATCH_ROWS, rowCount[0]);
                // x in [1,N]: x%3==0 is floor(N/3) rows; x%3==1 ceil(N/3); x%3==2 the remainder.
                // For N=12000: 4000 null, 4000 odd, 4000 even.
                Assert.assertEquals(MULTI_BATCH_ROWS / 3, nullCount[0]);
                Assert.assertEquals(MULTI_BATCH_ROWS / 3, oddCount[0]);
                Assert.assertEquals(MULTI_BATCH_ROWS / 3, evenCount[0]);
            }
        });
    }

    @Test
    public void testSingleUniqueValueManyRows() throws Exception {
        // Extreme dedup: 1000 rows, all with the same value. Dict has exactly
        // one entry. Per-row payload is 1000 varint ids all encoding 0 (one
        // byte each). Verifies the tiniest-dict case still works end-to-end.
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startFragmented()) {
                serverMain.execute("CREATE TABLE uniq(s SYMBOL, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("INSERT INTO uniq SELECT 'only', x::TIMESTAMP FROM long_sequence(1000)");
                serverMain.awaitTable("uniq");

                final int[] count = {0};
                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    client.execute("SELECT s FROM uniq", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            for (int r = 0; r < batch.getRowCount(); r++) {
                                DirectUtf8Sequence v = batch.getStrA(0, r);
                                Assert.assertNotNull(v);
                                Assert.assertEquals(4, v.size());
                                Assert.assertEquals((byte) 'o', v.byteAt(0));
                                Assert.assertEquals((byte) 'n', v.byteAt(1));
                                Assert.assertEquals((byte) 'l', v.byteAt(2));
                                Assert.assertEquals((byte) 'y', v.byteAt(3));
                                count[0]++;
                            }
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("egress error: " + message);
                        }
                    });
                }
                Assert.assertEquals(1000, count[0]);
            }
        });
    }

    @Test
    public void testStaticSymbolTableWrappedInFunctionUsesNativeKeyPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            ObjList<QwpEgressColumnDef> cols = new ObjList<>();
            QwpEgressColumnDef def = new QwpEgressColumnDef();
            def.of("s", ColumnType.SYMBOL);
            cols.add(def);

            final int[] valueOfCalls = {0};
            final StaticSymbolTable staticTable = new StaticSymbolTable() {
                @Override
                public boolean containsNullValue() {
                    return false;
                }

                @Override
                public int getSymbolCount() {
                    return 1;
                }

                @Override
                public int keyOf(CharSequence value) {
                    return "wrapped_static".contentEquals(value) ? 0 : VALUE_NOT_FOUND;
                }

                @Override
                public CharSequence valueBOf(int key) {
                    return valueOf(key);
                }

                @Override
                public CharSequence valueOf(int key) {
                    valueOfCalls[0]++;
                    return key == 0 ? "wrapped_static" : null;
                }
            };
            final SymbolFunction wrapper = new SymbolFunction() {
                @Override
                public int getInt(Record rec) {
                    return rec.getInt(0);
                }

                @Override
                public StaticSymbolTable getStaticSymbolTable() {
                    return staticTable;
                }

                @Override
                public CharSequence getSymbol(Record rec) {
                    return rec.getSymA(0);
                }

                @Override
                public CharSequence getSymbolB(Record rec) {
                    return rec.getSymB(0);
                }

                @Override
                public boolean isSymbolTableStatic() {
                    return true;
                }

                @Override
                public CharSequence valueBOf(int key) {
                    return staticTable.valueBOf(key);
                }

                @Override
                public CharSequence valueOf(int key) {
                    return staticTable.valueOf(key);
                }
            };
            final SymbolTableSource symbolTableSource = new SymbolTableSource() {
                @Override
                public SymbolTable getSymbolTable(int columnIndex) {
                    return wrapper;
                }

                @Override
                public SymbolTable newSymbolTable(int columnIndex) {
                    return wrapper;
                }
            };
            final Record record = new Record() {
                @Override
                public int getInt(int col) {
                    return 0;
                }

                @Override
                public CharSequence getSymA(int col) {
                    throw new AssertionError("wrapped static symbol must use the native-key path");
                }
            };

            try (QwpResultBatchBuffer batch = new QwpResultBatchBuffer();
                 QwpEgressConnSymbolDict dict = new QwpEgressConnSymbolDict()) {
                batch.beginBatch(cols, symbolTableSource, dict);
                batch.appendRow(record);
                batch.appendRow(record);
                Assert.assertEquals(2, batch.getRowCount());
                Assert.assertEquals(1, dict.size());
                Assert.assertEquals("native key must resolve only on first sight", 1, valueOfCalls[0]);
            }
        });
    }

    @Test
    public void testSymbolUnionRoundTrip() throws Exception {
        // Exercise the actual SYMBOL UNION ALL cursor through QWP. This covers the
        // union's dynamic dictionary together with egress type metadata, symbol ids,
        // NULL bitmap, client-side decoding, and a warm connection dictionary.
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startFragmented()) {
                serverMain.execute("CREATE TABLE union_a(s SYMBOL, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("CREATE TABLE union_b(s SYMBOL, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("INSERT INTO union_a VALUES "
                        + "('alpha', 1::TIMESTAMP), ('', 2::TIMESTAMP), "
                        + "(NULL, 3::TIMESTAMP), ('alpha', 4::TIMESTAMP)");
                serverMain.execute("INSERT INTO union_b VALUES "
                        + "('beta', 5::TIMESTAMP), ('', 6::TIMESTAMP), "
                        + "(NULL, 7::TIMESTAMP), ('alpha', 8::TIMESTAMP)");
                serverMain.awaitTable("union_a");
                serverMain.awaitTable("union_b");

                final String query = "SELECT s FROM union_a UNION ALL SELECT s FROM union_b";
                final String[] expected = {"alpha", "", null, "alpha", "beta", "", null, "alpha"};
                final String[] actual = new String[expected.length];
                final int[] symbolIds = new int[expected.length];
                final int[] firstRowIndex = {0};
                final int[] secondRowIndex = {0};
                final long[] firstPayloadBytes = {0};
                final long[] secondPayloadBytes = {0};

                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    client.execute(query, new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            Assert.assertEquals(QwpConstants.TYPE_SYMBOL, batch.getColumnWireType(0));
                            Assert.assertEquals(3, batch.getSymbolDictSize(0));
                            firstPayloadBytes[0] += batch.payloadLimit() - batch.payloadAddr();
                            for (int r = 0; r < batch.getRowCount(); r++) {
                                final int row = firstRowIndex[0]++;
                                actual[row] = batch.getSymbol(0, r);
                                symbolIds[row] = batch.getSymbolId(0, r);
                                Assert.assertEquals(expected[row] == null, batch.isNull(0, r));
                                if (symbolIds[row] < 0) {
                                    Assert.assertNull(actual[row]);
                                } else {
                                    Assert.assertSame(actual[row], batch.getSymbolForId(0, symbolIds[row]));
                                }
                            }
                        }

                        @Override
                        public void onEnd(long totalRows) {
                            Assert.assertEquals(expected.length, totalRows);
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("egress error on first union query: " + message);
                        }
                    });

                    // The second execution uses the same connection and result shape. All
                    // three symbol entries are already known, so its dictionary delta is empty.
                    client.execute(query, new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            Assert.assertEquals(QwpConstants.TYPE_SYMBOL, batch.getColumnWireType(0));
                            Assert.assertEquals(3, batch.getSymbolDictSize(0));
                            secondPayloadBytes[0] += batch.payloadLimit() - batch.payloadAddr();
                            for (int r = 0; r < batch.getRowCount(); r++) {
                                final int row = secondRowIndex[0]++;
                                Assert.assertEquals(expected[row], batch.getSymbol(0, r));
                                Assert.assertEquals(expected[row] == null, batch.isNull(0, r));
                            }
                        }

                        @Override
                        public void onEnd(long totalRows) {
                            Assert.assertEquals(expected.length, totalRows);
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("egress error on second union query: " + message);
                        }
                    });
                }

                Assert.assertArrayEquals(expected, actual);
                Assert.assertEquals(expected.length, firstRowIndex[0]);
                Assert.assertEquals(expected.length, secondRowIndex[0]);
                Assert.assertEquals(-1, symbolIds[2]);
                Assert.assertEquals(-1, symbolIds[6]);
                Assert.assertEquals(symbolIds[0], symbolIds[3]);
                Assert.assertEquals(symbolIds[0], symbolIds[7]);
                Assert.assertEquals(symbolIds[1], symbolIds[5]);
                Assert.assertNotEquals(symbolIds[0], symbolIds[1]);
                Assert.assertNotEquals(symbolIds[0], symbolIds[4]);
                Assert.assertNotEquals(symbolIds[1], symbolIds[4]);
                Assert.assertTrue(
                        "warm union dictionary should emit a smaller payload [first="
                                + firstPayloadBytes[0] + ", second=" + secondPayloadBytes[0] + ']',
                        secondPayloadBytes[0] < firstPayloadBytes[0]
                );
            }
        });
    }

    @Test
    public void testUnicode2ByteAnd3Byte() throws Exception {
        // Mixed 2-byte (Latin-1 Supplement: é = 0xC3 0xA9) and 3-byte
        // (CJK Unified Ideograph U+4E2D = 0xE4 0xB8 0xAD). Both go through
        // encodeUtf8's continuation-byte branches on the server and
        // stringFromUtf8 on the client.
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startFragmented()) {
                serverMain.execute("CREATE TABLE uni(s SYMBOL, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                // café (5 bytes: c a f 0xC3 0xA9), 中文 (6 bytes).
                serverMain.execute("""
                        INSERT INTO uni VALUES
                            ('café',  1::TIMESTAMP),
                            ('中文',  2::TIMESTAMP),
                            ('café',  3::TIMESTAMP),
                            ('ascii', 4::TIMESTAMP)
                        """);
                serverMain.awaitTable("uni");

                final String[] expected = {"café", "中文", "café", "ascii"};
                final int[] rowIdx = {0};
                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    client.execute("SELECT s FROM uni", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            for (int r = 0; r < batch.getRowCount(); r++) {
                                DirectUtf8Sequence v = batch.getStrA(0, r);
                                Assert.assertNotNull(v);
                                Assert.assertEquals(expected[rowIdx[0]], v.toString());
                                rowIdx[0]++;
                            }
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("egress error: " + message);
                        }
                    });
                }
                Assert.assertEquals(4, rowIdx[0]);
            }
        });
    }

}
