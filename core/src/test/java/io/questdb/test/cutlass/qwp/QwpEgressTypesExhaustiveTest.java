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

import io.questdb.client.cutlass.qwp.client.ColumnView;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;
import io.questdb.client.cutlass.qwp.client.QwpQueryClient;
import io.questdb.client.cutlass.qwp.client.RowView;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.std.Long256Impl;
import io.questdb.client.std.Uuid;
import io.questdb.client.std.bytes.DirectByteSequence;
import io.questdb.client.std.str.StringSink;
import io.questdb.client.std.str.Utf8Sequence;
import io.questdb.client.std.str.Utf8StringSink;
import io.questdb.client.std.str.Utf8s;
import io.questdb.std.BoolList;
import io.questdb.std.LongList;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Exhaustive boundary tests for every QWP wire type. One test per type covering:
 * <ul>
 *   <li>min / max / zero / mid-range non-null values</li>
 *   <li>NULL where the type can hold it (and the QuestDB-specific sentinel where it can't)</li>
 *   <li>variable-width edge cases: empty value, large value, dual A/B view, byte-content sanity (0x00, 0xFF)</li>
 *   <li>type-specific shape: GEOHASH at all four storage widths, DECIMAL at multiple scales, ARRAY 1-D/2-D, SYMBOL dict dedup</li>
 * </ul>
 * Each test asserts the schema's wire-type code and the per-cell round-trip. Runs against a
 * locally booted QuestDB; share via {@code -P local-client} so the locally-built client is used.
 */
public class QwpEgressTypesExhaustiveTest extends AbstractBootstrapTest {

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testAllNullColumn() throws Exception {
        // Edge case: every row in the column is NULL. Null bitmap fully set; no values
        // section. Pin that the per-column emit/decode handles the all-null path.
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute("CREATE TABLE t(s STRING, l LONG, d DOUBLE, part_ts TIMESTAMP) "
                        + "TIMESTAMP(part_ts) PARTITION BY DAY WAL");
                serverMain.execute("INSERT INTO t VALUES (NULL, NULL, NULL, 1::TIMESTAMP), "
                        + "(NULL, NULL, NULL, 2::TIMESTAMP), (NULL, NULL, NULL, 3::TIMESTAMP)");
                serverMain.awaitTable("t");

                final int[] nullCount = {0};
                final int[] rows = {0};
                try (QwpQueryClient client = QwpQueryClient.fromConfig("ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    client.execute("SELECT s, l, d FROM t", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            for (int r = 0; r < batch.getRowCount(); r++, rows[0]++) {
                                for (int c = 0; c < 3; c++) {
                                    if (batch.isNull(c, r)) nullCount[0]++;
                                }
                            }
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail(message);
                        }
                    });
                }
                Assert.assertEquals(3, rows[0]);
                Assert.assertEquals(9, nullCount[0]);
            }
        });
    }

    @Test
    public void testBinaryExhaustive() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute("CREATE TABLE t(b BINARY, part_ts TIMESTAMP) "
                        + "TIMESTAMP(part_ts) PARTITION BY DAY WAL");
                // Mix of sizes via rnd_bin: small (1-2 bytes), medium (32 bytes), explicit NULL.
                serverMain.execute("INSERT INTO t SELECT rnd_bin(1, 2, 0), x::TIMESTAMP FROM long_sequence(2)");
                serverMain.execute("INSERT INTO t SELECT rnd_bin(32, 32, 0), (x + 10)::TIMESTAMP FROM long_sequence(2)");
                serverMain.execute("INSERT INTO t VALUES (CAST(NULL AS BINARY), 100::TIMESTAMP)");
                serverMain.awaitTable("t");

                final List<byte[]> heapCopies = new ArrayList<>();
                final List<byte[]> viewCopies = new ArrayList<>();
                final List<byte[]> dualA = new ArrayList<>();
                final List<byte[]> dualB = new ArrayList<>();
                final byte[] wireType = {0};
                try (QwpQueryClient client = QwpQueryClient.fromConfig("ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    client.execute("SELECT b FROM t", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            wireType[0] = batch.getColumnWireType(0);
                            int n = batch.getRowCount();
                            for (int r = 0; r < n; r++) {
                                if (batch.isNull(0, r)) {
                                    heapCopies.add(null);
                                    viewCopies.add(null);
                                    continue;
                                }
                                // Heap copy (allocates)
                                heapCopies.add(batch.getBinary(0, r));
                                // Native zero-alloc view
                                DirectByteSequence view = batch.getBinaryA(0, r);
                                byte[] copy = new byte[view.size()];
                                for (int i = 0; i < view.size(); i++) copy[i] = view.byteAt(i);
                                viewCopies.add(copy);
                            }
                            // Dual-view: A and B point at different cells simultaneously.
                            // Pick two non-null rows to confirm A doesn't clobber B.
                            int firstNonNull = -1, secondNonNull = -1;
                            for (int r = 0; r < n; r++) {
                                if (!batch.isNull(0, r)) {
                                    if (firstNonNull < 0) {
                                        firstNonNull = r;
                                    } else {
                                        secondNonNull = r;
                                        break;
                                    }
                                }
                            }
                            if (firstNonNull >= 0 && secondNonNull >= 0) {
                                DirectByteSequence a = batch.getBinaryA(0, firstNonNull);
                                DirectByteSequence b = batch.getBinaryB(0, secondNonNull);
                                byte[] aCopy = new byte[a.size()];
                                byte[] bCopy = new byte[b.size()];
                                for (int i = 0; i < a.size(); i++) aCopy[i] = a.byteAt(i);
                                for (int i = 0; i < b.size(); i++) bCopy[i] = b.byteAt(i);
                                dualA.add(aCopy);
                                dualB.add(bCopy);
                            }
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail(message);
                        }
                    });
                }
                Assert.assertEquals(QwpConstants.TYPE_BINARY, wireType[0]);
                Assert.assertEquals(5, heapCopies.size());
                // First two are 1-2 bytes
                Assert.assertNotNull(heapCopies.get(0));
                Assert.assertTrue(heapCopies.get(0).length >= 1 && heapCopies.get(0).length <= 2);
                Assert.assertNotNull(heapCopies.get(1));
                Assert.assertTrue(heapCopies.get(1).length >= 1 && heapCopies.get(1).length <= 2);
                // Next two are 32 bytes
                Assert.assertEquals(32, heapCopies.get(2).length);
                Assert.assertEquals(32, heapCopies.get(3).length);
                // Last is NULL
                Assert.assertNull(heapCopies.get(4));
                // Native view matches heap copy byte-for-byte
                for (int r = 0; r < 5; r++) {
                    if (heapCopies.get(r) == null) Assert.assertNull(viewCopies.get(r));
                    else Assert.assertArrayEquals("row " + r, heapCopies.get(r), viewCopies.get(r));
                }
                // Dual-view holds two cells without clobber
                Assert.assertFalse("dual-view A and B should have captured two non-null cells", dualA.isEmpty());
                Assert.assertFalse(Arrays.equals(dualA.getFirst(), dualB.getFirst()) && dualA.getFirst().length == dualB.getFirst().length
                        && (dualA.getFirst().length == 0));
            }
        });
    }

    @Test
    public void testBoolean() throws Exception {
        // BOOLEAN cannot hold NULL in QuestDB -- INSERT NULL stores false.
        runRoundTrip(
                "CREATE TABLE t(b BOOLEAN)",
                "INSERT INTO t VALUES (true), (false), (NULL)",
                QwpConstants.TYPE_BOOLEAN,
                3,
                (batch, results) -> {
                    for (int r = 0; r < 3; r++) {
                        results.add(new Object[]{batch.isNull(0, r), batch.getBoolValue(0, r)});
                    }
                },
                rows -> {
                    Assert.assertEquals(Boolean.TRUE, ((Object[]) rows.get(0))[1]);
                    Assert.assertEquals(Boolean.FALSE, ((Object[]) rows.get(1))[1]);
                    // NULL -> false (no NULL representation for BOOLEAN in QuestDB)
                    Assert.assertEquals(Boolean.FALSE, ((Object[]) rows.get(2))[1]);
                    Assert.assertEquals(Boolean.FALSE, ((Object[]) rows.get(0))[0]);
                    Assert.assertEquals(Boolean.FALSE, ((Object[]) rows.get(1))[0]);
                    Assert.assertEquals(Boolean.FALSE, ((Object[]) rows.get(2))[0]);
                }
        );
    }

    @Test
    public void testByte() throws Exception {
        // BYTE cannot hold NULL in QuestDB.
        runRoundTrip(
                "CREATE TABLE t(b BYTE)",
                "INSERT INTO t VALUES (-128), (127), (0), (NULL)",
                QwpConstants.TYPE_BYTE,
                4,
                (batch, results) -> {
                    for (int r = 0; r < 4; r++) results.add(batch.getByteValue(0, r));
                },
                rows -> {
                    Assert.assertEquals((byte) -128, rows.get(0));
                    Assert.assertEquals((byte) 127, rows.get(1));
                    Assert.assertEquals((byte) 0, rows.get(2));
                    Assert.assertEquals((byte) 0, rows.get(3)); // NULL -> 0
                }
        );
    }

    @Test
    public void testChar() throws Exception {
        // CHAR can't hold NULL either.
        runRoundTrip(
                "CREATE TABLE t(c CHAR)",
                "INSERT INTO t VALUES ('A'), ('z'), ('0')",
                QwpConstants.TYPE_CHAR,
                3,
                (batch, results) -> {
                    for (int r = 0; r < 3; r++) results.add(batch.getCharValue(0, r));
                },
                rows -> {
                    Assert.assertEquals('A', rows.get(0));
                    Assert.assertEquals('z', rows.get(1));
                    Assert.assertEquals('0', rows.get(2));
                }
        );
    }

    @Test
    public void testColumnView() throws Exception {
        // Pins the column-pinned facade: column(c) returns a flyweight whose
        // single-arg accessors match the (col, row) primitives across types and
        // NULL rows. Two concurrent ColumnView instances must coexist.
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute("CREATE TABLE t(i INT, d DOUBLE, s SYMBOL, part_ts TIMESTAMP) "
                        + "TIMESTAMP(part_ts) PARTITION BY DAY WAL");
                serverMain.execute("""
                        INSERT INTO t VALUES
                            (1,    1.5,  'AAPL', 1::TIMESTAMP),
                            (NULL, 2.5,  'MSFT', 2::TIMESTAMP),
                            (3,    NULL, NULL,   3::TIMESTAMP),
                            (4,    4.5,  'AAPL', 4::TIMESTAMP)
                        """);
                serverMain.awaitTable("t");

                final int[] intVals = new int[4];
                final boolean[] intNulls = new boolean[4];
                final double[] dblVals = new double[4];
                final boolean[] dblNulls = new boolean[4];
                final String[] symVals = new String[4];
                final boolean[] sameInstance = {false};
                try (QwpQueryClient client = QwpQueryClient.fromConfig("ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    client.execute("SELECT i, d, s FROM t ORDER BY part_ts", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            ColumnView ints = batch.column(0);
                            ColumnView dbls = batch.column(1);
                            ColumnView syms = batch.column(2);
                            sameInstance[0] = batch.column(0) == ints;
                            for (int r = 0; r < batch.getRowCount(); r++) {
                                intNulls[r] = ints.isNull(r);
                                intVals[r] = ints.getIntValue(r);
                                dblNulls[r] = dbls.isNull(r);
                                dblVals[r] = dbls.getDoubleValue(r);
                                symVals[r] = syms.getSymbol(r);
                            }
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail(message);
                        }
                    });
                }
                Assert.assertTrue("repeated column(c) returns the same instance", sameInstance[0]);
                Assert.assertArrayEquals(new int[]{1, 0, 3, 4}, intVals);
                Assert.assertArrayEquals(new boolean[]{false, true, false, false}, intNulls);
                Assert.assertArrayEquals(new boolean[]{false, false, true, false}, dblNulls);
                Assert.assertEquals(1.5, dblVals[0], 0.0);
                Assert.assertEquals(2.5, dblVals[1], 0.0);
                Assert.assertTrue(Double.isNaN(dblVals[2]));
                Assert.assertEquals(4.5, dblVals[3], 0.0);
                Assert.assertEquals("AAPL", symVals[0]);
                Assert.assertEquals("MSFT", symVals[1]);
                Assert.assertNull(symVals[2]);
                Assert.assertEquals("AAPL", symVals[3]);
            }
        });
    }

    @Test
    public void testColumnViewRawAddrs() throws Exception {
        // SIMD/JNI consumers iterate ColumnView.valuesAddr() directly. Assert
        // that the raw addresses, stride, and null bitmap line up with what
        // the typed accessors return.
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute("CREATE TABLE t(d DOUBLE, part_ts TIMESTAMP) "
                        + "TIMESTAMP(part_ts) PARTITION BY DAY WAL");
                serverMain.execute("""
                        INSERT INTO t VALUES
                            (1.5,  1::TIMESTAMP),
                            (2.5,  2::TIMESTAMP),
                            (NULL, 3::TIMESTAMP),
                            (4.5,  4::TIMESTAMP)
                        """);
                serverMain.awaitTable("t");

                final long[] valuesAddr = {0};
                final long[] nullBitmapAddr = {0};
                final int[] stride = {0};
                final int[] nonNullCount = {0};
                final boolean[] nullForRow2 = {false};
                final double[] decoded = new double[3];
                try (QwpQueryClient client = QwpQueryClient.fromConfig("ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    client.execute("SELECT d FROM t ORDER BY part_ts", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            ColumnView col = batch.column(0);
                            valuesAddr[0] = col.valuesAddr();
                            nullBitmapAddr[0] = col.nullBitmapAddr();
                            stride[0] = col.bytesPerValue();
                            nonNullCount[0] = col.nonNullCount();
                            // Null bitmap: row 2 NULL; LSB-first within byte 0 -> bit 2 set.
                            byte bm = Unsafe.getByte(nullBitmapAddr[0]);
                            nullForRow2[0] = (bm & (1 << 2)) != 0;
                            // Walk the dense values array directly.
                            for (int i = 0; i < nonNullCount[0]; i++) {
                                decoded[i] = Unsafe.getDouble(valuesAddr[0] + (long) stride[0] * i);
                            }
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail(message);
                        }
                    });
                }
                Assert.assertEquals("DOUBLE stride", 8, stride[0]);
                Assert.assertEquals("3 non-null rows out of 4", 3, nonNullCount[0]);
                Assert.assertNotEquals("values address must be set", 0L, valuesAddr[0]);
                Assert.assertNotEquals("null bitmap must be set when nulls are present", 0L, nullBitmapAddr[0]);
                Assert.assertTrue("row 2 NULL bit must be set", nullForRow2[0]);
                Assert.assertArrayEquals(new double[]{1.5, 2.5, 4.5}, decoded, 0.0);
            }
        });
    }

    @Test
    public void testDate() throws Exception {
        runRoundTrip(
                "CREATE TABLE t(d DATE)",
                """
                        INSERT INTO t VALUES
                            ('1970-01-01'::DATE),
                            ('2024-01-15'::DATE),
                            (NULL)
                        """,
                QwpConstants.TYPE_DATE,
                3,
                (batch, results) -> {
                    for (int r = 0; r < 3; r++) {
                        results.add(new Object[]{batch.isNull(0, r), batch.getLongValue(0, r)});
                    }
                },
                rows -> {
                    Assert.assertEquals(0L, ((Object[]) rows.get(0))[1]);
                    Assert.assertTrue((Long) ((Object[]) rows.get(1))[1] > 0L);
                    Assert.assertEquals(Boolean.TRUE, ((Object[]) rows.get(2))[0]);
                }
        );
    }

    @Test
    public void testDecimal128() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute("CREATE TABLE t(d DECIMAL(38,6), part_ts TIMESTAMP) "
                        + "TIMESTAMP(part_ts) PARTITION BY DAY WAL");
                serverMain.execute("INSERT INTO t VALUES (123456789.123456m, 1::TIMESTAMP), "
                        + "(CAST(NULL AS DECIMAL(38,6)), 2::TIMESTAMP)");
                serverMain.awaitTable("t");

                final List<long[]> values = new ArrayList<>();
                final byte[] wireType = {0};
                final int[] scale = {0};
                try (QwpQueryClient client = QwpQueryClient.fromConfig("ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    client.execute("SELECT d FROM t", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            wireType[0] = batch.getColumnWireType(0);
                            scale[0] = batch.getDecimalScale(0);
                            for (int r = 0; r < batch.getRowCount(); r++) {
                                if (batch.isNull(0, r)) {
                                    values.add(null);
                                } else {
                                    values.add(new long[]{batch.getDecimal128Low(0, r), batch.getDecimal128High(0, r)});
                                }
                            }
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail(message);
                        }
                    });
                }
                Assert.assertEquals(QwpConstants.TYPE_DECIMAL128, wireType[0]);
                Assert.assertEquals(6, scale[0]);
                Assert.assertNotNull(values.get(0));
                Assert.assertEquals(2, values.get(0).length);
                Assert.assertNull(values.get(1));
            }
        });
    }

    @Test
    public void testDecimal256() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute("CREATE TABLE t(d DECIMAL(76,10), part_ts TIMESTAMP) "
                        + "TIMESTAMP(part_ts) PARTITION BY DAY WAL");
                serverMain.execute("INSERT INTO t VALUES (123456789.1234567890m, 1::TIMESTAMP), "
                        + "(CAST(NULL AS DECIMAL(76,10)), 2::TIMESTAMP)");
                serverMain.awaitTable("t");

                final List<long[]> values = new ArrayList<>();
                final byte[] wireType = {0};
                final int[] scale = {0};
                try (QwpQueryClient client = QwpQueryClient.fromConfig("ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    client.execute("SELECT d FROM t", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            wireType[0] = batch.getColumnWireType(0);
                            scale[0] = batch.getDecimalScale(0);
                            for (int r = 0; r < batch.getRowCount(); r++) {
                                if (batch.isNull(0, r)) {
                                    values.add(null);
                                } else {
                                    values.add(new long[]{
                                            batch.getLong256Word(0, r, 0),
                                            batch.getLong256Word(0, r, 1),
                                            batch.getLong256Word(0, r, 2),
                                            batch.getLong256Word(0, r, 3)
                                    });
                                }
                            }
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail(message);
                        }
                    });
                }
                Assert.assertEquals(QwpConstants.TYPE_DECIMAL256, wireType[0]);
                Assert.assertEquals(10, scale[0]);
                Assert.assertNotNull(values.get(0));
                Assert.assertNull(values.get(1));
            }
        });
    }

    @Test
    public void testDecimal64() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute("CREATE TABLE t(d DECIMAL(18,4), part_ts TIMESTAMP) "
                        + "TIMESTAMP(part_ts) PARTITION BY DAY WAL");
                serverMain.execute("INSERT INTO t VALUES (1234.5678m, 1::TIMESTAMP), (-1234.5678m, 2::TIMESTAMP), "
                        + "(0m, 3::TIMESTAMP), (CAST(NULL AS DECIMAL(18,4)), 4::TIMESTAMP)");
                serverMain.awaitTable("t");

                final LongList rawValues = new LongList();
                final BoolList nulls = new BoolList();
                final byte[] wireType = {0};
                final int[] scale = {0};
                try (QwpQueryClient client = QwpQueryClient.fromConfig("ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    client.execute("SELECT d FROM t", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            wireType[0] = batch.getColumnWireType(0);
                            scale[0] = batch.getDecimalScale(0);
                            for (int r = 0; r < batch.getRowCount(); r++) {
                                boolean isNull = batch.isNull(0, r);
                                nulls.add(isNull);
                                rawValues.add(isNull ? 0L : batch.getLongValue(0, r));
                            }
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail(message);
                        }
                    });
                }
                Assert.assertEquals(QwpConstants.TYPE_DECIMAL64, wireType[0]);
                Assert.assertEquals(4, scale[0]);
                // 1234.5678 with scale 4 -> unscaled 12_345_678
                Assert.assertEquals(12_345_678L, rawValues.getQuick(0));
                Assert.assertEquals(-12_345_678L, rawValues.getQuick(1));
                Assert.assertEquals(0L, rawValues.getQuick(2));
                Assert.assertTrue(nulls.get(3));
            }
        });
    }

    @Test
    public void testDecimalScalesAcrossColumns() throws Exception {
        // Covers two things:
        // 1. Decoder reads the per-column scale byte for every DECIMAL column
        //    independently. One shared field in the decoder would collapse all
        //    scales to the last column's value.
        // 2. Scale=0 byte decodes correctly (not confused with "scale unset").
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute("""
                        CREATE TABLE t(
                            d64_0   DECIMAL(18, 0),
                            d64_4   DECIMAL(18, 4),
                            d128_0  DECIMAL(38, 0),
                            d128_20 DECIMAL(38, 20),
                            d256_0  DECIMAL(76, 0),
                            d256_38 DECIMAL(76, 38),
                            part_ts TIMESTAMP
                        ) TIMESTAMP(part_ts) PARTITION BY DAY WAL
                        """);
                serverMain.execute("""
                        INSERT INTO t VALUES (
                            42m,
                            1.2345m,
                            1000m,
                            3.14159265358979323846m,
                            9999999999999999999999999999999999999m,
                            1.12345678901234567890123456789012345678m,
                            1::TIMESTAMP
                        )
                        """);
                serverMain.awaitTable("t");

                final byte[] wireTypes = new byte[6];
                final int[] scales = new int[6];
                try (QwpQueryClient client = QwpQueryClient.fromConfig("ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    client.execute(
                            "SELECT d64_0, d64_4, d128_0, d128_20, d256_0, d256_38 FROM t",
                            new QwpColumnBatchHandler() {
                                @Override
                                public void onBatch(QwpColumnBatch batch) {
                                    for (int c = 0; c < 6; c++) {
                                        wireTypes[c] = batch.getColumnWireType(c);
                                        scales[c] = batch.getDecimalScale(c);
                                    }
                                }

                                @Override
                                public void onEnd(long totalRows) {
                                }

                                @Override
                                public void onError(byte status, String message) {
                                    Assert.fail(message);
                                }
                            });
                }
                Assert.assertEquals(QwpConstants.TYPE_DECIMAL64, wireTypes[0]);
                Assert.assertEquals(QwpConstants.TYPE_DECIMAL64, wireTypes[1]);
                Assert.assertEquals(QwpConstants.TYPE_DECIMAL128, wireTypes[2]);
                Assert.assertEquals(QwpConstants.TYPE_DECIMAL128, wireTypes[3]);
                Assert.assertEquals(QwpConstants.TYPE_DECIMAL256, wireTypes[4]);
                Assert.assertEquals(QwpConstants.TYPE_DECIMAL256, wireTypes[5]);
                Assert.assertArrayEquals(new int[]{0, 4, 0, 20, 0, 38}, scales);
            }
        });
    }

    @Test
    public void testDouble() throws Exception {
        runRoundTrip(
                "CREATE TABLE t(d DOUBLE)",
                "INSERT INTO t VALUES (3.141592653589793), (-3.141592653589793), (0.0), (NULL), (0.0/0.0)",
                QwpConstants.TYPE_DOUBLE,
                5,
                (batch, results) -> {
                    for (int r = 0; r < 5; r++) {
                        results.add(new Object[]{batch.isNull(0, r), batch.getDoubleValue(0, r)});
                    }
                },
                rows -> {
                    Assert.assertEquals(3.141592653589793, (Double) ((Object[]) rows.get(0))[1], 1e-15);
                    Assert.assertEquals(-3.141592653589793, (Double) ((Object[]) rows.get(1))[1], 1e-15);
                    Assert.assertEquals(0.0, (Double) ((Object[]) rows.get(2))[1], 0.0);
                    Assert.assertEquals(Boolean.TRUE, ((Object[]) rows.get(3))[0]);
                    Assert.assertEquals(Boolean.TRUE, ((Object[]) rows.get(4))[0]);
                }
        );
    }

    @Test
    public void testDoubleArray1DAnd2D() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute("CREATE TABLE t(a DOUBLE[], b DOUBLE[][], part_ts TIMESTAMP) "
                        + "TIMESTAMP(part_ts) PARTITION BY DAY WAL");
                serverMain.execute("INSERT INTO t VALUES (ARRAY[1.0, 2.0, 3.0], ARRAY[[1.0, 2.0], [3.0, 4.0]], 1::TIMESTAMP)");
                serverMain.execute("INSERT INTO t VALUES (ARRAY[10.0], ARRAY[[5.0, 6.0]], 2::TIMESTAMP)");
                serverMain.awaitTable("t");

                final byte[] wt1d = {0};
                final byte[] wt2d = {0};
                final int[] count = {0};
                try (QwpQueryClient client = QwpQueryClient.fromConfig("ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    client.execute("SELECT a, b FROM t", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            wt1d[0] = batch.getColumnWireType(0);
                            wt2d[0] = batch.getColumnWireType(1);
                            for (int r = 0; r < batch.getRowCount(); r++, count[0]++) {
                                Assert.assertFalse("col 0 row " + r + " must be non-null", batch.isNull(0, r));
                                Assert.assertFalse("col 1 row " + r + " must be non-null", batch.isNull(1, r));
                            }
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail(message);
                        }
                    });
                }
                Assert.assertEquals(QwpConstants.TYPE_DOUBLE_ARRAY, wt1d[0]);
                Assert.assertEquals(QwpConstants.TYPE_DOUBLE_ARRAY, wt2d[0]);
                Assert.assertEquals(2, count[0]);
            }
        });
    }

    @Test
    public void testFloat() throws Exception {
        // FLOAT NULL convention: NaN. Real NaN cannot be distinguished from explicit NULL.
        runRoundTrip(
                "CREATE TABLE t(f FLOAT)",
                "INSERT INTO t VALUES (1.5f), (-1.5f), (0.0f), (NULL), (CAST(0.0/0.0 AS FLOAT))",
                QwpConstants.TYPE_FLOAT,
                5,
                (batch, results) -> {
                    for (int r = 0; r < 5; r++) {
                        results.add(new Object[]{batch.isNull(0, r), batch.getFloatValue(0, r)});
                    }
                },
                rows -> {
                    Assert.assertEquals(1.5f, (Float) ((Object[]) rows.get(0))[1], 0.0f);
                    Assert.assertEquals(-1.5f, (Float) ((Object[]) rows.get(1))[1], 0.0f);
                    Assert.assertEquals(0.0f, (Float) ((Object[]) rows.get(2))[1], 0.0f);
                    Assert.assertEquals(Boolean.TRUE, ((Object[]) rows.get(3))[0]);
                    Assert.assertEquals(Boolean.TRUE, ((Object[]) rows.get(4))[0]); // NaN -> null
                }
        );
    }

    @Test
    public void testForEachRow() throws Exception {
        // Pins the row-pinned facade: forEachRow walks every row in order,
        // RowView#getRowIndex tracks position, single-arg accessors match the
        // (col, row) primitives, and the same flyweight instance is reused.
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute("CREATE TABLE t(i INT, d DOUBLE, s SYMBOL, part_ts TIMESTAMP) "
                        + "TIMESTAMP(part_ts) PARTITION BY DAY WAL");
                serverMain.execute("""
                        INSERT INTO t VALUES
                            (1,    1.5,  'AAPL', 1::TIMESTAMP),
                            (NULL, 2.5,  'MSFT', 2::TIMESTAMP),
                            (3,    NULL, NULL,   3::TIMESTAMP)
                        """);
                serverMain.awaitTable("t");

                final List<Object[]> rows = new ArrayList<>();
                final java.util.IdentityHashMap<RowView, Boolean> seenInstances = new java.util.IdentityHashMap<>();
                final boolean[] sawBatchAccessor = {false};
                try (QwpQueryClient client = QwpQueryClient.fromConfig("ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    client.execute("SELECT i, d, s FROM t ORDER BY part_ts", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            sawBatchAccessor[0] = batch.row(0).batch() == batch;
                            batch.forEachRow(row -> {
                                seenInstances.put(row, Boolean.TRUE);
                                rows.add(new Object[]{
                                        row.getRowIndex(),
                                        row.isNull(0), row.getIntValue(0),
                                        row.isNull(1), row.getDoubleValue(1),
                                        row.getSymbol(2)
                                });
                            });
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail(message);
                        }
                    });
                }
                Assert.assertTrue("row().batch() returns the parent batch", sawBatchAccessor[0]);
                Assert.assertEquals("forEachRow reuses the same RowView", 1, seenInstances.size());
                Assert.assertEquals(3, rows.size());

                Object[] r0 = rows.getFirst();
                Assert.assertEquals(0, r0[0]);
                Assert.assertEquals(Boolean.FALSE, r0[1]);
                Assert.assertEquals(1, r0[2]);
                Assert.assertEquals(Boolean.FALSE, r0[3]);
                Assert.assertEquals(1.5, (Double) r0[4], 0.0);
                Assert.assertEquals("AAPL", r0[5]);

                Object[] r1 = rows.get(1);
                Assert.assertEquals(1, r1[0]);
                Assert.assertEquals(Boolean.TRUE, r1[1]);
                Assert.assertEquals(0, r1[2]); // NULL INT -> 0
                Assert.assertEquals(2.5, (Double) r1[4], 0.0);
                Assert.assertEquals("MSFT", r1[5]);

                Object[] r2 = rows.get(2);
                Assert.assertEquals(2, r2[0]);
                Assert.assertEquals(3, r2[2]);
                Assert.assertEquals(Boolean.TRUE, r2[3]);
                Assert.assertTrue("NULL DOUBLE -> NaN", Double.isNaN((Double) r2[4]));
                Assert.assertNull(r2[5]);
            }
        });
    }

    @Test
    public void testGeohashAllWidths() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                // 5 widths spanning all four storage classes (byte/short/int/long).
                serverMain.execute("""
                        CREATE TABLE t(
                            g4 GEOHASH(4b),
                            g8 GEOHASH(8b),
                            g16 GEOHASH(16b),
                            g40 GEOHASH(40b),
                            g60 GEOHASH(60b),
                            part_ts TIMESTAMP
                        ) TIMESTAMP(part_ts) PARTITION BY DAY WAL
                        """);
                serverMain.execute("INSERT INTO t VALUES (#0, #00, #0000, #00000000, #000000000000, 1::TIMESTAMP)");
                serverMain.execute("""
                        INSERT INTO t VALUES (
                            CAST(NULL AS GEOHASH(4b)),
                            CAST(NULL AS GEOHASH(8b)),
                            CAST(NULL AS GEOHASH(16b)),
                            CAST(NULL AS GEOHASH(40b)),
                            CAST(NULL AS GEOHASH(60b)),
                            2::TIMESTAMP
                        )
                        """);
                serverMain.awaitTable("t");

                final boolean[][] isNull = new boolean[2][5];
                final int[] precisionBits = new int[5];
                final byte[] wireType = {0};
                try (QwpQueryClient client = QwpQueryClient.fromConfig("ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    client.execute("SELECT * FROM t", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            wireType[0] = batch.getColumnWireType(0);
                            for (int c = 0; c < 5; c++) precisionBits[c] = batch.getGeohashPrecisionBits(c);
                            for (int r = 0; r < batch.getRowCount(); r++) {
                                for (int c = 0; c < 5; c++) isNull[r][c] = batch.isNull(c, r);
                            }
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail(message);
                        }
                    });
                }
                Assert.assertEquals(QwpConstants.TYPE_GEOHASH, wireType[0]);
                Assert.assertArrayEquals(new int[]{4, 8, 16, 40, 60}, precisionBits);
                for (int c = 0; c < 5; c++) {
                    Assert.assertFalse("non-null row, col " + c, isNull[0][c]);
                    Assert.assertTrue("null row, col " + c, isNull[1][c]);
                }
            }
        });
    }

    @Test
    public void testIPv4() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute("CREATE TABLE t(addr IPv4, part_ts TIMESTAMP) "
                        + "TIMESTAMP(part_ts) PARTITION BY DAY WAL");
                // QuestDB convention: 0.0.0.0 == NULL for IPv4.
                serverMain.execute("""
                        INSERT INTO t VALUES
                            ('192.168.1.1',     1::TIMESTAMP),
                            ('255.255.255.255', 2::TIMESTAMP),
                            ('10.0.0.1',        3::TIMESTAMP),
                            (NULL,              4::TIMESTAMP),
                            ('0.0.0.0',         5::TIMESTAMP)
                        """);
                serverMain.awaitTable("t");

                final LongList values = new LongList();
                final boolean[] nullSeen = new boolean[5];
                final byte[] wireType = {0};
                try (QwpQueryClient client = QwpQueryClient.fromConfig("ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    client.execute("SELECT addr FROM t", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            wireType[0] = batch.getColumnWireType(0);
                            for (int r = 0; r < batch.getRowCount(); r++) {
                                nullSeen[r] = batch.isNull(0, r);
                                values.add(nullSeen[r] ? 0L : batch.getIntValue(0, r));
                            }
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail(message);
                        }
                    });
                }
                Assert.assertEquals(QwpConstants.TYPE_IPv4, wireType[0]);
                // 192.168.1.1 = 0xC0A80101
                Assert.assertEquals(0xC0A80101L, values.getQuick(0) & 0xFFFFFFFFL);
                // 255.255.255.255 = 0xFFFFFFFF
                Assert.assertEquals(0xFFFFFFFFL, values.getQuick(1) & 0xFFFFFFFFL);
                // 10.0.0.1
                Assert.assertEquals(0x0A000001L, values.getQuick(2) & 0xFFFFFFFFL);
                // explicit NULL and 0.0.0.0 both surface as null
                Assert.assertTrue(nullSeen[3]);
                Assert.assertTrue(nullSeen[4]);
            }
        });
    }

    @Test
    public void testInt() throws Exception {
        runRoundTrip(
                "CREATE TABLE t(i INT)",
                "INSERT INTO t VALUES (-2147483647), (2147483647), (0), (-1), (NULL)",
                QwpConstants.TYPE_INT,
                5,
                (batch, results) -> {
                    for (int r = 0; r < 5; r++) {
                        results.add(new Object[]{batch.isNull(0, r), batch.getIntValue(0, r)});
                    }
                },
                rows -> {
                    Assert.assertEquals(-2147483647, ((Object[]) rows.get(0))[1]);
                    Assert.assertEquals(2147483647, ((Object[]) rows.get(1))[1]);
                    Assert.assertEquals(0, ((Object[]) rows.get(2))[1]);
                    Assert.assertEquals(-1, ((Object[]) rows.get(3))[1]);
                    Assert.assertEquals(Boolean.TRUE, ((Object[]) rows.get(4))[0]); // explicit NULL
                }
        );
    }

    @Test
    public void testLong() throws Exception {
        runRoundTrip(
                "CREATE TABLE t(l LONG)",
                "INSERT INTO t VALUES (-9223372036854775807L), (9223372036854775807L), (0L), (-1L), (NULL)",
                QwpConstants.TYPE_LONG,
                5,
                (batch, results) -> {
                    for (int r = 0; r < 5; r++) {
                        results.add(new Object[]{batch.isNull(0, r), batch.getLongValue(0, r)});
                    }
                },
                rows -> {
                    Assert.assertEquals(-9223372036854775807L, ((Object[]) rows.get(0))[1]);
                    Assert.assertEquals(9223372036854775807L, ((Object[]) rows.get(1))[1]);
                    Assert.assertEquals(0L, ((Object[]) rows.get(2))[1]);
                    Assert.assertEquals(-1L, ((Object[]) rows.get(3))[1]);
                    Assert.assertEquals(Boolean.TRUE, ((Object[]) rows.get(4))[0]);
                }
        );
    }

    @Test
    public void testLong256() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute("CREATE TABLE t(l256 LONG256, part_ts TIMESTAMP) "
                        + "TIMESTAMP(part_ts) PARTITION BY DAY WAL");
                serverMain.execute("""
                        INSERT INTO t VALUES
                            (CAST('0x01' AS LONG256),               1::TIMESTAMP),
                            (CAST('0xFFFFFFFFFFFFFFFF' AS LONG256), 2::TIMESTAMP),
                            (CAST(NULL AS LONG256),                 3::TIMESTAMP)
                        """);
                serverMain.awaitTable("t");

                final List<long[]> words = new ArrayList<>();
                try (QwpQueryClient client = QwpQueryClient.fromConfig("ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    client.execute("SELECT l256 FROM t", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            for (int r = 0; r < batch.getRowCount(); r++) {
                                if (batch.isNull(0, r)) {
                                    words.add(null);
                                } else {
                                    words.add(new long[]{
                                            batch.getLong256Word(0, r, 0),
                                            batch.getLong256Word(0, r, 1),
                                            batch.getLong256Word(0, r, 2),
                                            batch.getLong256Word(0, r, 3)
                                    });
                                }
                            }
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail(message);
                        }
                    });
                }
                // 0x01 = {1, 0, 0, 0}
                Assert.assertEquals(1L, words.getFirst()[0]);
                Assert.assertEquals(0L, words.getFirst()[1]);
                Assert.assertEquals(0L, words.get(0)[2]);
                Assert.assertEquals(0L, words.get(0)[3]);
                // 0xFFFF...FF in low word
                Assert.assertEquals(-1L, words.get(1)[0]);
                Assert.assertEquals(0L, words.get(1)[1]);
                Assert.assertNull(words.get(2));
            }
        });
    }

    @Test
    public void testLong256Sink() throws Exception {
        // Same data as testLong256, but read via the single-call Long256Sink API.
        // A reusable Long256Impl is handed to the batch for every row; the
        // four-word copy happens inside getLong256 with one virtual dispatch.
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute("CREATE TABLE t(l256 LONG256, part_ts TIMESTAMP) "
                        + "TIMESTAMP(part_ts) PARTITION BY DAY WAL");
                serverMain.execute("""
                        INSERT INTO t VALUES
                            (CAST('0x01' AS LONG256),               1::TIMESTAMP),
                            (CAST('0xFFFFFFFFFFFFFFFF' AS LONG256), 2::TIMESTAMP),
                            (CAST(NULL AS LONG256),                 3::TIMESTAMP)
                        """);
                serverMain.awaitTable("t");

                final List<long[]> words = new ArrayList<>();
                final boolean[] hits = new boolean[3];
                try (QwpQueryClient client = QwpQueryClient.fromConfig("ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    client.execute("SELECT l256 FROM t", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            // Reused across rows -- the whole point of the sink API.
                            Long256Impl sink = new Long256Impl();
                            for (int r = 0; r < batch.getRowCount(); r++) {
                                hits[r] = batch.getLong256(0, r, sink);
                                words.add(hits[r]
                                        ? new long[]{sink.getLong0(), sink.getLong1(), sink.getLong2(), sink.getLong3()}
                                        : null);
                            }
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail(message);
                        }
                    });
                }
                Assert.assertArrayEquals(new long[]{1L, 0L, 0L, 0L}, words.get(0));
                Assert.assertArrayEquals(new long[]{-1L, 0L, 0L, 0L}, words.get(1));
                Assert.assertNull(words.get(2));
                Assert.assertTrue(hits[0]);
                Assert.assertTrue(hits[1]);
                Assert.assertFalse("getLong256 must return false for NULL row", hits[2]);
            }
        });
    }

    @Test
    public void testShort() throws Exception {
        runRoundTrip(
                "CREATE TABLE t(s SHORT)",
                "INSERT INTO t VALUES (-32768), (32767), (0), (NULL)",
                QwpConstants.TYPE_SHORT,
                4,
                (batch, results) -> {
                    for (int r = 0; r < 4; r++) results.add(batch.getShortValue(0, r));
                },
                rows -> {
                    Assert.assertEquals((short) -32768, rows.get(0));
                    Assert.assertEquals((short) 32767, rows.get(1));
                    Assert.assertEquals((short) 0, rows.get(2));
                    Assert.assertEquals((short) 0, rows.get(3));
                }
        );
    }

    @Test
    public void testString() throws Exception {
        // Cover: empty, ASCII, multi-byte UTF-8, large, NULL.
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute("CREATE TABLE t(s STRING, part_ts TIMESTAMP) "
                        + "TIMESTAMP(part_ts) PARTITION BY DAY WAL");
                serverMain.execute("""
                        INSERT INTO t VALUES
                            ('',     1::TIMESTAMP),
                            ('hello', 2::TIMESTAMP),
                            ('héllo wörld', 3::TIMESTAMP),
                            ('日本語', 4::TIMESTAMP),
                            ('a string of moderate length to exercise heap growth', 5::TIMESTAMP),
                            (NULL,   6::TIMESTAMP)
                        """);
                serverMain.awaitTable("t");

                final List<String> values = new ArrayList<>();
                final boolean[] nullSeen = new boolean[6];
                final byte[] wireType = {0};
                try (QwpQueryClient client = QwpQueryClient.fromConfig("ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    client.execute("SELECT s FROM t", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            wireType[0] = batch.getColumnWireType(0);
                            for (int r = 0; r < batch.getRowCount(); r++) {
                                nullSeen[r] = batch.isNull(0, r);
                                values.add(nullSeen[r] ? null : batch.getString(0, r));
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
                // Egress advertises VARCHAR for both QuestDB STRING and VARCHAR columns.
                Assert.assertEquals(QwpConstants.TYPE_VARCHAR, wireType[0]);
                Assert.assertEquals("", values.get(0));
                Assert.assertEquals("hello", values.get(1));
                Assert.assertEquals("héllo wörld", values.get(2));
                Assert.assertEquals("日本語", values.get(3));
                Assert.assertEquals("a string of moderate length to exercise heap growth", values.get(4));
                Assert.assertNull(values.get(5));
                Assert.assertTrue(nullSeen[5]);
            }
        });
    }

    @Test
    public void testStringDualView() throws Exception {
        // Realistic dual-view usage: run-length-style transition counting within a
        // single batch. A single flyweight would be clobbered by the second call,
        // so the caller needs getStrA(current row) + getStrB(previous row) to hold
        // both bytes concurrently for the comparison.
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute("CREATE TABLE t(sym SYMBOL, part_ts TIMESTAMP) "
                        + "TIMESTAMP(part_ts) PARTITION BY DAY WAL");
                // Expected transitions in designated-ts order:
                //   A,A,B,B,B,C,A -> A->B at idx 2, B->C at idx 5, C->A at idx 6 = 3 transitions
                serverMain.execute("""
                        INSERT INTO t VALUES
                            ('A', 1::TIMESTAMP),
                            ('A', 2::TIMESTAMP),
                            ('B', 3::TIMESTAMP),
                            ('B', 4::TIMESTAMP),
                            ('B', 5::TIMESTAMP),
                            ('C', 6::TIMESTAMP),
                            ('A', 7::TIMESTAMP)
                        """);
                serverMain.awaitTable("t");

                final int[] transitions = {0};
                final int[] rowsSeen = {0};
                try (QwpQueryClient client = QwpQueryClient.fromConfig("ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    client.execute("SELECT sym FROM t ORDER BY part_ts", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            int n = batch.getRowCount();
                            for (int r = 1; r < n; r++) {
                                Utf8Sequence cur = batch.getStrA(0, r);
                                Utf8Sequence prev = batch.getStrB(0, r - 1);
                                if (!Utf8s.equals(prev, cur)) transitions[0]++;
                            }
                            rowsSeen[0] += n;
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail(message);
                        }
                    });
                }
                Assert.assertEquals(7, rowsSeen[0]);
                Assert.assertEquals(3, transitions[0]);
            }
        });
    }

    @Test
    public void testStringSink() throws Exception {
        // Exercises the zero-allocation getString(col, row, CharSink) overload on
        // both Utf16Sink (transcodes to UTF-16) and Utf8Sink (pass-through).
        // Multi-byte UTF-8 input catches any single-byte-only transcoding bug.
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute("CREATE TABLE t(s STRING, part_ts TIMESTAMP) "
                        + "TIMESTAMP(part_ts) PARTITION BY DAY WAL");
                serverMain.execute("""
                        INSERT INTO t VALUES
                            ('hello',       1::TIMESTAMP),
                            ('héllo wörld', 2::TIMESTAMP),
                            (NULL,          3::TIMESTAMP)
                        """);
                serverMain.awaitTable("t");

                final List<String> utf16Values = new ArrayList<>();
                final List<byte[]> utf8Values = new ArrayList<>();
                final boolean[] nullWrite = new boolean[3];
                try (QwpQueryClient client = QwpQueryClient.fromConfig("ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    client.execute("SELECT s FROM t", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            StringSink utf16 = new StringSink();
                            Utf8StringSink utf8 = new Utf8StringSink();
                            for (int r = 0; r < batch.getRowCount(); r++) {
                                utf16.clear();
                                utf8.clear();
                                boolean wrote16 = batch.getString(0, r, utf16);
                                boolean wrote8 = batch.getString(0, r, utf8);
                                // Must agree on null vs non-null
                                Assert.assertEquals("utf16 vs utf8 null agreement at row " + r, wrote16, wrote8);
                                if (wrote16) {
                                    utf16Values.add(utf16.toString());
                                    byte[] bytes = new byte[utf8.size()];
                                    for (int i = 0; i < utf8.size(); i++) bytes[i] = utf8.byteAt(i);
                                    utf8Values.add(bytes);
                                } else {
                                    utf16Values.add(null);
                                    utf8Values.add(null);
                                    nullWrite[r] = true;
                                }
                            }
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail(message);
                        }
                    });
                }
                Assert.assertEquals("hello", utf16Values.get(0));
                Assert.assertEquals("héllo wörld", utf16Values.get(1));
                Assert.assertNull(utf16Values.get(2));
                Assert.assertArrayEquals("hello".getBytes(java.nio.charset.StandardCharsets.UTF_8), utf8Values.get(0));
                Assert.assertArrayEquals("héllo wörld".getBytes(java.nio.charset.StandardCharsets.UTF_8), utf8Values.get(1));
                Assert.assertNull(utf8Values.get(2));
                Assert.assertTrue(nullWrite[2]);
            }
        });
    }

    @Test
    public void testSymbol() throws Exception {
        // SYMBOL: dict dedup (repeated value -> same dict entry), NULL, empty string, multiple unique.
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute("CREATE TABLE t(s SYMBOL, part_ts TIMESTAMP) "
                        + "TIMESTAMP(part_ts) PARTITION BY DAY WAL");
                serverMain.execute("INSERT INTO t VALUES ('A', 1::TIMESTAMP), ('B', 2::TIMESTAMP), "
                        + "('A', 3::TIMESTAMP), ('B', 4::TIMESTAMP), ('', 5::TIMESTAMP), "
                        + "('A', 6::TIMESTAMP), (NULL, 7::TIMESTAMP)");
                serverMain.awaitTable("t");

                final List<String> values = new ArrayList<>();
                final byte[] wireType = {0};
                try (QwpQueryClient client = QwpQueryClient.fromConfig("ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    client.execute("SELECT s FROM t", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            wireType[0] = batch.getColumnWireType(0);
                            for (int r = 0; r < batch.getRowCount(); r++) {
                                values.add(batch.isNull(0, r) ? null : batch.getString(0, r));
                            }
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail(message);
                        }
                    });
                }
                Assert.assertEquals(QwpConstants.TYPE_SYMBOL, wireType[0]);
                // QuestDB SYMBOL keeps '' and NULL distinct: '' is a valid zero-length
                // symbol value, NULL is carried through the null bitmap. Assert the full
                // seven rows.
                Assert.assertEquals(7, values.size());
                Assert.assertEquals("A", values.get(0));
                Assert.assertEquals("B", values.get(1));
                Assert.assertEquals("A", values.get(2));
                Assert.assertEquals("B", values.get(3));
                Assert.assertEquals("'' SYMBOL must surface as empty string, not null", "", values.get(4));
                Assert.assertEquals("A", values.get(5));
                Assert.assertNull("NULL SYMBOL must surface as null", values.get(6));
            }
        });
    }

    @Test
    public void testSymbolCache() throws Exception {
        // Pins the caching invariant for SYMBOL: a scan over N rows with K
        // distinct symbols returns K String instances, not N. Also exercises
        // the id-based API (getSymbolId / getSymbolForId / getSymbolDictSize)
        // and verifies getString shares the same cache.
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute("CREATE TABLE t(sym SYMBOL, part_ts TIMESTAMP) "
                        + "TIMESTAMP(part_ts) PARTITION BY DAY WAL");
                // 3 distinct symbols repeated across 6 rows + 1 NULL.
                serverMain.execute("""
                        INSERT INTO t VALUES
                            ('AAPL', 1::TIMESTAMP),
                            ('MSFT', 2::TIMESTAMP),
                            ('AAPL', 3::TIMESTAMP),
                            ('GOOG', 4::TIMESTAMP),
                            ('MSFT', 5::TIMESTAMP),
                            ('AAPL', 6::TIMESTAMP),
                            (NULL,   7::TIMESTAMP)
                        """);
                serverMain.awaitTable("t");

                final int[] dictSize = {-1};
                final int[] rowIds = new int[7];
                final String[] viaGetSymbol = new String[7];
                final String[] viaGetString = new String[7];
                final java.util.IdentityHashMap<String, Boolean> distinctInstances = new java.util.IdentityHashMap<>();
                try (QwpQueryClient client = QwpQueryClient.fromConfig("ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    client.execute("SELECT sym FROM t ORDER BY part_ts", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            dictSize[0] = batch.getSymbolDictSize(0);
                            for (int r = 0; r < batch.getRowCount(); r++) {
                                rowIds[r] = batch.getSymbolId(0, r);
                                viaGetSymbol[r] = batch.getSymbol(0, r);
                                viaGetString[r] = batch.getString(0, r);
                                if (viaGetSymbol[r] != null) distinctInstances.put(viaGetSymbol[r], Boolean.TRUE);
                            }
                            // getSymbolForId must resolve to the same cached instance as getSymbol(col, row).
                            for (int r = 0; r < batch.getRowCount(); r++) {
                                if (rowIds[r] < 0) continue;
                                Assert.assertSame("getSymbolForId must hit the per-dict cache",
                                        viaGetSymbol[r], batch.getSymbolForId(0, rowIds[r]));
                            }
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail(message);
                        }
                    });
                }
                Assert.assertEquals(3, dictSize[0]);
                Assert.assertEquals("AAPL", viaGetSymbol[0]);
                Assert.assertEquals("MSFT", viaGetSymbol[1]);
                Assert.assertEquals("AAPL", viaGetSymbol[2]);
                Assert.assertEquals("GOOG", viaGetSymbol[3]);
                Assert.assertEquals("MSFT", viaGetSymbol[4]);
                Assert.assertEquals("AAPL", viaGetSymbol[5]);
                Assert.assertNull(viaGetSymbol[6]);
                Assert.assertEquals(-1, rowIds[6]);
                // Identity: every row sharing a dict entry returns the SAME String instance.
                Assert.assertSame("rows 0/2/5 share dict entry for 'AAPL'", viaGetSymbol[0], viaGetSymbol[2]);
                Assert.assertSame(viaGetSymbol[0], viaGetSymbol[5]);
                Assert.assertSame("rows 1/4 share dict entry for 'MSFT'", viaGetSymbol[1], viaGetSymbol[4]);
                // getString on a SYMBOL column must hit the same cache as getSymbol.
                for (int r = 0; r < 6; r++) {
                    Assert.assertSame("getString must delegate to symbol cache at row " + r,
                            viaGetSymbol[r], viaGetString[r]);
                }
                // 6 non-null rows, 3 distinct dict entries, 3 cached String instances.
                Assert.assertEquals(3, distinctInstances.size());
            }
        });
    }

    @Test
    public void testTimestamp() throws Exception {
        runRoundTrip(
                "CREATE TABLE t(ts TIMESTAMP)",
                """
                        INSERT INTO t VALUES
                            ('1970-01-01T00:00:00.000000Z'),
                            ('2024-01-01T00:00:00.000000Z'),
                            ('2262-04-11T23:47:16.854775Z'),
                            (NULL)
                        """,
                QwpConstants.TYPE_TIMESTAMP,
                4,
                (batch, results) -> {
                    for (int r = 0; r < 4; r++) {
                        results.add(new Object[]{batch.isNull(0, r), batch.getLongValue(0, r)});
                    }
                },
                rows -> {
                    Assert.assertEquals(0L, ((Object[]) rows.get(0))[1]);
                    Assert.assertFalse("2024 timestamp must be non-null", (Boolean) ((Object[]) rows.get(1))[0]);
                    Assert.assertTrue((Long) ((Object[]) rows.get(1))[1] > 0L);
                    Assert.assertFalse("2262 timestamp must be non-null", (Boolean) ((Object[]) rows.get(2))[0]);
                    Assert.assertEquals(Boolean.TRUE, ((Object[]) rows.get(3))[0]);
                }
        );
    }

    @Test
    public void testTimestampNanos() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute("CREATE TABLE t(ts TIMESTAMP_NS) TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("""
                        INSERT INTO t VALUES
                            ('2024-01-01T00:00:00.000000001Z'),
                            ('2024-01-01T00:00:00.000000002Z'),
                            ('2024-01-01T00:00:00.000000999Z')
                        """);
                serverMain.awaitTxn("t", 1);

                final LongList values = new LongList();
                final byte[] wireType = {0};
                try (QwpQueryClient client = QwpQueryClient.fromConfig("ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    client.execute("SELECT ts FROM t ORDER BY ts", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            wireType[0] = batch.getColumnWireType(0);
                            for (int r = 0; r < batch.getRowCount(); r++) values.add(batch.getLongValue(0, r));
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail(message);
                        }
                    });
                }
                Assert.assertEquals(QwpConstants.TYPE_TIMESTAMP_NANOS, wireType[0]);
                Assert.assertEquals(3, values.size());
                Assert.assertEquals(1L, values.getQuick(1) - values.getQuick(0));
                Assert.assertEquals(997L, values.getQuick(2) - values.getQuick(1));
            }
        });
    }

    @Test
    public void testUuid() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute("CREATE TABLE t(u UUID, part_ts TIMESTAMP) "
                        + "TIMESTAMP(part_ts) PARTITION BY DAY WAL");
                serverMain.execute("""
                        INSERT INTO t VALUES
                            ('00000000-0000-0000-0000-000000000000', 1::TIMESTAMP),
                            ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 2::TIMESTAMP),
                            ('ffffffff-ffff-ffff-ffff-fffffffffffe', 3::TIMESTAMP),
                            (CAST(NULL AS UUID),                     4::TIMESTAMP)
                        """);
                serverMain.awaitTable("t");

                final List<long[]> values = new ArrayList<>();
                final byte[] wireType = {0};
                try (QwpQueryClient client = QwpQueryClient.fromConfig("ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    client.execute("SELECT u FROM t", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            wireType[0] = batch.getColumnWireType(0);
                            for (int r = 0; r < batch.getRowCount(); r++) {
                                if (batch.isNull(0, r)) {
                                    values.add(null);
                                } else {
                                    values.add(new long[]{batch.getUuidLo(0, r), batch.getUuidHi(0, r)});
                                }
                            }
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail(message);
                        }
                    });
                }
                Assert.assertEquals(QwpConstants.TYPE_UUID, wireType[0]);
                // 00000000-...0 = both halves zero (this *is* the all-zeros UUID, treated as non-null
                // because QuestDB UUID NULL is both halves Long.MIN_VALUE, not zero).
                Assert.assertNotNull(values.getFirst());
                Assert.assertEquals(0L, values.get(0)[0]);
                Assert.assertEquals(0L, values.get(0)[1]);
                // mid-range UUID
                Assert.assertNotNull(values.get(1));
                // ffff...fe (avoid exact ffff...ff which might overlap with sentinel)
                Assert.assertNotNull(values.get(2));
                Assert.assertNull(values.get(3));
            }
        });
    }

    @Test
    public void testUuidSink() throws Exception {
        // Same data as testUuid, but read via the single-call Uuid sink API.
        // Pins that getUuid returns false for NULL rows and leaves the sink
        // untouched (the caller's previous value stays intact).
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute("CREATE TABLE t(u UUID, part_ts TIMESTAMP) "
                        + "TIMESTAMP(part_ts) PARTITION BY DAY WAL");
                serverMain.execute("""
                        INSERT INTO t VALUES
                            ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 1::TIMESTAMP),
                            (CAST(NULL AS UUID),                     2::TIMESTAMP)
                        """);
                serverMain.awaitTable("t");

                final long[] loSeen = new long[2];
                final long[] hiSeen = new long[2];
                final boolean[] hits = new boolean[2];
                try (QwpQueryClient client = QwpQueryClient.fromConfig("ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    client.execute("SELECT u FROM t", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            Uuid sink = new Uuid();
                            for (int r = 0; r < batch.getRowCount(); r++) {
                                // Re-seed with a sentinel before every call so "untouched on NULL"
                                // is detectable independently of the previous row's contents.
                                sink.setAll(0xDEADBEEFL, 0xCAFEBABEL);
                                hits[r] = batch.getUuid(0, r, sink);
                                loSeen[r] = sink.getLo();
                                hiSeen[r] = sink.getHi();
                            }
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail(message);
                        }
                    });
                }
                Assert.assertTrue(hits[0]);
                // Cross-check against the part-wise accessors via parsed literal:
                // 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11' -> hi=0xa0eebc999c0b4ef8, lo=0xbb6d6bb9bd380a11
                Assert.assertEquals(0xa0eebc999c0b4ef8L, hiSeen[0]);
                Assert.assertEquals(0xbb6d6bb9bd380a11L, loSeen[0]);
                // NULL row: sink untouched, still holds the pre-seed values.
                Assert.assertFalse("getUuid must return false for NULL row", hits[1]);
                Assert.assertEquals(0xDEADBEEFL, loSeen[1]);
                Assert.assertEquals(0xCAFEBABEL, hiSeen[1]);
            }
        });
    }

    @Test
    public void testVarchar() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute("CREATE TABLE t(v VARCHAR, part_ts TIMESTAMP) "
                        + "TIMESTAMP(part_ts) PARTITION BY DAY WAL");
                serverMain.execute("""
                        INSERT INTO t VALUES
                            ('',     1::TIMESTAMP),
                            ('plain', 2::TIMESTAMP),
                            ('héllo', 3::TIMESTAMP),
                            (NULL,   4::TIMESTAMP)
                        """);
                serverMain.awaitTable("t");

                final List<String> values = new ArrayList<>();
                final byte[] wireType = {0};
                try (QwpQueryClient client = QwpQueryClient.fromConfig("ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    client.execute("SELECT v FROM t", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            wireType[0] = batch.getColumnWireType(0);
                            for (int r = 0; r < batch.getRowCount(); r++) {
                                values.add(batch.getString(0, r));
                            }
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail(message);
                        }
                    });
                }
                Assert.assertEquals(QwpConstants.TYPE_VARCHAR, wireType[0]);
                Assert.assertEquals("", values.get(0));
                Assert.assertEquals("plain", values.get(1));
                Assert.assertEquals("héllo", values.get(2));
                Assert.assertNull(values.get(3));
            }
        });
    }

    /**
     * Rewrites {@code CREATE TABLE t(<cols>)} into a WAL table by appending a
     * trailing {@code part_ts TIMESTAMP} column and designating it. Every call
     * site passes a plain {@code CREATE TABLE t(...)} without partitioning; this
     * centralises the conversion so individual tests stay focused on the type
     * under test.
     */
    private static String walifyCreate(String createSql) {
        int lastClose = createSql.lastIndexOf(')');
        return createSql.substring(0, lastClose)
                + ", part_ts TIMESTAMP) TIMESTAMP(part_ts) PARTITION BY DAY WAL";
    }

    /**
     * Appends a {@code , N::TIMESTAMP} literal before the closing paren of each
     * top-level row in an {@code INSERT INTO t VALUES (...), (...)} statement.
     * Nested parens inside casts (e.g. {@code CAST(0.0/0.0 AS FLOAT)}) are
     * tracked via depth counting so we only touch the row-level parens.
     */
    private static String walifyValuesInsert(String insertSql) {
        StringBuilder sb = new StringBuilder(insertSql.length() + 32);
        int cursor = 0;
        int rowIdx = 0;
        while (cursor < insertSql.length()) {
            int open = insertSql.indexOf('(', cursor);
            if (open < 0) {
                sb.append(insertSql, cursor, insertSql.length());
                break;
            }
            sb.append(insertSql, cursor, open);
            // Find the matching top-level ')'.
            int depth = 1;
            int i = open + 1;
            while (i < insertSql.length() && depth > 0) {
                char c = insertSql.charAt(i);
                if (c == '(') depth++;
                else if (c == ')') {
                    depth--;
                    if (depth == 0) break;
                }
                i++;
            }
            rowIdx++;
            sb.append(insertSql, open, i).append(", ").append(rowIdx).append("::TIMESTAMP)");
            cursor = i + 1;
        }
        return sb.toString();
    }

    /**
     * Common harness: create + insert, run a SELECT, collect per-row values via the supplied
     * {@code rowExtractor}, then assert via {@code asserter}. Verifies the wire-type code
     * surfaces as expected and the row count matches.
     */
    private void runRoundTrip(
            String createSql,
            String insertSql,
            byte expectedWireType,
            int expectedRowCount,
            RowExtractor rowExtractor,
            Asserter asserter
    ) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute(walifyCreate(createSql));
                serverMain.execute(walifyValuesInsert(insertSql));
                serverMain.awaitTable("t");

                final List<Object> rows = new ArrayList<>();
                final byte[] wireType = {0};
                try (QwpQueryClient client = QwpQueryClient.fromConfig("ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    // Explicit column projection over SELECT * so the part_ts column added
                    // by walifyCreate stays hidden from the test's row extractor.
                    client.execute("SELECT " + extractColumnNames(createSql) + " FROM t", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            wireType[0] = batch.getColumnWireType(0);
                            rowExtractor.extract(batch, rows);
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("egress query error: " + message);
                        }
                    });
                }
                Assert.assertEquals("wire type code", expectedWireType, wireType[0]);
                Assert.assertEquals("row count", expectedRowCount, rows.size());
                asserter.run(rows);
            }
        });
    }

    /**
     * Extracts the comma-separated column names from the original (non-walified)
     * {@code CREATE TABLE t(name TYPE, name TYPE, ...)} so the egress SELECT
     * returns only the columns the test cares about, matching the pre-WAL shape.
     */
    private static String extractColumnNames(String createSql) {
        int open = createSql.indexOf('(');
        int close = createSql.lastIndexOf(')');
        String cols = createSql.substring(open + 1, close);
        StringBuilder sb = new StringBuilder();
        int depth = 0;
        int start = 0;
        for (int i = 0; i <= cols.length(); i++) {
            char c = i < cols.length() ? cols.charAt(i) : ',';
            if (c == '(') depth++;
            else if (c == ')') depth--;
            else if (c == ',' && depth == 0) {
                String segment = cols.substring(start, i).trim();
                if (!segment.isEmpty()) {
                    int sp = firstWhitespace(segment);
                    String name = sp < 0 ? segment : segment.substring(0, sp);
                    if (!sb.isEmpty()) sb.append(", ");
                    sb.append(name);
                }
                start = i + 1;
            }
        }
        return sb.toString();
    }

    private static int firstWhitespace(String s) {
        for (int i = 0; i < s.length(); i++) {
            if (Character.isWhitespace(s.charAt(i))) return i;
        }
        return -1;
    }

    @FunctionalInterface
    private interface Asserter {
        void run(List<Object> rows);
    }

    @FunctionalInterface
    private interface RowExtractor {
        void extract(QwpColumnBatch batch, List<Object> sink);
    }
}
