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

import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;
import io.questdb.client.cutlass.qwp.client.QwpQueryClient;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Regression for the columnar egress fast-path's handling of column-tops on
 * non-nullable wire types (BOOLEAN, BYTE, SHORT, CHAR). The wire spec
 * (https://questdb.com/docs/connect/wire-protocols/qwp-egress-websocket/)
 * states that these types "cannot carry
 * NULL in QuestDB; INSERT NULL stores false/0 and the wire row has the null
 * bitmap bit clear."
 * <p>
 * The columnar code path in {@code QwpResultBatchBuffer.appendPageFrame}
 * used to call {@code fillNulls} for these types when
 * {@code frame.getPageAddress(ci)} returned 0 (page-frame has no backing
 * storage for the column -- the column-top condition that follows
 * ALTER TABLE ADD COLUMN on a table that already held rows in older
 * partitions). {@code fillNulls} routes to
 * {@code QwpColumnScratch.appendNullColumn(n)}, which set bits in the null
 * bitmap and incremented {@code nullCount}; {@code emitColumn} then wrote
 * {@code null_flag = 0x01} plus the bitmap, violating the spec. Each test
 * below executes the trigger scenario end-to-end and pins both
 * {@code isNull == false} and the zero-value semantics so the server can
 * never silently regress back to bitmap mode for these types.
 * <p>
 * Row count {@link #COLUMN_TOP_ROWS} is chosen to span many bytes of the
 * BOOLEAN bitmap and to leave the last byte only partially used (8195 bits
 * = 1024 bytes plus 3 trailing bits), so the column-top fill paths exercise
 * multi-byte memset, capacity growth, and a non-byte-aligned tail.
 */
public class QwpEgressColumnTopNonNullableTypesTest extends AbstractBootstrapTest {

    private static final int COLUMN_TOP_ROWS = 8195;

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testBooleanColumnTopAndBackedMixed() throws Exception {
        // Mixes a multi-byte column-top fill with a backed-storage frame that
        // starts at a non-byte-aligned bit position. With 1023 column-top
        // rows, the boolean scratch arrives at the backed frame with
        // nonNullCount = 1023 (firstBitInFirstByte = 7), forcing
        // appendColumnBoolean to OR its first row into the partial last byte
        // left behind by appendColumnBooleanZero. Verifies both regions
        // round-trip correctly.
        final int columnTopRows = 1023;
        final int backedRows = 2049;
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute("CREATE TABLE ct(ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                // Day 0: 1023 rows with no col backing.
                serverMain.execute("INSERT INTO ct SELECT x::TIMESTAMP FROM long_sequence(" + columnTopRows + ")");
                serverMain.awaitTable("ct");
                serverMain.execute("ALTER TABLE ct ADD COLUMN col BOOLEAN");
                serverMain.awaitTable("ct");
                // Day 1: backed rows with alternating col values starting at x=1 (false), x=2 (true), ...
                serverMain.execute(
                        "INSERT INTO ct(ts, col) SELECT (86_400_000_000 + x)::TIMESTAMP, " +
                                "CASE WHEN x % 2 = 0 THEN true ELSE false END FROM long_sequence(" + backedRows + ")");
                serverMain.awaitTable("ct");

                final int[] totalRows = {0};
                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    client.execute("SELECT col FROM ct ORDER BY ts", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            int n = batch.getRowCount();
                            for (int r = 0; r < n; r++) {
                                int abs = totalRows[0] + r;
                                Assert.assertFalse(
                                        "row " + abs + " must not report null (wire-egress.md sec 11.5)",
                                        batch.isNull(0, r));
                                if (abs < columnTopRows) {
                                    Assert.assertFalse(
                                            "column-top row " + abs + " must read as false",
                                            batch.getBoolValue(0, r));
                                } else {
                                    // Backed x = abs - columnTopRows + 1; col = (x % 2 == 0)
                                    int x = abs - columnTopRows + 1;
                                    Assert.assertEquals(
                                            "backed row " + abs + " (x=" + x + ")",
                                            (x % 2) == 0, batch.getBoolValue(0, r));
                                }
                            }
                            totalRows[0] += n;
                        }

                        @Override
                        public void onEnd(long rows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("egress error: " + message);
                        }
                    });
                }
                Assert.assertEquals(columnTopRows + backedRows, totalRows[0]);
            }
        });
    }

    @Test
    public void testBooleanColumnTopShipsFalseNotNulls() throws Exception {
        runColumnTopTest("BOOLEAN", (batch, rowsBefore) -> {
            int n = batch.getRowCount();
            for (int r = 0; r < n; r++) {
                int abs = rowsBefore + r;
                Assert.assertFalse(
                        "BOOLEAN row " + abs + " must not report null (wire-egress.md sec 11.5)",
                        batch.isNull(0, r));
                Assert.assertFalse(
                        "BOOLEAN row " + abs + " (column-top) must read as false",
                        batch.getBoolValue(0, r));
            }
        });
    }

    @Test
    public void testByteColumnTopShipsZeroNotNulls() throws Exception {
        runColumnTopTest("BYTE", (batch, rowsBefore) -> {
            int n = batch.getRowCount();
            for (int r = 0; r < n; r++) {
                int abs = rowsBefore + r;
                Assert.assertFalse(
                        "BYTE row " + abs + " must not report null (wire-egress.md sec 11.5)",
                        batch.isNull(0, r));
                Assert.assertEquals(
                        "BYTE row " + abs + " (column-top) must read as 0",
                        (byte) 0, batch.getByteValue(0, r));
            }
        });
    }

    @Test
    public void testCharColumnTopShipsZeroNotNulls() throws Exception {
        runColumnTopTest("CHAR", (batch, rowsBefore) -> {
            int n = batch.getRowCount();
            for (int r = 0; r < n; r++) {
                int abs = rowsBefore + r;
                Assert.assertFalse(
                        "CHAR row " + abs + " must not report null (wire-egress.md sec 11.5)",
                        batch.isNull(0, r));
                Assert.assertEquals(
                        "CHAR row " + abs + " (column-top) must read as (char) 0",
                        (char) 0, batch.getCharValue(0, r));
            }
        });
    }

    @Test
    public void testShortColumnTopShipsZeroNotNulls() throws Exception {
        runColumnTopTest("SHORT", (batch, rowsBefore) -> {
            int n = batch.getRowCount();
            for (int r = 0; r < n; r++) {
                int abs = rowsBefore + r;
                Assert.assertFalse(
                        "SHORT row " + abs + " must not report null (wire-egress.md sec 11.5)",
                        batch.isNull(0, r));
                Assert.assertEquals(
                        "SHORT row " + abs + " (column-top) must read as 0",
                        (short) 0, batch.getShortValue(0, r));
            }
        });
    }

    /**
     * Drives the column-top scenario for {@code typeName}: creates a WAL
     * table with only a TIMESTAMP, ingests {@link #COLUMN_TOP_ROWS} rows
     * (so the partition has backing storage), ALTER TABLE ADD COLUMN of the
     * type under test, then SELECTs the new column over QWP and runs
     * {@code asserter} on each batch. The pre-existing rows have no backing
     * storage for the added column, so the columnar fast-path's
     * {@code base == 0} branch fires for every frame -- the buggy code path
     * before the fix. The row count crosses many byte boundaries (so the
     * multi-byte memset / memcpy paths run) and is not a multiple of 8
     * (so the BOOLEAN bitmap's tail byte is only partially written).
     */
    private void runColumnTopTest(String typeName, BatchAsserter asserter) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute("CREATE TABLE ct(ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("INSERT INTO ct SELECT x::TIMESTAMP FROM long_sequence(" + COLUMN_TOP_ROWS + ")");
                serverMain.awaitTable("ct");
                serverMain.execute("ALTER TABLE ct ADD COLUMN col " + typeName);
                serverMain.awaitTable("ct");

                final int[] totalRows = {0};
                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    client.execute("SELECT col FROM ct", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            asserter.check(batch, totalRows[0]);
                            totalRows[0] += batch.getRowCount();
                        }

                        @Override
                        public void onEnd(long rows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("egress error: " + message);
                        }
                    });
                }
                Assert.assertEquals(COLUMN_TOP_ROWS, totalRows[0]);
            }
        });
    }

    @FunctionalInterface
    private interface BatchAsserter {
        void check(QwpColumnBatch batch, int rowsBefore);
    }
}
