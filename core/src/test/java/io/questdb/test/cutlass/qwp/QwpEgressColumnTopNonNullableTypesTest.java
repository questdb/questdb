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
 * (docs/qwp/wire-egress.md sec 11.5) states that these types "cannot carry
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
 */
public class QwpEgressColumnTopNonNullableTypesTest extends AbstractBootstrapTest {

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testBooleanColumnTopShipsFalseNotNulls() throws Exception {
        runColumnTopTest("BOOLEAN", batch -> {
            int n = batch.getRowCount();
            for (int r = 0; r < n; r++) {
                Assert.assertFalse(
                        "BOOLEAN row " + r + " must not report null (wire-egress.md sec 11.5)",
                        batch.isNull(0, r));
                Assert.assertFalse(
                        "BOOLEAN row " + r + " (column-top) must read as false",
                        batch.getBoolValue(0, r));
            }
        });
    }

    @Test
    public void testByteColumnTopShipsZeroNotNulls() throws Exception {
        runColumnTopTest("BYTE", batch -> {
            int n = batch.getRowCount();
            for (int r = 0; r < n; r++) {
                Assert.assertFalse(
                        "BYTE row " + r + " must not report null (wire-egress.md sec 11.5)",
                        batch.isNull(0, r));
                Assert.assertEquals(
                        "BYTE row " + r + " (column-top) must read as 0",
                        (byte) 0, batch.getByteValue(0, r));
            }
        });
    }

    @Test
    public void testCharColumnTopShipsZeroNotNulls() throws Exception {
        runColumnTopTest("CHAR", batch -> {
            int n = batch.getRowCount();
            for (int r = 0; r < n; r++) {
                Assert.assertFalse(
                        "CHAR row " + r + " must not report null (wire-egress.md sec 11.5)",
                        batch.isNull(0, r));
                Assert.assertEquals(
                        "CHAR row " + r + " (column-top) must read as (char) 0",
                        (char) 0, batch.getCharValue(0, r));
            }
        });
    }

    @Test
    public void testShortColumnTopShipsZeroNotNulls() throws Exception {
        runColumnTopTest("SHORT", batch -> {
            int n = batch.getRowCount();
            for (int r = 0; r < n; r++) {
                Assert.assertFalse(
                        "SHORT row " + r + " must not report null (wire-egress.md sec 11.5)",
                        batch.isNull(0, r));
                Assert.assertEquals(
                        "SHORT row " + r + " (column-top) must read as 0",
                        (short) 0, batch.getShortValue(0, r));
            }
        });
    }

    /**
     * Drives the column-top scenario for {@code typeName}: creates a WAL
     * table with only a TIMESTAMP, ingests a small batch (so the partition
     * has backing storage), ALTER TABLE ADD COLUMN of the type under test,
     * then SELECTs the new column over QWP and runs {@code asserter} on
     * each batch. The pre-existing rows have no backing storage for the
     * added column, so the columnar fast-path's {@code base == 0} branch
     * fires for every frame -- the buggy code path before the fix.
     */
    private void runColumnTopTest(String typeName, BatchAsserter asserter) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute("CREATE TABLE ct(ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("INSERT INTO ct SELECT x::TIMESTAMP FROM long_sequence(7)");
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
                            asserter.check(batch);
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
                Assert.assertEquals(7, totalRows[0]);
            }
        });
    }

    @FunctionalInterface
    private interface BatchAsserter {
        void check(QwpColumnBatch batch);
    }
}
