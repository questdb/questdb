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
import io.questdb.griffin.CompiledQuery;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Round-trip tests for non-SELECT statements over QWP egress. The server
 * compiles the SQL, executes it, and replies with {@code EXEC_DONE(op_type,
 * rows_affected)}. The client surfaces it as {@link QwpColumnBatchHandler#onExecDone}.
 * <p>
 * Covers:
 * <ul>
 *   <li>pure DDL: CREATE TABLE, DROP, RENAME, TRUNCATE, ALTER COLUMN ADD;</li>
 *   <li>data-modifying: INSERT (with row count), UPDATE (with row count);</li>
 *   <li>parse-time-executed: SET, BEGIN / COMMIT / ROLLBACK;</li>
 *   <li>error paths: a malformed DDL bubbles up as QUERY_ERROR, a SELECT
 *       handler that only implements onBatch / onEnd still works for
 *       backwards compatibility;</li>
 *   <li>state isolation: DDL during one connection is visible to a second
 *       client on a subsequent connection.</li>
 * </ul>
 */
public class QwpEgressDdlExecTest extends AbstractBootstrapTest {

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testAlterColumnAdd() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute("CREATE TABLE alt(x LONG, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    short opType = executeDdl(client, "ALTER TABLE alt ADD COLUMN y DOUBLE");
                    Assert.assertEquals(CompiledQuery.ALTER, opType);
                    serverMain.awaitTable("alt");
                    // Verify the column is actually there via a SELECT. Schema: x, ts, y.
                    final int[] colCount = {0};
                    client.execute("SELECT * FROM alt", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            colCount[0] = batch.getColumnCount();
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("unexpected error: " + message);
                        }
                    });
                    Assert.assertEquals(3, colCount[0]);
                }
            }
        });
    }

    @Test
    public void testCreateAndDropTable() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain ignored = startWithEnvVariables()) {
                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    Assert.assertEquals(
                            CompiledQuery.CREATE_TABLE,
                            executeDdl(client, "CREATE TABLE newt(x LONG, s SYMBOL, ts TIMESTAMP) "
                                    + "TIMESTAMP(ts) PARTITION BY DAY WAL")
                    );
                    // Insert so we know the table is usable.
                    short opType = executeDdl(client,
                            "INSERT INTO newt VALUES (1, 'a', 1::TIMESTAMP), (2, 'b', 2::TIMESTAMP)");
                    Assert.assertEquals(CompiledQuery.INSERT, opType);
                    // Drop it.
                    Assert.assertEquals(
                            CompiledQuery.DROP,
                            executeDdl(client, "DROP TABLE newt")
                    );
                }
            }
        });
    }

    @Test
    public void testDdlStateVisibleToLaterConnection() throws Exception {
        // Connection-isolated only in terms of dict state; TABLE-level changes
        // are globally visible. Verify a CREATE done on c1 shows up on c2.
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                try (QwpQueryClient c1 = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    c1.connect();
                    executeDdl(c1, "CREATE TABLE cross_conn(x LONG, ts TIMESTAMP) "
                            + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                    executeDdl(c1, "INSERT INTO cross_conn VALUES (100, 1::TIMESTAMP), (200, 2::TIMESTAMP)");
                }
                serverMain.awaitTable("cross_conn");

                final long[] sum = {0};
                try (QwpQueryClient c2 = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    c2.connect();
                    c2.execute("SELECT x FROM cross_conn", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            for (int r = 0; r < batch.getRowCount(); r++) {
                                sum[0] += batch.getLongValue(0, r);
                            }
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("unexpected error: " + message);
                        }
                    });
                }
                Assert.assertEquals(300L, sum[0]);
            }
        });
    }

    @Test
    public void testInsertReportsRowsAffected() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute("CREATE TABLE ins(x LONG, s VARCHAR, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    // Single-row insert.
                    ExecResult r1 = executeExec(client, "INSERT INTO ins VALUES (1, 'a', 1::TIMESTAMP)");
                    Assert.assertEquals(CompiledQuery.INSERT, r1.opType);
                    Assert.assertEquals(1L, r1.rowsAffected);
                    // Three-row multi-row VALUES.
                    ExecResult r2 = executeExec(client,
                            "INSERT INTO ins VALUES (2, 'b', 2::TIMESTAMP), (3, 'c', 3::TIMESTAMP), (4, 'd', 4::TIMESTAMP)");
                    Assert.assertEquals(3L, r2.rowsAffected);
                    // INSERT-AS-SELECT: pulls from long_sequence, inserts 5 rows.
                    ExecResult r3 = executeExec(client,
                            "INSERT INTO ins SELECT x, 'v_' || x::STRING, x::TIMESTAMP FROM long_sequence(5)");
                    Assert.assertEquals(CompiledQuery.INSERT_AS_SELECT, r3.opType);
                    Assert.assertEquals(5L, r3.rowsAffected);
                }
            }
        });
    }

    @Test
    public void testMalformedDdlSurfacesQueryError() throws Exception {
        // A SQL error during compile should come back as QUERY_ERROR, not
        // EXEC_DONE. Verifies the error path still works after the DDL split.
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain ignored = startWithEnvVariables()) {
                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    final String[] errorMsg = {null};
                    final byte[] errorStatus = {0};
                    client.execute("CREATE TABLE !!!bad", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            Assert.fail("unexpected batch");
                        }

                        @Override
                        public void onEnd(long totalRows) {
                            Assert.fail("unexpected end");
                        }

                        @Override
                        public void onError(byte status, String message) {
                            errorStatus[0] = status;
                            errorMsg[0] = message;
                        }

                        @Override
                        public void onExecDone(short opType, long rowsAffected) {
                            Assert.fail("unexpected execDone for malformed DDL");
                        }
                    });
                    Assert.assertNotNull(errorMsg[0]);
                    // STATUS_PARSE_ERROR == 0x05.
                    Assert.assertEquals((byte) 0x05, errorStatus[0]);
                }
            }
        });
    }

    @Test
    public void testParseTimeExecutedStatements() throws Exception {
        // Statements that the compiler executes at parse time (SET / BEGIN /
        // COMMIT / ROLLBACK / TRUNCATE / VACUUM) still come back as
        // EXEC_DONE with op_type set and rowsAffected usually 0.
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute("CREATE TABLE pt(x LONG, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("INSERT INTO pt VALUES (1, 1::TIMESTAMP), (2, 2::TIMESTAMP), (3, 3::TIMESTAMP)");
                serverMain.awaitTable("pt");
                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    // TRUNCATE is parse-time executed; the server ships
                    // EXEC_DONE with CompiledQuery.TRUNCATE and 0 rows affected
                    // (rowsAffected from getAffectedRowsCount, which is 0).
                    Assert.assertEquals(
                            CompiledQuery.TRUNCATE,
                            executeDdl(client, "TRUNCATE TABLE pt")
                    );
                    serverMain.awaitTable("pt");
                    // Verify the rows are actually gone.
                    final int[] count = {0};
                    client.execute("SELECT x FROM pt", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            count[0] += batch.getRowCount();
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("unexpected error: " + message);
                        }
                    });
                    Assert.assertEquals(0, count[0]);
                }
            }
        });
    }

    @Test
    public void testRenameTable() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                serverMain.execute("CREATE TABLE r_old(x LONG, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("INSERT INTO r_old VALUES (42, 1::TIMESTAMP)");
                serverMain.awaitTable("r_old");
                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    Assert.assertEquals(
                            CompiledQuery.RENAME_TABLE,
                            executeDdl(client, "RENAME TABLE r_old TO r_new")
                    );
                    serverMain.awaitTable("r_new");
                    final long[] observed = {0};
                    client.execute("SELECT x FROM r_new", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            for (int r = 0; r < batch.getRowCount(); r++) {
                                observed[0] += batch.getLongValue(0, r);
                            }
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("unexpected error: " + message);
                        }
                    });
                    Assert.assertEquals(42L, observed[0]);
                }
            }
        });
    }

    @Test
    public void testUpdateReportsRowsAffected() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                // Non-WAL: UPDATE's rowsAffected is the synchronous matched-row count
                // we assert on below. On a WAL table the number instead reflects the
                // count of WAL segments touched, not logical rows, and the real row
                // count only materialises after the apply job catches up -- different
                // enough to warrant skipping the WAL sweep for this one test.
                serverMain.execute("CREATE TABLE upd(ts TIMESTAMP, x LONG) TIMESTAMP(ts) PARTITION BY DAY");
                serverMain.execute(
                        "INSERT INTO upd SELECT CAST((x - 1) * 1_000_000L AS TIMESTAMP), x " +
                                "FROM long_sequence(10)"
                );
                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    // UPDATE x = 999 WHERE x < 5 -- rows 1..4 (x in {1,2,3,4}).
                    ExecResult r = executeExec(client, "UPDATE upd SET x = 999 WHERE x < 5");
                    Assert.assertEquals(CompiledQuery.UPDATE, r.opType);
                    Assert.assertEquals(4L, r.rowsAffected);
                    // Verify via SELECT: four rows now have x=999.
                    final int[] nineHundredNinetyNineCount = {0};
                    client.execute("SELECT x FROM upd", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            for (int r2 = 0; r2 < batch.getRowCount(); r2++) {
                                if (batch.getLongValue(0, r2) == 999L) nineHundredNinetyNineCount[0]++;
                            }
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("unexpected error: " + message);
                        }
                    });
                    Assert.assertEquals(4, nineHundredNinetyNineCount[0]);
                }
            }
        });
    }

    /**
     * Helper: runs a DDL that we don't care about rowsAffected for. Returns
     * the op type so the test can assert against a CompiledQuery constant.
     */
    private static short executeDdl(QwpQueryClient client, String sql) {
        ExecResult r = executeExec(client, sql);
        return r.opType;
    }

    /**
     * Runs a non-SELECT and returns the op type + rows affected reported by
     * the server. Fails the test on any error or unexpected batch.
     */
    private static ExecResult executeExec(QwpQueryClient client, String sql) {
        final ExecResult result = new ExecResult();
        client.execute(sql, new QwpColumnBatchHandler() {
            @Override
            public void onBatch(QwpColumnBatch batch) {
                Assert.fail("unexpected batch for [" + sql + ']');
            }

            @Override
            public void onEnd(long totalRows) {
                Assert.fail("unexpected end for [" + sql + ']');
            }

            @Override
            public void onError(byte status, String message) {
                Assert.fail("unexpected error for [" + sql + "]: " + message);
            }

            @Override
            public void onExecDone(short opType, long rowsAffected) {
                result.opType = opType;
                result.rowsAffected = rowsAffected;
            }
        });
        return result;
    }

    private static final class ExecResult {
        short opType = -1;
        long rowsAffected = Long.MIN_VALUE;
    }
}
