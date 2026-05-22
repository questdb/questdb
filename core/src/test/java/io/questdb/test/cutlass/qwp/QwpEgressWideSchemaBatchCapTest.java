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
 * Regression for the egress batch size cap. The streaming loop in {@code
 * QwpEgressUpgradeProcessor.streamResults} used to bound batches purely by
 * row count ({@code MAX_ROWS_PER_BATCH = 16_384}). Wide schemas (many
 * VARCHAR / BINARY / array columns, often grown via concurrent
 * ALTER TABLE ADD COLUMN) blew past the rawSocket send buffer in a single
 * batch, and the resulting {@code emitDeltaSection} / {@code emitTableBlock}
 * overflow surfaced as {@code STATUS_INTERNAL_ERROR (0x06)} mid-stream,
 * tearing down the query.
 * <p>
 * After the fix, the loop scales the row cap down for wide schemas via
 * {@code dynamicBatchRowCap}, splitting the stream into multiple batches
 * that each fit the send buffer. The residual overflow path (a single row
 * whose worst-case width still exceeds the buffer) now surfaces as
 * {@code STATUS_LIMIT_EXCEEDED (0x0B)}, which the C client reports as
 * {@code line_reader_error_server_limit_exceeded = 19} rather than the
 * misleading {@code server_internal_error = 16}.
 */
public class QwpEgressWideSchemaBatchCapTest extends AbstractBootstrapTest {

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    /**
     * Pins the residual overflow arm of the batch-cap fix: when a single
     * row's payload exceeds the rawSocket send buffer (even at the
     * {@code MIN_BATCH_ROWS} floor of {@code dynamicBatchRowCap}),
     * {@code emitTableBlock} returns -1 and the throw site converts that
     * into {@code CairoException.setOutOfMemory(true)}.
     * {@code mapErrorStatus} translates it to {@code STATUS_LIMIT_EXCEEDED
     * (0x0B)} on the wire, which the C client reports as
     * {@code line_reader_error_server_limit_exceeded = 19} rather than the
     * masking {@code server_internal_error = 16}.
     * <p>
     * Companion to {@link #testWideSchemaQueryDoesNotOverflowBatch} which
     * pins the happy path; together they cover both arms of the cap fix.
     */
    @Test
    public void testSingleRowExceedingSendBufferReportsLimitExceeded() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // The egress handshake reserves an upper bound for the SERVER_INFO
            // frame (~131 KB worst-case for the cluster + node id strings), so
            // the send buffer cannot drop below that and still complete the WS
            // upgrade. 160 KiB clears the handshake check; a 256-KiB VARCHAR
            // row then overflows the post-handshake RESULT_BATCH budget and
            // forces emitTableBlock to return -1.
            try (final TestServerMain serverMain = startWithEnvVariables(
                    "QDB_HTTP_SEND_BUFFER_SIZE", "163840")) {
                serverMain.execute("CREATE TABLE one_big_row(v VARCHAR, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                final StringBuilder bigValue = new StringBuilder(262_144);
                bigValue.repeat("x", 262_144);
                serverMain.execute("INSERT INTO one_big_row VALUES ('"
                        + bigValue + "'::VARCHAR, 1::TIMESTAMP)");
                serverMain.awaitTable("one_big_row");

                final byte[] status = {0};
                final String[] message = {null};
                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    client.execute("SELECT * FROM one_big_row", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            Assert.fail("unexpected batch: a 256 KiB row cannot fit a 160 KiB send buffer");
                        }

                        @Override
                        public void onEnd(long rows) {
                            Assert.fail("unexpected end: expected STATUS_LIMIT_EXCEEDED instead");
                        }

                        @Override
                        public void onError(byte s, String m) {
                            status[0] = s;
                            message[0] = m;
                        }
                    });
                }
                Assert.assertEquals(
                        "expected STATUS_LIMIT_EXCEEDED (0x0B), got 0x"
                                + Integer.toHexString(status[0] & 0xff)
                                + " msg=" + message[0],
                        (byte) 0x0B, status[0]);
                TestUtils.assertContains(message[0], "batch too large for send buffer");
            }
        });
    }

    /**
     * Wide-schema SELECT must complete cleanly without surfacing a batch
     * overflow error. Builds a 41-column table (20 VARCHAR + 10 BINARY +
     * 5 UUID + 5 LONG256 + ts) and ingests 4000 rows. Total payload
     * comfortably exceeds the default 2 MiB send buffer at 16_384 rows /
     * batch but fits across multiple batches once the row cap is
     * schema-aware. SYMBOL columns are deliberately omitted so the test
     * pins the dynamic row cap rather than the symbol-dict reserve --
     * dict overhead has its own coverage in QwpEgressBootstrapTest.
     */
    @Test
    public void testWideSchemaQueryDoesNotOverflowBatch() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables()) {
                final StringBuilder createCols = new StringBuilder();
                final StringBuilder insertCols = new StringBuilder();
                final StringBuilder insertVals = new StringBuilder();
                for (int i = 0; i < 20; i++) {
                    createCols.append("v").append(i).append(" VARCHAR, ");
                    insertCols.append("v").append(i).append(", ");
                    insertVals.append("rnd_varchar(20, 40, 0), ");
                }
                for (int i = 0; i < 10; i++) {
                    createCols.append("b").append(i).append(" BINARY, ");
                    insertCols.append("b").append(i).append(", ");
                    insertVals.append("rnd_bin(20, 40, 0), ");
                }
                for (int i = 0; i < 5; i++) {
                    createCols.append("u").append(i).append(" UUID, ");
                    insertCols.append("u").append(i).append(", ");
                    insertVals.append("rnd_uuid4(), ");
                }
                for (int i = 0; i < 5; i++) {
                    createCols.append("l").append(i).append(" LONG256, ");
                    insertCols.append("l").append(i).append(", ");
                    insertVals.append("rnd_long256(), ");
                }
                createCols.append("ts TIMESTAMP");
                insertCols.append("ts");
                insertVals.append("x::TIMESTAMP");

                serverMain.execute("CREATE TABLE wide(" + createCols
                        + ") TIMESTAMP(ts) PARTITION BY DAY WAL");
                serverMain.execute("INSERT INTO wide(" + insertCols + ")\n"
                        + "SELECT " + insertVals + " FROM long_sequence(4000)");
                serverMain.awaitTable("wide");

                final int[] totalRows = {0};
                final int[] batchCount = {0};
                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    client.execute("SELECT * FROM wide", new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            totalRows[0] += batch.getRowCount();
                            batchCount[0]++;
                        }

                        @Override
                        public void onEnd(long rows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                            Assert.fail("egress error status=0x" + Integer.toHexString(status & 0xff)
                                    + " msg=" + message);
                        }
                    });
                }
                Assert.assertEquals(4000, totalRows[0]);
                // With 40 wide columns the 4000-row payload comfortably
                // exceeds the 2 MiB send buffer for a single batch. The
                // dynamic cap must split the stream into multiple batches;
                // a single-batch result means the cap regressed back to
                // row-count-only.
                Assert.assertTrue(
                        "wide-schema query must split across multiple batches (got " + batchCount[0] + ")",
                        batchCount[0] > 1);
            }
        });
    }
}
