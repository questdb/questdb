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
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Property-based fuzz for the client-side bind encoder. Each iteration picks
 * random scalar bind values, sends them via
 * {@link QwpQueryClient#execute(String, io.questdb.client.cutlass.qwp.client.QwpBindSetter, QwpColumnBatchHandler)},
 * and asserts the round-trip value per-cell. Complements
 * {@code QwpEgressBindRoundTripTest}, which pins specific boundary values per
 * type; this class stresses the encoder with arbitrary random inputs and
 * catches bit-level encoding bugs that might slip past hand-picked cases.
 */
public class QwpEgressBindFuzzTest extends AbstractBootstrapTest {

    private static final int ITERATIONS_PER_TEST = 25;
    private static final Log LOG = LogFactory.getLog(QwpEgressBindFuzzTest.class);
    private Rnd random;

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
        random = TestUtils.generateRandom(LOG);
    }

    @Test
    public void testFuzzDoubleBinds() throws Exception {
        // Covers NaN and arbitrary finite doubles. FLOAT is intentionally not
        // fuzzed here: the ::FLOAT cast projection form renormalises some
        // values (-0.0 -> 0.0, sub-millionth rounding) so a per-iteration
        // random comparison would flap on things unrelated to the encoder.
        // FLOAT bit-level encoding is pinned by QwpEgressBindRoundTripTest.
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain ignored = startWithEnvVariables()) {
                try (QwpQueryClient client = QwpQueryClient.fromConfig("ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    for (int idx = 0; idx < ITERATIONS_PER_TEST; idx++) {
                        final int i = idx;
                        double d = pickSpecialOrRandomDouble();
                        final double[] gotD = {0};
                        client.execute(
                                "SELECT $1::DOUBLE AS d FROM long_sequence(1)",
                                binds -> binds.setDouble(0, d),
                                new QwpColumnBatchHandler() {
                                    @Override
                                    public void onBatch(QwpColumnBatch batch) {
                                        gotD[0] = batch.getDoubleValue(0, 0);
                                    }

                                    @Override
                                    public void onEnd(long totalRows) {
                                    }

                                    @Override
                                    public void onError(byte status, String message) {
                                        Assert.fail("iter " + i + " d=" + d + ": " + message);
                                    }
                                }
                        );
                        if (Double.isNaN(d)) {
                            Assert.assertTrue("iter " + i + " expected NaN", Double.isNaN(gotD[0]));
                        } else {
                            // == treats -0.0 == 0.0 as equal, which matches
                            // QuestDB's cast normalisation.
                            Assert.assertEquals("iter " + i + " d=" + d + " got=" + gotD[0], d, gotD[0], 0.0);
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testFuzzIntegralBindsProjection() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain ignored = startWithEnvVariables()) {
                try (QwpQueryClient client = QwpQueryClient.fromConfig("ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    for (int idx = 0; idx < ITERATIONS_PER_TEST; idx++) {
                        final int i = idx;
                        // Avoid NULL sentinels for integer types.
                        long longVal = pickNonNullLong();
                        int intVal = pickNonNullInt();
                        short shortVal = (short) random.nextInt();
                        byte byteVal = (byte) random.nextInt();
                        boolean boolVal = random.nextBoolean();

                        final long[] gotLong = {0};
                        final int[] gotInt = {0};
                        final short[] gotShort = {0};
                        final byte[] gotByte = {0};
                        final boolean[] gotBool = {false};
                        client.execute(
                                "SELECT $1::LONG AS l, $2::INT AS i, $3::SHORT AS s, $4::BYTE AS b, $5::BOOLEAN AS x FROM long_sequence(1)",
                                binds -> binds
                                        .setLong(0, longVal)
                                        .setInt(1, intVal)
                                        .setShort(2, shortVal)
                                        .setByte(3, byteVal)
                                        .setBoolean(4, boolVal),
                                new QwpColumnBatchHandler() {
                                    @Override
                                    public void onBatch(QwpColumnBatch batch) {
                                        gotLong[0] = batch.getLongValue(0, 0);
                                        gotInt[0] = batch.getIntValue(1, 0);
                                        gotShort[0] = batch.getShortValue(2, 0);
                                        gotByte[0] = batch.getByteValue(3, 0);
                                        gotBool[0] = batch.getBoolValue(4, 0);
                                    }

                                    @Override
                                    public void onEnd(long totalRows) {
                                    }

                                    @Override
                                    public void onError(byte status, String message) {
                                        Assert.fail("iter " + i + ": " + message);
                                    }
                                }
                        );
                        Assert.assertEquals("iter " + i + " long", longVal, gotLong[0]);
                        Assert.assertEquals("iter " + i + " int", intVal, gotInt[0]);
                        Assert.assertEquals("iter " + i + " short", shortVal, gotShort[0]);
                        Assert.assertEquals("iter " + i + " byte", byteVal, gotByte[0]);
                        Assert.assertEquals("iter " + i + " bool", boolVal, gotBool[0]);
                    }
                }
            }
        });
    }

    @Test
    public void testFuzzSameSqlDifferentBindsCacheReuse() throws Exception {
        // Stresses the same-SQL-different-binds path that the factory cache
        // is meant to accelerate. Random integer values, 50 iterations.
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain server = startWithEnvVariables()) {
                server.execute("CREATE TABLE t(id LONG, v LONG, part_ts TIMESTAMP) TIMESTAMP(part_ts) PARTITION BY DAY WAL");
                StringBuilder insert = new StringBuilder("INSERT INTO t VALUES ");
                int rows = 100;
                for (int r = 0; r < rows; r++) {
                    if (r > 0) insert.append(", ");
                    insert.append('(').append(r).append(", ").append(r * 7L).append(", ").append(r + 1).append("::TIMESTAMP)");
                }
                server.execute(insert.toString());
                server.awaitTable("t");

                try (QwpQueryClient client = QwpQueryClient.fromConfig("ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    String sql = "SELECT v FROM t WHERE id = $1";
                    for (int idx = 0; idx < 50; idx++) {
                        final int i = idx;
                        final int target = random.nextInt(rows);
                        final long[] observed = {-1L};
                        client.execute(sql, binds -> binds.setInt(0, target), new QwpColumnBatchHandler() {
                            @Override
                            public void onBatch(QwpColumnBatch batch) {
                                Assert.assertEquals(1, batch.getRowCount());
                                observed[0] = batch.getLongValue(0, 0);
                            }

                            @Override
                            public void onEnd(long totalRows) {
                            }

                            @Override
                            public void onError(byte status, String message) {
                                Assert.fail("iter " + i + " target=" + target + ": " + message);
                            }
                        });
                        Assert.assertEquals("iter " + i + " target=" + target, target * 7L, observed[0]);
                    }
                }
            }
        });
    }

    @Test
    public void testFuzzUuidBinds() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain ignored = startWithEnvVariables()) {
                try (QwpQueryClient client = QwpQueryClient.fromConfig("ws::addr=127.0.0.1:" + HTTP_PORT + ";")) {
                    client.connect();
                    for (int idx = 0; idx < ITERATIONS_PER_TEST; idx++) {
                        final int i = idx;
                        long lo = pickNonNullLong();
                        long hi = pickNonNullLong();
                        final long[] gotLo = {0};
                        final long[] gotHi = {0};
                        client.execute(
                                "SELECT $1::UUID AS u FROM long_sequence(1)",
                                binds -> binds.setUuid(0, lo, hi),
                                new QwpColumnBatchHandler() {
                                    @Override
                                    public void onBatch(QwpColumnBatch batch) {
                                        gotLo[0] = batch.getUuidLo(0, 0);
                                        gotHi[0] = batch.getUuidHi(0, 0);
                                    }

                                    @Override
                                    public void onEnd(long totalRows) {
                                    }

                                    @Override
                                    public void onError(byte status, String message) {
                                        Assert.fail("iter " + i + ": " + message);
                                    }
                                }
                        );
                        Assert.assertEquals("iter " + i + " lo", lo, gotLo[0]);
                        Assert.assertEquals("iter " + i + " hi", hi, gotHi[0]);
                    }
                }
            }
        });
    }

    private int pickNonNullInt() {
        int v;
        do {
            v = random.nextInt();
        } while (v == Integer.MIN_VALUE);
        return v;
    }

    private long pickNonNullLong() {
        long v;
        do {
            v = random.nextLong();
        } while (v == Long.MIN_VALUE);
        return v;
    }

    private double pickSpecialOrRandomDouble() {
        // Small odds of a special value; otherwise a random finite double.
        // Skip +/- Infinity and -0.0: QuestDB's cast layer normalises them,
        // so a raw-bits comparison after round-trip would produce spurious
        // failures unrelated to the bind encoder.
        int pick = random.nextInt(4);
        switch (pick) {
            case 0:
                return Double.NaN;
            case 1:
                return 0.0;
            default:
                while (true) {
                    double d = Double.longBitsToDouble(random.nextLong());
                    if (!Double.isInfinite(d)) {
                        return d;
                    }
                }
        }
    }
}
