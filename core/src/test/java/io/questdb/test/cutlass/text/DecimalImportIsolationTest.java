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

package io.questdb.test.cutlass.text;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cutlass.text.DefaultTextConfiguration;
import io.questdb.cutlass.text.TextConfiguration;
import io.questdb.cutlass.text.types.TypeAdapter;
import io.questdb.cutlass.text.types.TypeManager;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.Decimal256;
import io.questdb.std.str.DirectUtf16Sink;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractTest;
import io.questdb.test.cutlass.http.HttpQueryTestBuilder;
import io.questdb.test.cutlass.http.HttpServerConfigurationBuilder;
import io.questdb.test.cutlass.http.SendAndReceiveRequestBuilder;
import io.questdb.test.griffin.RowAsserter;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicReference;

public class DecimalImportIsolationTest extends AbstractTest {
    private static final String BOUNDARY = "------------------------27d997ca93d2689d";
    private static final int ROWS_PER_UPLOAD = 200;
    private static final int UPLOADS_PER_THREAD = 8;

    @Test
    public void testConcurrentImportsKeepDecimalsApart() throws Exception {
        new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withWorkerCount(2)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                .withTelemetry(false)
                .run((engine, sqlExecutionContext) -> {
                    final CyclicBarrier start = new CyclicBarrier(2);
                    final SOCountDownLatch done = new SOCountDownLatch(2);
                    final AtomicReference<Throwable> error = new AtomicReference<>();
                    for (int t = 0; t < 2; t++) {
                        final int index = t;
                        new Thread(() -> {
                            try {
                                final String request = importRequest("dec" + index, index + 1);
                                start.await();
                                for (int i = 0; i < UPLOADS_PER_THREAD; i++) {
                                    new SendAndReceiveRequestBuilder()
                                            .withCompareLength(16)
                                            .execute(request, "HTTP/1.1 200 OK\r\n");
                                }
                            } catch (Throwable e) {
                                error.set(e);
                            } finally {
                                done.countDown();
                            }
                        }).start();
                    }
                    done.await();
                    if (error.get() != null) {
                        throw new AssertionError(error.get());
                    }

                    final StringSink sink = new StringSink();
                    final int rows = ROWS_PER_UPLOAD * UPLOADS_PER_THREAD;
                    for (int t = 0; t < 2; t++) {
                        try (TableReader reader = engine.getReader("dec" + t)) {
                            final RecordMetadata metadata = reader.getMetadata();
                            final int type = metadata.getColumnType(metadata.getColumnIndex("amount"));
                            Assert.assertEquals(ColumnType.DECIMAL_DEFAULT_TYPE, type);
                        }
                        TestUtils.assertEquals(
                                "cnt\tmn\tmx\n" + rows + "\t" + (t + 1) + ".001\t" + (t + 1) + ".200\n",
                                TestUtils.printSqlToString(
                                        engine,
                                        sqlExecutionContext,
                                        "select count() cnt, min(amount) mn, max(amount) mx from dec" + t,
                                        sink
                                )
                        );
                        TestUtils.assertEquals(
                                "cnt\n0\n",
                                TestUtils.printSqlToString(
                                        engine,
                                        sqlExecutionContext,
                                        "select count() cnt from dec" + t + " where amount is null",
                                        sink
                                )
                        );
                    }
                });
    }

    @Test
    public void testDecimalWriteAdapterIsNotShared() throws Exception {
        final TextConfiguration configuration = new DefaultTextConfiguration();
        final Decimal256 decimalA = new Decimal256();
        final Decimal256 decimalB = new Decimal256();
        try (
                DirectUtf16Sink utf16SinkA = new DirectUtf16Sink(64);
                DirectUtf8Sink utf8SinkA = new DirectUtf8Sink(64);
                DirectUtf16Sink utf16SinkB = new DirectUtf16Sink(64);
                DirectUtf8Sink utf8SinkB = new DirectUtf8Sink(64);
                DirectUtf8Sink value = new DirectUtf8Sink(64)
        ) {
            final TypeManager managerA = new TypeManager(configuration, utf16SinkA, utf8SinkA, decimalA);
            final TypeManager managerB = new TypeManager(configuration, utf16SinkB, utf8SinkB, decimalB);
            final TypeAdapter adapterA = decimalProbe(managerA);
            final TypeAdapter adapterB = decimalProbe(managerB);
            Assert.assertNotSame(adapterA, adapterB);

            final CapturingRow rowA = new CapturingRow();
            value.put("1.234m");
            adapterA.write(rowA, 0, value);

            final CapturingRow rowB = new CapturingRow();
            value.clear();
            value.put("5.678m");
            adapterB.write(rowB, 1, value);

            Assert.assertEquals(0, rowA.column);
            Assert.assertEquals(1234, rowA.value);
            Assert.assertEquals(1, rowB.column);
            Assert.assertEquals(5678, rowB.value);
            // each type manager must write through the scratch it was given, not through a shared one
            Assert.assertEquals(1234, decimalA.getLl());
            Assert.assertEquals(5678, decimalB.getLl());
        }
    }

    private static TypeAdapter decimalProbe(TypeManager typeManager) {
        for (int i = 0, n = typeManager.getProbeCount(); i < n; i++) {
            final TypeAdapter probe = typeManager.getProbe(i);
            if (ColumnType.isDecimal(probe.getType())) {
                return probe;
            }
        }
        throw new AssertionError("decimal probe not registered");
    }

    private static String importRequest(String tableName, int unit) {
        final StringBuilder csv = new StringBuilder("name,amount\r\n");
        for (int i = 0; i < ROWS_PER_UPLOAD; i++) {
            csv.append("row").append(i).append(',')
                    .append(unit).append('.').append(String.format("%03d", i + 1)).append("m\r\n");
        }
        return "POST /imp?name=" + tableName + " HTTP/1.1\r\n" +
                "Host: localhost:9001\r\n" +
                "User-Agent: curl/7.64.0\r\n" +
                "Accept: */*\r\n" +
                "Content-Length: 437760673\r\n" +
                "Content-Type: multipart/form-data; boundary=" + BOUNDARY + "\r\n" +
                "Expect: 100-continue\r\n" +
                "\r\n" +
                "--" + BOUNDARY + "\r\n" +
                "Content-Disposition: form-data; name=\"data\"; filename=\"" + tableName + ".csv\"\r\n" +
                "Content-Type: application/octet-stream\r\n" +
                "\r\n" +
                csv +
                "\r\n--" + BOUNDARY + "--";
    }

    private static class CapturingRow extends RowAsserter {
        int column = -1;
        long value = Long.MIN_VALUE;

        @Override
        public void putLong(int columnIndex, long value) {
            this.column = columnIndex;
            this.value = value;
        }
    }
}
