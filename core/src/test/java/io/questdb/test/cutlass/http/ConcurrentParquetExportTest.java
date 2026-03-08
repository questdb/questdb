/*******************************************************************************
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

package io.questdb.test.cutlass.http;

import io.questdb.cairo.vm.MemoryCARWImpl;
import io.questdb.cutlass.http.client.Fragment;
import io.questdb.cutlass.http.client.HttpClient;
import io.questdb.cutlass.http.client.HttpClientFactory;
import io.questdb.griffin.engine.table.parquet.PartitionDecoder;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class ConcurrentParquetExportTest extends AbstractBootstrapTest {

    @Test
    public void testConcurrentParquetExports() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        assertMemoryLeak(() -> getConcurrentExportTester(rnd)
                .run((engine, sqlExecutionContext) -> {
                    int totalRows = 1000;
                    engine.execute(
                            "CREATE TABLE corrupt_test AS (" +
                                    "SELECT x, rnd_str(200, 200, 0) as payload, timestamp_sequence(0, 1000000L) as ts " +
                                    "FROM long_sequence(" + totalRows + ")" +
                                    ")",
                            sqlExecutionContext
                    );

                    int threadCount = 16;
                    CyclicBarrier barrier = new CyclicBarrier(threadCount);
                    AtomicInteger successCount = new AtomicInteger(0);
                    AtomicReference<Throwable> firstError = new AtomicReference<>();
                    Thread[] threads = new Thread[threadCount];
                    MemoryCARWImpl[] buffers = new MemoryCARWImpl[threadCount];
                    int[] limits = new int[threadCount];

                    try {
                        for (int t = 0; t < threadCount; t++) {
                            buffers[t] = new MemoryCARWImpl(1024 * 1024, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
                            limits[t] = rnd.nextInt(totalRows) + 10;
                        }
                        for (int t = 0; t < threadCount; t++) {
                            final int threadId = t;
                            final int limit = limits[t];
                            final MemoryCARWImpl buf = buffers[t];
                            threads[t] = new Thread(() -> {
                                HttpClient client = null;
                                try {
                                    barrier.await();
                                    client = HttpClientFactory.newPlainTextInstance();
                                    HttpClient.Request req = client.newRequest("localhost", 9001);
                                    req.GET().url("/exp")
                                            .query("query", "SELECT * FROM corrupt_test LIMIT " + limit)
                                            .query("fmt", "parquet");

                                    try (HttpClient.ResponseHeaders respHeaders = req.send()) {
                                        respHeaders.await();
                                        TestUtils.assertEquals("200", respHeaders.getStatusCode());

                                        Fragment fragment;
                                        while ((fragment = respHeaders.getResponse().recv()) != null) {
                                            long lo = fragment.lo();
                                            long len = fragment.hi() - lo;
                                            long dest = buf.appendAddressFor(len);
                                            Unsafe.getUnsafe().copyMemory(lo, dest, len);
                                        }
                                    }
                                    successCount.incrementAndGet();
                                } catch (Throwable e) {
                                    firstError.compareAndSet(null, e);
                                    LOG.error().$("Thread ").$(threadId).$(" failed: ").$(e).$();
                                } finally {
                                    Misc.free(client);
                                    Path.clearThreadLocals();
                                }
                            });
                            threads[t].start();
                        }

                        for (Thread thread : threads) {
                            thread.join(120_000);
                        }

                        if (firstError.get() != null) {
                            Assert.fail("Thread failed with: " + firstError.get());
                        }
                        Assert.assertEquals("All threads should complete successfully", threadCount, successCount.get());

                        try (PartitionDecoder decoder = new PartitionDecoder()) {
                            for (int t = 0; t < threadCount; t++) {
                                long fileSize = buffers[t].getAppendOffset();
                                Assert.assertTrue("Thread " + t + " parquet too small: " + fileSize, fileSize > 0);

                                long addr = buffers[t].addressOf(0);
                                decoder.of(addr, fileSize, MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);
                                long rowCount = decoder.metadata().getRowCount();
                                int expectedLimit = Math.min(limits[t], totalRows);
                                Assert.assertEquals(
                                        "Thread " + t + " row count mismatch",
                                        expectedLimit,
                                        rowCount
                                );
                                LOG.info()
                                        .$("Thread ").$(t)
                                        .$(" valid parquet: ").$(fileSize).$(" bytes, ")
                                        .$(rowCount).$(" rows").$();
                            }
                        }
                    } finally {
                        for (int t = 0; t < threadCount; t++) {
                            Misc.free(buffers[t]);
                        }
                    }
                }));
    }

    private HttpQueryTestBuilder getConcurrentExportTester(Rnd rnd) {
        return new HttpQueryTestBuilder()
                .withTempFolder(root)
                .withWorkerCount(2)
                .withHttpServerConfigBuilder(new HttpServerConfigurationBuilder())
                .withTelemetry(false)
                .withForceSendFragmentationChunkSize(1024)
                .withSendBufferSize(1024 + rnd.nextInt(1024));
    }
}
