/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.test.cairo;

import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.std.Chars;
import io.questdb.std.Rnd;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class BitmapIndexConcurrentTest extends AbstractCairoTest {

    @Test
    public void testConcurrentBitmapIndexAccess() throws Exception {
        assertMemoryLeak(() -> {
            // Create table with hourly partitioning and indexed symbol column
            execute(
                    "CREATE TABLE trades (" +
                            "  symbol SYMBOL INDEX," +
                            "  ts TIMESTAMP" +
                            ") TIMESTAMP(ts) PARTITION BY HOUR"
            );

            // Prepopulate with 10 partitions and 1000 distinct symbols
            populateTable();

            // Test concurrent access
            testConcurrentOperations();
        });
    }

    private void populateTable() {
        // Generate 1000 distinct symbols across 10 partitions using TableWriter API
        try (TableWriter w = TestUtils.getWriter(engine, "trades")) {
            long baseTimestamp = 1704067200000000L; // 2024-01-01T00:00:00.000000Z in micros

            for (int partition = 0; partition < 10; partition++) {
                long partitionTimestamp = baseTimestamp + (partition * 3600000000L); // Add hours in micros

                for (int i = 1; i <= 100; i++) {
                    TableWriter.Row r = w.newRow(partitionTimestamp + (i * 100000L)); // 100ms intervals
                    r.putSym(0, "SYM" + i);
                    r.append();
                }
            }
            w.commit();
        }
    }

    private void testConcurrentOperations() throws Exception {
        final AtomicInteger queryErrorCount = new AtomicInteger(0);
        final AtomicInteger insertErrorCount = new AtomicInteger(0);
        final AtomicReference<String> lastError = new AtomicReference<>();
        final SOCountDownLatch queryLatch = new SOCountDownLatch(1);
        final SOCountDownLatch insertLatch = new SOCountDownLatch(1);
        final CyclicBarrier startBarrier = new CyclicBarrier(2); // 2 background threads only
        final long testDurationMs = 20_000; // Run for 20 seconds

        // Background thread 1: Out-of-order inserts
        Thread inserterThread = new Thread(() -> {
            try {
                startBarrier.await();
                Rnd rnd = new Rnd();
                long startTime = System.currentTimeMillis();
                int insertCount = 0;

                try (TableWriter w = TestUtils.getWriter(engine, "trades")) {
                    while (System.currentTimeMillis() - startTime < testDurationMs) {
                        try {
                            // Insert multiple rows in batch before committing
                            for (int batch = 0; batch < 10; batch++) {
                                // Use random symbol from existing ones (SYM1 to SYM100)
                                String randomSymbol = "SYM" + (rnd.nextInt(100) + 1);
                                
                                // Create out-of-order timestamp (random minutes in the past)
                                long oooTimestamp = System.currentTimeMillis() * 1000L - (rnd.nextInt(600) * 60000000L); // micros

                                TableWriter.Row r = w.newRow(oooTimestamp);
                                r.putSym(0, randomSymbol); // Use pre-existing symbol
                                r.append();
                                insertCount++;
                            }
                            
                            // Commit batch
                            w.commit();

                            if (insertCount % 100 == 0) {
                                Thread.sleep(5); // Small pause after each batch
                            }
                        } catch (Exception e) {
                            insertErrorCount.incrementAndGet();
                            lastError.set("Insert error: " + e.getMessage());
                            break;
                        }
                    }
                }
                System.out.println("Inserter thread completed " + insertCount + " inserts");
            } catch (Exception e) {
                insertErrorCount.incrementAndGet();
                lastError.set("Insert thread error: " + e.getMessage());
            } finally {
                Path.clearThreadLocals();
                insertLatch.countDown();
            }
        });

        // Background thread 2: Concurrent queries
        Thread queryThread = new Thread(() -> {
            try {
                startBarrier.await();
                Rnd rnd = new Rnd();
                long startTime = System.currentTimeMillis();
                int queryCount = 0;

                while (System.currentTimeMillis() - startTime < testDurationMs) {
                    try {
                        // Use random symbol from existing ones (SYM1 to SYM100)
                        String randomSymbol = "SYM" + (rnd.nextInt(100) + 1);

                        String querySql = String.format(
                                "SELECT symbol, count(*) FROM trades WHERE symbol = '%s' GROUP BY symbol",
                                randomSymbol
                        );

                        // Execute query and validate results
                        try (RecordCursorFactory factory = select(querySql)) {
                            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                                int rowCount = 0;
                                while (cursor.hasNext()) {
                                    rowCount++;
                                    if (rowCount > 1) {
                                        queryErrorCount.incrementAndGet();
                                        lastError.set("Query returned more than one row for symbol: " + randomSymbol);
                                        return;
                                    }

                                    CharSequence resultSymbol = cursor.getRecord().getSymA(0);
                                    if (!Chars.equals(resultSymbol, randomSymbol)) {
                                        queryErrorCount.incrementAndGet();
                                        lastError.set("Expected symbol '" + randomSymbol + "' but got '" + resultSymbol + "'");
                                        return;
                                    }
                                }

                                if (rowCount != 1) {
                                    queryErrorCount.incrementAndGet();
                                    lastError.set("Expected exactly 1 row for symbol '" + randomSymbol + "' but got " + rowCount);
                                    return;
                                }
                            }
                        }
                        queryCount++;

                        if (queryCount % 10 == 0) {
                            Thread.sleep(10); // Small pause
                        }
                    } catch (Exception e) {
                        queryErrorCount.incrementAndGet();
                        lastError.set("Query error: " + e.getMessage());
                        break;
                    }
                }
                System.out.println("Query thread completed " + queryCount + " queries");
            } catch (Exception e) {
                queryErrorCount.incrementAndGet();
                lastError.set("Query thread error: " + e.getMessage());
            } finally {
                Path.clearThreadLocals();
                queryLatch.countDown();
            }
        });

        // Start both threads
        inserterThread.start();
        queryThread.start();

        // Wait for completion
        insertLatch.await();
        queryLatch.await();

        // Validate results
        String errorMessage = lastError.get();
        if (errorMessage != null) {
            System.err.println("Test error: " + errorMessage);
        }

        Assert.assertEquals("Query errors detected", 0, queryErrorCount.get());
        Assert.assertEquals("Insert errors detected", 0, insertErrorCount.get());

        // Final validation query
        try (RecordCursorFactory factory = select("SELECT count(*) FROM trades")) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Assert.assertTrue("Should have results", cursor.hasNext());
                long count = cursor.getRecord().getLong(0);
                Assert.assertTrue("Should have records", count > 1000); // Initial 1000 + inserted records
                System.out.println("Final total count: " + count);
            }
        }
    }
}