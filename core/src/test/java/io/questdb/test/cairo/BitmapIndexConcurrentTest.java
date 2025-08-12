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
        final AtomicInteger errorCount = new AtomicInteger(0);
        final AtomicReference<String> lastError = new AtomicReference<>();
        final long testDurationMs = 20_000; // Run for 20 seconds

        Rnd rnd = new Rnd();
        long startTime = System.currentTimeMillis();
        int insertCount = 0;
        int queryCount = 0;

        try (TableWriter w = TestUtils.getWriter(engine, "trades")) {
            while (System.currentTimeMillis() - startTime < testDurationMs) {
                try {
                    // Insert a batch of rows
                    for (int batch = 0; batch < 10; batch++) {
                        // Use random symbol from existing ones (SYM1 to SYM100)
                        String randomSymbol = "SYM" + (rnd.nextInt(100) + 1);
                        
                        // Create out-of-order timestamp (random minutes in the past)
                        long oooTimestamp = System.currentTimeMillis() * 1000L - (rnd.nextInt(600) * 60000000L); // micros

                        TableWriter.Row r = w.newRow(oooTimestamp);
                        r.putSym(0, randomSymbol);
                        r.append();
                        insertCount++;
                    }
                    
                    // Commit batch
                    w.commit();

                    // Immediately validate with queries after each commit
                    for (int q = 0; q < 5; q++) {
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
                                        errorCount.incrementAndGet();
                                        lastError.set("Query returned more than one row for symbol: " + randomSymbol);
                                        System.err.println("Error at insert count " + insertCount + ", query count " + queryCount + ": " + lastError.get());
                                        Assert.fail(lastError.get());
                                        return;
                                    }

                                    CharSequence resultSymbol = cursor.getRecord().getSymA(0);
                                    if (!Chars.equals(resultSymbol, randomSymbol)) {
                                        errorCount.incrementAndGet();
                                        lastError.set("Expected symbol '" + randomSymbol + "' but got '" + resultSymbol + "'");
                                        System.err.println("Error at insert count " + insertCount + ", query count " + queryCount + ": " + lastError.get());
                                        Assert.fail(lastError.get());
                                        return;
                                    }
                                }

                                if (rowCount != 1) {
                                    errorCount.incrementAndGet();
                                    lastError.set("Expected exactly 1 row for symbol '" + randomSymbol + "' but got " + rowCount);
                                    System.err.println("Error at insert count " + insertCount + ", query count " + queryCount + ": " + lastError.get());
                                    Assert.fail(lastError.get());
                                    return;
                                }
                            }
                        }
                        queryCount++;
                    }

                    if (insertCount % 100 == 0) {
                        Thread.sleep(5); // Small pause after every 100 inserts
                    }
                } catch (Exception e) {
                    errorCount.incrementAndGet();
                    lastError.set("Operation error: " + e.getMessage());
                    System.err.println("Error at insert count " + insertCount + ", query count " + queryCount + ": " + lastError.get());
                    Assert.fail(lastError.get());
                    return;
                }
            }
        }

        System.out.println("Test completed: " + insertCount + " inserts, " + queryCount + " queries");

        // Validate results
        Assert.assertEquals("Errors detected", 0, errorCount.get());

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