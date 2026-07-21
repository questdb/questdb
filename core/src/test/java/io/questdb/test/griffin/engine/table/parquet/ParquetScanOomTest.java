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

package io.questdb.test.griffin.engine.table.parquet;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Verifies that scanning a parquet partition releases its page-frame memory pool
 * buffers when a native allocation fails while a per-frame buffer is acquired.
 * <p>
 * {@code PageFrameMemoryPool} pulls a buffer off its free list and then calls
 * {@code reopen()} on it, which allocates the per-frame page-address lists one by
 * one. If that allocation trips the RSS memory limit, the buffer has already been
 * removed from the free list but not yet recorded in the cache, so the pool can
 * no longer reach it on close and the lists it managed to allocate leak. The
 * query fuzzer's malloc fault injection surfaced this as a small
 * {@code NATIVE_DEFAULT} leak.
 */
public class ParquetScanOomTest extends AbstractCairoTest {

    @Test
    public void testParquetScanCleansUpWhenBufferReopenRunsOutOfMemory() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x AS (" +
                    "  SELECT x, rnd_double() d," +
                    "  timestamp_sequence(0, 30 * 60 * 1000000L) ts" +
                    "  FROM long_sequence(100)" +
                    ") TIMESTAMP(ts) PARTITION BY DAY");
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '1970-01-01'");

            // Aggregates a constant, so the scan navigates the parquet frame but
            // decodes no columns; the page-frame pool's per-frame buffer reopen is
            // then the dominant native allocation, which is the path the fix guards.
            final String query = "SELECT min((length('OS'::VARCHAR))::TIMESTAMP) AS a0 FROM x";

            // Warm the reader and compiler pools so the swept allocation failure lands
            // inside the scan, not in first-touch table open.
            drain(query);

            boolean sawOom = false;
            // Fine sweep so the ceiling lands inside the buffer reopen, where the
            // page-address lists allocate one by one.
            for (int slack = 0; slack <= 24 * 1024; slack += 64) {
                Unsafe.setRssMemLimit(Unsafe.getRssMemUsed() + slack);
                try {
                    drain(query);
                } catch (CairoException e) {
                    Assert.assertTrue("expected an out-of-memory error, got: " + e.getMessage(), e.isOutOfMemory());
                    sawOom = true;
                } finally {
                    Unsafe.setRssMemLimit(0);
                }
            }
            Assert.assertTrue("sweep never tripped the RSS limit; widen the range", sawOom);

            // Recovery: with the ceiling removed the same query runs cleanly. The
            // enclosing assertMemoryLeak is the authoritative net leak check.
            Unsafe.setRssMemLimit(0);
            drain(query);
        });
    }

    private static void drain(String query) throws Exception {
        final StringSink sink = new StringSink();
        try (RecordCursorFactory factory = select(query)) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                final RecordMetadata metadata = factory.getMetadata();
                final int columnCount = metadata.getColumnCount();
                final Record record = cursor.getRecord();
                while (cursor.hasNext()) {
                    for (int i = 0; i < columnCount; i++) {
                        CursorPrinter.printColumn(record, metadata, i, sink, false);
                    }
                    sink.clear();
                }
            }
        }
    }
}
