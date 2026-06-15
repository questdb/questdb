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

package io.questdb.test.griffin.engine.groupby.vect;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.std.Unsafe;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Verifies that the vectorized (rosti) keyed GROUP BY backed by
 * {@code GroupByRecordCursorFactory} releases its resources when a native
 * allocation fails while the cursor is being opened.
 * <p>
 * {@code RostiRecordCursor.of()} reopens the factory's {@code PageFrameAddressCache},
 * which reallocates four off-heap {@code DirectLongList}s. If a later reopen trips
 * the RSS memory limit after an earlier one has already allocated, {@code of()}
 * throws and {@code getCursor()} never returns the cursor, so the caller never
 * closes it; the factory's {@code _close()} does not free the cache either, leaking
 * the already-reopened buffer (512 bytes, {@code NATIVE_DEFAULT}). The query fuzzer's
 * malloc fault injection surfaced this leak.
 */
public class GroupByVectorizedOomTest extends AbstractCairoTest {

    @Test
    public void testVectorizedGroupByCleansUpWhenCursorRunsOutOfMemory() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (k INT, v LONG)");
            execute("INSERT INTO tab SELECT (x % 16)::int, x FROM long_sequence(2000)");
            final String query = "SELECT k, sum(v) FROM tab GROUP BY k";

            // Confirm the plan really exercises the vectorized rosti cursor.
            printSql("EXPLAIN " + query);
            TestUtils.assertContains(sink, "GroupBy vectorized: true");

            // Warm the reader and compiler pools so the swept allocation failure lands
            // inside cursor open (the PageFrameAddressCache reopen), not in first-touch
            // table open.
            drain(query);

            boolean sawOom = false;
            // Sweep the native-memory ceiling across the cursor-open allocation points.
            // Some ceiling lets an earlier PageFrameAddressCache list reopen() succeed
            // and trips a later one; the pre-fix code then leaked the earlier buffer.
            for (int slack = 0; slack <= 128 * 1024; slack += 16) {
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

            // Recovery: with the ceiling removed the same query runs cleanly.
            Unsafe.setRssMemLimit(0);
            drain(query);
        });
    }

    private static void drain(String query) throws Exception {
        final StringSink localSink = new StringSink();
        try (RecordCursorFactory factory = select(query)) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                final RecordMetadata metadata = factory.getMetadata();
                final int columnCount = metadata.getColumnCount();
                final Record record = cursor.getRecord();
                while (cursor.hasNext()) {
                    for (int i = 0; i < columnCount; i++) {
                        CursorPrinter.printColumn(record, metadata, i, localSink, false);
                    }
                    localSink.clear();
                }
            }
        }
    }
}
