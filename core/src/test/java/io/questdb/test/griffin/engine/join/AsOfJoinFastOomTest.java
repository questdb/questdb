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

package io.questdb.test.griffin.engine.join;

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
 * Verifies that a keyed ASOF JOIN backed by {@code AsOfJoinFastRecordCursorFactory}
 * (and its filtered variant) releases its resources when a native allocation fails
 * while the cursor is being opened.
 * <p>
 * The cursor reopens two {@code SingleRecordSink} heaps in {@code of()}; if the
 * second {@code reopen()} trips the RSS memory limit after the first has already
 * allocated, {@code getCursor()} throws and the half-opened cursor is orphaned
 * (the factory's {@code _close()} does not free the reusable cursor). That leaked
 * the first sink's 8-byte heap, tagged {@code NATIVE_RECORD_CHAIN}. The query
 * fuzzer's malloc fault injection surfaced this leak.
 */
public class AsOfJoinFastOomTest extends AbstractCairoTest {

    @Test
    public void testFilteredKeyedAsOfJoinCleansUpWhenCursorRunsOutOfMemory() throws Exception {
        // The slave-side filter routes the plan through FilteredAsOfJoinFastRecordCursorFactory,
        // whose of() reopens the same pair of sinks.
        assertNoLeakOnCursorOom(
                "SELECT m.k1, m.v, s.v FROM master m ASOF JOIN slave s ON (k1, k2) WHERE s.v > 0"
        );
    }

    @Test
    public void testKeyedAsOfJoinCleansUpWhenCursorRunsOutOfMemory() throws Exception {
        assertNoLeakOnCursorOom(
                "SELECT m.k1, m.v, s.v FROM master m ASOF JOIN slave s ON (k1, k2)"
        );
    }

    private void assertNoLeakOnCursorOom(String query) throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE master AS (" +
                            "  SELECT rnd_symbol('a','b','c') k1, rnd_symbol('x','y') k2, rnd_int() v," +
                            "  timestamp_sequence(0, 60 * 1_000_000L) ts" +
                            "  FROM long_sequence(200)" +
                            ") TIMESTAMP(ts) PARTITION BY DAY"
            );
            execute(
                    "CREATE TABLE slave AS (" +
                            "  SELECT rnd_symbol('a','b','c') k1, rnd_symbol('x','y') k2, rnd_int() v," +
                            "  timestamp_sequence(0, 30 * 1_000_000L) ts" +
                            "  FROM long_sequence(200)" +
                            ") TIMESTAMP(ts) PARTITION BY DAY"
            );

            // Confirm the plan really exercises the fast keyed cursor.
            printSql("EXPLAIN " + query);
            TestUtils.assertContains(sink, "AsOf Join Fast");

            // Warm the reader and compiler pools so the swept allocation failure lands
            // inside cursor open (the sink reopen()s), not in first-touch table open.
            drain(query);

            boolean sawOom = false;
            // Sweep the native-memory ceiling across the cursor-open allocation points.
            // Some ceiling lets the first sink reopen() succeed and trips the second; the
            // pre-fix code then leaked the first sink's 8-byte heap.
            for (int slack = 0; slack <= 64 * 1024; slack += 8) {
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

    private void drain(String query) throws Exception {
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
