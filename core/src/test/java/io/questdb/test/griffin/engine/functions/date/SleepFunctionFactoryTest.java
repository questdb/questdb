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

package io.questdb.test.griffin.engine.functions.date;

import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class SleepFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testNegativeSeconds() throws Exception {
        assertMemoryLeak(() -> {
            try (RecordCursorFactory factory = select("select sleep(-1.0) from long_sequence(1)")) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    cursor.getRecord().getTimestamp(0);
                    Assert.fail("expected CairoException");
                } catch (io.questdb.cairo.CairoException e) {
                    Assert.assertTrue(e.getMessage(), e.getMessage().contains("sleep duration must be"));
                }
            }
        });
    }

    @Test
    public void testNonFiniteSeconds() throws Exception {
        assertMemoryLeak(() -> {
            try (RecordCursorFactory factory = select("select sleep(cast('NaN' as double)) from long_sequence(1)")) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    cursor.getRecord().getTimestamp(0);
                    Assert.fail("expected CairoException");
                } catch (io.questdb.cairo.CairoException e) {
                    Assert.assertTrue(e.getMessage(), e.getMessage().contains("sleep duration must be"));
                }
            }
        });
    }

    @Test
    public void testSecondsExceedingMaximum() throws Exception {
        assertMemoryLeak(() -> {
            // 24 hours + 1 second, just over the 24 hour cap.
            try (RecordCursorFactory factory = select("select sleep(24 * 60 * 60 + 1) from long_sequence(1)")) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    cursor.getRecord().getTimestamp(0);
                    Assert.fail("expected CairoException");
                } catch (io.questdb.cairo.CairoException e) {
                    Assert.assertTrue(e.getMessage(), e.getMessage().contains("exceeds 24 hour maximum"));
                }
            }
        });
    }

    @Test
    public void testSecondsOverflowingLong() throws Exception {
        assertMemoryLeak(() -> {
            // 1e15 seconds is finite and non-negative, but * 1000 overflows long range.
            try (RecordCursorFactory factory = select("select sleep(1e15) from long_sequence(1)")) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    cursor.getRecord().getTimestamp(0);
                    Assert.fail("expected CairoException");
                } catch (io.questdb.cairo.CairoException e) {
                    Assert.assertTrue(e.getMessage(), e.getMessage().contains("exceeds 24 hour maximum"));
                }
            }
        });
    }

    @Test
    public void testSleepReturnsCurrentServerTime() throws Exception {
        assertMemoryLeak(() -> {
            long start = System.nanoTime();
            try (RecordCursorFactory factory = select("select sleep(0.2) from long_sequence(1)")) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    long ts = cursor.getRecord().getTimestamp(0);
                    Assert.assertTrue("returned timestamp must be positive: " + ts, ts > 0);
                }
            }
            long elapsedMs = (System.nanoTime() - start) / 1_000_000L;
            Assert.assertTrue("did not sleep long enough, elapsed=" + elapsedMs + "ms",
                    elapsedMs >= 150);
        });
    }

    @Test
    public void testZeroSeconds() throws Exception {
        assertMemoryLeak(() -> {
            long start = System.nanoTime();
            try (RecordCursorFactory factory = select("select sleep(0.0) from long_sequence(1)")) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    long ts = cursor.getRecord().getTimestamp(0);
                    Assert.assertTrue(ts > 0);
                }
            }
            long elapsedMs = (System.nanoTime() - start) / 1_000_000L;
            Assert.assertTrue("zero sleep should be near-instant, elapsed=" + elapsedMs + "ms",
                    elapsedMs < 200);
        });
    }
}
