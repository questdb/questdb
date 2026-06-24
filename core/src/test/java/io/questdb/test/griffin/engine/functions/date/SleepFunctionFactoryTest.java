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
            try (RecordCursorFactory factory = select("sleep(-1.0)")) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    cursor.hasNext();
                    Assert.fail("expected CairoException");
                } catch (io.questdb.cairo.CairoException e) {
                    Assert.assertTrue(e.getMessage(), e.getMessage().contains("sleep duration must be"));
                }
            }
        });
    }

    @Test
    public void testInfiniteSeconds() throws Exception {
        assertMemoryLeak(() -> {
            // 1e308 * 1e10 overflows the double range to +Infinity, so the isInfinite
            // branch (not the 24 hour cap) rejects it.
            try (RecordCursorFactory factory = select("sleep(1e308 * 1e10)")) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    cursor.hasNext();
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
            try (RecordCursorFactory factory = select("sleep(cast('NaN' as double))")) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    cursor.hasNext();
                    Assert.fail("expected CairoException");
                } catch (io.questdb.cairo.CairoException e) {
                    Assert.assertTrue(e.getMessage(), e.getMessage().contains("sleep duration must be"));
                }
            }
        });
    }

    @Test
    public void testSubMillisecondSecondsRoundsToZeroAndSkipsSleep() throws Exception {
        assertMemoryLeak(() -> {
            // 0.0001s * 1000 = 0.1ms, truncated to 0 by the (long) cast, so the sleep is
            // skipped entirely and the call returns the current server time near-instantly.
            long start = System.nanoTime();
            try (RecordCursorFactory factory = select("sleep(0.0001)")) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    long ts = cursor.getRecord().getTimestamp(0);
                    Assert.assertTrue(ts > 0);
                    Assert.assertFalse(cursor.hasNext());
                }
            }
            long elapsedMs = (System.nanoTime() - start) / 1_000_000L;
            Assert.assertTrue("sub-millisecond sleep should be near-instant, elapsed=" + elapsedMs + "ms",
                    elapsedMs < 200);
        });
    }

    @Test
    public void testSecondsExceedingMaximum() throws Exception {
        assertMemoryLeak(() -> {
            // 24 hours + 1 second, just over the 24 hour cap.
            try (RecordCursorFactory factory = select("sleep((24 * 60 * 60 + 1)::double)")) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    cursor.hasNext();
                    Assert.fail("expected CairoException");
                } catch (io.questdb.cairo.CairoException e) {
                    Assert.assertTrue(e.getMessage(), e.getMessage().contains("exceeds 24 hour maximum"));
                }
            }
        });
    }

    @Test
    public void testSecondsVeryLarge() throws Exception {
        assertMemoryLeak(() -> {
            // 1e15 seconds is finite and non-negative; * 1000 is 1e18 ms (well under Long.MAX_VALUE),
            // so the long conversion does not saturate. The cap check rejects it as exceeding 24h.
            try (RecordCursorFactory factory = select("sleep(1e15)")) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    cursor.hasNext();
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
            try (RecordCursorFactory factory = select("sleep(0.2)")) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    long ts = cursor.getRecord().getTimestamp(0);
                    Assert.assertTrue("returned timestamp must be positive: " + ts, ts > 0);
                    Assert.assertFalse(cursor.hasNext());
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
            try (RecordCursorFactory factory = select("sleep(0.0)")) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    long ts = cursor.getRecord().getTimestamp(0);
                    Assert.assertTrue(ts > 0);
                    Assert.assertFalse(cursor.hasNext());
                }
            }
            long elapsedMs = (System.nanoTime() - start) / 1_000_000L;
            Assert.assertTrue("zero sleep should be near-instant, elapsed=" + elapsedMs + "ms",
                    elapsedMs < 200);
        });
    }
}
