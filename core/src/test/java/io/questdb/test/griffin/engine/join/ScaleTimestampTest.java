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

import io.questdb.griffin.engine.join.AbstractAsOfJoinFastRecordCursor;
import org.junit.Assert;
import org.junit.Test;

/**
 * Pins {@link AbstractAsOfJoinFastRecordCursor#scaleTimestamp(long, long)}.
 * <p>
 * It is a {@code public static} helper reached from ~270 references across 32 production files -
 * the AsOf, Lt, Splice and window join cursors and their time-frame helpers - and it had no direct
 * test at all. Its overflow behaviour is the part that matters: a mixed-resolution join scales a
 * micros timestamp into nanos, and the two saturation directions decide whether an out-of-range
 * bound reads as "before everything" or "after everything". Getting that backwards silently
 * changes which slave rows a join considers.
 * <p>
 * These are pure arithmetic assertions over a static method, so they allocate no native memory and
 * need no {@code assertMemoryLeak}.
 */
public class ScaleTimestampTest {

    private static final long MICROS_TO_NANOS = 1000L;

    @Test
    public void testExactBoundaryDoesNotSaturate() {
        // The largest and smallest values that still multiply exactly must come back exact, so the
        // saturation arms cannot be over-eager.
        final long maxExact = Long.MAX_VALUE / MICROS_TO_NANOS;
        Assert.assertEquals(maxExact * MICROS_TO_NANOS, AbstractAsOfJoinFastRecordCursor.scaleTimestamp(maxExact, MICROS_TO_NANOS));

        final long minExact = (Long.MIN_VALUE + 1) / MICROS_TO_NANOS;
        Assert.assertEquals(minExact * MICROS_TO_NANOS, AbstractAsOfJoinFastRecordCursor.scaleTimestamp(minExact, MICROS_TO_NANOS));
    }

    @Test
    public void testNegativeOverflowSaturatesToMin() {
        // The direction that matters. A pre-1677 micros timestamp scaled to nanos overflows
        // negative; saturating it to Long.MAX_VALUE would turn "earlier than every slave row" into
        // "later than every slave row" and invert a window's lower bound.
        Assert.assertEquals(
                Long.MIN_VALUE,
                AbstractAsOfJoinFastRecordCursor.scaleTimestamp(Long.MIN_VALUE + 1, MICROS_TO_NANOS)
        );
        Assert.assertEquals(
                Long.MIN_VALUE,
                AbstractAsOfJoinFastRecordCursor.scaleTimestamp(Long.MIN_VALUE / 2, MICROS_TO_NANOS)
        );
    }

    @Test
    public void testNoOverflowMultipliesExactly() {
        Assert.assertEquals(1_000_000L, AbstractAsOfJoinFastRecordCursor.scaleTimestamp(1_000L, MICROS_TO_NANOS));
        Assert.assertEquals(-1_000_000L, AbstractAsOfJoinFastRecordCursor.scaleTimestamp(-1_000L, MICROS_TO_NANOS));
        Assert.assertEquals(0L, AbstractAsOfJoinFastRecordCursor.scaleTimestamp(0L, MICROS_TO_NANOS));
    }

    @Test
    public void testNullSentinelPassesThroughUnscaled() {
        // Long.MIN_VALUE is the timestamp NULL sentinel, not a value to scale: it must survive any
        // scale unchanged, otherwise a NULL slave timestamp would read as a real instant.
        Assert.assertEquals(Long.MIN_VALUE, AbstractAsOfJoinFastRecordCursor.scaleTimestamp(Long.MIN_VALUE, MICROS_TO_NANOS));
        Assert.assertEquals(Long.MIN_VALUE, AbstractAsOfJoinFastRecordCursor.scaleTimestamp(Long.MIN_VALUE, 1L));
        Assert.assertEquals(Long.MIN_VALUE, AbstractAsOfJoinFastRecordCursor.scaleTimestamp(Long.MIN_VALUE, 1_000_000L));
    }

    @Test
    public void testPositiveOverflowSaturatesToMax() {
        Assert.assertEquals(
                Long.MAX_VALUE,
                AbstractAsOfJoinFastRecordCursor.scaleTimestamp(Long.MAX_VALUE, MICROS_TO_NANOS)
        );
        Assert.assertEquals(
                Long.MAX_VALUE,
                AbstractAsOfJoinFastRecordCursor.scaleTimestamp(Long.MAX_VALUE / 2, MICROS_TO_NANOS)
        );
    }

    @Test
    public void testScaleOfOneIsIdentity() {
        Assert.assertEquals(Long.MAX_VALUE, AbstractAsOfJoinFastRecordCursor.scaleTimestamp(Long.MAX_VALUE, 1L));
        Assert.assertEquals(Long.MIN_VALUE, AbstractAsOfJoinFastRecordCursor.scaleTimestamp(Long.MIN_VALUE, 1L));
        Assert.assertEquals(-42L, AbstractAsOfJoinFastRecordCursor.scaleTimestamp(-42L, 1L));
        Assert.assertEquals(42L, AbstractAsOfJoinFastRecordCursor.scaleTimestamp(42L, 1L));
    }
}
