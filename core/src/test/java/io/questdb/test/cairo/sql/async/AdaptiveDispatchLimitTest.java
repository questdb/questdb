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

package io.questdb.test.cairo.sql.async;

import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.async.AdaptiveDispatchLimit;
import org.junit.Assert;
import org.junit.Test;

// The expected limits follow from the rules in the javadoc of AdaptiveDispatchLimit: the larger of
// the initial limit, the collected frames rounded down to a power of two and, under a known row
// count, ceil(neededRows * collectedFrames / max(foundRows, 1)); under an unknown row count, no
// limit once the frames of the initial limit held no row.
public class AdaptiveDispatchLimitTest {
    private static final int INITIAL_LIMIT = 4;

    @Test
    public void testAnnouncedEveryRowIsUnbounded() {
        // LIMIT 5, 9223372036854775807 reads every row past the fifth
        final AdaptiveDispatchLimit limit = limitedRead();
        limit.onSkip(0, 5, Long.MAX_VALUE - 5);
        Assert.assertEquals(Integer.MAX_VALUE, limit.get());

        // a bound past the last representable row saturates to every row
        final AdaptiveDispatchLimit saturated = limitedRead();
        saturated.onSkip(0, 5, Long.MAX_VALUE - 1);
        Assert.assertEquals(Integer.MAX_VALUE, saturated.get());
    }

    @Test
    public void testCursorLimitAndAnnouncedRowsTakeTheSmaller() {
        // LIMIT 10 on the cursor, the reader announces 100 rows: 10 rows count
        final AdaptiveDispatchLimit cursorSmaller = new AdaptiveDispatchLimit(INITIAL_LIMIT);
        cursorSmaller.of(10);
        cursorSmaller.onSkip(0, 0, 100);
        collect(cursorSmaller, 4, 1);
        // 6 rows needed at 1 row per frame
        Assert.assertEquals(6, cursorSmaller.get());

        // LIMIT 100 on the cursor, the reader announces 10 rows: 10 rows count
        final AdaptiveDispatchLimit announcedSmaller = new AdaptiveDispatchLimit(INITIAL_LIMIT);
        announcedSmaller.of(100);
        announcedSmaller.onSkip(0, 0, 10);
        collect(announcedSmaller, 4, 1);
        Assert.assertEquals(6, announcedSmaller.get());

        // a reader that announces every row leaves the LIMIT of the cursor in charge
        final AdaptiveDispatchLimit everyRow = new AdaptiveDispatchLimit(INITIAL_LIMIT);
        everyRow.of(10);
        everyRow.onSkip(0, 0, Long.MAX_VALUE);
        everyRow.onSkip(0, 1, Long.MAX_VALUE - 1);
        Assert.assertEquals(INITIAL_LIMIT, everyRow.get());
        collect(everyRow, 4, 1);
        Assert.assertEquals(6, everyRow.get());
    }

    @Test
    public void testInitialLimitBelowOneStays() {
        // a dispatch limit below one makes the owner thread reduce every frame, at any yield
        for (int initialLimit = -1; initialLimit < 1; initialLimit++) {
            final AdaptiveDispatchLimit limit = new AdaptiveDispatchLimit(initialLimit);
            limit.of(1_000);
            Assert.assertEquals(initialLimit, limit.get());
            collect(limit, 64, 0);
            Assert.assertEquals(initialLimit, limit.get());
        }
    }

    @Test
    public void testIsLimitedRead() {
        final AdaptiveDispatchLimit limit = new AdaptiveDispatchLimit(INITIAL_LIMIT);
        Assert.assertFalse(limit.isLimitedRead());
        // a read without a LIMIT of its own is limited once its parent announces it
        limit.of(Long.MAX_VALUE);
        Assert.assertFalse(limit.isLimitedRead());
        limit.expectLimitedRead();
        Assert.assertTrue(limit.isLimitedRead());
        // toTop() keeps the read limited, and opening it again drops the parent's announcement
        limit.toTop();
        Assert.assertTrue(limit.isLimitedRead());
        limit.of(Long.MAX_VALUE);
        Assert.assertFalse(limit.isLimitedRead());
        // a LIMIT of its own makes the read limited
        limit.of(10);
        Assert.assertTrue(limit.isLimitedRead());
    }

    @Test
    public void testKnownRowCountSizesToTheFramesStillNeeded() {
        final AdaptiveDispatchLimit limit = new AdaptiveDispatchLimit(INITIAL_LIMIT);
        // LIMIT 10 on the cursor
        limit.of(10);
        Assert.assertEquals(INITIAL_LIMIT, limit.get());
        // 4 frames, 1 row each: 6 rows needed take 6 frames
        collect(limit, 4, 1);
        Assert.assertEquals(6, limit.get());
        // 6 frames, 6 rows: 4 rows take 4 frames
        collect(limit, 2, 1);
        Assert.assertEquals(4, limit.get());
        // 9 frames, 9 rows: 1 row takes 1 frame, the 9 collected frames keep the limit at 8
        collect(limit, 3, 1);
        Assert.assertEquals(8, limit.get());
        // 10 rows found: the reader needs no more, yet asks; 1 row at 10 rows per 10 frames, 8 frames stay
        collect(limit, 1, 1);
        Assert.assertEquals(8, limit.get());
    }

    @Test
    public void testNeededFramesRoundUp() {
        final AdaptiveDispatchLimit limit = new AdaptiveDispatchLimit(1);
        limit.of(1_000);
        // 3 frames, 300 rows: 700 rows needed at 100 rows per frame take exactly 7 frames
        collect(limit, 3, 100);
        Assert.assertEquals(7, limit.get());
        // 4 frames, 301 rows: 699 * 4 / 301 = 9.29 frames, rounded up to 10
        limit.onFrameCollected(1);
        Assert.assertEquals(10, limit.get());
    }

    @Test
    public void testNoRowFoundCountsAsOne() {
        final AdaptiveDispatchLimit limit = new AdaptiveDispatchLimit(INITIAL_LIMIT);
        limit.of(10);
        // 4 empty frames: 10 rows at 1 row per 4 frames take 40 frames
        collect(limit, 4, 0);
        Assert.assertEquals(40, limit.get());
        // one row in 5 frames: 9 rows take 45 frames
        limit.onFrameCollected(1);
        Assert.assertEquals(45, limit.get());
    }

    @Test
    public void testOfOpensAnUnlimitedRead() {
        final AdaptiveDispatchLimit limit = limitedRead();
        limit.onSkip(0, 0, 10);
        collect(limit, 8, 0);
        // a read without a LIMIT and without a parent's call dispatches every frame
        limit.of(Long.MAX_VALUE);
        Assert.assertEquals(Integer.MAX_VALUE, limit.get());
        limit.onSkip(0, 0, 10);
        Assert.assertEquals(Integer.MAX_VALUE, limit.get());
        collect(limit, 4, 1);
        Assert.assertEquals(Integer.MAX_VALUE, limit.get());
    }

    @Test
    public void testOverflowSaturates() {
        // LIMIT 9223372036854775806: rows needed times frames collected overflows a long
        final AdaptiveDispatchLimit limit = new AdaptiveDispatchLimit(INITIAL_LIMIT);
        limit.of(Long.MAX_VALUE - 1);
        collect(limit, 3, 0);
        Assert.assertEquals(Integer.MAX_VALUE, limit.get());

        // one frame: (2^63 - 2) * 1 / 1 fits a long, but not an int
        final AdaptiveDispatchLimit oneFrame = new AdaptiveDispatchLimit(INITIAL_LIMIT);
        oneFrame.of(Long.MAX_VALUE - 1);
        oneFrame.onFrameCollected(0);
        Assert.assertEquals(Integer.MAX_VALUE, oneFrame.get());

        // skipped and consumed rows that sum past a long saturate the announced rows
        final AdaptiveDispatchLimit skip = limitedRead();
        skip.onFrameCollected(Long.MAX_VALUE / 2);
        skip.onSkip(0, Long.MAX_VALUE / 2 + 2, 0);
        Assert.assertEquals(Integer.MAX_VALUE, skip.get());
    }

    @Test
    public void testSkipCountsTheRowsTheReaderConsumed() {
        final AdaptiveDispatchLimit limit = limitedRead();
        // one frame of 100 rows, of which the reader read 30
        limit.onFrameCollected(100);
        limit.onSkip(70, 5, 10);
        // 30 + 5 + 10 = 45 rows, fewer than the frame holds: the collected frame keeps the initial limit
        Assert.assertEquals(INITIAL_LIMIT, limit.get());
        // 1 more row needed at 100 rows per frame takes 1 frame
        collect(limit, 7, 0);
        // 8 frames, 100 rows: max(1 * 8 / 100 rounded up, 8) = 8
        Assert.assertEquals(8, limit.get());

        // pending rows above the collected ones count as no row consumed
        final AdaptiveDispatchLimit pending = limitedRead();
        pending.onSkip(5, 0, 12);
        collect(pending, 4, 2);
        // 12 rows announced, 8 found in 4 frames: 4 rows take 2 frames
        Assert.assertEquals(INITIAL_LIMIT, pending.get());
        collect(pending, 4, 0);
        // 8 frames, 8 rows: 4 rows take 4 frames, 8 frames collected
        Assert.assertEquals(8, pending.get());
    }

    @Test
    public void testSkipSizesTheRead() {
        // LIMIT 50, 60 announces 60 rows
        final AdaptiveDispatchLimit limit = limitedRead();
        limit.onSkip(0, 50, 10);
        Assert.assertEquals(INITIAL_LIMIT, limit.get());
        collect(limit, 4, 1);
        // 56 rows at 1 row per frame take 56 frames
        Assert.assertEquals(56, limit.get());
        // the latest bound replaces the earlier one: 4 consumed + 2 skipped + 2 = 8 rows
        limit.onSkip(0, 2, 2);
        Assert.assertEquals(INITIAL_LIMIT, limit.get());
        // a bound the reader does not know leaves the last one in charge
        limit.onSkip(0, 0, RecordCursor.UNBOUNDED_ROW_COUNT);
        Assert.assertEquals(INITIAL_LIMIT, limit.get());
        collect(limit, 3, 0);
        // 7 frames, 4 rows: 4 rows take 7 frames
        Assert.assertEquals(7, limit.get());
    }

    @Test
    public void testToTopRestartsALimitedRead() {
        final AdaptiveDispatchLimit limit = limitedRead();
        limit.onSkip(0, 0, 100);
        collect(limit, 4, 1);
        Assert.assertEquals(96, limit.get());
        limit.toTop();
        // still limited, with no frame collected and no bound announced yet
        Assert.assertEquals(INITIAL_LIMIT, limit.get());
        collect(limit, 4, 1);
        Assert.assertEquals(INITIAL_LIMIT, limit.get());

        // toTop() keeps the LIMIT of the cursor
        final AdaptiveDispatchLimit cursorLimit = new AdaptiveDispatchLimit(INITIAL_LIMIT);
        cursorLimit.of(10);
        collect(cursorLimit, 4, 1);
        cursorLimit.toTop();
        Assert.assertEquals(INITIAL_LIMIT, cursorLimit.get());
        collect(cursorLimit, 4, 1);
        Assert.assertEquals(6, cursorLimit.get());
    }

    @Test
    public void testUnknownRowCountDoublesWithTheCollectedFrames() {
        final AdaptiveDispatchLimit limit = limitedRead();
        // the collected frames rounded down to a power of two, at least the initial limit
        final int[] expected = {
                4, 4, 4, 4, 4, 4, 4, 4, // 0..7 frames
                8, 8, 8, 8, 8, 8, 8, 8, // 8..15 frames
                16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, // 16..31 frames
                32
        };
        for (int frames = 0; frames < expected.length; frames++) {
            Assert.assertEquals("frames: " + frames, expected[frames], limit.get());
            // the yield of the frames does not count without a known row count
            limit.onFrameCollected(frames % 3 == 0 ? 0 : 1_000);
        }
    }

    @Test
    public void testUnknownRowCountWithoutRowsLiftsTheLimit() {
        final AdaptiveDispatchLimit limit = limitedRead();
        // fewer empty frames than the initial limit keep the limit
        collect(limit, 3, 0);
        Assert.assertEquals(INITIAL_LIMIT, limit.get());
        // the frames of the initial limit held no row: nothing to stop at
        limit.onFrameCollected(0);
        Assert.assertEquals(Integer.MAX_VALUE, limit.get());
        collect(limit, 20, 0);
        Assert.assertEquals(Integer.MAX_VALUE, limit.get());
        // a row found later returns to doubling: 25 frames rounded down to 16
        limit.onFrameCollected(1);
        Assert.assertEquals(16, limit.get());
        // toTop() restarts the read
        limit.toTop();
        Assert.assertEquals(INITIAL_LIMIT, limit.get());

        // a row in the frames of the initial limit keeps doubling
        final AdaptiveDispatchLimit row = limitedRead();
        collect(row, 3, 0);
        row.onFrameCollected(1);
        Assert.assertEquals(INITIAL_LIMIT, row.get());
        collect(row, 4, 0);
        Assert.assertEquals(8, row.get());

        // a known row count projects the yield instead: 10 rows at 1 row per 4 frames take 40 frames
        final AdaptiveDispatchLimit known = new AdaptiveDispatchLimit(INITIAL_LIMIT);
        known.of(10);
        collect(known, 4, 0);
        Assert.assertEquals(40, known.get());
    }

    @Test
    public void testUnlimitedReadIsUnbounded() {
        final AdaptiveDispatchLimit limit = new AdaptiveDispatchLimit(INITIAL_LIMIT);
        Assert.assertEquals(Integer.MAX_VALUE, limit.get());
        limit.of(Long.MAX_VALUE);
        Assert.assertEquals(Integer.MAX_VALUE, limit.get());
        // a skip bound alone does not make the read limited: a negative LIMIT reads to the end
        limit.onSkip(0, 0, 10);
        collect(limit, 4, 1);
        Assert.assertEquals(Integer.MAX_VALUE, limit.get());
        limit.toTop();
        Assert.assertEquals(Integer.MAX_VALUE, limit.get());
    }

    private static void collect(AdaptiveDispatchLimit limit, int frameCount, long rowsPerFrame) {
        for (int i = 0; i < frameCount; i++) {
            limit.onFrameCollected(rowsPerFrame);
        }
    }

    private static AdaptiveDispatchLimit limitedRead() {
        final AdaptiveDispatchLimit limit = new AdaptiveDispatchLimit(INITIAL_LIMIT);
        limit.of(Long.MAX_VALUE);
        limit.expectLimitedRead();
        return limit;
    }
}
