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

package io.questdb.test.griffin.engine.window;

import io.questdb.griffin.engine.functions.window.SwingingDoor;
import org.junit.Assert;
import org.junit.Test;

public class SwingingDoorTest {

    // Drives SDT over the given series and returns the final keep flags.
    private static boolean[] run(long[] ts, double[] value, boolean[] isNull, double compdev, boolean ignoreNulls) {
        boolean[] keep = new boolean[ts.length];
        SwingingDoor sd = new SwingingDoor();
        sd.configure(compdev);
        sd.reset();
        SwingingDoor.Sink sink = (index, k) -> keep[(int) index] = k;
        for (int i = 0; i < ts.length; i++) {
            sd.accept(i, ts[i], value[i], isNull != null && isNull[i], ignoreNulls, sink);
        }
        return keep;
    }

    private static boolean[] run(long[] ts, double[] value, double compdev) {
        return run(ts, value, null, compdev, false);
    }

    @Test
    public void testSinglePointKept() {
        Assert.assertArrayEquals(new boolean[]{true}, run(new long[]{1}, new double[]{5}, 0.5));
    }

    @Test
    public void testTwoPointsBothKept() {
        Assert.assertArrayEquals(new boolean[]{true, true}, run(new long[]{1, 2}, new double[]{1, 2}, 0.5));
    }

    @Test
    public void testMonotonicRampKeepsOnlyEndpoints() {
        // perfectly linear 1..5 -> keep first and last only
        boolean[] k = run(new long[]{1, 2, 3, 4, 5}, new double[]{1, 2, 3, 4, 5}, 0.5);
        Assert.assertArrayEquals(new boolean[]{true, false, false, false, true}, k);
    }

    @Test
    public void testWithinBandNoiseKeepsOnlyEndpoints() {
        // small wiggle within +/-0.5 of a flat line -> keep endpoints only
        boolean[] k = run(new long[]{1, 2, 3, 4, 5}, new double[]{0, 0.1, 0, 0.1, 0}, 0.5);
        Assert.assertArrayEquals(new boolean[]{true, false, false, false, true}, k);
    }

    @Test
    public void testBreakpointKeepsInteriorPoint() {
        // 0,1,2 then jump to 10,11 with compdev 0.5 -> keep 1,3,4,5 (index 2 dropped)
        boolean[] k = run(new long[]{1, 2, 3, 4, 5}, new double[]{0, 1, 2, 10, 11}, 0.5);
        Assert.assertArrayEquals(new boolean[]{true, false, true, true, true}, k);
    }

    @Test
    public void testCompdevZeroKeepsNonCollinear() {
        // zero tolerance: collinear middle dropped, bend kept
        // 0,1,2 collinear (slope 1); then 2->2 flat: bend at index 2
        boolean[] k = run(new long[]{1, 2, 3, 4}, new double[]{0, 1, 2, 2}, 0.0);
        Assert.assertArrayEquals(new boolean[]{true, false, true, true}, k);
    }

    @Test
    public void testRespectNullsFlushesLastPointBeforeGap() {
        // RESPECT NULLS: a null forces a kept boundary and resets the series.
        // The last real sample before the gap is flushed (kept); interior points drop.
        boolean[] k = run(new long[]{1, 2, 3, 4, 5, 6},
                new double[]{0, 0, 0, Double.NaN, 5, 5},
                new boolean[]{false, false, false, true, false, false},
                0.5, false);
        // idx0 anchor(T), idx1 interior(F), idx2 last-before-gap flushed(T),
        // idx3 null boundary(T), idx4 new anchor(T), idx5 last pending(T)
        Assert.assertArrayEquals(new boolean[]{true, false, true, true, true, true}, k);
    }

    @Test
    public void testIgnoreNullsSkipsNull() {
        // IGNORE NULLS: index 2 dropped and does not affect the door; series is 0,0,_,0,0 flat
        boolean[] k = run(new long[]{1, 2, 3, 4, 5},
                new double[]{0, 0, Double.NaN, 0, 0},
                new boolean[]{false, false, true, false, false},
                0.5, true);
        // flat line, null skipped -> keep first and last non-null only
        Assert.assertArrayEquals(new boolean[]{true, false, false, false, true}, k);
    }

    @Test
    public void testResetStartsNewSeries() {
        SwingingDoor sd = new SwingingDoor();
        sd.configure(0.5);
        sd.reset();
        boolean[] keep = new boolean[6];
        SwingingDoor.Sink sink = (index, k) -> keep[(int) index] = k;
        long[] ts = {1, 2, 3};
        double[] v = {1, 2, 3};
        for (int i = 0; i < 3; i++) sd.accept(i, ts[i], v[i], false, false, sink);
        sd.reset();
        for (int i = 3; i < 6; i++) sd.accept(i, ts[i - 3], v[i - 3], false, false, sink);
        // each 3-point ramp keeps its endpoints
        Assert.assertArrayEquals(new boolean[]{true, false, true, true, false, true}, keep);
    }

    @Test
    public void testBackwardSpanWiderThanLongMaxIsABoundary() {
        // A backward jump wider than Long.MAX wraps ts - anchorTs POSITIVE, so it reads as a
        // forward step unless the guard compares the timestamps themselves. Reachable with
        // valid nanosecond timestamps, which span only 292 years: 2100 -> 1700 -> 2150.
        // Without the ordering test the middle point is folded into the corridor and dropped.
        boolean[] k = run(new long[]{4_102_444_800_000_000_000L, -8_520_336_000_000_000_000L, 5_681_318_400_000_000_000L},
                new double[]{0, 0, 0}, 0.0);
        Assert.assertArrayEquals(new boolean[]{true, true, true}, k);
    }

    @Test
    public void testForwardSpanWiderThanLongMaxIsABoundary() {
        // The opposite wrap: a forward span over Long.MAX always wraps negative, so dt <= 0
        // already sees it. The corridor cannot represent such a span, so the points stay put
        // rather than being reconstructed from a wrapped denominator - keeping data is the
        // safe direction for a lossy filter. Collinear points would otherwise drop the middle.
        boolean[] k = run(new long[]{-9_000_000_000_000_000_000L, 0, 9_000_000_000_000_000_000L},
                new double[]{0, 50, 100}, 0.0);
        Assert.assertArrayEquals(new boolean[]{true, true, true}, k);
    }

    @Test
    public void testNullSentinelTimestampIsABoundary() {
        // Long.MIN_VALUE is the NULL timestamp sentinel. SdtWindowFunctionFactory folds a NULL
        // timestamp into the isNull flag before it reaches accept(), but the state machine is
        // engine-independent and must not mistake the sentinel for a forward step on its own.
        boolean[] k = run(new long[]{1_704_067_200_000_000L, Long.MIN_VALUE, 1_704_067_202_000_000L},
                new double[]{0, 0, 0}, 0.0);
        Assert.assertArrayEquals(new boolean[]{true, true, true}, k);
    }

    @Test
    public void testEqualTimestampsKeptAndReset() {
        // duplicate timestamp against the anchor -> dt<=0 branch: point is kept
        // and re-anchors, without dividing by zero.
        boolean[] k = run(new long[]{1, 1, 2}, new double[]{0, 9, 9}, 0.5);
        Assert.assertArrayEquals(new boolean[]{true, true, true}, k);
    }

    @Test
    public void testAsymmetricStepReconstructionExceedsCompdev() {
        // Keep-flag SDT selects original rows: the discarded point stays within
        // compdev of the door ENVELOPE, but the piecewise-linear reconstruction
        // between kept points can exceed compdev on a step. Here idx1 is dropped
        // yet the line idx0->idx2 gives 1.25 at t=1 vs actual 2.5 (err 1.25 > 1.0).
        // This matches PI/IoTDB; the test pins it so the behavior can't silently change.
        boolean[] k = run(new long[]{0, 1, 2, 3}, new double[]{0, 2.5, 2.5, 2.5}, 1.0);
        Assert.assertArrayEquals(new boolean[]{true, false, true, true}, k);
    }
}
