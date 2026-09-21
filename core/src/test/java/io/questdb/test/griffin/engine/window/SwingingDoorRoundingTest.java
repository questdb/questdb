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
import io.questdb.std.Rnd;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;

public class SwingingDoorRoundingTest {
    @Test
    public void testExactBoundsAcrossExponentsAndTimestampSpans() {
        for (int exponent : new int[]{-1074, -1070, -1022, -512, -53, -1, 0, 52, 53, 100, 512, 1000, 1023}) {
            for (double sign : new double[]{-1, 1}) {
                double base = sign * Math.scalb(1.0, exponent);
                double ulp = Math.ulp(base);
                for (double width : new double[]{1, 1.75, 4.25}) {
                    double compdev = ulp * width;
                    double[] values = new double[6];
                    int[] offsets = {0, 2, 4, 6, 10, 8};
                    for (int i = 0; i < values.length; i++) {
                        values[i] = base + sign * offsets[i] * ulp;
                    }
                    for (long step : new long[]{1, 1_000_000, (1L << 53) + 1, Long.MAX_VALUE / 5}) {
                        long[] ts = new long[values.length];
                        for (int i = 0; i < ts.length; i++) {
                            ts[i] = i * step;
                        }
                        assertSeries(ts, values, compdev);
                    }
                }
            }
        }
    }

    @Test
    public void testExactBoundsAfterReanchoring() {
        double base = 0x1.0p53;
        // Row 2 crosses the first corridor and promotes row 1. The remaining rows reproduce
        // the large-offset failure against that promoted anchor, including its first interval.
        boolean[] isKept = assertSeries(
                new long[]{0, 1, 2, 3, 4, 5, 6},
                new double[]{base - 100, base, base + 4, base + 8, base + 12, base + 20, base + 16},
                3.5
        );
        Assert.assertArrayEquals(new boolean[]{true, true, false, false, false, true, true}, isKept);
    }

    @Test
    public void testExactBoundsForMixedMagnitudes() {
        assertSeries(new long[]{0, 1, 2}, new double[]{-1e20, 1000, 1e20}, 1);
        assertSeries(new long[]{0, 1, 2, 3}, new double[]{0, -0x1.0p80, 0x1.0p80, 0x1.8p81 + 0x1.0p29}, 2e8);
        assertSeries(new long[]{0, 1, 2, 3}, new double[]{0, -1e308, 1e308, 1e308}, 1);
        assertSeries(new long[]{0, 1, 2}, new double[]{1.7e308, 1e308, 1e308}, Double.MAX_VALUE);
        // Include both signed zeros, subnormal numerators and a divisor whose double conversion
        // saturates when cast back to long. The oracle never converts a timestamp to double.
        assertSeries(new long[]{0, (1L << 53) + 1, Long.MAX_VALUE}, new double[]{-0.0, Double.MIN_VALUE, 0.0}, Double.MIN_VALUE);
        assertSeries(new long[]{0, 1, Long.MAX_VALUE}, new double[]{0.0, -Double.MIN_VALUE, -0.0}, Double.MIN_VALUE);
    }

    @Test
    public void testExactBoundsWithRandomizedInputs() {
        Rnd rnd = new Rnd(0x5d7, 0x53);
        for (int trial = 0; trial < 512; trial++) {
            int exponent = rnd.nextInt(2098) - 1074;
            double base = Math.scalb(rnd.nextBoolean() ? 1.0 : -1.0, exponent);
            double ulp = Math.ulp(base);
            double compdev = Math.max(Double.MIN_VALUE, ulp * (0.25 + rnd.nextDouble() * 8));
            double[] values = new double[12];
            long[] ts = new long[values.length];
            long step = switch (trial % 3) {
                case 0 -> 1;
                case 1 -> (1L << 53) + 1;
                default -> Long.MAX_VALUE / (values.length - 1);
            };
            for (int i = 0; i < values.length; i++) {
                values[i] = base + (rnd.nextInt(41) - 20) * ulp;
                ts[i] = i * step;
            }
            if (trial % 5 == 0) {
                values[0] = -base;
            }
            assertSeries(ts, values, compdev);
        }
    }

    @Test
    public void testFlatSeriesWithSubUlpToleranceStillCompresses() {
        boolean[] isKept = assertSeries(new long[]{0, 1, 2, 3}, new double[]{0x1.0p53, 0x1.0p53, 0x1.0p53, 0x1.0p53}, 0.5);
        Assert.assertArrayEquals(new boolean[]{true, false, false, true}, isKept);
    }

    private static void assertCorridor(SwingingDoor sd, long[] ts, double[] values, double compdev, int end) {
        if ((sd.packFlags() & 2) == 0) {
            return;
        }
        // BigDecimal(double) expands the exact IEEE value, not its shortest decimal rendering.
        BigDecimal anchor = new BigDecimal(sd.anchorValue());
        BigDecimal dev = new BigDecimal(compdev);
        BigDecimal lo = new BigDecimal(sd.slopeLo());
        BigDecimal hi = new BigDecimal(sd.slopeHi());
        Assert.assertTrue(lo.compareTo(hi) <= 0);
        for (int i = (int) sd.anchorIndex() + 1; i <= end; i++) {
            BigDecimal dt = BigDecimal.valueOf(ts[i] - sd.anchorTs());
            BigDecimal delta = new BigDecimal(values[i]).subtract(anchor);
            Assert.assertTrue("lower slope widens the exact corridor at row " + i,
                    lo.multiply(dt).compareTo(delta.subtract(dev)) >= 0);
            Assert.assertTrue("upper slope widens the exact corridor at row " + i,
                    hi.multiply(dt).compareTo(delta.add(dev)) <= 0);
        }
    }

    private static void assertReconstruction(long[] ts, double[] values, double compdev, boolean[] isKept, int end) {
        Assert.assertTrue(isKept[0]);
        Assert.assertTrue(isKept[end]);
        int left = 0;
        for (int right = 1; right <= end; right++) {
            if (isKept[right]) {
                BigDecimal dt = BigDecimal.valueOf(ts[right] - ts[left]);
                BigDecimal anchor = new BigDecimal(values[left]);
                BigDecimal delta = new BigDecimal(values[right]).subtract(anchor);
                BigDecimal budget = new BigDecimal(compdev).multiply(BigDecimal.valueOf(2)).multiply(dt);
                for (int i = left + 1; i < right; i++) {
                    BigDecimal error = new BigDecimal(values[i]).subtract(anchor).multiply(dt)
                            .subtract(delta.multiply(BigDecimal.valueOf(ts[i] - ts[left]))).abs();
                    Assert.assertTrue("reconstruction exceeds 2 * compdev at row " + i
                                    + " [value=" + values[i] + ", compdev=" + compdev + ", end=" + end + ']',
                            error.compareTo(budget) <= 0);
                }
                left = right;
            }
        }
    }

    private static boolean[] assertSeries(long[] ts, double[] values, double compdev) {
        SwingingDoor sd = new SwingingDoor();
        sd.configure(compdev);
        sd.reset();
        boolean[] isKept = new boolean[values.length];
        SwingingDoor.Sink sink = (index, isKeep) -> isKept[(int) index] = isKeep;
        for (int i = 0; i < values.length; i++) {
            sd.accept(i, ts[i], values[i], false, false, sink);
            assertCorridor(sd, ts, values, compdev, i);
            assertReconstruction(ts, values, compdev, isKept, i);

            // Exercise the same serialized state that the SQL PARTITION BY map carries.
            SwingingDoor restored = new SwingingDoor();
            restored.configure(compdev);
            restored.load(sd.packFlags(), sd.anchorIndex(), sd.anchorTs(), sd.anchorValue(),
                    sd.slopeHi(), sd.slopeLo(), sd.pendingIndex(), sd.pendingTs(), sd.pendingValue());
            sd = restored;
        }
        return isKept;
    }
}
