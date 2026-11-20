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

package io.questdb.test.griffin.engine.functions.conditional;

import io.questdb.griffin.engine.functions.conditional.DoubleKeyIndex;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.DoubleList;
import io.questdb.std.Numbers;
import io.questdb.std.Rnd;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Compares DoubleKeyIndex with a scan that tries the keys in order with Numbers.equals(), as '='
 * and the searched form of CASE do.
 */
public class DoubleKeyIndexTest {
    private static final Log LOG = LogFactory.getLog(DoubleKeyIndexTest.class);
    private static final double TOL = Numbers.DOUBLE_TOLERANCE;
    // 0, where truncation widens cell 0; 2^19, from which equality is exact; 2^30, from which the
    // index hashes bits; small values a cell or two from 0; and the extremes
    private static final double[] ANCHORS = {
            0.0, TOL, 2 * TOL, 0.1, 1.0, 3.0, 100.5, 1L << 18, 1L << 19, 1L << 20, 1L << 29, 1L << 30, 1L << 31,
            1e15, 1L << 53, Double.MAX_VALUE, Double.MIN_VALUE, Double.MIN_NORMAL
    };
    // multiples of the tolerance at which a value stops or starts being equal to a key
    private static final double[] OFFSETS = {0, 0.5, 0.999, 1.0, 1.001, 1.5, 2.0, 2.5};
    // more keys than the index compares one by one, none near a test value: appending them makes
    // the index answer from its hash table
    private static final double[] PADDING = {77_700_001.5, 77_700_003.5, 77_700_005.5, 77_700_007.5, 77_700_009.5};

    @Test
    public void testPointsAroundBoundaries() {
        // each anchor with both signs: the 4 doubles next to it on each side, and the values a
        // few tolerances away with the doubles next to them
        final DoubleList points = new DoubleList();
        final int pointsPerAnchor = 8 + 4 * OFFSETS.length;
        for (double anchor : ANCHORS) {
            for (double a : new double[]{anchor, -anchor}) {
                double up = a;
                double down = a;
                for (int i = 0; i < 4; i++) {
                    points.add(up);
                    points.add(down);
                    up = Math.nextUp(up);
                    down = Math.nextDown(down);
                }
                for (double offset : OFFSETS) {
                    points.add(a + offset * TOL);
                    points.add(a - offset * TOL);
                    points.add(Math.nextUp(a + offset * TOL));
                    points.add(Math.nextDown(a - offset * TOL));
                }
            }
        }
        points.add(Double.NaN);
        points.add(Double.POSITIVE_INFINITY);
        points.add(Double.NEGATIVE_INFINITY);

        // Each point is a key on its own, against every point as a value. Pairs of points are keys
        // against the values around both: the pairs from one anchor and sign, the pairs within a
        // few tolerances of each other, which tie or clash, and every 97th of the other pairs.
        final DoubleList values = new DoubleList();
        for (int i = 0, n = points.size(); i < n; i++) {
            final double a = points.getQuick(i);
            assertAgrees(points, new double[]{a});
            for (int j = 0; j < n; j++) {
                final double b = points.getQuick(j);
                if (i / pointsPerAnchor == j / pointsPerAnchor || Math.abs(a - b) <= 8 * TOL || j % 97 == 0) {
                    values.clear();
                    addAround(values, a);
                    addAround(values, b);
                    assertAgrees(values, new double[]{a, b});
                }
            }
        }
    }

    @Test
    public void testRandomKeySets() {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        final DoubleList values = new DoubleList();
        for (int iteration = 0; iteration < 500; iteration++) {
            // up to 64 keys, so that the hash table grows several times
            final double[] keys = new double[1 + rnd.nextInt(64)];
            values.clear();
            for (int i = 0; i < keys.length; i++) {
                keys[i] = nextDouble(rnd);
                addAround(values, keys[i]);
                values.add(nextDouble(rnd));
            }
            assertAgrees(values, keys);
        }
    }

    private static void addAround(DoubleList values, double key) {
        for (double offset : OFFSETS) {
            values.add(key + offset * TOL);
            values.add(key - offset * TOL);
        }
        values.add(Math.nextUp(key));
        values.add(Math.nextDown(key));
    }

    // checks the keys on their own, which the index compares one by one, and with the padding
    private static void assertAgrees(DoubleList values, double[] keys) {
        assertAgrees0(values, keys);
        final double[] padded = new double[keys.length + PADDING.length];
        System.arraycopy(keys, 0, padded, 0, keys.length);
        System.arraycopy(PADDING, 0, padded, keys.length, PADDING.length);
        assertAgrees0(values, padded);
    }

    private static void assertAgrees0(DoubleList values, double[] keys) {
        final DoubleKeyIndex index = new DoubleKeyIndex();
        final DoubleList added = new DoubleList();
        for (double key : keys) {
            final int expected = firstEqual(added, key);
            Assert.assertEquals("duplicate of key " + key + " in " + added, expected, index.add(key));
            if (expected < 0) {
                added.add(key);
            }
        }
        for (int i = 0, n = values.size(); i < n; i++) {
            final double value = values.getQuick(i);
            Assert.assertEquals("value " + value + " in " + added, firstEqual(added, value), index.indexOf(value));
        }
    }

    private static int firstEqual(DoubleList keys, double value) {
        for (int i = 0, n = keys.size(); i < n; i++) {
            if (Numbers.equals(keys.getQuick(i), value)) {
                return i;
            }
        }
        return -1;
    }

    // whole numbers, decimals, values a few tolerances or ulps from an anchor, values just over
    // the tolerance apart, NULLs, any bits and any magnitude
    private static double nextDouble(Rnd rnd) {
        final double sign = rnd.nextBoolean() ? 1 : -1;
        return switch (rnd.nextInt(8)) {
            case 0 -> sign * rnd.nextInt(1_000);
            case 1 -> sign * rnd.nextInt(1_000) / 1000.0;
            case 2 -> sign * ANCHORS[rnd.nextInt(ANCHORS.length)] + (rnd.nextInt(9) - 4) * TOL * 0.5;
            case 3 -> sign * rnd.nextInt(8) * TOL * 1.1;
            case 4 -> {
                double v = sign * ANCHORS[rnd.nextInt(ANCHORS.length)];
                for (int i = rnd.nextInt(5); i > 0; i--) {
                    v = rnd.nextBoolean() ? Math.nextUp(v) : Math.nextDown(v);
                }
                yield v;
            }
            case 5 -> switch (rnd.nextInt(3)) {
                case 0 -> Double.NaN;
                case 1 -> Double.POSITIVE_INFINITY;
                default -> Double.NEGATIVE_INFINITY;
            };
            case 6 -> Double.longBitsToDouble(rnd.nextLong());
            default -> sign * rnd.nextDouble() * (1L << rnd.nextInt(40));
        };
    }
}
