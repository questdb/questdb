/*******************************************************************************
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

package io.questdb.test.cairo;

import io.questdb.cairo.PartitionGeometry;
import io.questdb.std.LongList;
import io.questdb.std.Rnd;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

/**
 * insertResolved and findResolved are pure over the private {@code resolved} cache - no table, no writer,
 * no files - so this drives the real methods by reflection.
 * <p>
 * The compaction sweep resolves composite partitions on an ascending partition walk and drops the whole
 * cache every sweep ({@code of() -> discard()}), so building C composite slots ran the old linear
 * {@code insertResolved} once per existing slot: O(C) an insert, O(C^2) a sweep. The fix binary-searches
 * the insertion point over the same (partitionTimestamp, nameTxn) ordering {@code findResolved} already
 * binary-searches. Two things have to hold: the search lands on the SAME slot the linear scan did (so the
 * cache stays sorted, the invariant {@code findResolved} depends on), and its cost is log-linear, not
 * quadratic. The reflection tests pin the first on the production method; the counter tests pin the second
 * on an exact operation count.
 */
public class PartitionGeometryInsertResolvedTest {

    private static final int LONGS_PER_RESOLVED = staticInt("LONGS_PER_RESOLVED");
    private static final int RES_NAME_TXN = staticInt("RES_NAME_TXN");
    private static final int RES_PARTITION_TS = staticInt("RES_PARTITION_TS");

    /**
     * The exact insert cost the OLD linear scan and the NEW binary search each pay, in slot comparisons,
     * to resolve C composite partitions on the ascending walk the sweep drives. The linear count is the
     * closed form C*(C-1)/2 - every insert appends at the tail but scans every slot to find that out - and
     * the binary count grows as C*log(C). Doubling C therefore near-quadruples the linear work and only
     * a bit more than doubles the binary work; the ratio is the proof, independent of any wall clock.
     */
    @Test
    public void testInsertCostIsLogLinearNotQuadratic() {
        final int c = 4096;

        final long linearC = linearInsertComparisons(c);
        final long linear2C = linearInsertComparisons(2 * c);
        final long binaryC = binaryInsertComparisons(c);
        final long binary2C = binaryInsertComparisons(2 * c);

        // The linear scan is exactly quadratic: it appends at the tail every time yet reads every slot.
        Assert.assertEquals((long) c * (c - 1) / 2, linearC);
        Assert.assertEquals((long) (2 * c) * (2 * c - 1) / 2, linear2C);

        // The binary search is bounded by C*(ceil(log2 C)+1) comparisons - log-linear.
        Assert.assertTrue("binary(C)=" + binaryC, binaryC <= (long) c * (ceilLog2(c) + 1));
        Assert.assertTrue("binary(2C)=" + binary2C, binary2C <= (long) (2 * c) * (ceilLog2(2 * c) + 1));

        // Doubling C: linear work near-quadruples (~4x), binary work barely more than doubles (<2.5x).
        Assert.assertTrue("linear ratio " + ((double) linear2C / linearC), (double) linear2C / linearC > 3.9);
        Assert.assertTrue("binary ratio " + ((double) binary2C / binaryC), (double) binary2C / binaryC < 2.5);

        // And the gap the fix closes: at C=4096 the linear scan pays ~200x the binary search's comparisons.
        Assert.assertTrue("gap " + ((double) linearC / binaryC), (double) linearC / binaryC > 150);
    }

    /**
     * The binary search must return the byte-for-byte same insertion index as the old linear scan for
     * every shape - empty cache, tail append, head insert, an interior gap, and equal-timestamp runs split
     * by nameTxn - across thousands of randomized-but-sorted caches. Same index means the fix is a drop-in
     * that cannot reorder the cache.
     */
    @Test
    public void testBinaryInsertPositionMatchesLinearScan() {
        final Rnd rnd = new Rnd();
        for (int iter = 0; iter < 20_000; iter++) {
            final int blocks = rnd.nextInt(40);
            final LongList resolved = new LongList();
            long ts = 0;
            long nameTxn = 0;
            for (int b = 0; b < blocks; b++) {
                // Ascending keys, sometimes repeating the timestamp with a higher nameTxn to build an
                // equal-timestamp run - the case findResolved's linear inner walk exists for.
                if (rnd.nextInt(3) == 0) {
                    nameTxn += 1 + rnd.nextInt(3);
                } else {
                    ts += 1 + rnd.nextInt(5);
                    nameTxn = rnd.nextInt(3);
                }
                appendResolved(resolved, ts, nameTxn);
            }
            // A query key that is absent from the cache (insertResolved only ever runs for one).
            final long qTs = rnd.nextInt((int) (ts + 3));
            final long qName = rnd.nextInt(6) - 1;
            if (containsKey(resolved, qTs, qName)) {
                continue;
            }
            Assert.assertEquals(
                    "iter " + iter + " ts=" + qTs + " name=" + qName,
                    linearInsertPosition(resolved, qTs, qName),
                    binaryInsertPosition(resolved, qTs, qName)
            );
        }
    }

    /**
     * Drives the REAL private insertResolved for shuffled keys, then asserts the cache came out sorted by
     * (partitionTimestamp, nameTxn) - the invariant findResolved's binary search relies on - and that the
     * REAL findResolved locates every inserted key at its slot and rejects a key that was never inserted.
     */
    @Test
    public void testRealInsertResolvedKeepsCacheSortedAndRetrievable() throws Exception {
        final PartitionGeometry geometry = new PartitionGeometry();
        try {
            final Method insertResolved = privateMethod("insertResolved", long.class, long.class);
            final Method findResolved = privateMethod("findResolved", long.class, long.class);
            final Field resolvedField = privateField("resolved");
            final LongList resolved = (LongList) resolvedField.get(geometry);

            // 300 distinct keys: several nameTxns share a timestamp, so equal-timestamp runs are exercised.
            final int keyCount = 300;
            final long[] tsKeys = new long[keyCount];
            final long[] nameKeys = new long[keyCount];
            for (int i = 0; i < keyCount; i++) {
                tsKeys[i] = (i / 3) * 1000L;
                nameKeys[i] = i % 3;
            }
            // Shuffle the insertion order so the fix cannot rely on already-ascending input.
            final Rnd rnd = new Rnd();
            for (int i = keyCount - 1; i > 0; i--) {
                final int j = rnd.nextInt(i + 1);
                final long tmpTs = tsKeys[i];
                tsKeys[i] = tsKeys[j];
                tsKeys[j] = tmpTs;
                final long tmpName = nameKeys[i];
                nameKeys[i] = nameKeys[j];
                nameKeys[j] = tmpName;
            }

            for (int i = 0; i < keyCount; i++) {
                final int at = (int) insertResolved.invoke(geometry, tsKeys[i], nameKeys[i]);
                Assert.assertEquals(tsKeys[i], resolved.getQuick(at + RES_PARTITION_TS));
                Assert.assertEquals(nameKeys[i], resolved.getQuick(at + RES_NAME_TXN));
            }

            // Invariant: the cache is sorted ascending by (partitionTimestamp, nameTxn).
            Assert.assertEquals((long) keyCount * LONGS_PER_RESOLVED, resolved.size());
            for (int i = LONGS_PER_RESOLVED; i < resolved.size(); i += LONGS_PER_RESOLVED) {
                final long prevTs = resolved.getQuick(i - LONGS_PER_RESOLVED + RES_PARTITION_TS);
                final long prevName = resolved.getQuick(i - LONGS_PER_RESOLVED + RES_NAME_TXN);
                final long ts = resolved.getQuick(i + RES_PARTITION_TS);
                final long name = resolved.getQuick(i + RES_NAME_TXN);
                Assert.assertTrue(
                        "unsorted at " + i + ": (" + prevTs + "," + prevName + ") !< (" + ts + "," + name + ")",
                        prevTs < ts || (prevTs == ts && prevName < name)
                );
            }

            // The real findResolved must locate every inserted key at a slot carrying that key.
            for (int i = 0; i < keyCount; i++) {
                final int slot = (int) findResolved.invoke(geometry, tsKeys[i], nameKeys[i]);
                Assert.assertTrue("missing key ts=" + tsKeys[i] + " name=" + nameKeys[i], slot > -1);
                Assert.assertEquals(tsKeys[i], resolved.getQuick(slot + RES_PARTITION_TS));
                Assert.assertEquals(nameKeys[i], resolved.getQuick(slot + RES_NAME_TXN));
            }
            // A key that was never inserted is rejected.
            Assert.assertEquals(-1, (int) findResolved.invoke(geometry, 7L, 7L));
        } finally {
            geometry.close();
        }
    }

    private static void appendResolved(LongList resolved, long ts, long nameTxn) {
        final int at = resolved.size();
        resolved.setPos(at + LONGS_PER_RESOLVED);
        for (int s = 0; s < LONGS_PER_RESOLVED; s++) {
            resolved.setQuick(at + s, 0);
        }
        resolved.setQuick(at + RES_PARTITION_TS, ts);
        resolved.setQuick(at + RES_NAME_TXN, nameTxn);
    }

    /**
     * The new insertResolved search, instrumented to count slot comparisons. Kept byte-for-byte equivalent
     * to the production loop so the count it reports is the count the real method pays.
     */
    private static int binaryInsertPosition(LongList resolved, long partitionTimestamp, long nameTxn) {
        return binaryInsertPosition(resolved, partitionTimestamp, nameTxn, null);
    }

    private static int binaryInsertPosition(LongList resolved, long partitionTimestamp, long nameTxn, long[] comparisons) {
        final int blocks = resolved.size() / LONGS_PER_RESOLVED;
        int lo = 0;
        int hi = blocks;
        while (lo < hi) {
            final int mid = (lo + hi) >>> 1;
            if (comparisons != null) {
                comparisons[0]++;
            }
            final long ts = resolved.getQuick(mid * LONGS_PER_RESOLVED + RES_PARTITION_TS);
            if (ts < partitionTimestamp
                    || (ts == partitionTimestamp && resolved.getQuick(mid * LONGS_PER_RESOLVED + RES_NAME_TXN) <= nameTxn)) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo * LONGS_PER_RESOLVED;
    }

    private static long binaryInsertComparisons(int c) {
        final LongList resolved = new LongList();
        final long[] comparisons = {0};
        for (int i = 0; i < c; i++) {
            final long ts = i * 1000L;
            final int at = binaryInsertPosition(resolved, ts, 0, comparisons);
            resolved.insert(at, LONGS_PER_RESOLVED);
            for (int s = 0; s < LONGS_PER_RESOLVED; s++) {
                resolved.setQuick(at + s, 0);
            }
            resolved.setQuick(at + RES_PARTITION_TS, ts);
        }
        return comparisons[0];
    }

    private static int ceilLog2(int n) {
        int bits = 0;
        int v = n - 1;
        while (v > 0) {
            v >>>= 1;
            bits++;
        }
        return bits;
    }

    private static boolean containsKey(LongList resolved, long ts, long nameTxn) {
        for (int i = 0; i < resolved.size(); i += LONGS_PER_RESOLVED) {
            if (resolved.getQuick(i + RES_PARTITION_TS) == ts && resolved.getQuick(i + RES_NAME_TXN) == nameTxn) {
                return true;
            }
        }
        return false;
    }

    /**
     * The OLD insertResolved search, instrumented to count slot comparisons - the linear scan from index 0.
     */
    private static int linearInsertPosition(LongList resolved, long partitionTimestamp, long nameTxn) {
        return linearInsertPosition(resolved, partitionTimestamp, nameTxn, null);
    }

    private static int linearInsertPosition(LongList resolved, long partitionTimestamp, long nameTxn, long[] comparisons) {
        final int n = resolved.size();
        int at = n;
        for (int i = 0; i < n; i += LONGS_PER_RESOLVED) {
            if (comparisons != null) {
                comparisons[0]++;
            }
            final long ts = resolved.getQuick(i + RES_PARTITION_TS);
            if (ts > partitionTimestamp || (ts == partitionTimestamp && resolved.getQuick(i + RES_NAME_TXN) > nameTxn)) {
                at = i;
                break;
            }
        }
        return at;
    }

    private static long linearInsertComparisons(int c) {
        final LongList resolved = new LongList();
        final long[] comparisons = {0};
        for (int i = 0; i < c; i++) {
            final long ts = i * 1000L;
            final int at = linearInsertPosition(resolved, ts, 0, comparisons);
            resolved.insert(at, LONGS_PER_RESOLVED);
            for (int s = 0; s < LONGS_PER_RESOLVED; s++) {
                resolved.setQuick(at + s, 0);
            }
            resolved.setQuick(at + RES_PARTITION_TS, ts);
        }
        return comparisons[0];
    }

    private static Field privateField(String name) throws NoSuchFieldException {
        final Field field = PartitionGeometry.class.getDeclaredField(name);
        field.setAccessible(true);
        return field;
    }

    private static Method privateMethod(String name, Class<?>... params) throws NoSuchMethodException {
        final Method method = PartitionGeometry.class.getDeclaredMethod(name, params);
        method.setAccessible(true);
        return method;
    }

    private static int staticInt(String name) {
        try {
            final Field field = PartitionGeometry.class.getDeclaredField(name);
            field.setAccessible(true);
            return field.getInt(null);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }
}
