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
 * quadratic. Both are pinned on the production method: the sorted-cache tests drive it by reflection, and
 * the cost test counts the slot reads it makes over a {@link LongList} that counts its own reads.
 */
public class PartitionGeometryInsertResolvedTest {

    private static final int LONGS_PER_RESOLVED = staticInt("LONGS_PER_RESOLVED");
    private static final int RES_NAME_TXN = staticInt("RES_NAME_TXN");
    private static final int RES_PARTITION_TS = staticInt("RES_PARTITION_TS");

    /**
     * The insert cost the REAL insertResolved pays, in slot reads, to resolve C composite partitions on the
     * ascending partition walk the sweep drives. The old linear scan read every slot already present before
     * appending at the tail - the closed form C*(C-1)/2 - while the binary search stays under
     * C*(ceil(log2 C)+1). Doubling C therefore near-quadruples the old cost and only a bit more than doubles
     * the current one, so the ratio is the regression signal, independent of any wall clock.
     */
    @Test
    public void testInsertCostIsLogLinearNotQuadratic() throws Exception {
        final int c = 4096;

        final long readsC = productionInsertSlotReads(c);
        final long reads2C = productionInsertSlotReads(2 * c);

        // Log-linear: one slot read per binary-search step, C inserts deep.
        Assert.assertTrue("reads(C)=" + readsC, readsC <= (long) c * (ceilLog2(c) + 1));
        Assert.assertTrue("reads(2C)=" + reads2C, reads2C <= (long) (2 * c) * (ceilLog2(2 * c) + 1));

        // Doubling C barely more than doubles the work (<2.5x); a quadratic insert near-quadruples it.
        Assert.assertTrue("reads ratio " + ((double) reads2C / readsC), (double) reads2C / readsC < 2.5);

        // The gap over the linear scan's closed form at C=4096: the scan pays more than 150x the reads.
        final long linearScanReads = (long) c * (c - 1) / 2;
        Assert.assertTrue("gap " + ((double) linearScanReads / readsC), (double) linearScanReads / readsC > 150);

        // A count, not a timing: the same C reports the same count on every run.
        Assert.assertEquals(readsC, productionInsertSlotReads(c));
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
     * The new insertResolved search. Kept byte-for-byte equivalent to the production loop so the index it
     * reports is the index the real method picks.
     */
    private static int binaryInsertPosition(LongList resolved, long partitionTimestamp, long nameTxn) {
        final int blocks = resolved.size() / LONGS_PER_RESOLVED;
        int lo = 0;
        int hi = blocks;
        while (lo < hi) {
            final int mid = (lo + hi) >>> 1;
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
     * The OLD insertResolved search - the linear scan from index 0.
     */
    private static int linearInsertPosition(LongList resolved, long partitionTimestamp, long nameTxn) {
        final int n = resolved.size();
        int at = n;
        for (int i = 0; i < n; i += LONGS_PER_RESOLVED) {
            final long ts = resolved.getQuick(i + RES_PARTITION_TS);
            if (ts > partitionTimestamp || (ts == partitionTimestamp && resolved.getQuick(i + RES_NAME_TXN) > nameTxn)) {
                at = i;
                break;
            }
        }
        return at;
    }

    /**
     * Slot reads the REAL insertResolved makes while resolving {@code compositeCount} composite partitions in
     * ascending partition order, the order the compaction sweep walks them in. The production method reaches
     * its resolved cache only through {@link LongList#getQuick}, so a cache that counts its own reads,
     * installed over the private field, reports the production search's own cost.
     */
    private static long productionInsertSlotReads(int compositeCount) throws Exception {
        final Method insertResolved = privateMethod("insertResolved", long.class, long.class);
        final Field resolvedField = privateField("resolved");
        final PartitionGeometry geometry = new PartitionGeometry();
        try {
            final CountingLongList resolved = new CountingLongList();
            resolvedField.set(geometry, resolved);
            for (int i = 0; i < compositeCount; i++) {
                final int at = (int) insertResolved.invoke(geometry, i * 1000L, 0L);
                // Ascending keys append at the tail, which is what makes the linear scan's cost quadratic.
                Assert.assertEquals(i * LONGS_PER_RESOLVED, at);
            }
            Assert.assertEquals(compositeCount * LONGS_PER_RESOLVED, resolved.size());
            return resolved.getReadCount();
        } finally {
            geometry.close();
        }
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

    /**
     * A resolved cache that counts the slot reads made over it. PartitionGeometry reads every slot through
     * {@link LongList#getQuick}, so overriding that one method turns the production search's cost into an
     * observable count and needs no instrumentation in production code.
     */
    private static final class CountingLongList extends LongList {
        private long readCount;

        @Override
        public long getQuick(int index) {
            readCount++;
            return super.getQuick(index);
        }

        long getReadCount() {
            return readCount;
        }
    }
}
