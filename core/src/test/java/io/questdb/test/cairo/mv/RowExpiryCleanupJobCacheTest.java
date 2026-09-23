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

package io.questdb.test.cairo.mv;

import io.questdb.cairo.RowExpiryCleanupJob;
import io.questdb.cairo.TableToken;
import io.questdb.std.CharSequenceLongHashMap;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.LongList;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

public class RowExpiryCleanupJobCacheTest extends AbstractCairoTest {
    private static final long HOUR_MICROS = 3_600_000_000L;
    private static final int OTHER_CACHE_SIZE = 4;
    private static final String PREDICATE = "v < 0";

    @Test
    public void testReleaseAllAbsentFloorsFromNonEmptySnapshot() throws Exception {
        assertReleaseAbsentFloors(16_380, 0, 16_380);
    }

    @Test
    public void testReleaseEmptySnapshotClearsOnlyAffectedCache() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken token = token("expired", 1);
            final TableToken otherToken = token("kept", 2);
            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                final RemovalCountingMap original = createCache(job, token, 16_380);
                final RemovalCountingMap other = createCache(job, otherToken, OTHER_CACHE_SIZE);

                release(job, token);

                Assert.assertEquals("bulk release must not remove keys individually", 0, original.removalCount);
                Assert.assertSame("an empty snapshot clears the existing map", original, generations(job, token));
                Assert.assertEquals(0, original.size());
                Assert.assertEquals(OTHER_CACHE_SIZE, cachedPartitionCount(job));
                Assert.assertSame(other, generations(job, otherToken));
                assertEntries(other, OTHER_CACHE_SIZE, 0, 0);

                release(job, token);
                Assert.assertEquals("releasing an empty cache must not debit the count twice",
                        OTHER_CACHE_SIZE, cachedPartitionCount(job));
            }
        });
    }

    @Test
    public void testReleaseManyAbsentFloorsPreservesOtherViews() throws Exception {
        assertReleaseAbsentFloors(16_380, 0, 12_285);
    }

    @Test
    public void testReleaseMissingFirstMiddleAndLastFloor() throws Exception {
        assertReleaseAbsentFloors(8, 0, 1);
        assertReleaseAbsentFloors(8, 4, 5);
        assertReleaseAbsentFloors(8, 7, 8);
    }

    @Test
    public void testReleaseUnchangedSnapshotDoesNotRebuild() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken token = token("kept", 1);
            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                final RemovalCountingMap original = createCache(job, token, 8);
                final LongList floors = partitionFloors(job);
                for (int i = 0; i < 9; i++) {
                    floors.add(floor(i));
                }

                release(job, token);

                Assert.assertSame("an unchanged cache must not allocate a replacement map",
                        original, generations(job, token));
                Assert.assertEquals(0, original.removalCount);
                Assert.assertEquals(8, cachedPartitionCount(job));
                assertEntries(original, 8, 0, 0);
                Assert.assertEquals("release must not invent a verdict for an uncached floor",
                        Long.MIN_VALUE, original.get(Long.toString(floor(8))));
            }
        });
    }

    private static void assertEntries(CharSequenceLongHashMap generations, int size, int removeFrom, int removeTo) {
        Assert.assertEquals(size - (removeTo - removeFrom), generations.size());
        for (int i = 0; i < size; i++) {
            Assert.assertEquals(
                    "generation at floor " + i,
                    i >= removeFrom && i < removeTo ? Long.MIN_VALUE : generation(i),
                    generations.get(Long.toString(floor(i)))
            );
        }
    }

    private static void assertReleaseAbsentFloors(int size, int removeFrom, int removeTo) throws Exception {
        assertMemoryLeak(() -> {
            final TableToken token = token("expired", 1);
            final TableToken otherToken = token("kept", 2);
            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                final RemovalCountingMap original = createCache(job, token, size);
                final RemovalCountingMap other = createCache(job, otherToken, OTHER_CACHE_SIZE);
                final LongList floors = partitionFloors(job);
                for (int i = 0; i < size; i++) {
                    if (i < removeFrom || i >= removeTo) {
                        floors.add(floor(i));
                    }
                }
                // A live, uncached partition must not acquire a verdict during release.
                floors.add(floor(size));

                release(job, token);

                Assert.assertEquals("bulk release must not remove keys individually", 0, original.removalCount);
                final CharSequenceLongHashMap retained = generations(job, token);
                Assert.assertNotSame(original, retained);
                assertEntries(retained, size, removeFrom, removeTo);
                Assert.assertEquals(Long.MIN_VALUE, retained.get(Long.toString(floor(size))));
                Assert.assertEquals(size - (removeTo - removeFrom) + OTHER_CACHE_SIZE, cachedPartitionCount(job));
                Assert.assertSame(other, generations(job, otherToken));
                Assert.assertEquals(0, other.removalCount);
                assertEntries(other, OTHER_CACHE_SIZE, 0, 0);
                Assert.assertEquals(PREDICATE, field(cache(job, token).getClass(), "predicate").get(cache(job, token)));

                release(job, token);
                Assert.assertSame("the next unchanged sweep must reuse the rebuilt map",
                        retained, generations(job, token));
                Assert.assertEquals(size - (removeTo - removeFrom) + OTHER_CACHE_SIZE, cachedPartitionCount(job));
            }
        });
    }

    private static Object cache(RowExpiryCleanupJob job, TableToken token) throws Exception {
        final CharSequenceObjHashMap<?> caches =
                (CharSequenceObjHashMap<?>) field(RowExpiryCleanupJob.class, "scalarPartitionCaches").get(job);
        return caches.get(token.getDirName());
    }

    private static int cachedPartitionCount(RowExpiryCleanupJob job) throws Exception {
        return field(RowExpiryCleanupJob.class, "cachedPartitionCount").getInt(job);
    }

    private static RemovalCountingMap createCache(RowExpiryCleanupJob job, TableToken token, int size) throws Exception {
        final Method method = RowExpiryCleanupJob.class.getDeclaredMethod("scalarPartitionCache", TableToken.class, String.class);
        method.setAccessible(true);
        final Object cache = method.invoke(job, token, PREDICATE);
        final RemovalCountingMap generations = new RemovalCountingMap(size);
        for (int i = 0; i < size; i++) {
            generations.put(Long.toString(floor(i)), generation(i));
        }
        field(cache.getClass(), "generations").set(cache, generations);
        field(RowExpiryCleanupJob.class, "cachedPartitionCount").setInt(job, cachedPartitionCount(job) + size);
        return generations;
    }

    private static Field field(Class<?> type, String name) throws Exception {
        final Field field = type.getDeclaredField(name);
        field.setAccessible(true);
        return field;
    }

    private static long floor(int index) {
        // Exercise negative floors and zero as well as positive timestamps.
        return (index - 4L) * HOUR_MICROS;
    }

    private static long generation(int index) {
        return 31L * index + 7;
    }

    private static CharSequenceLongHashMap generations(RowExpiryCleanupJob job, TableToken token) throws Exception {
        final Object cache = cache(job, token);
        return (CharSequenceLongHashMap) field(cache.getClass(), "generations").get(cache);
    }

    private static LongList partitionFloors(RowExpiryCleanupJob job) throws Exception {
        return (LongList) field(RowExpiryCleanupJob.class, "partitionFloors").get(job);
    }

    private static void release(RowExpiryCleanupJob job, TableToken token) throws Exception {
        final Method method = RowExpiryCleanupJob.class.getDeclaredMethod("releaseScalarPartitionsAbsentFromSnapshot", TableToken.class);
        method.setAccessible(true);
        method.invoke(job, token);
    }

    private static TableToken token(String name, int id) {
        return new TableToken(name, name + "~" + id, null, id, TableToken.Type.MAT_VIEW, true, false, false, false);
    }

    // Count the operation that scans the key list instead of asserting wall-clock timings.
    private static final class RemovalCountingMap extends CharSequenceLongHashMap {
        private int removalCount;

        private RemovalCountingMap(int capacity) {
            super(capacity, 0.5, Long.MIN_VALUE);
        }

        @Override
        public void removeAt(int index) {
            removalCount++;
            super.removeAt(index);
        }
    }
}
