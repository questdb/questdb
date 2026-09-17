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

package io.questdb.test.cairo.lv;

import io.questdb.PropertyKey;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineStoreWriter;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.std.str.StringSink;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;

/**
 * A refresh worker outlives every view it serves, and so do the previous-boundary shells
 * of its checkpoint writer. Each seal looks the live keys up in the root below it, and the
 * entries and partition-map nodes those lookups go through reuse exact-width key arrays.
 * This test serves views with disjoint key-length domains on one worker, drops each one,
 * and checks that no pool or cache of those shells keeps more than its retention limit,
 * however many widths the views before brought.
 */
public class LiveViewCheckpointWidthCacheRetentionTest extends AbstractLiveViewTest {

    private static final int BATCHES = 3;
    private static final long BATCH_STEP_MICROS = 10_000_000;
    // 2026-01-01T00:00:00Z: every batch lands inside one anchor day.
    private static final long DAY_START_MICROS = 1_767_225_600_000_000L;
    private static final int NARROW_KEY_COUNT = 16;
    /**
     * Image bytes a single width cache of an entry, or a single node decode pool, may keep
     * once its operation ends. Wide view i holds 1,024 keys of i * 1,024 + 1 to
     * (i + 1) * 1,024 characters, each encoded as a 4-byte length plus 2 bytes per
     * character, and a seal looks every one of them up through the same entry and node. A
     * cache that kept every width it has seen would hold 16,797,696 bytes after view 3 and
     * 26,240,000 bytes after view 4.
     */
    private static final long RETAINED_BYTES_LIMIT = 16_777_216;
    private static final int WIDE_CYCLES = 5;
    private static final int WIDE_KEY_COUNT = 1_024;

    @Before
    public void setUpCadence() {
        // One logical root per row, so every batch seals over the root below it.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setCurrentMicros(0);
    }

    @After
    public void resetClock() {
        setCurrentMicros(-1);
    }

    @Test
    public void testWorkerKeepsBoundedKeyWidthsAcrossDroppedViews() throws Exception {
        assertMemoryLeak(() -> {
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                for (int cycle = 0; cycle < WIDE_CYCLES; cycle++) {
                    final int firstKeyChars = cycle * WIDE_KEY_COUNT + 1;
                    createViewAndIngest(job, 'x', firstKeyChars, WIDE_KEY_COUNT);
                    Assert.assertTrue(
                            "the seals of view " + cycle + " must have looked every key up in the root below",
                            assertViewSealedCleanly().getCheckpointCaptureWindowElisionProbes() >= WIDE_KEY_COUNT
                    );
                    assertQuery("SELECT count() c, sum(s) total FROM lv")
                            .noRandomAccess()
                            .expectSize()
                            .returns("c\ttotal\n" + (BATCHES * WIDE_KEY_COUNT) + '\t' + (6.0 * WIDE_KEY_COUNT) + '\n');

                    if (cycle == 0) {
                        final long liveBytes = timelineWriter(job).getLargestRetainedPreviousBoundaryBufferBytesForTest();
                        Assert.assertTrue(
                                "the lookups of the first view must have pooled every key width [largestRetainedBytes="
                                        + liveBytes + ']',
                                liveBytes >= encodedKeyBytes(firstKeyChars, WIDE_KEY_COUNT)
                        );
                    }

                    dropViewAndBase(job);
                    assertRetainedBytesWithinBound(job, "after dropping wide view " + cycle);
                }

                createViewAndIngest(job, 'y', 1, NARROW_KEY_COUNT);
                assertViewSealedCleanly();
                assertQuery("SELECT * FROM lv")
                        .timestamp("ts")
                        .expectSize()
                        .returns(narrowViewRows());
                dropViewAndBase(job);
                assertRetainedBytesWithinBound(job, "after dropping the narrow view");
            }
        });
    }

    @Test
    public void testViewRestoresFromSealsThatOverflowTheWidthCaches() throws Exception {
        assertMemoryLeak(() -> {
            // Keys of 8,193 to 9,216 characters: 1,024 widths of 16,390 to 18,436 bytes, 17,830,912
            // bytes in all, more than a width cache or a decode pool may keep once an operation
            // ends, so the end of every seal and of the restore drops what its lookups and visits
            // pooled.
            final int firstKeyChars = 8_193;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                createViewAndIngest(job, 'x', firstKeyChars, WIDE_KEY_COUNT);
                assertViewSealedCleanly();
            }

            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                ingestBatch(job, 'x', firstKeyChars, WIDE_KEY_COUNT, BATCHES);
                assertRestoredFromTimeline("lv");
                assertViewSealedCleanly();
                // Each key's anchored running sum reaches 4 on the batch above the restored root.
                assertQuery("SELECT count() c, sum(s) total FROM lv")
                        .noRandomAccess()
                        .expectSize()
                        .returns("c\ttotal\n" + ((BATCHES + 1) * WIDE_KEY_COUNT) + '\t' + (10.0 * WIDE_KEY_COUNT) + '\n');
            }
        });
    }

    // Encoded bytes of keyCount keys of firstKeyChars characters and up, one character more per key.
    private static long encodedKeyBytes(int firstKeyChars, int keyCount) {
        return (long) keyCount * (Integer.BYTES + Character.BYTES * firstKeyChars)
                + (long) Character.BYTES * keyCount * (keyCount - 1) / 2;
    }

    private static String narrowViewRows() {
        final StringSink expected = new StringSink();
        expected.put("ts\tk\ts\n");
        for (int batch = 0; batch < BATCHES; batch++) {
            for (int i = 0; i < NARROW_KEY_COUNT; i++) {
                expected.putISODate(DAY_START_MICROS + batch * BATCH_STEP_MICROS + i).put('\t');
                for (int c = 0; c <= i; c++) {
                    expected.put('y');
                }
                expected.put('\t').put(batch + 1.0).put('\n');
            }
        }
        return expected.toString();
    }

    private static LiveViewCheckpointTimelineStoreWriter timelineWriter(LiveViewRefreshJob job) throws Exception {
        final Field field = LiveViewRefreshJob.class.getDeclaredField("checkpointTimelineStoreWriter");
        field.setAccessible(true);
        final LiveViewCheckpointTimelineStoreWriter writer = (LiveViewCheckpointTimelineStoreWriter) field.get(job);
        Assert.assertNotNull("the job must have sealed through its timeline writer", writer);
        return writer;
    }

    private void assertRetainedBytesWithinBound(LiveViewRefreshJob job, String phase) throws Exception {
        final long largestRetainedBytes = timelineWriter(job).getLargestRetainedPreviousBoundaryBufferBytesForTest();
        Assert.assertTrue(
                "no pool or cache of the worker's previous-boundary shells may keep more than its limit "
                        + phase + " [largestRetainedBytes=" + largestRetainedBytes + ", limit=" + RETAINED_BYTES_LIMIT + ']',
                largestRetainedBytes <= RETAINED_BYTES_LIMIT
        );
    }

    private LiveViewInstance assertViewSealedCleanly() {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull(instance);
        assertNoRefreshFaults("lv");
        Assert.assertEquals("the view must not fail a checkpoint seal", 0, instance.getCheckpointSealFailures());
        Assert.assertTrue(
                "the view must have sealed a fused window root",
                instance.getCheckpointCaptureWindowRoots() > 0
        );
        return instance;
    }

    private void createViewAndIngest(LiveViewRefreshJob job, char keyChar, int firstKeyChars, int keyCount) throws Exception {
        execute("CREATE TABLE tx (ts TIMESTAMP, k SYMBOL NOCACHE, v DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("""
                CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM BEGINNING AS
                SELECT ts, k, sum(v) OVER w s FROM tx
                WINDOW w AS (PARTITION BY k ORDER BY ts ANCHOR DAILY '00:00')
                """);
        driveRefreshToQuiescence(job);
        for (int batch = 0; batch < BATCHES; batch++) {
            ingestBatch(job, keyChar, firstKeyChars, keyCount, batch);
        }
    }

    private void dropViewAndBase(LiveViewRefreshJob job) throws Exception {
        execute("DROP LIVE VIEW lv");
        execute("DROP TABLE tx");
        driveRefreshToQuiescence(job);
    }

    // One row per key, the i-th key being firstKeyChars + i characters long, then a refresh.
    private void ingestBatch(LiveViewRefreshJob job, char keyChar, int firstKeyChars, int keyCount, int batch) {
        final StringSink key = new StringSink();
        try (WalWriter writer = engine.getWalWriter(engine.verifyTableName("tx"))) {
            for (int i = 0; i < keyCount; i++) {
                key.clear();
                for (int c = 0, n = firstKeyChars + i; c < n; c++) {
                    key.put(keyChar);
                }
                final TableWriter.Row row = writer.newRow(DAY_START_MICROS + batch * BATCH_STEP_MICROS + i);
                row.putSym(1, key);
                row.putDouble(2, 1);
                row.append();
            }
            writer.commit();
        }
        driveRefreshToQuiescence(job);
    }
}
