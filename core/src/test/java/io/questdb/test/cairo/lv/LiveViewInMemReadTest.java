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

import io.questdb.cairo.TableReader;
import io.questdb.cairo.lv.LiveViewInMemoryBuffer;
import io.questdb.cairo.lv.LiveViewInMemoryTier;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.lv.LiveViewRecordCursor;
import io.questdb.griffin.engine.lv.LiveViewRecordCursorFactory;
import io.questdb.mp.Job;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Phase-3a in-memory-tier read path. Step 1 covers the consistency fence only:
 * a query may serve the in-mem tier (rather than disk) for a slot solely when
 * the slot's stamped LV-table seqTxn matches the disk reader's seqTxn. Routing
 * itself still runs through max-disk-ts, so these tests assert the fence
 * predicate (LiveViewRecordCursor.isRoutingEligible) and the stamp coordinate,
 * not query results.
 */
public class LiveViewInMemReadTest extends AbstractCairoTest {

    // A non-BACKFILL view drops rows below its CREATE wall-clock floor; pin the
    // clock below the (2026) test data so every row stays in-frame.
    @Before
    public void pinClockBelowTestData() {
        setCurrentMicros(0L);
    }

    @Test
    public void testFenceEligibleAndStampMatchesReaderSeqTxn() throws Exception {
        assertMemoryLeak(() -> {
            createIngestRefresh();
            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            LiveViewInMemoryTier tier = instance.getInMemoryTier();
            Assert.assertNotNull("tier must be allocated after refresh", tier);
            LiveViewInMemoryBuffer slot = tier.getSlot(tier.getPublishedIdx());
            Assert.assertTrue("published slot must hold rows", slot.rowCount() > 0);

            // The stamp source (sequencer writerTxn) must equal what a query
            // reader reports via getSeqTxn() - this is what the fence compares.
            try (TableReader reader = engine.getReader(instance.getLiveViewToken())) {
                Assert.assertEquals(reader.getSeqTxn(), slot.lvSeqTxn());
            }

            // Aligned identity read: same LV-table version on both sides.
            try (
                    RecordCursorFactory factory = select("SELECT * FROM lv");
                    LiveViewRecordCursor cursor = openLvCursor(factory)
            ) {
                Assert.assertTrue("aligned identity read must be routing-eligible", cursor.isRoutingEligible());
            }
        });
    }

    @Test
    public void testFenceNotEligibleForPrunedProjection() throws Exception {
        assertMemoryLeak(() -> {
            createIngestRefresh();
            // Pruning the timestamp leaves no full-schema identity projection, so
            // the in-mem tier cannot be addressed -> disk-only.
            try (
                    RecordCursorFactory factory = select("SELECT rn FROM lv");
                    LiveViewRecordCursor cursor = openLvCursor(factory)
            ) {
                Assert.assertFalse("pruned projection must not be routing-eligible", cursor.isRoutingEligible());
            }
        });
    }

    @Test
    public void testFenceNotEligibleOnSeqTxnMismatch() throws Exception {
        assertMemoryLeak(() -> {
            createIngestRefresh();
            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            LiveViewInMemoryTier tier = instance.getInMemoryTier();
            Assert.assertNotNull(tier);
            LiveViewInMemoryBuffer slot = tier.getSlot(tier.getPublishedIdx());
            Assert.assertTrue(slot.rowCount() > 0);

            // Force a mismatch: stamp the slot with a seqTxn the reader cannot
            // report. The fence must fall back to disk-only.
            slot.setLvSeqTxn(slot.lvSeqTxn() + 1000);
            try (
                    RecordCursorFactory factory = select("SELECT * FROM lv");
                    LiveViewRecordCursor cursor = openLvCursor(factory)
            ) {
                Assert.assertFalse("seqTxn mismatch must not be routing-eligible", cursor.isRoutingEligible());
            }
        });
    }

    // Creates a fixed-width LV with the in-mem tier on, ingests two rows, and
    // drives one refresh cycle so the published slot is populated and stamped.
    private void createIngestRefresh() throws Exception {
        execute("CREATE TABLE base (ts TIMESTAMP, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("CREATE LIVE VIEW lv FLUSH EVERY 1s IN MEMORY 30m AS " +
                "SELECT ts, x, row_number() OVER () AS rn FROM base WHERE x > 0");
        execute("INSERT INTO base (ts, x) VALUES " +
                "('2026-05-12T00:00:00.000001Z', 4), " +
                "('2026-05-12T00:00:00.000002Z', 9)");
        drainWalQueue();
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            drainJob(job);
        }
        drainWalQueue();
    }

    // Unwraps any QueryProgress wrapper to the LiveViewRecordCursorFactory and
    // opens its cursor, so the test can read the fence predicate directly.
    private static LiveViewRecordCursor openLvCursor(RecordCursorFactory factory) throws SqlException {
        RecordCursorFactory f = factory;
        while (f != null && !(f instanceof LiveViewRecordCursorFactory)) {
            f = f.getBaseFactory();
        }
        Assert.assertNotNull("expected a LiveViewRecordCursorFactory in the plan", f);
        return (LiveViewRecordCursor) f.getCursor(sqlExecutionContext);
    }

    private static boolean drainJob(Job job) {
        boolean any = false;
        for (int i = 0; i < 64 && job.run(); i++) {
            any = true;
        }
        return any;
    }
}
