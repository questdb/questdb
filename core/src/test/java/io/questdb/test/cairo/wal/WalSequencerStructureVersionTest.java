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

package io.questdb.test.cairo.wal;

import io.questdb.cairo.TableToken;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * The sequencer publishes its metadata (column structure) version on the table's txn
 * tracker, where QWP commit batching compares it with a writer's version without taking
 * the sequencer lock. Every path that changes or (re)loads the version must publish it.
 */
public class WalSequencerStructureVersionTest extends AbstractCairoTest {

    @Test
    public void testAlterPublishesVersion() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken token = createTable("alter_t");
            execute("ALTER TABLE alter_t ADD COLUMN y INT");
            assertTrackerMatchesWriter(token, 1);
            execute("ALTER TABLE alter_t RENAME COLUMN y TO z");
            assertTrackerMatchesWriter(token, 2);
            execute("ALTER TABLE alter_t DROP COLUMN z");
            assertTrackerMatchesWriter(token, 3);
        });
    }

    @Test
    public void testCreatePublishesVersion() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken token = createTable("create_t");
            assertTrackerMatchesWriter(token, 0);
        });
    }

    @Test
    public void testDropMarksTrackerDropped() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken token = createTable("drop_t");
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(token);
            execute("DROP TABLE drop_t");
            Assert.assertTrue(tracker.isDropped());
        });
    }

    @Test
    public void testRenameTablePublishesVersion() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken token = createTable("rename_t");
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(token);
            execute("RENAME TABLE rename_t TO renamed_t");
            // The tracker is keyed by directory, which the rename keeps.
            final TableToken renamed = engine.verifyTableName("renamed_t");
            Assert.assertSame(tracker, engine.getTableSequencerAPI().getTxnTracker(renamed));
            assertTrackerMatchesWriter(renamed, 1);
        });
    }

    @Test
    public void testReopenedSequencerPublishesVersion() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken token = createTable("reopen_t");
            execute("ALTER TABLE reopen_t ADD COLUMN y INT");
            // Drops every tracker and closes every sequencer: the fresh tracker knows nothing
            // until a sequencer opens again and publishes.
            engine.getTableSequencerAPI().releaseAll();
            final SeqTxnTracker fresh = engine.getTableSequencerAPI().getTxnTracker(token);
            Assert.assertEquals(-1, fresh.getStructureVersion());
            engine.getTableSequencerAPI().lastTxn(token);
            assertTrackerMatchesWriter(token, 1);
        });
    }

    @Test
    public void testSuspendedAlterPublishesVersion() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken token = createTable("suspended_t");
            execute("ALTER TABLE suspended_t SUSPEND WAL");
            execute("ALTER TABLE suspended_t ADD COLUMN y INT");
            Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(token));
            assertTrackerMatchesWriter(token, 1);
        });
    }

    private static void assertTrackerMatchesWriter(TableToken token, long expectedVersion) {
        final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(token);
        Assert.assertEquals(expectedVersion, tracker.getStructureVersion());
        try (WalWriter writer = engine.getWalWriter(token)) {
            Assert.assertEquals(writer.getMetadataVersion(), tracker.getStructureVersion());
        }
    }

    private TableToken createTable(String name) throws Exception {
        execute("CREATE TABLE " + name + " (x LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
        return engine.verifyTableName(name);
    }
}
