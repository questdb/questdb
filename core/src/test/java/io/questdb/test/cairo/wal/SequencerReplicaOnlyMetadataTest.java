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

import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Verifies that the replica-only index flag is serialized in the sequencer's own on-disk
 * metadata format ({@code _txn_seq/_meta}) and survives a cold reload of the sequencer from
 * disk for indexes that are part of the table's structure at CREATE time. This guards replica
 * reconstruction and post-failover skip behaviour, which rebuild table metadata from the
 * downloaded sequencer metadata rather than from the table {@code _meta}.
 * <p>
 * Note on ALTER: {@code ALTER TABLE ... ADD INDEX} is a non-structural change for WAL tables.
 * It is recorded as a deferred SQL command in the WAL (it does NOT call
 * {@code TableSequencer.nextStructureTxn}), so it never rewrites the sequencer's own
 * {@code _txn_seq/_meta}; the index — including its replica-only flag — materializes when the
 * WAL apply job replays the SQL against the {@code TableWriter}, updating the table
 * {@code _meta}. The replica-only flag for ALTER-added indexes is therefore asserted against
 * the table metadata after a cold reload, which is where it is authoritative.
 */
public class SequencerReplicaOnlyMetadataTest extends AbstractCairoTest {

    @Test
    public void testReplicaOnlyFlagFromAlterSurvivesColdReload() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (s symbol capacity 256, n symbol index capacity 128, ts timestamp) " +
                    "timestamp(ts) partition by day wal");
            // ALTER ADD INDEX ... REPLICA ONLY is applied to the table metadata via the WAL apply job.
            execute("alter table x alter column s add index capacity 256 replica only");
            drainWalQueue();

            // Cold reload of all cached metadata, then assert the table metadata round-trips the flag.
            engine.clear();
            assertTableReplicaOnly("x", true, false);
        });
    }

    @Test
    public void testReplicaOnlyFlagSurvivesSequencerReload() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (s symbol index capacity 256 replica only, n symbol index capacity 128, ts timestamp) " +
                    "timestamp(ts) partition by day wal");

            TableToken tableToken = engine.verifyTableName("x");
            // CREATE-time indexes are part of the sequencer's own metadata; assert the flag
            // survives a cold reload that re-reads _txn_seq/_meta through the sequencer.
            assertSequencerReplicaOnlyAfterColdReload(tableToken, true, false);
        });
    }

    /**
     * Forces the sequencer to re-read its metadata from disk and asserts the per-column
     * replica-only flags. The reload is forced by {@code engine.clear()}, which releases the
     * cached sequencer; the subsequent {@code engine.getSequencerMetadata()} reopens the
     * sequencer and parses the on-disk {@code _txn_seq/_meta} via
     * {@code SequencerMetadata.loadSequencerMetadata}, exercising the replica-only optional
     * section read path, then copies the columns into the pooled metadata view.
     */
    private void assertSequencerReplicaOnlyAfterColdReload(TableToken tableToken, boolean expectedS, boolean expectedN) {
        engine.clear();
        try (TableRecordMetadata metadata = engine.getSequencerMetadata(tableToken)) {
            int sIdx = metadata.getColumnIndexQuiet("s");
            int nIdx = metadata.getColumnIndexQuiet("n");
            Assert.assertTrue("expected column 's' to exist", sIdx > -1);
            Assert.assertTrue("expected column 'n' to exist", nIdx > -1);

            Assert.assertEquals(
                    "replica-only flag for 's' lost after sequencer reload",
                    expectedS,
                    metadata.isColumnReplicaOnlyIndex(sIdx)
            );
            // Back-compat / negative case: a normal indexed column must round-trip as replicaOnly=false.
            Assert.assertEquals(
                    "non-replica-only indexed column 'n' must not become replica-only after reload",
                    expectedN,
                    metadata.isColumnReplicaOnlyIndex(nIdx)
            );
        }
    }

    private void assertTableReplicaOnly(String tableName, boolean expectedS, boolean expectedN) {
        try (TableReader reader = engine.getReader(tableName)) {
            TableRecordMetadata metadata = reader.getMetadata();
            int sIdx = metadata.getColumnIndexQuiet("s");
            int nIdx = metadata.getColumnIndexQuiet("n");
            Assert.assertTrue("expected column 's' to exist", sIdx > -1);
            Assert.assertTrue("expected column 'n' to exist", nIdx > -1);

            Assert.assertEquals(
                    "replica-only flag for 's' lost after cold reload",
                    expectedS,
                    metadata.isColumnReplicaOnlyIndex(sIdx)
            );
            Assert.assertEquals(
                    "non-replica-only indexed column 'n' must not become replica-only after cold reload",
                    expectedN,
                    metadata.isColumnReplicaOnlyIndex(nIdx)
            );
        }
    }
}
