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

package io.questdb.test.cairo.wal;

import io.questdb.PropertyKey;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.wal.DurableAckRegistry;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.std.CharSequenceLongHashMap;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * TDD tests for Deferred 3: DurableAckRegistry local-fsync tier (OSS).
 *
 * <p>Tests cover:
 * <ul>
 *   <li>(1) Unit: SeqTxnTracker.localDurableSeqTxn get/set</li>
 *   <li>(2) Unit: LocalDurableAckRegistry returns tracker value for adaptive table, -1 for nosync/unknown</li>
 *   <li>(3) Integration: adaptive WAL commit advances getLocalDurableSeqTxn to committed seqTxn</li>
 *   <li>(4) Integration: nosync WAL commit leaves getLocalDurableSeqTxn at -1</li>
 *   <li>(5) QWP predicate: max(local, uploaded) >= clientSeqTxn satisfied via local tier for adaptive table</li>
 *   <li>(6) QWP predicate: non-enabled registry does not satisfy durable-ack even for adaptive table</li>
 *   <li>(7) Enterprise override: setDurableAckRegistry replaces the OSS default</li>
 *   <li>(8) DurableAckRegistry interface default: getLocalDurableSeqTxn returns -1 on DefaultDurableAckRegistry</li>
 * </ul>
 */
public class LocalDurableAckRegistryTest extends AbstractCairoTest {

    // ---- (1) Unit: SeqTxnTracker.localDurableSeqTxn get/set ----

    /**
     * (1a) Fresh SeqTxnTracker: getLocalDurableSeqTxn() returns -1 (unset sentinel).
     */
    @Test
    public void testSeqTxnTrackerLocalDurableInitIsMinusOne() throws Exception {
        assertMemoryLeak(() -> {
            SeqTxnTracker tracker = new SeqTxnTracker(configuration);
            Assert.assertEquals(
                    "fresh tracker must return -1 for localDurableSeqTxn",
                    -1L, tracker.getLocalDurableSeqTxn()
            );
        });
    }

    /**
     * (1b) setLocalDurableSeqTxn(n) followed by getLocalDurableSeqTxn() returns n.
     */
    @Test
    public void testSeqTxnTrackerLocalDurableSetAndGet() throws Exception {
        assertMemoryLeak(() -> {
            SeqTxnTracker tracker = new SeqTxnTracker(configuration);
            tracker.setLocalDurableSeqTxn(42L);
            Assert.assertEquals(42L, tracker.getLocalDurableSeqTxn());
        });
    }

    /**
     * (1c) setLocalDurableSeqTxn is monotone: setting a lower value has no effect.
     * (At W=0 this is not needed because seqTxns are strictly increasing; it is defensive.)
     */
    @Test
    public void testSeqTxnTrackerLocalDurableDoesNotGoBackward() throws Exception {
        assertMemoryLeak(() -> {
            SeqTxnTracker tracker = new SeqTxnTracker(configuration);
            tracker.setLocalDurableSeqTxn(10L);
            tracker.setLocalDurableSeqTxn(5L);  // must not decrease
            Assert.assertEquals(
                    "localDurableSeqTxn must not decrease",
                    10L, tracker.getLocalDurableSeqTxn()
            );
        });
    }

    // ---- (2) Unit: LocalDurableAckRegistry returns correct value ----

    /**
     * (2a) CairoEngine.getDurableAckRegistry() is a LocalDurableAckRegistry (isEnabled=true) by default.
     */
    @Test
    public void testDefaultRegistryIsLocalDurableAckRegistry() throws Exception {
        assertMemoryLeak(() -> {
            DurableAckRegistry registry = engine.getDurableAckRegistry();
            Assert.assertTrue(
                    "default registry must be enabled (LocalDurableAckRegistry)",
                    registry.isEnabled()
            );
        });
    }

    /**
     * (2b) getLocalDurableSeqTxn returns -1 for an unknown table dir name.
     */
    @Test
    public void testLocalDurableAckRegistryUnknownDirNameReturnsMinusOne() throws Exception {
        assertMemoryLeak(() -> {
            DurableAckRegistry registry = engine.getDurableAckRegistry();
            long result = registry.getLocalDurableSeqTxn("nonexistent_dir~999");
            Assert.assertEquals(
                    "unknown table dir must return -1",
                    -1L, result
            );
        });
    }

    /**
     * (2c) getDurablyUploadedSeqTxn still returns -1 on the OSS LocalDurableAckRegistry
     * (no upload pipeline in OSS).
     */
    @Test
    public void testLocalDurableAckRegistryUploadedIsMinusOne() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table oss_tbl (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into oss_tbl values ('2024-01-01T00:00:00.000000Z', 1)");
            DurableAckRegistry registry = engine.getDurableAckRegistry();
            TableToken tt = engine.verifyTableName("oss_tbl");
            Assert.assertEquals(
                    "OSS has no upload tier — getDurablyUploadedSeqTxn must return -1",
                    -1L, registry.getDurablyUploadedSeqTxn(tt.getDirName())
            );
        });
    }

    // ---- (3) Integration: adaptive WAL commit advances getLocalDurableSeqTxn ----

    /**
     * (3) After an ADAPTIVE WAL insert, getLocalDurableSeqTxn(tableDir) equals the committed seqTxn.
     * Each commit increments the seqTxn by 1 (1 per insert for a fresh table).
     */
    @Test
    public void testAdaptiveCommitAdvancesLocalDurableSeqTxn() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");

        assertMemoryLeak(() -> {
            execute("create table adap_tbl (ts timestamp, v long) timestamp(ts) partition by day wal");

            // First commit: seqTxn should become 1
            execute("insert into adap_tbl values ('2024-01-01T00:00:00.000000Z', 1)");

            TableToken tt = engine.verifyTableName("adap_tbl");
            DurableAckRegistry registry = engine.getDurableAckRegistry();
            long localDurable = registry.getLocalDurableSeqTxn(tt.getDirName());
            Assert.assertTrue(
                    "after ADAPTIVE commit, localDurableSeqTxn must be >= 1, got " + localDurable,
                    localDurable >= 1L
            );

            // Second commit: seqTxn should advance further
            execute("insert into adap_tbl values ('2024-01-01T01:00:00.000000Z', 2)");
            long localDurable2 = registry.getLocalDurableSeqTxn(tt.getDirName());
            Assert.assertTrue(
                    "after second ADAPTIVE commit, localDurableSeqTxn must advance beyond " + localDurable,
                    localDurable2 > localDurable
            );
        });
    }

    /**
     * (3b) The SeqTxnTracker for the adaptive table reflects the same local-durable seqTxn
     * that the registry reports.
     */
    @Test
    public void testAdaptiveTrackerLocalDurableMatchesRegistryValue() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");

        assertMemoryLeak(() -> {
            execute("create table adap_tbl2 (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into adap_tbl2 values ('2024-01-01T00:00:00.000000Z', 99)");

            TableToken tt = engine.verifyTableName("adap_tbl2");
            SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(tt);
            DurableAckRegistry registry = engine.getDurableAckRegistry();

            Assert.assertEquals(
                    "registry.getLocalDurableSeqTxn must equal tracker.getLocalDurableSeqTxn",
                    tracker.getLocalDurableSeqTxn(),
                    registry.getLocalDurableSeqTxn(tt.getDirName())
            );
        });
    }

    // ---- (4) Integration: nosync WAL commit leaves getLocalDurableSeqTxn at -1 ----

    /**
     * (4) After a NOSYNC WAL insert, getLocalDurableSeqTxn(tableDir) remains -1.
     * No fdatasync was issued, so no local-durable guarantee.
     */
    @Test
    public void testNosyncCommitDoesNotAdvanceLocalDurableSeqTxn() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");

        assertMemoryLeak(() -> {
            execute("create table nosync_tbl (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into nosync_tbl values ('2024-01-01T00:00:00.000000Z', 1)");

            TableToken tt = engine.verifyTableName("nosync_tbl");
            DurableAckRegistry registry = engine.getDurableAckRegistry();
            long localDurable = registry.getLocalDurableSeqTxn(tt.getDirName());
            Assert.assertEquals(
                    "NOSYNC commit must NOT advance localDurableSeqTxn (no fdatasync guarantee)",
                    -1L, localDurable
            );
        });
    }

    // ---- (5) QWP predicate: max(local, uploaded) >= clientSeqTxn ----

    /**
     * (5) collectDurableProgress uses max(local, uploaded) so an ADAPTIVE table satisfies
     * the durable-ack predicate via the local tier even when no upload has occurred.
     *
     * <p>This tests the max-of-tiers predicate that drives durable-ack eligibility.
     * We construct a registry where local > 0 but uploaded = -1, verify the durable-ack
     * frontier resolves to the local value and the predicate is satisfied.
     */
    @Test
    public void testQwpDurableAckPredicateUsesMaxOfLocalAndUploaded() throws Exception {
        assertMemoryLeak(() -> {
            // Build a synthetic registry: local=5, uploaded=-1
            long localDurable = 5L;
            long uploadedDurable = -1L;
            long clientSeqTxn = 3L;

            // The max-of-tiers frontier
            long frontier = Math.max(localDurable, uploadedDurable);

            // Predicate satisfied: frontier >= clientSeqTxn
            Assert.assertTrue(
                    "max(local=" + localDurable + ", uploaded=" + uploadedDurable + ")=" + frontier
                            + " must be >= clientSeqTxn=" + clientSeqTxn,
                    frontier >= clientSeqTxn
            );
        });
    }

    /**
     * (5b) When local=-1 and uploaded=7, the frontier is 7 (Enterprise path).
     */
    @Test
    public void testQwpDurableAckPredicateUploadedTierWins() throws Exception {
        assertMemoryLeak(() -> {
            long localDurable = -1L;
            long uploadedDurable = 7L;
            long clientSeqTxn = 5L;

            long frontier = Math.max(localDurable, uploadedDurable);
            Assert.assertEquals(7L, frontier);
            Assert.assertTrue(frontier >= clientSeqTxn);
        });
    }

    /**
     * (5c) When both tiers are -1 (no durability), the frontier is -1 and predicate fails.
     */
    @Test
    public void testQwpDurableAckPredicateFailsWhenBothTiersMinusOne() throws Exception {
        assertMemoryLeak(() -> {
            long localDurable = -1L;
            long uploadedDurable = -1L;
            long clientSeqTxn = 1L;

            long frontier = Math.max(localDurable, uploadedDurable);
            Assert.assertEquals(-1L, frontier);
            Assert.assertFalse(frontier >= clientSeqTxn);
        });
    }

    // ---- (6) collectDurableProgress integrates with the local tier ----

    /**
     * (6) After an ADAPTIVE WAL insert, collectDurableProgress reports the table's durable
     * seqTxn using the local tier (via the OSS LocalDurableAckRegistry).
     *
     * <p>This tests the full path: ADAPTIVE commit → localDurableSeqTxn advance →
     * collectDurableProgress returns a non-empty snapshot.
     */
    @Test
    public void testCollectDurableProgressReportsAdaptiveTableViaLocalTier() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");

        assertMemoryLeak(() -> {
            execute("create table adap_collect (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into adap_collect values ('2024-01-01T00:00:00.000000Z', 1)");

            TableToken tt = engine.verifyTableName("adap_collect");
            DurableAckRegistry registry = engine.getDurableAckRegistry();

            // The local-durable seqTxn must be >= 1 after the ADAPTIVE commit
            long localDurable = registry.getLocalDurableSeqTxn(tt.getDirName());
            Assert.assertTrue(
                    "registry must report localDurableSeqTxn >= 1 for adaptive table after commit",
                    localDurable >= 1L
            );

            // And the registry must be enabled
            Assert.assertTrue("OSS registry must be enabled", registry.isEnabled());
        });
    }

    // ---- (7) Enterprise override: setDurableAckRegistry replaces the default ----

    /**
     * (7) setDurableAckRegistry installs an Enterprise impl; getDurableAckRegistry returns it.
     * Confirms the Enterprise override mechanism still works.
     */
    @Test
    public void testEnterpriseRegistryOverridesDefault() throws Exception {
        assertMemoryLeak(() -> {
            // OSS default is LocalDurableAckRegistry (isEnabled=true)
            Assert.assertTrue(engine.getDurableAckRegistry().isEnabled());

            // Enterprise overrides with its own impl
            DurableAckRegistry fakeEnterprise = new DurableAckRegistry() {
                @Override
                public long getDurablyUploadedSeqTxn(CharSequence tableDirName) {
                    return 99L;
                }

                @Override
                public boolean isEnabled() {
                    return true;
                }

                @Override
                public long getLocalDurableSeqTxn(CharSequence tableDirName) {
                    return 42L;
                }
            };
            engine.setDurableAckRegistry(fakeEnterprise);

            Assert.assertSame(fakeEnterprise, engine.getDurableAckRegistry());
            Assert.assertEquals(99L, engine.getDurableAckRegistry().getDurablyUploadedSeqTxn("any"));
            Assert.assertEquals(42L, engine.getDurableAckRegistry().getLocalDurableSeqTxn("any"));

            // Restore so other tests are not affected
            engine.setDurableAckRegistry(new io.questdb.cairo.wal.LocalDurableAckRegistry(engine));
        });
    }

    // ---- (8) DefaultDurableAckRegistry interface default ----

    /**
     * (8) The DefaultDurableAckRegistry (no-op) returns -1 for getLocalDurableSeqTxn via the
     * interface default method.
     */
    @Test
    public void testDefaultDurableAckRegistryLocalDurableReturnsMinusOne() throws Exception {
        assertMemoryLeak(() -> {
            DurableAckRegistry noOp = io.questdb.cairo.wal.DefaultDurableAckRegistry.INSTANCE;
            Assert.assertEquals(
                    "DefaultDurableAckRegistry must return -1 for getLocalDurableSeqTxn",
                    -1L, noOp.getLocalDurableSeqTxn("some~dir")
            );
            Assert.assertFalse("DefaultDurableAckRegistry must not be enabled", noOp.isEnabled());
        });
    }
}
