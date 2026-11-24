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

package io.questdb.test.cairo;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.std.ObjHashSet;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.griffin.HashJoinBuildMode;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Unit tests for the property override store every {@code AbstractCairoTest} subclass writes to.
 * Overrides rebuilds the configuration on the first read after a batch of setProperty() calls
 * that changed anything. A call that changes nothing must not hide an earlier change in the
 * same batch, a call made while another thread rebuilds or reads the configuration must reach
 * the next read, and a read that rebuilds returns what it built. Tests
 * that use a standalone {@code Overrides} allocate no native memory beyond what building a
 * {@code CairoConfiguration} does, so they do not use {@code assertMemoryLeak()}.
 */
public class OverridesTest extends AbstractCairoTest {
    private static final int MAX_UNCOMMITTED_ROWS = 12_345;
    private static final int PART_COUNT = 16;
    private static final long PARK_TIMEOUT_SECONDS = 30;
    // The directory name the soak's view gets, as listed in cairo.wal.apply.suspended.tables.
    private static final String SUSPENDED_DIR_NAME = "lv~2";
    private static final long WINDOW_US = 5_000;
    // cairo.max.uncommitted.rows as Overrides' own default test properties set it.
    private static final int DEFAULT_TEST_MAX_UNCOMMITTED_ROWS = 1000;
    // cairo.sql.page.frame.min.rows as Overrides' own default test properties set it.
    private static final int DEFAULT_TEST_PAGE_FRAME_MIN_ROWS = 1000;

    @Test
    public void testBuildModeReappliedAfterAnotherMode() throws Exception {
        // PARALLEL sets the minimum row count and the rows per partition. Once SERIAL replaces the
        // minimum, applying PARALLEL again changes the minimum and sets the rows per partition to
        // the value it already holds.
        assertMemoryLeak(() -> {
            final Overrides overrides = node1.getConfigurationOverrides();
            HashJoinBuildMode.PARALLEL.applyProperties(overrides);
            Assert.assertEquals(0, configuration.getSqlParallelHashJoinGroupByBuildParallelMinRows());
            HashJoinBuildMode.SERIAL.applyProperties(overrides);
            Assert.assertEquals(Long.MAX_VALUE, configuration.getSqlParallelHashJoinGroupByBuildParallelMinRows());
            HashJoinBuildMode.PARALLEL.applyProperties(overrides);
            Assert.assertEquals(0, configuration.getSqlParallelHashJoinGroupByBuildParallelMinRows());
            Assert.assertEquals(1, configuration.getSqlParallelHashJoinGroupByBuildRowsPerPartition());
        });
    }

    /**
     * A read that rebuilds the configuration returns the configuration it built, even when an older rebuild publishes
     * over it before it returns. The older rebuild copies the overrides with the view not listed in
     * cairo.wal.apply.suspended.tables and parks before it publishes. The test then lists the view, and a newer read
     * rebuilds, publishes and parks before it returns. The older rebuild publishes its copy while the newer read is
     * parked.
     */
    @Test
    public void testReadReturnsItsOwnRebuildWhenOlderRebuildPublishesOverIt() throws Exception {
        final ParkingOverrides overrides = new ParkingOverrides();
        // Keeps the overrides non-empty whatever the listing, as in the soak.
        overrides.setProperty(PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_WRITE_DENIED, true);
        overrides.setProperty(PropertyKey.CAIRO_MAX_UNCOMMITTED_ROWS, 1_234);
        final ConfigurationReader olderReader = new ConfigurationReader(overrides, "overrides-older-reader");
        final ConfigurationReader newerReader = new ConfigurationReader(overrides, "overrides-newer-reader");
        overrides.beforePublished.arm();
        olderReader.start();
        try {
            Assert.assertTrue("the older rebuild must park before it publishes", overrides.beforePublished.awaitParked());
            setListed(overrides, true);
            overrides.afterPublished.arm();
            newerReader.start();
            Assert.assertTrue("the newer rebuild must park after it publishes", overrides.afterPublished.awaitParked());
            overrides.beforePublished.release();
            olderReader.join();
        } finally {
            overrides.beforePublished.release();
            overrides.afterPublished.release();
            olderReader.join();
            newerReader.join();
        }
        Assert.assertNull("the older read", olderReader.error);
        Assert.assertNull("the newer read", newerReader.error);
        assertSuspensionOverrides("the older read", olderReader.result, false);
        assertSuspensionOverrides("the newer read", newerReader.result, true);
        assertSuspensionOverrides("a read after both", overrides.getConfiguration(root), true);
    }

    @Test
    public void testRebuildsOncePerBatchOfChanges() {
        // The rebuild is lazy: the first read after a batch of changes builds one configuration, and a call
        // that changes nothing builds none. Every rebuild runs the hook, which counts it.
        final CountingOverrides overrides = new CountingOverrides();
        overrides.setProperty(PropertyKey.CAIRO_MAX_UNCOMMITTED_ROWS, 1_234);
        for (int read = 0; read < 3; read++) {
            Assert.assertEquals(1_234, overrides.getConfiguration(root).getMaxUncommittedRows());
        }
        Assert.assertEquals(1, overrides.rebuildCount);

        // The override already holds this value, and no override of the other key exists.
        overrides.setProperty(PropertyKey.CAIRO_MAX_UNCOMMITTED_ROWS, 1_234);
        overrides.setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MIN_ROWS, null);
        Assert.assertEquals(1_234, overrides.getConfiguration(root).getMaxUncommittedRows());
        Assert.assertEquals(1, overrides.rebuildCount);

        overrides.setProperty(PropertyKey.CAIRO_MAX_UNCOMMITTED_ROWS, 4_321);
        overrides.setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MIN_ROWS, 4_321);
        Assert.assertEquals(4_321, overrides.getConfiguration(root).getMaxUncommittedRows());
        Assert.assertEquals(4_321, overrides.getConfiguration(root).getSqlPageFrameMinRows());
        Assert.assertEquals(2, overrides.rebuildCount);

        overrides.reset();
        overrides.setProperty(PropertyKey.CAIRO_MAX_UNCOMMITTED_ROWS, 1_234);
        final CairoConfiguration afterReset = overrides.getConfiguration(root);
        Assert.assertEquals(1_234, afterReset.getMaxUncommittedRows());
        Assert.assertEquals(DEFAULT_TEST_PAGE_FRAME_MIN_ROWS, afterReset.getSqlPageFrameMinRows());
        Assert.assertEquals(3, overrides.rebuildCount);
    }

    @Test
    public void testRemovingAbsentPropertyKeepsEarlierChange() throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, 10);
            Assert.assertEquals(10, configuration.getSqlParallelHashJoinGroupByBuildParallelMinRows());
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, 20);
            // No override of this key exists, so removing it changes nothing.
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_ROWS_PER_PARTITION, null);
            Assert.assertEquals(20, configuration.getSqlParallelHashJoinGroupByBuildParallelMinRows());
        });
    }

    @Test
    public void testResetDropsOverrides() {
        final Overrides overrides = new Overrides();
        overrides.setProperty(PropertyKey.CAIRO_MAX_UNCOMMITTED_ROWS, 1_234);
        overrides.setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MIN_ROWS, 4_321);
        Assert.assertEquals(1_234, overrides.getConfiguration(root).getMaxUncommittedRows());

        overrides.reset();
        Assert.assertEquals(DEFAULT_TEST_MAX_UNCOMMITTED_ROWS, overrides.getConfiguration(root).getMaxUncommittedRows());
        Assert.assertEquals(DEFAULT_TEST_PAGE_FRAME_MIN_ROWS, overrides.getConfiguration(root).getSqlPageFrameMinRows());
    }

    /**
     * A setProperty() on one thread that lands while another thread rebuilds the configuration reaches the next
     * read, although the rebuild copied the overrides before the change and publishes after it. Repeating the
     * change afterwards changes nothing, so it cannot repair a configuration the late publication left stale.
     * The rows list the view in cairo.wal.apply.suspended.tables or unlist it, as the operator thread of
     * LiveViewConcurrencyTest#testOperatorSuspendResumeUnderIngest does while the refresh workers read the
     * configuration. In the rows with a newer read, the test thread reads the configuration while the rebuild
     * is parked, so the late publication replaces a newer one.
     */
    @Test
    public void testSetPropertyDuringRebuildOnAnotherThreadReachesNextRead() throws Exception {
        // {isListedBefore, isListedDuring, isNewerReadBeforeLatePublish}
        final boolean[][] rows = {
                {true, false, false},
                {true, false, true},
                {false, true, false},
                {false, true, true},
        };
        for (boolean[] row : rows) {
            final boolean isListedBefore = row[0];
            final boolean isListedDuring = row[1];
            final boolean isNewerReadBeforeLatePublish = row[2];
            final String rowName = (isListedBefore ? "listed" : "unlisted") + " -> " + (isListedDuring ? "listed" : "unlisted")
                    + (isNewerReadBeforeLatePublish ? ", newer read before the late publication" : "");
            final ParkingOverrides overrides = new ParkingOverrides();
            // Keeps the overrides non-empty whatever the listing, as in the soak.
            overrides.setProperty(PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_WRITE_DENIED, true);
            setListed(overrides, isListedBefore);
            // A change the parked rebuild copies.
            overrides.setProperty(PropertyKey.CAIRO_MAX_UNCOMMITTED_ROWS, 1_234);
            overrides.beforePublished.arm();
            final ConfigurationReader rebuilder = new ConfigurationReader(overrides, "overrides-rebuilder");
            rebuilder.start();
            try {
                Assert.assertTrue(rowName + ": the rebuild must park after copying the overrides", overrides.beforePublished.awaitParked());
                setListed(overrides, isListedDuring);
                if (isNewerReadBeforeLatePublish) {
                    assertSuspensionOverrides(rowName + ", read while the rebuild is parked", overrides.getConfiguration(root), isListedDuring);
                }
            } finally {
                overrides.beforePublished.release();
                rebuilder.join();
            }
            Assert.assertNull(rowName, rebuilder.error);
            assertSuspensionOverrides(rowName + ", the parked rebuild's own read", rebuilder.result, isListedBefore);

            // Changes nothing: the overrides already hold the listing the test set during the rebuild.
            setListed(overrides, isListedDuring);
            Assert.assertEquals(rowName, isListedDuring, overrides.isPropertySet(PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_TABLES));
            assertSuspensionOverrides(rowName + ", read after the late publication", overrides.getConfiguration(root), isListedDuring);
        }
    }

    @Test
    public void testSetPropertyNullRemovesOverrideAfterConfigurationRead() {
        // A configuration read over a non-empty override map publishes a configuration built at the
        // current version, so the removal is what has to move the version for the next read to see
        // the default.
        final Overrides overrides = new Overrides();
        overrides.setProperty(PropertyKey.CAIRO_MAX_UNCOMMITTED_ROWS, 1_234);
        Assert.assertEquals(1_234, overrides.getConfiguration(root).getMaxUncommittedRows());

        overrides.setProperty(PropertyKey.CAIRO_MAX_UNCOMMITTED_ROWS, null);
        Assert.assertEquals(DEFAULT_TEST_MAX_UNCOMMITTED_ROWS, overrides.getConfiguration(root).getMaxUncommittedRows());
    }

    @Test
    public void testSetPropertyNullRemovesOverrideBeforeConfigurationRead() {
        // A fresh Overrides has built no configuration yet, so its first read rebuilds anyway. With
        // the boolean rebuild flag Overrides once kept, that state made the removal short-circuit
        // away and leave the cancelled override in the map.
        final Overrides overrides = new Overrides();
        overrides.setProperty(PropertyKey.CAIRO_MAX_UNCOMMITTED_ROWS, 1_234);
        overrides.setProperty(PropertyKey.CAIRO_MAX_UNCOMMITTED_ROWS, null);
        Assert.assertEquals(DEFAULT_TEST_MAX_UNCOMMITTED_ROWS, overrides.getConfiguration(root).getMaxUncommittedRows());
    }

    @Test
    public void testSetPropertyNullRemovesOverrideWithAnotherOverrideStanding() {
        // Removal with another override still standing: the map stays non-empty, so the read goes
        // through the rebuilt props configuration rather than falling back to the default one.
        final Overrides overrides = new Overrides();
        overrides.setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MIN_ROWS, 4_321);
        Assert.assertEquals(4_321, overrides.getConfiguration(root).getSqlPageFrameMinRows());

        overrides.setProperty(PropertyKey.CAIRO_MAX_UNCOMMITTED_ROWS, 1_234);
        overrides.setProperty(PropertyKey.CAIRO_MAX_UNCOMMITTED_ROWS, null);
        Assert.assertEquals(DEFAULT_TEST_MAX_UNCOMMITTED_ROWS, overrides.getConfiguration(root).getMaxUncommittedRows());
        Assert.assertEquals(4_321, overrides.getConfiguration(root).getSqlPageFrameMinRows());
    }

    @Test
    public void testSetPropertyReplacesOverrideAfterConfigurationRead() {
        // The sibling branch of the same version move: a plain overwrite still reaches the next read.
        final Overrides overrides = new Overrides();
        overrides.setProperty(PropertyKey.CAIRO_MAX_UNCOMMITTED_ROWS, 1_234);
        Assert.assertEquals(1_234, overrides.getConfiguration(root).getMaxUncommittedRows());

        overrides.setProperty(PropertyKey.CAIRO_MAX_UNCOMMITTED_ROWS, 4_321);
        Assert.assertEquals(4_321, overrides.getConfiguration(root).getMaxUncommittedRows());
    }

    /**
     * A setProperty() parked inside its write to the overrides, before the write lands, leaves a configuration read
     * on another thread with the overrides as they were. Once setProperty() returns, the change reaches the next
     * read, and repeating it, which changes nothing, keeps it there. The rows list the view in
     * cairo.wal.apply.suspended.tables and unlist it, as the operator thread of
     * LiveViewConcurrencyTest#testOperatorSuspendResumeUnderIngest does while the refresh workers read the
     * configuration.
     */
    @Test
    public void testSetPropertyWithReadDuringItsWriteReachesNextRead() throws Exception {
        for (boolean isListing : new boolean[]{true, false}) {
            final String rowName = isListing ? "list" : "unlist";
            final ParkingOverrides overrides = new ParkingOverrides();
            // Keeps the overrides non-empty whatever the listing, as in the soak.
            overrides.setProperty(PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_WRITE_DENIED, true);
            setListed(overrides, !isListing);
            overrides.setProperty(PropertyKey.CAIRO_MAX_UNCOMMITTED_ROWS, 1_234);
            // Publishes a configuration, as the refresh workers' reads keep doing in the soak.
            assertSuspensionOverrides(rowName + ", read before the write", overrides.getConfiguration(root), !isListing);
            overrides.beforeWritten.arm();
            final AtomicReference<Throwable> writeError = new AtomicReference<>();
            final Thread writer = new Thread(() -> {
                try {
                    setListed(overrides, isListing);
                } catch (Throwable th) {
                    writeError.set(th);
                }
            }, "overrides-writer");
            writer.start();
            try {
                Assert.assertTrue(rowName + ": the write must park before it lands", overrides.beforeWritten.awaitParked());
                assertSuspensionOverrides(rowName + ", read while the write is parked", overrides.getConfiguration(root), !isListing);
            } finally {
                overrides.beforeWritten.release();
                writer.join();
            }
            Assert.assertNull(rowName, writeError.get());
            assertSuspensionOverrides(rowName + ", read after the write", overrides.getConfiguration(root), isListing);

            // Changes nothing: the overrides already hold this listing.
            setListed(overrides, isListing);
            Assert.assertEquals(rowName, isListing, overrides.isPropertySet(PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_TABLES));
            assertSuspensionOverrides(rowName + ", read after repeating the write", overrides.getConfiguration(root), isListing);
        }
    }

    @Test
    public void testSettingCurrentValueKeepsEarlierChange() throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, 10);
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_ROWS_PER_PARTITION, 5);
            Assert.assertEquals(10, configuration.getSqlParallelHashJoinGroupByBuildParallelMinRows());
            Assert.assertEquals(5, configuration.getSqlParallelHashJoinGroupByBuildRowsPerPartition());
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, 20);
            // The override already holds this value, so this call changes nothing.
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_ROWS_PER_PARTITION, 5);
            Assert.assertEquals(20, configuration.getSqlParallelHashJoinGroupByBuildParallelMinRows());
            Assert.assertEquals(5, configuration.getSqlParallelHashJoinGroupByBuildRowsPerPartition());
        });
    }

    /**
     * A setProperty call that leaves the stored properties unchanged must not cancel an earlier pending change.
     */
    @Test
    public void testUnchangingSetPropertyKeepsAPendingChange() {
        final Overrides overrides = node1.getConfigurationOverrides();
        overrides.reset();
        final int defaultPartCount = configuration.getDefaultSeqPartTxnCount();
        final long defaultWindowUs = configuration.getAdaptiveCommitGroupWindowUs();
        final int defaultMaxUncommittedRows = configuration.getMaxUncommittedRows();
        Assert.assertNotEquals(PART_COUNT, defaultPartCount);
        Assert.assertNotEquals(2 * PART_COUNT, defaultPartCount);
        Assert.assertNotEquals(WINDOW_US, defaultWindowUs);
        Assert.assertNotEquals(MAX_UNCOMMITTED_ROWS, defaultMaxUncommittedRows);

        final StringSink lost = new StringSink();
        for (Change change : Change.values()) {
            for (Unchanging unchanging : Unchanging.values()) {
                overrides.reset();
                overrides.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, PART_COUNT);
                overrides.setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, WINDOW_US);
                Assert.assertEquals(PART_COUNT, configuration.getDefaultSeqPartTxnCount());

                int expectedPartCount = PART_COUNT;
                int expectedMaxUncommittedRows = defaultMaxUncommittedRows;
                switch (change) {
                    case SET_NEW_KEY -> {
                        overrides.setProperty(PropertyKey.CAIRO_MAX_UNCOMMITTED_ROWS, MAX_UNCOMMITTED_ROWS);
                        expectedMaxUncommittedRows = MAX_UNCOMMITTED_ROWS;
                    }
                    case CHANGE_VALUE -> {
                        overrides.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, 2 * PART_COUNT);
                        expectedPartCount = 2 * PART_COUNT;
                    }
                    case REMOVE_KEY -> {
                        overrides.setProperty(PropertyKey.CAIRO_DEFAULT_SEQ_PART_TXN_COUNT, (String) null);
                        expectedPartCount = defaultPartCount;
                    }
                    case RESET -> {
                        overrides.reset();
                        overrides.setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, WINDOW_US);
                        expectedPartCount = defaultPartCount;
                    }
                }

                switch (unchanging) {
                    case SAME_VALUE -> overrides.setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, WINDOW_US);
                    case REMOVE_ABSENT_KEY -> overrides.setProperty(PropertyKey.QUERY_TIMEOUT, (String) null);
                    case DEFAULT_OF_ABSENT_KEY -> overrides.setProperty(PropertyKey.QUERY_TIMEOUT, 0);
                }

                final int partCount = configuration.getDefaultSeqPartTxnCount();
                final long windowUs = configuration.getAdaptiveCommitGroupWindowUs();
                final int maxUncommittedRows = configuration.getMaxUncommittedRows();
                if (partCount != expectedPartCount || windowUs != WINDOW_US || maxUncommittedRows != expectedMaxUncommittedRows) {
                    lost.put(change.name()).put(" then ").put(unchanging.name())
                            .put(": partCount=").put(partCount).put(" (expected ").put(expectedPartCount)
                            .put("), windowUs=").put(windowUs).put(" (expected ").put(WINDOW_US)
                            .put("), maxUncommittedRows=").put(maxUncommittedRows).put(" (expected ").put(expectedMaxUncommittedRows)
                            .put(")\n");
                }
            }
        }
        Assert.assertEquals("a stale configuration was served after:\n" + lost, 0, lost.length());
    }

    private static void assertSuspensionOverrides(String context, CairoConfiguration configuration, boolean isListed) {
        final ObjHashSet<String> listed = configuration.getWalApplySuspendedTables();
        Assert.assertNotNull(context, listed);
        Assert.assertEquals(context + ": " + listed, isListed ? 1 : 0, listed.size());
        Assert.assertEquals(context + ": " + listed, isListed, listed.contains(SUSPENDED_DIR_NAME));
        Assert.assertEquals(context, 1_234, configuration.getMaxUncommittedRows());
        Assert.assertTrue(context, configuration.isWalApplySuspendedWriteDenied());
    }

    private static void setListed(Overrides overrides, boolean isListed) {
        overrides.setProperty(PropertyKey.CAIRO_WAL_APPLY_SUSPENDED_TABLES, isListed ? SUSPENDED_DIR_NAME : null);
    }

    private enum Change {
        SET_NEW_KEY, CHANGE_VALUE, REMOVE_KEY, RESET
    }

    private enum Unchanging {
        SAME_VALUE, REMOVE_ABSENT_KEY, DEFAULT_OF_ABSENT_KEY
    }

    // Reads the configuration on a thread of its own and keeps what the read returned or threw.
    private static class ConfigurationReader extends Thread {
        private final Overrides overrides;
        private volatile Throwable error;
        private volatile CairoConfiguration result;

        ConfigurationReader(Overrides overrides, String name) {
            super(name);
            this.overrides = overrides;
        }

        @Override
        public void run() {
            try {
                result = overrides.getConfiguration(root);
            } catch (Throwable th) {
                error = th;
            }
        }
    }

    private static class CountingOverrides extends Overrides {
        private int rebuildCount;

        @Override
        protected void beforeConfigurationPublished() {
            rebuildCount++;
        }
    }

    // Parks the first caller of each hook after the test armed the hook's spot, until the test releases it.
    private static class ParkingOverrides extends Overrides {
        private final ParkingSpot afterPublished = new ParkingSpot();
        private final ParkingSpot beforePublished = new ParkingSpot();
        private final ParkingSpot beforeWritten = new ParkingSpot();

        @Override
        protected void afterConfigurationPublished() {
            afterPublished.parkIfArmed();
        }

        @Override
        protected void beforeConfigurationPublished() {
            beforePublished.parkIfArmed();
        }

        @Override
        protected void beforeOverrideWritten() {
            beforeWritten.parkIfArmed();
        }
    }

    // Parks the first thread that reaches it after arm() until the test releases it.
    private static class ParkingSpot {
        private final AtomicBoolean isArmed = new AtomicBoolean();
        private final CountDownLatch parked = new CountDownLatch(1);
        private final CountDownLatch released = new CountDownLatch(1);

        void arm() {
            isArmed.set(true);
        }

        boolean awaitParked() throws InterruptedException {
            return parked.await(PARK_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }

        void parkIfArmed() {
            if (isArmed.compareAndSet(true, false)) {
                parked.countDown();
                try {
                    Assert.assertTrue("the test must release the parked thread", released.await(PARK_TIMEOUT_SECONDS, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError("interrupted while parked", e);
                }
            }
        }

        void release() {
            released.countDown();
        }
    }
}
