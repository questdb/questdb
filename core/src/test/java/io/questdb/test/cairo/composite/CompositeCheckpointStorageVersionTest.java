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

package io.questdb.test.cairo.composite;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.cairo.vm.api.MemoryMR;
import io.questdb.std.MemoryTag;
import io.questdb.std.Os;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

/**
 * A checkpoint image must never pair a {@code _txn} that carries composite partition references with a
 * {@code _meta} that stamps {@link ColumnType#VERSION}. The downgrade guard documented in
 * {@code COMPOSITE_PARTITIONS.md} rests entirely on that pairing: an older binary validates {@code _meta}
 * with an exact-match check and refuses a table stamped {@link ColumnType#MAX_STORAGE_VERSION}, so it
 * never reads composite pieces as a flat {@code [0, liveRows)} range. Checkpoint recovery copies the
 * checkpoint's {@code _meta} verbatim over the table directory
 * ({@code TableSnapshotRestore.copyMetadataFiles}), so whatever the checkpoint stamped is what the
 * restored table carries.
 */
public class CompositeCheckpointStorageVersionTest extends AbstractCairoTest {

    @After
    public void tearDown() throws Exception {
        // Clear any checkpoint this class left in progress, and remove the checkpoint directory: the
        // engine refuses a second CHECKPOINT CREATE while one is outstanding. This runs before
        // super.tearDown() so the cleanup executes against a live engine.
        execute("CHECKPOINT RELEASE");
        try (Path path = new Path()) {
            path.of(configuration.getCheckpointRoot()).concat(configuration.getDbDirectory()).slash();
            configuration.getFilesFacade().rmdir(path);
        }
        super.tearDown();
    }

    /**
     * The mirror image of the composite case: once the last composite partition folds back to plain, the
     * image ships a plain {@code _txn} and must stamp {@link ColumnType#VERSION} again. This is the case
     * the symmetric stamp can newly get wrong by leaving an image over-strict at
     * {@link ColumnType#MAX_STORAGE_VERSION}, which would make an older binary refuse a perfectly plain
     * table.
     */
    @Test
    public void testCheckpointAfterFoldBackToPlainStampsBaseVersion() throws Exception {
        // CHECKPOINT CREATE calls sync(), which is unavailable on Windows.
        Assume.assumeTrue(Os.type != Os.WINDOWS);
        assertMemoryLeak(() -> {
            createPlainTable();
            final TableToken tableToken = engine.verifyTableName("x");

            try (TableReader pinned = engine.getReader(tableToken)) {
                makeComposite();
                Assert.assertEquals(ColumnType.MAX_STORAGE_VERSION, metaStorageVersion(tableToken));

                // Snapshot the metadata copy while the table IS composite, so the copy's private _meta
                // bytes stamp MAX_STORAGE_VERSION and go stale in the opposite direction to the main
                // test: the stamp derived from the _txn has to LOWER this image, not raise it.
                try (TableReader copy = engine.getReaderAtTxn(pinned, sqlExecutionContext)) {
                    Assert.assertNotSame("expected the reader pool copy slow path", pinned, copy);
                    Assert.assertEquals(ColumnType.MAX_STORAGE_VERSION, dumpedMetaStorageVersion(copy));
                }

                // Turning merge-append off folds every composite partition back to plain at writer open,
                // and the fold's commit downgrades _meta in place back to ColumnType.VERSION.
                node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
                engine.releaseAllWriters();
                engine.getWriter(tableToken, "test").close();
                Assert.assertEquals(ColumnType.VERSION, metaStorageVersion(tableToken));

                try {
                    execute("CHECKPOINT CREATE");
                    Assert.assertFalse(
                            "fixture did not fold the composite partitions away",
                            checkpointTxnHasCompositePartitions(tableToken)
                    );
                    Assert.assertEquals(
                            "a checkpoint of a table whose _txn is plain again must stamp ColumnType.VERSION",
                            ColumnType.VERSION,
                            checkpointMetaStorageVersion(tableToken)
                    );
                } finally {
                    execute("CHECKPOINT RELEASE");
                }
            }
        });
    }

    /**
     * The ordinary production path: a composite table checkpointed through a reader whose metadata is NOT
     * a stale copy. It is correct today - the live {@code _meta} already stamps
     * {@link ColumnType#MAX_STORAGE_VERSION} and the checkpoint copies those bytes - and it is exactly the
     * case the derived stamp rewrites, so it must stay green after the fix too.
     */
    @Test
    public void testCheckpointOfCompositeTableFromFreshReaderStampsMaxStorageVersion() throws Exception {
        Assume.assumeTrue(Os.type != Os.WINDOWS);
        assertMemoryLeak(() -> {
            createPlainTable();
            final TableToken tableToken = engine.verifyTableName("x");
            makeComposite();
            Assert.assertEquals(ColumnType.MAX_STORAGE_VERSION, metaStorageVersion(tableToken));

            // No pinned reader and no reader copy anywhere in the pool: the checkpoint's own
            // getReaderWithRepair() gets a reader that maps the live _meta.
            try (TableReader fresh = engine.getReader(tableToken)) {
                Assert.assertEquals(ColumnType.MAX_STORAGE_VERSION, dumpedMetaStorageVersion(fresh));
            }

            try {
                execute("CHECKPOINT CREATE");
                Assert.assertTrue(
                        "fixture did not put composite references into the checkpoint _txn",
                        checkpointTxnHasCompositePartitions(tableToken)
                );
                Assert.assertEquals(
                        "a composite table checkpointed from a non-stale reader must stamp"
                                + " MAX_STORAGE_VERSION",
                        ColumnType.MAX_STORAGE_VERSION,
                        checkpointMetaStorageVersion(tableToken)
                );
            } finally {
                execute("CHECKPOINT RELEASE");
            }
        });
    }

    @Test
    public void testCheckpointOfCompositeTableStampsMaxStorageVersion() throws Exception {
        Assume.assumeTrue(Os.type != Os.WINDOWS);
        assertMemoryLeak(() -> {
            createPlainTable();
            final TableToken tableToken = engine.verifyTableName("x");

            try (TableReader pinned = engine.getReader(tableToken)) {
                // Manufacture the pooled reader copy while the table is still plain: the copy takes a
                // private byte snapshot of _meta, which at this point stamps ColumnType.VERSION.
                try (TableReader copy = engine.getReaderAtTxn(pinned, sqlExecutionContext)) {
                    Assert.assertNotSame("expected the reader pool copy slow path", pinned, copy);
                }

                // The table goes composite. TableWriter.writeStorageVersionToMeta() stamps
                // MAX_STORAGE_VERSION into _meta in place, without bumping the metadata version.
                makeComposite();
                Assert.assertEquals(ColumnType.MAX_STORAGE_VERSION, metaStorageVersion(tableToken));

                // Fixture precondition, not a contract: the pinned reader holds the pool's first slot, so
                // this returns the copy from above, reloaded to the composite transaction while its
                // private metadata snapshot still reads ColumnType.VERSION. The stale copy is a hazard
                // this fix deliberately leaves in place, not behaviour to preserve - it is asserted here
                // only so that the reproduction failing to reproduce shows up as fixture rot instead of
                // silently exercising nothing.
                try (TableReader stale = engine.getReader(tableToken)) {
                    Assert.assertNotSame(pinned, stale);
                    Assert.assertTrue(
                            "fixture did not make the reader's _txn composite",
                            stale.getTxFile().hasCompositePartitions()
                    );
                    Assert.assertEquals(
                            "the fixture no longer reproduces the stale metadata copy",
                            ColumnType.VERSION,
                            dumpedMetaStorageVersion(stale)
                    );
                }

                // The checkpoint's own getReaderWithRepair() lands on that same copy slot.
                try {
                    execute("CHECKPOINT CREATE");
                    Assert.assertTrue(
                            "fixture did not put composite references into the checkpoint _txn",
                            checkpointTxnHasCompositePartitions(tableToken)
                    );
                    Assert.assertEquals(
                            "checkpoint _meta stamps a storage version an older binary accepts, while its"
                                    + " _txn carries composite partition references",
                            ColumnType.MAX_STORAGE_VERSION,
                            checkpointMetaStorageVersion(tableToken)
                    );
                } finally {
                    execute("CHECKPOINT RELEASE");
                }
            }
        });
    }

    @Test
    public void testCheckpointOfPlainTableKeepsBaseStorageVersion() throws Exception {
        Assume.assumeTrue(Os.type != Os.WINDOWS);
        assertMemoryLeak(() -> {
            createPlainTable();
            final TableToken tableToken = engine.verifyTableName("x");
            Assert.assertEquals(ColumnType.VERSION, metaStorageVersion(tableToken));

            try {
                execute("CHECKPOINT CREATE");
                Assert.assertFalse(checkpointTxnHasCompositePartitions(tableToken));
                Assert.assertEquals(
                        "a plain table must not be promoted to MAX_STORAGE_VERSION by the checkpoint",
                        ColumnType.VERSION,
                        checkpointMetaStorageVersion(tableToken)
                );
            } finally {
                execute("CHECKPOINT RELEASE");
            }
        });
    }

    /**
     * End-to-end: what the checkpoint stamped is what the restored table carries.
     * {@code TableSnapshotRestore.copyMetadataFiles} copies the checkpoint's {@code _meta} verbatim over
     * the table directory, so recovery does not repair the stamp.
     */
    @Test
    public void testCheckpointRecoveryCarriesTheStampedStorageVersion() throws Exception {
        Assume.assumeTrue(Os.type != Os.WINDOWS);
        final String checkpointId = "00000000-0000-0000-0000-000000000000";
        final String restartedId = "123e4567-e89b-12d3-a456-426614174000";
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_LEGACY_SNAPSHOT_INSTANCE_ID, checkpointId);
            createPlainTable();
            final TableToken tableToken = engine.verifyTableName("x");

            try {
                try (TableReader pinned = engine.getReader(tableToken)) {
                    try (TableReader copy = engine.getReaderAtTxn(pinned, sqlExecutionContext)) {
                        Assert.assertNotSame("expected the reader pool copy slow path", pinned, copy);
                    }
                    makeComposite();
                    execute("CHECKPOINT CREATE");
                }

                engine.clear();
                setProperty(PropertyKey.CAIRO_LEGACY_SNAPSHOT_INSTANCE_ID, restartedId);
                engine.checkpointRecover();

                Assert.assertTrue(
                        "fixture did not restore a composite _txn",
                        dbRootTxnHasCompositePartitions(tableToken)
                );
                // The restored image is a perfectly good composite table on this binary: its _geometry
                // files never left the database root, so an interval scan still resolves the pieces. Only
                // the storage version stamp lies about what it takes to read it.
                assertQuery("SELECT count() FROM x WHERE ts IN '2024-01-01'")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("count\n20400\n");
                assertOldBinaryRefusesRestoredTable(tableToken);
                Assert.assertEquals(
                        "the restored table stamps a storage version an older binary accepts, while its"
                                + " _txn carries composite partition references",
                        ColumnType.MAX_STORAGE_VERSION,
                        metaStorageVersion(tableToken)
                );
            } finally {
                // Release inside the memory-leak block: CHECKPOINT CREATE parks a TxnScoreboard
                // reference in DatabaseCheckpointAgent, and only checkpointRelease() frees it
                // (releaseScoreboardTxns). engine.clear() above already evicted the scoreboard from its
                // pool without freeing it, so without this the block ends one scoreboard short. It
                // cannot run any earlier than here: checkpointRelease() rmdirs the checkpoint's db
                // directory, which checkpointRecover() reads.
                execute("CHECKPOINT RELEASE");
            }
        });
    }

    /**
     * The negative control that bounds the defect: a pooled reader that is NOT a copy maps the live
     * {@code _meta} file MAP_SHARED, so the in-place write in
     * {@code TableWriter.writeStorageVersionToMeta} reaches it even though the metadata version never
     * moved. Only the metadata COPY goes stale.
     */
    @Test
    public void testNonCopyPooledReaderTracksInPlaceStorageVersionWrite() throws Exception {
        assertMemoryLeak(() -> {
            createPlainTable();
            final TableToken tableToken = engine.verifyTableName("x");

            try (TableReader reader = engine.getReader(tableToken)) {
                Assert.assertEquals(ColumnType.VERSION, dumpedMetaStorageVersion(reader));
                makeComposite();
                reader.reload();
                Assert.assertTrue(
                        "fixture did not make the reader's _txn composite",
                        reader.getTxFile().hasCompositePartitions()
                );
                Assert.assertEquals(ColumnType.MAX_STORAGE_VERSION, dumpedMetaStorageVersion(reader));
            }
        });
    }

    /**
     * Runs the exact-match version check a binary that only knows {@link ColumnType#VERSION} performs on
     * table open - {@code TableUtils.validateMetaVersion}'s single-value overload, still present here and
     * still what an older binary's {@code validateMeta} calls. It must refuse the restored table rather
     * than open it and read its composite pieces as a flat range.
     */
    private static void assertOldBinaryRefusesRestoredTable(TableToken tableToken) {
        try (
                MemoryMR mem = Vm.getCMRInstance();
                Path path = new Path()
        ) {
            path.of(configuration.getDbRoot()).concat(tableToken).concat(TableUtils.META_FILE_NAME);
            mem.smallFile(configuration.getFilesFacade(), path.$(), MemoryTag.MMAP_DEFAULT);
            try {
                TableUtils.validateMetaVersion(path.$(), mem, TableUtils.META_OFFSET_VERSION, ColumnType.VERSION);
                Assert.fail("a binary that only knows ColumnType.VERSION opens the restored composite table");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "metadata version does not match runtime version");
            }
        }
    }

    private static int checkpointMetaStorageVersion(TableToken tableToken) {
        try (Path path = new Path()) {
            path.of(configuration.getCheckpointRoot())
                    .concat(configuration.getDbDirectory())
                    .concat(tableToken)
                    .concat(TableUtils.META_FILE_NAME);
            return readStorageVersion(path);
        }
    }

    private static boolean checkpointTxnHasCompositePartitions(TableToken tableToken) {
        try (Path path = new Path()) {
            path.of(configuration.getCheckpointRoot())
                    .concat(configuration.getDbDirectory())
                    .concat(tableToken)
                    .concat(TableUtils.TXN_FILE_NAME);
            return txnHasCompositePartitions(tableToken, path);
        }
    }

    private static void createPlainTable() throws Exception {
        execute("CREATE TABLE x AS (" +
                "SELECT cast(x AS int) i, rnd_str(5, 16, 2) s," +
                " timestamp_sequence('2024-01-01', 1_000_000L) ts" +
                " FROM long_sequence(20_000)) TIMESTAMP(ts) PARTITION BY DAY WAL");
        drainWalQueue();
    }

    private static boolean dbRootTxnHasCompositePartitions(TableToken tableToken) {
        try (Path path = new Path()) {
            path.of(configuration.getDbRoot()).concat(tableToken).concat(TableUtils.TXN_FILE_NAME);
            return txnHasCompositePartitions(tableToken, path);
        }
    }

    /**
     * The storage version the reader would serialize into a checkpoint, taken through the same
     * {@code dumpTo()} the checkpoint writer uses rather than off the live {@code _meta} file.
     */
    private static int dumpedMetaStorageVersion(TableReader reader) {
        try (Path path = new Path()) {
            path.of(root).concat("meta-dump");
            try (MemoryCMARW mem = Vm.getSmallCMARWInstance(
                    configuration.getFilesFacade(),
                    path.$(),
                    MemoryTag.MMAP_DEFAULT,
                    configuration.getWriterFileOpenOpts()
            )) {
                reader.getMetadata().dumpTo(mem);
            }
            return readStorageVersion(path);
        }
    }

    /**
     * A composite day that is NOT the last partition. Built with merge-append ON, the test-suite default.
     */
    private static void makeComposite() throws Exception {
        execute("INSERT INTO x SELECT cast(x AS int) + 100_000 i, rnd_str(5, 16, 2) s," +
                " timestamp_sequence('2024-01-03', 1_000_000L) ts FROM long_sequence(1_000)");
        drainWalQueue();

        for (int i = 0; i < 2; i++) {
            execute("INSERT INTO x SELECT cast(x AS int) + 300_000 i, rnd_str(5, 16, 2) s," +
                    " timestamp_sequence('2024-01-01T01:00:00', 1_000_000L) ts FROM long_sequence(200)");
            drainWalQueue();
        }
    }

    private static int metaStorageVersion(TableToken tableToken) {
        try (Path path = new Path()) {
            path.of(configuration.getDbRoot()).concat(tableToken).concat(TableUtils.META_FILE_NAME);
            return readStorageVersion(path);
        }
    }

    private static int readStorageVersion(Path path) {
        try (MemoryMR mem = Vm.getCMRInstance()) {
            mem.smallFile(configuration.getFilesFacade(), path.$(), MemoryTag.MMAP_DEFAULT);
            return mem.getInt(TableUtils.META_OFFSET_VERSION);
        }
    }

    private static boolean txnHasCompositePartitions(TableToken tableToken, Path txnPath) {
        final int timestampType;
        final int partitionBy;
        try (TableMetadata metadata = engine.getTableMetadata(tableToken)) {
            timestampType = metadata.getTimestampType();
            partitionBy = metadata.getPartitionBy();
        }
        try (TxReader txReader = new TxReader(configuration.getFilesFacade())) {
            txReader.ofRO(txnPath.$(), timestampType, partitionBy);
            txReader.unsafeLoadAll();
            return txReader.hasCompositePartitions();
        }
    }
}
