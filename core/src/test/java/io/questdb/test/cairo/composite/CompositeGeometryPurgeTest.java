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
import io.questdb.cairo.ColumnPurgeJob;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.PartitionGeometryFile;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.TxWriter;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.io.File;

/**
 * A composite partition's {@code _geometry} chain rotates to a fresh generation once a record would push the current
 * generation's file past {@link PartitionGeometryFile#MAX_FILE_SIZE}. The rotation stays inside the SAME partition
 * directory: nothing renames it, so the ordinary partition purge - which is what waits on the {@link
 * io.questdb.cairo.TxnScoreboard} and on a running checkpoint - never sees the retired generation at all.
 * <p>
 * A reader resolves its geometry record lazily, out of the generation its own {@code _txn} snapshot names, and a
 * checkpoint's copied {@code _txn} names one too. So the generation a rotation leaves behind is still live for
 * everything pinned below the rotating commit, and must not be removed until they are gone.
 */
public class CompositeGeometryPurgeTest extends AbstractCairoTest {
    private static final long DAY_03 = MicrosTimestampDriver.floor("2020-02-03T00:00:00.000000Z");

    @Test
    public void testACheckpointKeepsTheRotatedOutGeometryGeneration() throws Exception {
        // CHECKPOINT CREATE calls sync(), which Windows does not have.
        Assume.assumeTrue(Os.type != Os.WINDOWS);
        assertMemoryLeak(() -> {
            buildNearFullGenerationZero();
            final File generationZero = geometryFileOfDay(0);
            Assert.assertTrue("fixture wrote no generation-0 file", generationZero.exists());
            final long nameTxnBefore = nameTxnOfDay();

            execute("checkpoint create");
            try {
                rotateGeneration();
                assertRotatedInPlace(nameTxnBefore);
                Assert.assertTrue(
                        "the generation the checkpoint still resolves was removed: " + generationZero,
                        generationZero.exists()
                );
            } finally {
                execute("checkpoint release");
            }
        });
    }

    @Test
    public void testAPinnedReaderKeepsTheRotatedOutGeometryGeneration() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken tt = buildNearFullGenerationZero();
            final File generationZero = geometryFileOfDay(0);
            Assert.assertTrue("fixture wrote no generation-0 file", generationZero.exists());
            final long nameTxnBefore = nameTxnOfDay();

            try (TableReader pinned = engine.getReader(tt)) {
                // Nothing here resolves the reader's geometry yet. That happens after the rotation, which is the
                // point: the record it goes looking for lives in the generation the rotation retired.
                rotateGeneration();
                assertRotatedInPlace(nameTxnBefore);
                Assert.assertTrue(
                        "the generation the pinned reader still resolves was removed: " + generationZero,
                        generationZero.exists()
                );

                final int partitionIndex = pinned.getTxFile().getPartitionIndex(DAY_03);
                Assert.assertTrue("pinned reader lost the day", partitionIndex > -1);
                Assert.assertEquals(
                        "the pinned reader is not on the retired generation",
                        0,
                        TxReader.geometryGeneration(pinned.getTxFile().getGeometryRef(partitionIndex))
                );
                Assert.assertTrue(
                        "pinned reader resolved an empty geometry",
                        pinned.getGeometry().getPieceCount(partitionIndex) > 0
                );
            }
        });
    }

    @Test
    public void testTheCurrentGenerationSurvivesThePurge() throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(0);
            try (ColumnPurgeJob purgeJob = new ColumnPurgeJob(engine)) {
                buildNearFullGenerationZero();
                rotateGeneration();
                final File generationOne = geometryFileOfDay(1);
                engine.releaseInactive();
                runPurgeJob(purgeJob);
                Assert.assertTrue("the live generation was purged: " + generationOne, generationOne.exists());
            }
        });
    }

    @Test
    public void testTheRetiredGenerationGoesOnceNothingIsPinned() throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(0);
            try (ColumnPurgeJob purgeJob = new ColumnPurgeJob(engine)) {
                final TableToken tt = buildNearFullGenerationZero();
                final File generationZero = geometryFileOfDay(0);

                try (TableReader pinned = engine.getReader(tt)) {
                    Assert.assertNotNull(pinned);
                    rotateGeneration();
                    runPurgeJob(purgeJob);
                    runPurgeJob(purgeJob);
                    Assert.assertTrue(
                            "purged a generation a reader still resolves: " + generationZero,
                            generationZero.exists()
                    );
                }

                engine.releaseInactive();
                runPurgeJob(purgeJob);
                Assert.assertFalse(
                        "the retired generation was never purged: " + generationZero,
                        generationZero.exists()
                );
            }
        });
    }

    /**
     * The rotation must have happened, and must have happened WITHOUT a new partition version - otherwise the ordinary
     * directory purge, not the geometry chain, is what governs the old file's lifetime and this test proves nothing.
     */
    private static void assertRotatedInPlace(long nameTxnBefore) throws Exception {
        try (TableReader reader = engine.getReader(engine.verifyTableName("x"))) {
            final int partitionIndex = reader.getTxFile().getPartitionIndex(DAY_03);
            Assert.assertEquals(
                    "geometry did not rotate to a new generation",
                    1,
                    TxReader.geometryGeneration(reader.getTxFile().getGeometryRef(partitionIndex))
            );
        }
        Assert.assertEquals("the rotation wrote a new partition version", nameTxnBefore, nameTxnOfDay());
        Assert.assertTrue("the rotation wrote no generation-1 file", geometryFileOfDay(1).exists());
    }

    /**
     * A composite day whose committed record sits 8 bytes short of {@link PartitionGeometryFile#MAX_FILE_SIZE} in
     * generation 0, so the next commit that publishes for it has to rotate.
     */
    private static TableToken buildNearFullGenerationZero() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 2 * 1024);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_AVG_ROWS_PIECE_LIM, 16);
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_PRESPLIT_MAX_CUTS, 1);

        execute("CREATE TABLE x AS (" +
                "SELECT x::INT i, -x j," +
                " timestamp_sequence('2020-02-03', 15*1000000L) ts" +
                " FROM long_sequence(5760)" +
                ") TIMESTAMP(ts) PARTITION BY DAY WAL");
        drainWalQueue();

        // A backdated stride relocates a piece to the tail, publishing this partition's first real _geometry
        // record: generation 0, a small offset.
        execute("INSERT INTO x SELECT x::INT + 1000000 i, -x - 1000000L AS j," +
                " timestamp_sequence('2020-02-03T04:00:07', 5*1000000L) ts FROM long_sequence(200)");
        drainWalQueue();

        final TableToken tt = engine.verifyTableName("x");
        try (TableReader reader = engine.getReader(tt)) {
            Assert.assertTrue(
                    "fixture day is not composite",
                    reader.getTxFile().isPartitionComposite(reader.getTxFile().getPartitionIndex(DAY_03))
            );
        }
        plantFakeGeometryRecordNearFileLimit("x", DAY_03, 0);
        return tt;
    }

    private static File geometryFileOfDay(int generation) throws Exception {
        final TableToken tt = engine.verifyTableName("x");
        try (Path path = new Path()) {
            path.of(configuration.getDbRoot()).concat(tt);
            TableUtils.setPathForNativePartition(path, ColumnType.TIMESTAMP, PartitionBy.DAY, DAY_03, nameTxnOfDay());
            path.concat(TableUtils.PARTITION_GEOMETRY_FILE_NAME).put('.').put(generation);
            return new File(path.toString());
        }
    }

    private static long nameTxnOfDay() throws Exception {
        try (TableReader reader = engine.getReader(engine.verifyTableName("x"))) {
            final int partitionIndex = reader.getTxFile().getPartitionIndex(DAY_03);
            Assert.assertTrue("day has no partition", partitionIndex > -1);
            return reader.getTxFile().getPartitionNameTxn(partitionIndex);
        }
    }

    /**
     * Plants a byte-identical copy of the day's currently-committed {@code _geometry} record 8 bytes short of {@link
     * PartitionGeometryFile#MAX_FILE_SIZE} - a sparse write, not 100MB of real I/O - and re-points {@code _txn}'s
     * geometry ref at that copy, so the next commit sees a genuinely near-full generation. Same fake-up as {@code
     * O3PartitionPreSplitTest.plantFakeGeometryRecordNearFileLimit}.
     */
    private static void plantFakeGeometryRecordNearFileLimit(String tableName, long partitionTs, int fakeGeneration) throws Exception {
        try (TableWriter writer = getWriter(tableName)) {
            final TxWriter tx = writer.getTxWriter();
            final int partitionIndex = tx.getPartitionIndex(partitionTs);
            final long committedRef = tx.getGeometryRef(partitionIndex);
            Assert.assertTrue("partition is not composite ahead of the fake-up", tx.isPartitionComposite(partitionIndex));
            final int realGeneration = TxReader.geometryGeneration(committedRef);
            final long realOffset = TxReader.geometryOffset(committedRef);
            final long partitionNameTxn = tx.getPartitionNameTxn(partitionIndex);

            final long fakeOffset = PartitionGeometryFile.MAX_FILE_SIZE - 8;
            final FilesFacade geometryFf = configuration.getFilesFacade();
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(writer.getTableToken());
                TableUtils.setPathForNativePartition(path, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTs, partitionNameTxn);
                try (PartitionGeometryFile geometryFile = new PartitionGeometryFile(MemoryTag.NATIVE_TABLE_WRITER)) {
                    geometryFile.read(geometryFf, path, realGeneration, realOffset);
                    geometryFile.append(geometryFf, path, fakeGeneration, fakeOffset, configuration.getCommitMode());
                }
            }

            tx.setPartitionGeometryRef(partitionTs, TxReader.packGeometryRef(fakeGeneration, fakeOffset));
            tx.commit(new ObjList<>());
        }
    }

    /**
     * One drain of the purge queue into the log table, then one processing pass. The job schedules a queued task
     * {@code column.purge.retry.delay} into the future, so the clock has to move between the two.
     */
    private static void runPurgeJob(ColumnPurgeJob purgeJob) {
        engine.releaseInactive();
        setCurrentMicros(currentMicros + Micros.SECOND_MICROS);
        purgeJob.run();
        setCurrentMicros(currentMicros + Micros.SECOND_MICROS);
        purgeJob.run();
    }

    /**
     * Another relocation into the same day, landing on the now-near-full generation: publish has to rotate rather than
     * grow past MAX_FILE_SIZE.
     */
    private static void rotateGeneration() throws Exception {
        execute("INSERT INTO x SELECT x::INT + 2000000 i, -x - 2000000L AS j," +
                " timestamp_sequence('2020-02-03T06:00:07', 5*1000000L) ts FROM long_sequence(200)");
        drainWalQueue();
        drainPurgeJob();
        Assert.assertFalse(
                "the rotation commit suspended the table",
                engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x"))
        );
    }
}
