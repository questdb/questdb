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
import io.questdb.cairo.ColumnPurgeJob;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.PartitionGeometry;
import io.questdb.cairo.PartitionGeometryFile;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.TxWriter;
import io.questdb.griffin.PurgingOperator;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.concurrent.atomic.AtomicBoolean;

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
    private static final long DAY_04 = MicrosTimestampDriver.floor("2020-02-04T00:00:00.000000Z");
    // EMFILE on POSIX, ERROR_NOT_READY on Windows: an OS-level failure either way, and one the next attempt may well
    // survive. All the purge is allowed to conclude from it is that it does not know what is in the file.
    private static final int ERRNO_TOO_MANY_OPEN_FILES = 24;
    private static final String GENERATION_ZERO_FILE_NAME = TableUtils.PARTITION_GEOMETRY_FILE_NAME + ".0";
    private static final Log LOG = LogFactory.getLog(CompositeGeometryPurgeTest.class);

    /**
     * A generation number is reused: after MAKE-PLAIN retires a partition's {@code _geometry.0} and the purge
     * removes the file, the next composite commit restarts the chain at generation 0 - the first generation with no
     * file on disk - and re-creates {@code _geometry.0} in the same directory, under the same {@code nameTxn}. The
     * retired-generation-0 purge note the MAKE-PLAIN left incomplete must not delete the
     * re-created LIVE file during ordinary (BAU) queue processing. Before the fix the reader window
     * {@code [firstWriterTxn, updateTxn)} was inverted for a re-created generation, so the scoreboard reported it
     * free and the purge removed a live file - the partition then failed every read with
     * "could not open read-only ... _geometry.0".
     */
    @Test
    public void testAReusedGenerationZeroSurvivesBauProcessing() throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(0);
            try (ColumnPurgeJob purgeJob = new ColumnPurgeJob(engine)) {
                final TableToken tt = buildNearFullGenerationZero();
                final File generationZero = geometryFileOfDay(0);
                Assert.assertTrue("fixture wrote no generation-0 file", generationZero.exists());

                // The live generation-0 record at offset 0 carries the txn the on-disk generation became current at.
                // A note whose updateTxn precedes it is exactly the stale note a MAKE-PLAIN-then-re-create leaves
                // behind: it retired an OLDER incarnation of generation 0, not the live one now on disk.
                final long firstWriterTxn = firstWriterTxnOfGeneration(0);
                queueRetiredGeometryGeneration(tt, 0, firstWriterTxn - 1);

                runPurgeJob(purgeJob);

                Assert.assertTrue(
                        "BAU purge deleted the re-created live generation: " + generationZero,
                        generationZero.exists()
                );
                assertDayResolvesItsPieces(tt);
            }
        });
    }

    /**
     * Same reuse as {@link #testAReusedGenerationZeroSurvivesBauProcessing}, but the note is replayed on restart. The
     * STARTUP_ONLY replay ({@link ColumnPurgeJob}'s constructor) skips the scoreboard reader check entirely, so
     * before the fix nothing at all stood between the stale note and the re-created live file.
     */
    @Test
    public void testAReusedGenerationZeroSurvivesTheStartupOnlyReplay() throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(0);
            final TableToken tt = buildNearFullGenerationZero();
            final File generationZero = geometryFileOfDay(0);
            Assert.assertTrue("fixture wrote no generation-0 file", generationZero.exists());
            final long firstWriterTxn = firstWriterTxnOfGeneration(0);

            queueRetiredGeometryGeneration(tt, 0, firstWriterTxn - 1);

            // One job drains the queued note into the purge log with completed=null, but does not process it: the
            // retry is scheduled a delay into the future and the clock has not moved.
            try (ColumnPurgeJob drainer = new ColumnPurgeJob(engine)) {
                drainer.run();
            }

            engine.releaseInactive();
            // A fresh job replays the log from its constructor in STARTUP_ONLY mode. It must still refuse to delete
            // the re-created live generation.
            try (ColumnPurgeJob replay = new ColumnPurgeJob(engine)) {
                Assert.assertNotNull(replay);
            }

            Assert.assertTrue(
                    "the STARTUP_ONLY replay deleted the re-created live generation: " + generationZero,
                    generationZero.exists()
            );
            assertDayResolvesItsPieces(tt);
        });
    }

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

    /**
     * A rotation may not land on a generation whose file holds a record - that file is one a pinned reader may still
     * resolve out of, and re-opening a chain on it would grow over the very bytes the reader names. It may land on a
     * generation whose file holds NO record: a reader names a record by {@code (generation, offset)} out of a
     * {@code _txn} that committed it, and a generation's first record goes to offset 0, so an empty file is one no
     * reader can name. That is the file an {@code append} that failed its write leaves behind, and nothing else ever
     * reclaims it.
     */
    @Test
    public void testARotationSkipsAGenerationHoldingARecordAndTakesAnEmptyOne() throws Exception {
        assertMemoryLeak(() -> {
            buildNearFullGenerationZero();
            final long nameTxnBefore = nameTxnOfDay();
            plantGeometryRecordAtGeneration(1);
            plantEmptyGeometryFileAtGeneration(2);

            rotateGeneration();

            Assert.assertEquals("the rotation wrote a new partition version", nameTxnBefore, nameTxnOfDay());
            Assert.assertEquals("the rotation did not skip the generation holding a record", 2, committedGenerationOfDay());
            Assert.assertTrue("the skipped generation was removed: " + geometryFileOfDay(1), geometryFileOfDay(1).exists());
            assertDayResolvesItsPieces(engine.verifyTableName("x"));
        });
    }

    /**
     * With all sixteen generations holding a record a publish has nowhere to land and fails loudly, naming the
     * partition it failed on - it does NOT silently overwrite one. Emptying a single generation's file is enough for
     * the very same publish to succeed, which is what keeps a stray file from costing a generation permanently.
     * <p>
     * The exception is the last resort for a publish site that cannot rewrite the directory - the compaction, squash
     * and trim sites in {@code TableWriter}, none of which can start a chain, so only a size-cap rotation reaches it
     * there. The COMMIT path no longer does: see
     * {@link #testEveryGenerationHoldingARecordRewritesTheDayIntoAFreshVersion}. So the publish is driven directly,
     * off the same fixture, rather than through an apply that now recovers.
     */
    @Test
    public void testEveryGenerationHoldingARecordFailsAPublishThatCannotRewriteTheDirectory() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken tt = buildNearFullGenerationZero();
            occupyEveryGenerationOfTheDay();

            try {
                republishTheDaysGeometry();
                Assert.fail("publish took a generation that holds a record");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "partition geometry generations exhausted");
                TestUtils.assertContains(e.getFlyweightMessage(), "partitionTimestamp=" + DAY_03);
                TestUtils.assertContains(e.getFlyweightMessage(), "generations=16");
            }
            Assert.assertEquals("the failed publish moved the day's geometry anyway", 0, committedGenerationOfDay());

            // What a publish that failed its write leaves behind: the file exists, but holds no record.
            try (RandomAccessFile emptied = new RandomAccessFile(geometryFileOfDay(1), "rw")) {
                emptied.setLength(0);
            }
            Assert.assertEquals(
                    "the publish did not open on the emptied generation",
                    1,
                    TxReader.geometryGeneration(republishTheDaysGeometry())
            );
            assertDayResolvesItsPieces(tt);
        });
    }

    /**
     * The same exhaustion met while STARTING a chain rather than rotating one: a PLAIN directory that still carries
     * all sixteen files, which is what MAKE-PLAIN - or the JOIN that folds a partition back to the ordinary shape -
     * leaves once the purge notes for the generations it retired are dropped. The pre-existing escape hatch never
     * covered this: it asked for a partition that was composite ALREADY, and a chain start is by definition not one,
     * so the commit met the exception instead.
     */
    @Test
    public void testAChainStartWithEveryGenerationHeldRewritesThePlainDayIntoAFreshVersion() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken tt = buildNearFullGenerationZero();
            appendAPlainSecondDay();
            for (int generation = 0; generation <= 15; generation++) {
                plantGeometryRecordAt(DAY_04, generation);
            }
            final long nameTxnBefore = nameTxnOf(DAY_04);
            try (TableReader reader = engine.getReader(tt)) {
                Assert.assertFalse(
                        "the second day is already composite, so this commit would not START a chain",
                        reader.getTxFile().isPartitionComposite(reader.getTxFile().getPartitionIndex(DAY_04))
                );
            }

            insertABackdatedStrideIntoTheSecondDay();

            Assert.assertFalse(
                    "the chain start with no generation left to publish on suspended the table",
                    engine.getTableSequencerAPI().isSuspended(tt)
            );
            Assert.assertNotEquals(
                    "the plain day was not rewritten into a fresh partition version",
                    nameTxnBefore,
                    nameTxnOf(DAY_04)
            );
            for (int generation = 0; generation <= 15; generation++) {
                Assert.assertFalse(
                        "the fresh partition version carries a geometry file: " + geometryFileOf(DAY_04, generation),
                        geometryFileOf(DAY_04, generation).exists()
                );
            }
            // 2880 rows of the plain second day plus this commit's 200, none lost or duplicated by the rewrite.
            assertQuery("SELECT count() c, sum(i) si, sum(j) sj FROM x WHERE ts IN '2020-02-04'")
                    .noRandomAccess()
                    .expectSize()
                    .returns("c\tsi\tsj\n3080\t12524168740\t-12524168740\n");
        });
    }

    /**
     * With all sixteen generations of the day's directory holding a record, the commit has nowhere to publish a
     * geometry record - and must not fail over it. {@code O3PartitionJob} asks {@link
     * PartitionGeometry#hasGenerationForNextPublish} BEFORE it writes a byte of the plan, and assembles the partition
     * afresh under a new {@code nameTxn} instead. Nothing links or copies a {@code _geometry} file into that
     * directory, so it starts with all sixteen generations free again; that rewrite is the only thing that reclaims a
     * stray geometry file, since the ordinary partition purge never sees one in a directory that stays put and
     * {@code VACUUM TABLE} does not know the name.
     * <p>
     * Before this, {@link PartitionGeometry#publish} threw a critical {@link CairoException} on the apply path, the
     * WAL apply job suspended the table, and {@code RESUME WAL} re-attempted the same seqTxn and threw again: the
     * table stayed down until an operator deleted a geometry file by hand.
     */
    @Test
    public void testEveryGenerationHoldingARecordRewritesTheDayIntoAFreshVersion() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken tt = buildNearFullGenerationZero();
            occupyEveryGenerationOfTheDay();
            final long nameTxnBefore = nameTxnOfDay();

            insertIntoTheNearFullDay();

            Assert.assertFalse(
                    "the commit with no generation left to publish on suspended the table",
                    engine.getTableSequencerAPI().isSuspended(tt)
            );
            Assert.assertNotEquals(
                    "the day was not rewritten into a fresh partition version",
                    nameTxnBefore,
                    nameTxnOfDay()
            );
            // 5760 fixture rows + 200 of the fixture's backdated stride + 200 of this commit's, none lost or
            // duplicated by the rewrite, and the day's dead space gone with the old directory.
            assertQuery("SELECT count() c, sum(i) si, sum(j) sj, max(ts) maxTs FROM x")
                    .noRandomAccess()
                    .expectSize()
                    .returns("c\tsi\tsj\tmaxTs\n6160\t616631880\t-616631880\t2020-02-03T23:59:45.000000Z\n");

            for (int generation = 0; generation <= 15; generation++) {
                Assert.assertFalse(
                        "the fresh partition version carries a geometry file: " + geometryFileOfDay(generation),
                        geometryFileOfDay(generation).exists()
                );
            }
            try (TableReader reader = engine.getReader(tt)) {
                Assert.assertFalse(
                        "the rewritten day is still composite",
                        reader.getTxFile().isPartitionComposite(reader.getTxFile().getPartitionIndex(DAY_03))
                );
            }

            // The reclaimed generation space is a working one, not merely an empty one: the next backdated stride
            // opens a chain on generation 0 of the new directory, in place, and the day still reads back whole.
            final long nameTxnAfterRewrite = nameTxnOfDay();
            insertAFurtherBackdatedStrideIntoTheDay();
            Assert.assertFalse(
                    "the commit after the rewrite suspended the table",
                    engine.getTableSequencerAPI().isSuspended(tt)
            );
            Assert.assertEquals("the commit after the rewrite wrote a new partition version", nameTxnAfterRewrite, nameTxnOfDay());
            Assert.assertEquals("the chain did not open on generation 0 of the fresh directory", 0, committedGenerationOfDay());
            assertDayResolvesItsPieces(tt);
            assertQuery("SELECT count() c, sum(i) si, sum(j) sj FROM x")
                    .noRandomAccess()
                    .expectSize()
                    .returns("c\tsi\tsj\n6360\t1216651980\t-1216651980\n");
        });
    }

    /**
     * A generation whose record the purge cannot READ is not a generation the purge may remove. An open that fails at
     * the OS level - here the descriptor exhaustion the very next attempt survives - says nothing about which
     * incarnation of the generation is on disk, nor about who still resolves it. Before the fix every {@link
     * CairoException} flattened to the same "does not verify, so nothing can be holding it" sentinel, which
     * short-circuited both the incarnation check and the scoreboard check, so the purge unlinked a generation the
     * pinned reader below still needed and its lazy resolve then failed with a misleading
     * "could not open, file does not exist: ... _geometry.0".
     */
    @Test
    public void testATransientGeometryReadFailureKeepsTheGenerationAPinnedReaderNeeds() throws Exception {
        final AtomicBoolean isFaultArmed = new AtomicBoolean();
        final AtomicBoolean hasFaultFired = new AtomicBoolean();
        assertMemoryLeak(new TestFilesFacadeImpl() {
            @Override
            public int errno() {
                return isFaultArmed.get() ? ERRNO_TOO_MANY_OPEN_FILES : super.errno();
            }

            @Override
            public long openRO(LPSZ name) {
                if (isFaultArmed.get()
                        && Utf8s.endsWithAscii(name, GENERATION_ZERO_FILE_NAME)
                        && hasFaultFired.compareAndSet(false, true)) {
                    return -1;
                }
                return super.openRO(name);
            }
        }, () -> {
            setCurrentMicros(0);
            try (ColumnPurgeJob purgeJob = new ColumnPurgeJob(engine)) {
                final TableToken tt = buildNearFullGenerationZero();
                final File generationZero = geometryFileOfDay(0);

                try (TableReader pinned = engine.getReader(tt)) {
                    // Nothing here resolves the reader's geometry yet: the record it will go looking for lives in the
                    // generation the rotation is about to retire.
                    rotateGeneration();

                    isFaultArmed.set(true);
                    runPurgeJob(purgeJob);
                    isFaultArmed.set(false);
                    Assert.assertTrue("the geometry open fault never fired", hasFaultFired.get());

                    Assert.assertTrue(
                            "a geometry read that failed at the OS level purged the generation: " + generationZero,
                            generationZero.exists()
                    );
                    final int partitionIndex = pinned.getTxFile().getPartitionIndex(DAY_03);
                    Assert.assertTrue("pinned reader lost the day", partitionIndex > -1);
                    Assert.assertTrue(
                            "the pinned reader could not resolve its geometry",
                            pinned.getGeometry().getPieceCount(partitionIndex) > 0
                    );

                    // The fault deferred the note, it did not drop it: this pass reads the record for real and now
                    // defers on the reader instead.
                    runPurgeJob(purgeJob);
                    Assert.assertTrue(
                            "purged a generation the reader still resolves: " + generationZero,
                            generationZero.exists()
                    );
                    Assert.assertEquals("the deferred note was dropped", 1, purgeJob.getOutstandingPurgeTasks());
                }

                engine.releaseInactive();
                runPurgeJob(purgeJob);
                Assert.assertFalse(
                        "the note deferred by the read fault never completed: " + generationZero,
                        generationZero.exists()
                );
                Assert.assertEquals("the completed note stayed outstanding", 0, purgeJob.getOutstandingPurgeTasks());
            }
        });
    }

    /**
     * A read that NEVER recovers must not talk the purge into the unlink either, and the note it defers must neither
     * multiply nor wedge: one note stays outstanding across every pass, and completes the moment the read does.
     * Nothing is pinned here, so the read is the only thing standing between the note and the unlink.
     */
    @Test
    public void testAPermanentlyUnreadableGeometryGenerationIsRetriedRatherThanPurged() throws Exception {
        final AtomicBoolean isFaultArmed = new AtomicBoolean();
        assertMemoryLeak(new TestFilesFacadeImpl() {
            @Override
            public int errno() {
                return isFaultArmed.get() ? ERRNO_TOO_MANY_OPEN_FILES : super.errno();
            }

            @Override
            public long openRO(LPSZ name) {
                if (isFaultArmed.get() && Utf8s.endsWithAscii(name, GENERATION_ZERO_FILE_NAME)) {
                    return -1;
                }
                return super.openRO(name);
            }
        }, () -> {
            setCurrentMicros(0);
            try (ColumnPurgeJob purgeJob = new ColumnPurgeJob(engine)) {
                buildNearFullGenerationZero();
                final File generationZero = geometryFileOfDay(0);
                rotateGeneration();
                engine.releaseInactive();

                isFaultArmed.set(true);
                runPurgeJob(purgeJob);
                for (int pass = 0; pass < 4; pass++) {
                    runPurgeJobPastTheRetryBackoff(purgeJob);
                    Assert.assertTrue(
                            "a permanently failing read purged the generation on pass " + pass + ": " + generationZero,
                            generationZero.exists()
                    );
                    Assert.assertEquals(
                            "the retried note multiplied on pass " + pass,
                            1,
                            purgeJob.getOutstandingPurgeTasks()
                    );
                }

                isFaultArmed.set(false);
                runPurgeJobPastTheRetryBackoff(purgeJob);
                Assert.assertFalse(
                        "the note never completed once the read recovered: " + generationZero,
                        generationZero.exists()
                );
                Assert.assertEquals("the completed note stayed outstanding", 0, purgeJob.getOutstandingPurgeTasks());
            }
        });
    }

    /**
     * The other side of the same classification, and the one
     * {@link io.questdb.cairo.PartitionGeometry#publish} leans on: a record that IS readable and does NOT verify -
     * wrong magic, wrong piece count, or a checksum that does not match - names no bytes any reader can resolve, so
     * the purge still removes the file, reader or no reader. Removing it fails such a reader loudly; leaving it would
     * cost a generation of the sixteen for good.
     */
    @Test
    public void testAGenerationWhoseRecordDoesNotVerifyIsPurgedEvenUnderAPinnedReader() throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(0);
            try (ColumnPurgeJob purgeJob = new ColumnPurgeJob(engine)) {
                final TableToken tt = buildNearFullGenerationZero();
                final File generationZero = geometryFileOfDay(0);

                try (TableReader pinned = engine.getReader(tt)) {
                    Assert.assertNotNull(pinned);
                    rotateGeneration();
                    Assert.assertTrue("fixture lost the retired generation", generationZero.exists());
                    corruptTheChecksumOfGeneration(0);

                    runPurgeJob(purgeJob);
                    Assert.assertFalse(
                            "a generation whose record does not verify was kept: " + generationZero,
                            generationZero.exists()
                    );
                    Assert.assertEquals("the completed note stayed outstanding", 0, purgeJob.getOutstandingPurgeTasks());
                }
            }
        });
    }

    /**
     * A {@code _geometry.<n>} the host truncated INSIDE its header is just as unresolvable, and the purge must
     * reclaim it for the same reason - here the short read does not even get as far as the magic word.
     */
    @Test
    public void testAGenerationTruncatedInsideItsHeaderIsPurgedDespiteAStaleErrno() throws Exception {
        assertATruncatedGenerationIsPurgedDespiteAStaleErrno(true);
    }

    /**
     * A {@code _geometry.<n>} whose tail the host lost holds a header that reads back fine and a record the file is
     * too short for. That is a record no reader can resolve - the bytes its {@code (generation, offset)} names are
     * not on disk - so the purge must reclaim the generation, exactly as it does for a checksum mismatch.
     * <p>
     * What used to stop it is that a read stopping short of what it asked for sets no errno: both short-read throws
     * in {@link PartitionGeometryFile#read} reported {@link FilesFacade#errno()} anyway, which is whatever the purge
     * thread last left there - and a purge pass leaves non-zero values behind routinely. The classifier then read a
     * permanently torn file as one it merely could not read, and deferred the note on every retry forever: one of
     * the sixteen generations spent for good, plus an ERROR line per note per retry.
     */
    @Test
    public void testAGenerationTornMidRecordIsPurgedDespiteAStaleErrno() throws Exception {
        assertATruncatedGenerationIsPurgedDespiteAStaleErrno(false);
    }

    /**
     * The STARTUP_ONLY replay skips the scoreboard check entirely, so the incarnation check - is the file on disk a
     * NEWER generation than the one this note retired? - is all that protects the live file, and a read that fails at
     * the OS level is exactly what makes that check unanswerable. Flattening it to "does not verify" deleted the LIVE
     * geometry of a composite partition, not merely one a reader was holding.
     */
    @Test
    public void testAGeometryReadFailureDoesNotLetTheStartupOnlyReplayDeleteALiveGeneration() throws Exception {
        final AtomicBoolean isFaultArmed = new AtomicBoolean();
        final AtomicBoolean hasFaultFired = new AtomicBoolean();
        assertMemoryLeak(new TestFilesFacadeImpl() {
            @Override
            public int errno() {
                return isFaultArmed.get() ? ERRNO_TOO_MANY_OPEN_FILES : super.errno();
            }

            @Override
            public long openRO(LPSZ name) {
                if (isFaultArmed.get()
                        && Utf8s.endsWithAscii(name, GENERATION_ZERO_FILE_NAME)
                        && hasFaultFired.compareAndSet(false, true)) {
                    return -1;
                }
                return super.openRO(name);
            }
        }, () -> {
            setCurrentMicros(0);
            final TableToken tt = buildNearFullGenerationZero();
            final File generationZero = geometryFileOfDay(0);
            Assert.assertTrue("fixture wrote no generation-0 file", generationZero.exists());
            // A note that retired an OLDER incarnation of generation 0, of the kind a MAKE-PLAIN-then-re-create
            // leaves behind. The file now on disk is the LIVE one.
            queueRetiredGeometryGeneration(tt, 0, firstWriterTxnOfGeneration(0) - 1);

            try (ColumnPurgeJob drainer = new ColumnPurgeJob(engine)) {
                drainer.run();
            }
            engine.releaseInactive();

            isFaultArmed.set(true);
            try (ColumnPurgeJob replay = new ColumnPurgeJob(engine)) {
                Assert.assertNotNull(replay);
            }
            isFaultArmed.set(false);
            Assert.assertTrue("the geometry open fault never fired", hasFaultFired.get());

            Assert.assertTrue(
                    "the STARTUP_ONLY replay deleted the live generation on an unreadable record: " + generationZero,
                    generationZero.exists()
            );
            assertDayResolvesItsPieces(tt);
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

    private static void assertDayResolvesItsPieces(TableToken tt) throws Exception {
        engine.releaseInactive();
        try (TableReader reader = engine.getReader(tt)) {
            final int partitionIndex = reader.getTxFile().getPartitionIndex(DAY_03);
            Assert.assertTrue("day lost its partition", partitionIndex > -1);
            Assert.assertTrue("day is not composite any more", reader.getTxFile().isPartitionComposite(partitionIndex));
            // Resolving the pieces opens _geometry.0 read-only; had the purge deleted it, this throws
            // "could not open read-only ... _geometry.0".
            Assert.assertTrue(
                    "reader resolved an empty geometry",
                    reader.getGeometry().getPieceCount(partitionIndex) > 0
            );
        }
    }

    /**
     * A retired generation the host truncated, read by a purge pass whose thread carries a non-zero errno from an
     * earlier failed syscall. The file must be reclaimed, not deferred: it holds no record any reader can name, and
     * nothing else ever reclaims it.
     *
     * @param isTornInsideTheHeader tear the file below one header, leaving a generation {@code
     *                              PartitionGeometry.firstFreeGeneration} would hand out again, rather than mid-record,
     *                              which leaves one it would not
     */
    private static void assertATruncatedGenerationIsPurgedDespiteAStaleErrno(boolean isTornInsideTheHeader) throws Exception {
        final AtomicBoolean isStaleErrnoArmed = new AtomicBoolean();
        assertMemoryLeak(new TestFilesFacadeImpl() {
            @Override
            public int errno() {
                // Nothing fails here. This is the value an EARLIER failed syscall left on the purge thread - the one
                // a short read, which sets no errno of its own, used to report as though it were its own failure.
                return isStaleErrnoArmed.get() ? ERRNO_TOO_MANY_OPEN_FILES : super.errno();
            }
        }, () -> {
            setCurrentMicros(0);
            try (ColumnPurgeJob purgeJob = new ColumnPurgeJob(engine)) {
                final TableToken tt = buildNearFullGenerationZero();
                final File generationZero = geometryFileOfDay(0);
                rotateGeneration();
                engine.releaseInactive();
                Assert.assertTrue("fixture lost the retired generation", generationZero.exists());

                final long tornLength = isTornInsideTheHeader
                        ? PartitionGeometryFile.HEADER_SIZE - 8
                        : PartitionGeometryFile.recordSize(pieceCountOfGeneration(0)) - 8;
                truncateGenerationTo(0, tornLength);
                if (isTornInsideTheHeader) {
                    Assert.assertTrue(
                            "a header tear must leave a file too short to occupy its generation",
                            tornLength < PartitionGeometryFile.recordSize(1)
                    );
                } else {
                    // The case that costs a generation: long enough that firstFreeGeneration still counts it as
                    // occupied, so the purge is the only thing that can ever reclaim it.
                    Assert.assertTrue(
                            "a mid-record tear must leave a file long enough to occupy its generation",
                            tornLength >= PartitionGeometryFile.recordSize(1)
                    );
                }

                isStaleErrnoArmed.set(true);
                runPurgeJob(purgeJob);
                isStaleErrnoArmed.set(false);

                Assert.assertFalse(
                        "a torn geometry record was deferred rather than reclaimed: " + generationZero,
                        generationZero.exists()
                );
                Assert.assertEquals(
                        "the note the torn record deferred stayed outstanding",
                        0,
                        purgeJob.getOutstandingPurgeTasks()
                );
                assertDayResolvesItsPieces(tt);
            }
        });
    }

    /**
     * The writer txn stamped on the record at offset 0 of the day's {@code _geometry.<generation>} - the txn that
     * generation became current at.
     */
    private static long firstWriterTxnOfGeneration(int generation) throws Exception {
        final TableToken tt = engine.verifyTableName("x");
        final FilesFacade ff = configuration.getFilesFacade();
        try (
                Path path = new Path();
                PartitionGeometryFile geometryFile = new PartitionGeometryFile(MemoryTag.NATIVE_TABLE_READER)
        ) {
            path.of(configuration.getDbRoot()).concat(tt);
            TableUtils.setPathForNativePartition(path, ColumnType.TIMESTAMP, PartitionBy.DAY, DAY_03, nameTxnOfDay());
            geometryFile.read(ff, path, generation, 0);
            return geometryFile.getWriterTxn();
        }
    }

    /**
     * The generation the day's committed {@code _txn} geometry ref names.
     */
    private static int committedGenerationOfDay() throws Exception {
        try (TableReader reader = engine.getReader(engine.verifyTableName("x"))) {
            final int partitionIndex = reader.getTxFile().getPartitionIndex(DAY_03);
            Assert.assertTrue("day has no partition", partitionIndex > -1);
            return TxReader.geometryGeneration(reader.getTxFile().getGeometryRef(partitionIndex));
        }
    }

    /**
     * Flips the checksum word of the record at offset 0 of the day's {@code _geometry.<generation>}, so the record
     * reads back whole but does not verify. {@link PartitionGeometryFile#read} raises this with {@code errno} 0,
     * which is what tells the purge it is a verification failure and not an I/O one.
     */
    private static void corruptTheChecksumOfGeneration(int generation) throws Exception {
        try (RandomAccessFile corrupted = new RandomAccessFile(geometryFileOfDay(generation), "rw")) {
            corrupted.seek(PartitionGeometryFile.HEADER_OFFSET_CHECKSUM_64);
            final long stored = corrupted.readLong();
            corrupted.seek(PartitionGeometryFile.HEADER_OFFSET_CHECKSUM_64);
            corrupted.writeLong(~stored);
        }
    }

    private static File geometryFileOf(long partitionTs, int generation) throws Exception {
        final TableToken tt = engine.verifyTableName("x");
        try (Path path = new Path()) {
            path.of(configuration.getDbRoot()).concat(tt);
            TableUtils.setPathForNativePartition(path, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTs, nameTxnOf(partitionTs));
            path.concat(TableUtils.PARTITION_GEOMETRY_FILE_NAME).put('.').put(generation);
            return new File(path.toString());
        }
    }

    private static File geometryFileOfDay(int generation) throws Exception {
        return geometryFileOf(DAY_03, generation);
    }

    /**
     * Queues a retired {@code _geometry.<generation>} note for the day, exactly as
     * {@code TableWriter.publishRetiredGeometryGenerations} does: a {@link ColumnType#NULL} entry whose
     * column-version slot carries the generation, keyed on the partition's timestamp and name txn, with
     * {@code updateTxn} the txn of the commit that retired it.
     */
    private static void queueRetiredGeometryGeneration(TableToken tt, int generation, long updateTxn) throws Exception {
        final long truncateVersion;
        try (TableReader reader = engine.getReader(tt)) {
            truncateVersion = reader.getTxFile().getTruncateVersion();
        }
        final LongList note = new LongList();
        note.add(generation, DAY_03, nameTxnOfDay(), 0L);
        PurgingOperator.purgeColumnVersionAsync(
                LOG,
                engine.getMessageBus(),
                tt,
                TableUtils.PARTITION_GEOMETRY_FILE_NAME,
                tt.getTableId(),
                (int) truncateVersion,
                ColumnType.NULL,
                IndexType.NONE,
                ColumnType.TIMESTAMP,
                PartitionBy.DAY,
                updateTxn,
                note,
                0,
                note.size()
        );
    }

    /**
     * A whole second day of in-order rows, which stays PLAIN: nothing is backdated into it, so no commit relocates a
     * piece and it publishes no geometry record.
     */
    private static void appendAPlainSecondDay() throws Exception {
        execute("""
                INSERT INTO x
                SELECT x::INT + 4_000_000 i, -x - 4_000_000L AS j,
                       timestamp_sequence('2020-02-04', 30*1_000_000L) ts
                FROM long_sequence(2880)""");
        drainWalQueue();
    }

    /**
     * A relocation into the plain second day, which is what makes it want to publish a geometry record - and so START
     * a chain in its directory.
     */
    private static void insertABackdatedStrideIntoTheSecondDay() throws Exception {
        execute("""
                INSERT INTO x
                SELECT x::INT + 5_000_000 i, -x - 5_000_000L AS j,
                       timestamp_sequence('2020-02-04T04:00:07', 5*1_000_000L) ts
                FROM long_sequence(200)""");
        drainWalQueue();
    }

    /**
     * A relocation into the day at a stride none of the other inserts uses, so the rows it adds are countable on
     * their own.
     */
    private static void insertAFurtherBackdatedStrideIntoTheDay() throws Exception {
        execute("""
                INSERT INTO x
                SELECT x::INT + 3_000_000 i, -x - 3_000_000L AS j,
                       timestamp_sequence('2020-02-03T08:00:07', 5*1_000_000L) ts
                FROM long_sequence(200)""");
        drainWalQueue();
    }

    /**
     * Another relocation into the day {@link #buildNearFullGenerationZero} left 8 bytes short of the file size cap, so
     * the commit has to rotate to another generation. Unlike {@link #rotateGeneration} this makes no claim about the
     * commit succeeding.
     */
    private static void insertIntoTheNearFullDay() throws Exception {
        execute("""
                INSERT INTO x
                SELECT x::INT + 2_000_000 i, -x - 2_000_000L AS j,
                       timestamp_sequence('2020-02-03T06:00:07', 5*1_000_000L) ts
                FROM long_sequence(200)""");
        drainWalQueue();
    }

    /**
     * The number of pieces the record at offset 0 of the day's {@code _geometry.<generation>} carries, and so how
     * many bytes that record occupies.
     */
    private static int pieceCountOfGeneration(int generation) throws Exception {
        final TableToken tt = engine.verifyTableName("x");
        final FilesFacade ff = configuration.getFilesFacade();
        try (
                Path path = new Path();
                PartitionGeometryFile geometryFile = new PartitionGeometryFile(MemoryTag.NATIVE_TABLE_READER)
        ) {
            path.of(configuration.getDbRoot()).concat(tt);
            TableUtils.setPathForNativePartition(path, ColumnType.TIMESTAMP, PartitionBy.DAY, DAY_03, nameTxnOfDay());
            geometryFile.read(ff, path, generation, 0);
            Assert.assertTrue(
                    "the fixture record has too few pieces to tear in two",
                    geometryFile.getPieceCount() > 1
            );
            return geometryFile.getPieceCount();
        }
    }

    /**
     * Truncates the day's {@code _geometry.<generation>} to {@code length} bytes - what a host that lost the tail of
     * the file leaves behind. The bytes still there read back fine; the read that wants the rest stops short of it.
     */
    private static void truncateGenerationTo(int generation, long length) throws Exception {
        final File torn = geometryFileOfDay(generation);
        try (RandomAccessFile file = new RandomAccessFile(torn, "rw")) {
            file.setLength(length);
        }
        Assert.assertEquals("the tear did not take", length, torn.length());
    }

    private static long nameTxnOf(long partitionTs) throws Exception {
        try (TableReader reader = engine.getReader(engine.verifyTableName("x"))) {
            final int partitionIndex = reader.getTxFile().getPartitionIndex(partitionTs);
            Assert.assertTrue("day has no partition", partitionIndex > -1);
            return reader.getTxFile().getPartitionNameTxn(partitionIndex);
        }
    }

    private static long nameTxnOfDay() throws Exception {
        return nameTxnOf(DAY_03);
    }

    /**
     * Leaves every one of the day's sixteen generations holding a record: 1..15 by hand, generation 0 from {@link
     * #buildNearFullGenerationZero}. The geometry ref packs 4 bits of generation, so that is all of them
     * ({@code TxReader.PARTITION_GEOMETRY_MAX_GENERATION}, which is protected and so spelled out here).
     */
    private static void occupyEveryGenerationOfTheDay() throws Exception {
        for (int generation = 1; generation <= 15; generation++) {
            plantGeometryRecordAtGeneration(generation);
        }
    }

    /**
     * Creates an empty {@code _geometry.<generation>} for the day: the file an {@code append} leaves behind when its
     * write fails, since it creates the file before writing it.
     */
    private static void plantEmptyGeometryFileAtGeneration(int generation) throws Exception {
        final File emptyGeneration = geometryFileOfDay(generation);
        Assert.assertTrue("generation " + generation + " already has a file", emptyGeneration.createNewFile());
        Assert.assertEquals("planted generation " + generation + " is not empty", 0, emptyGeneration.length());
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

            writer.setPartitionGeometryRefForTest(partitionTs, TxReader.packGeometryRef(fakeGeneration, fakeOffset));
            tx.commit(new ObjList<>());
        }
    }

    /**
     * Plants a byte-identical copy of the day's currently-committed {@code _geometry} record at offset 0 of {@code
     * _geometry.<generation>}, leaving {@code _txn} alone: a generation occupied by a record nothing points at, which
     * is what a retirement note the purge queue dropped leaves behind.
     */
    private static void plantGeometryRecordAtGeneration(int generation) throws Exception {
        try (TableReader reader = engine.getReader(engine.verifyTableName("x"))) {
            final int partitionIndex = reader.getTxFile().getPartitionIndex(DAY_03);
            Assert.assertNotEquals(
                    "the plant would overwrite the live generation",
                    TxReader.geometryGeneration(reader.getTxFile().getGeometryRef(partitionIndex)),
                    generation
            );
        }
        plantGeometryRecordAt(DAY_03, generation);
    }

    /**
     * The same plant, into any day's directory. Only the file's LENGTH decides whether a generation is occupied
     * ({@code PartitionGeometry.firstFreeGeneration}), so the record the copy carries need not describe {@code
     * partitionTs} - the whole point of the fixture is that {@code _txn} points at none of these, so nothing resolves
     * them.
     */
    private static void plantGeometryRecordAt(long partitionTs, int generation) throws Exception {
        final TableToken tt = engine.verifyTableName("x");
        final int liveGeneration;
        final long liveOffset;
        try (TableReader reader = engine.getReader(tt)) {
            final int compositeIndex = reader.getTxFile().getPartitionIndex(DAY_03);
            Assert.assertTrue(
                    "the composite day the record is copied from is not composite",
                    reader.getTxFile().isPartitionComposite(compositeIndex)
            );
            final long committedRef = reader.getTxFile().getGeometryRef(compositeIndex);
            liveGeneration = TxReader.geometryGeneration(committedRef);
            liveOffset = TxReader.geometryOffset(committedRef);
        }

        final FilesFacade ff = configuration.getFilesFacade();
        try (
                Path source = new Path();
                Path target = new Path();
                PartitionGeometryFile geometryFile = new PartitionGeometryFile(MemoryTag.NATIVE_TABLE_WRITER)
        ) {
            source.of(configuration.getDbRoot()).concat(tt);
            TableUtils.setPathForNativePartition(source, ColumnType.TIMESTAMP, PartitionBy.DAY, DAY_03, nameTxnOfDay());
            target.of(configuration.getDbRoot()).concat(tt);
            TableUtils.setPathForNativePartition(target, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTs, nameTxnOf(partitionTs));
            geometryFile.read(ff, source, liveGeneration, liveOffset);
            geometryFile.append(ff, target, generation, 0, configuration.getCommitMode());
        }
        Assert.assertTrue(
                "planted generation " + generation + " holds no record",
                geometryFileOf(partitionTs, generation).length() >= PartitionGeometryFile.recordSize(1)
        );
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
     * One processing pass over an already-queued note, with the clock moved past the job's retry backoff:
     * {@code column.purge.retry.delay} multiplies by {@code column.purge.retry.delay.multiplier} on every failed
     * attempt up to {@code column.purge.retry.delay.limit} (60s by default), so a pass that moved the clock by a
     * second would stop running the task after the third attempt and prove nothing.
     */
    private static void runPurgeJobPastTheRetryBackoff(ColumnPurgeJob purgeJob) {
        engine.releaseInactive();
        setCurrentMicros(currentMicros + 2 * Micros.MINUTE_MICROS);
        purgeJob.run();
    }

    /**
     * Re-publishes the day's committed geometry shape unchanged, off a {@link PartitionGeometry} of this test's own -
     * the fixture's generation 0 sits 8 bytes short of the file size cap, so the publish has to rotate, and where it
     * rotates to is the whole point. Nothing here touches {@code _txn}.
     *
     * @return the geometry ref the publish produced
     */
    private static long republishTheDaysGeometry() throws Exception {
        final TableToken tt = engine.verifyTableName("x");
        try (TableReader reader = engine.getReader(tt)) {
            final int partitionIndex = reader.getTxFile().getPartitionIndex(DAY_03);
            Assert.assertTrue("day has no partition", partitionIndex > -1);
            try (Path root = new Path()) {
                root.of(configuration.getDbRoot()).concat(tt.getDirName());
                try (PartitionGeometry geometry = new PartitionGeometry().of(
                        configuration.getFilesFacade(),
                        reader.getTxFile(),
                        root.toString(),
                        ColumnType.TIMESTAMP,
                        PartitionBy.DAY,
                        MemoryTag.NATIVE_TABLE_READER
                )) {
                    geometry.beginUpdate(partitionIndex);
                    for (int p = 0, n = geometry.getPieceCount(partitionIndex); p < n; p++) {
                        geometry.addPiece(
                                geometry.getPieceTimestampLo(partitionIndex, p),
                                geometry.getPieceTimestampHi(partitionIndex, p),
                                geometry.getPieceRowOffset(partitionIndex, p),
                                geometry.getPieceRowCount(partitionIndex, p),
                                geometry.getPieceWriterTxn(partitionIndex, p),
                                geometry.getPieceLastWriteMicros(partitionIndex, p)
                        );
                    }
                    geometry.commitUpdate(partitionIndex, geometry.getE(partitionIndex));
                    return geometry.publish(
                            partitionIndex,
                            reader.getTxFile().getTxn() + 1,
                            reader.getTxFile().getSeqTxn(),
                            0,
                            configuration.getCommitMode()
                    );
                }
            }
        }
    }

    /**
     * Another relocation into the same day, landing on the now-near-full generation: publish has to rotate rather than
     * grow past MAX_FILE_SIZE.
     */
    private static void rotateGeneration() throws Exception {
        insertIntoTheNearFullDay();
        drainPurgeJob();
        Assert.assertFalse(
                "the rotation commit suspended the table",
                engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x"))
        );
    }
}
