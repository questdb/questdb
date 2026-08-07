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

package io.questdb.test.cairo.crash;

import io.questdb.PropertyKey;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.RecoveryCoordinator;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.std.MemoryTag;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * A durable epoch whose {@code _txn.epoch} and {@code _meta.epoch} describe DIFFERENT table shapes must be
 * rejected, not restored.
 * <p>
 * Recovery restores the epoch's copies together, so they are only usable if they were captured from the same
 * table shape. Nothing in the integrity layers can establish that: the A/B checksums prove each payload is
 * intact, and {@link io.questdb.cairo.DurableEpochManifest} proves each is byte-for-byte the file this
 * generation published — but a checksum cannot distinguish a faithfully-recorded skew from a faithfully
 * recorded consistent cut. That is why {@code RecoveryCoordinator.epochCopiesValid} also compares the two
 * SEMANTICALLY, on the one property that ties them together: {@code _txn}'s symbol area is written from
 * {@code denseSymbolMapWriters}, and {@code _meta} declares the live symbol columns.
 * <p>
 * <b>The failure this prevents.</b> Restoring a mismatched pair leaves {@code _txn} naming more symbol
 * columns than {@code _meta} has: {@code TableWriter.rollbackSymbolTables} then indexes past the writer list
 * inside the TableWriter CONSTRUCTOR, and past that WAL apply rejects the table with "unexpected new WAL
 * structure version". The table never opens again while the sequencer runs ahead. It reached production as an
 * {@code ArrayIndexOutOfBoundsException} in the adaptive crash-fuzz soak.
 * <p>
 * <b>Why the state is fabricated through a facade.</b> Editing {@code _meta.epoch.N} after the writer
 * published it would be caught by the manifest — for the wrong reason — and the test would pass with or
 * without the check under test. {@link StaleEpochMetaFilesFacade} instead swaps the content mid-publish, so
 * the PRODUCT computes the manifest over the substituted bytes. The epoch that lands on disk is therefore
 * flawless to every checksum and still semantically skewed: precisely what the write-side defect produced,
 * and the only state in which this check is load-bearing.
 */
public class AdaptiveRecoveryInconsistentEpochCopyCrashTest extends AbstractCairoTest {

    private static final int K = 4; // rows before the first (generation 0) epoch
    private static final int M = 5; // rows applied lazily after it
    private static final int P = 3; // rows applied lazily AFTER the poisoned epoch, so recovery must roll back

    @Test
    public void testRecoveryRejectsEpochWhoseTxnAndMetaDisagreeOnSymbolColumns() throws Exception {
        final StaleEpochMetaFilesFacade ff = new StaleEpochMetaFilesFacade();
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        // Drive every epoch explicitly, at a known cut; the cadence must not fire one behind our back.
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
        try {
            assertMemoryLeak(ff, () -> {
                Assert.assertEquals(CommitMode.ADAPTIVE, engine.getConfiguration().getCommitMode());

                execute("create table t (ts timestamp, sym symbol, v long) timestamp(ts) partition by day wal");
                for (int i = 0; i < K; i++) {
                    execute("insert into t values ('2024-10-01T0" + i + ":00:00.000000Z', 's" + i + "', " + i + ")");
                }
                drainWalQueue();

                final TableToken tt = engine.verifyTableName("t");

                // Generation 0: a genuine, consistent epoch. This is the fallback the rejection must land on.
                try (TableWriter w = getWriter(tt)) {
                    w.advanceDurableEpoch(1L);
                }

                // Stash the live _meta while the table still has exactly ONE symbol column.
                final String staleMeta = stashLiveMeta(tt);

                // A SECOND symbol column, so the live _txn's symbol area grows to 2 while the stashed _meta
                // still declares 1 -- the shape difference the check has to notice.
                execute("alter table t add column sym2 symbol");
                for (int i = K; i < K + M; i++) {
                    execute("insert into t values ('2024-10-01T0" + i + ":00:00.000000Z', 's" + i + "', " + i + ", 'x')");
                }
                drainWalQueue();

                // Equal metadataVersions are what make this case interesting: with them differing, the
                // existing identity check rejects the candidate first and the semantic check is never
                // reached, which would make this control vacuous. The real defect produced them equal
                // (changeColumnType moves the symbol count on a commit that does not bump the version), so
                // reproduce that here rather than testing a case the code already covered.
                alignStaleMetaVersion(tt, staleMeta);

                // Generation 1: published with the stashed _meta swapped in mid-copy. The manifest is then
                // written by the product over the substituted bytes, so every integrity check still passes.
                // Stay armed across the lazy tail below too: the apply cadence publishes epochs of its own,
                // and an un-poisoned one landing afterwards becomes the active candidate and quietly makes
                // this control vacuous -- which is exactly what happened on the first attempt. No further DDL
                // runs past this point, so metadataVersion is stable and every epoch published inside this
                // window carries the same skew.
                ff.armWith(staleMeta);
                try (TableWriter w = getWriter(tt)) {
                    w.advanceDurableEpoch(2L);
                }

                // P more rows applied LAZILY, so the live _txn advances past the poisoned epoch. Without a
                // tail there is nothing for recovery to roll back, it never consults the candidate at all,
                // and this test would pass whether or not the check exists.
                for (int i = K + M; i < K + M + P; i++) {
                    execute("insert into t values ('2024-10-0" + (i - K - M + 2) + "T00:00:00.000000Z', 's" + i + "', " + i + ", 'x')");
                }
                drainWalQueue();
                ff.disarm();

                // Assert the injection FIRED. It silently did not on the first attempt -- LPSZ.toString() is
                // not the path text, so every match test returned false and the facade was a no-op that still
                // looked armed, which made the whole control vacuous.
                Assert.assertFalse("the facade never substituted an epoch _meta; the injection did not fire",
                        ff.substituted.isEmpty());

                // The candidate recovery will actually consider must be a poisoned one, or nothing below
                // proves anything.
                Assert.assertEquals(
                        "test setup failed: the marker's ACTIVE epoch generation is not a poisoned one, so "
                                + "recovery validates a consistent cut and this control proves nothing",
                        1, epochMetaSymbolColumns(tt, activeEpochGeneration(tt))
                );

                engine.releaseAllWriters();
                engine.releaseAllReaders();

                // Recovery must refuse the skewed generation and fall back to the consistent one.
                new RecoveryCoordinator(engine).recover();

                // Replay forward from the fallback floor; every durable WAL row must come back.
                engine.notifyWalTxnRepublisher(tt);
                drainWalQueue();

                Assert.assertFalse(
                        "recovery restored an epoch whose _txn and _meta describe different table shapes. The "
                                + "restored _txn names more symbol columns than the restored _meta declares, so "
                                + "rollbackSymbolTables indexes past denseSymbolMapWriters inside the TableWriter "
                                + "constructor and the table can never be opened again. Neither the payload "
                                + "checksums nor DurableEpochManifest can catch this — both confirm the bytes are "
                                + "exactly what was published; only comparing the two copies to EACH OTHER does.",
                        engine.getTableSequencerAPI().isSuspended(tt)
                );
                Assert.assertEquals("every durable WAL row must survive the fallback + replay",
                        K + M + P, countRows());

                // The structural harm, and the sharpest signal that the skewed cut was adopted. Restoring
                // generation N's _meta rewinds the schema to a shape that never matched its _txn, while the
                // restored _txn pins the replay floor PAST the ALTER that created the column -- so the WAL
                // never re-applies it and sym2 is gone for good. Falling back to the consistent generation
                // instead rewinds further, to a floor BEFORE the ALTER, and the replay rebuilds it.
                try (io.questdb.cairo.sql.TableMetadata m = engine.getTableMetadata(tt)) {
                    Assert.assertTrue(
                            "column sym2 was silently lost: recovery adopted an epoch whose _meta and _txn "
                                    + "describe different table shapes, restoring a schema without sym2 while "
                                    + "pinning the WAL replay floor past the ALTER that added it. Every "
                                    + "integrity check passed -- the payloads are byte-for-byte what was "
                                    + "published -- so only comparing the two copies to EACH OTHER catches it.",
                            m.getColumnIndexQuiet("sym2") > -1
                    );
                }
            });
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
        }
    }

    private long countRows() {
        try (
                io.questdb.griffin.SqlExecutionContext ctx =
                        io.questdb.test.tools.TestUtils.createSqlExecutionCtx(engine);
                io.questdb.cairo.sql.RecordCursorFactory f = engine.select("select count() from t", ctx);
                io.questdb.cairo.sql.RecordCursor c = f.getCursor(ctx)
        ) {
            Assert.assertTrue(c.hasNext());
            return c.getRecord().getLong(0);
        } catch (io.questdb.griffin.SqlException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Rewrites the stashed {@code _meta}'s metadataVersion to the live table's, through
     * {@link TableUtils#resetMetadataVersion} so the coupled meta-format checksum stays valid.
     */
    private void alignStaleMetaVersion(TableToken tt, String staleMeta) {
        final long liveVersion;
        try (io.questdb.cairo.sql.TableMetadata meta = engine.getTableMetadata(tt)) {
            liveVersion = meta.getMetadataVersion();
        }
        try (Path p = new Path(); MemoryMARW mem = Vm.getCMARWInstance()) {
            p.of(staleMeta);
            mem.smallFile(engine.getConfiguration().getFilesFacade(), p.$(), MemoryTag.MMAP_DEFAULT);
            TableUtils.resetMetadataVersion(mem, liveVersion);
        }
    }

    private int activeEpochGeneration(TableToken tt) {
        try (Path p = new Path()) {
            p.of(engine.getConfiguration().getDbRoot()).concat(tt).concat(TableUtils.SNAPSHOT_FILE_NAME);
            final io.questdb.cairo.SnapshotMarker marker = new io.questdb.cairo.SnapshotMarker(engine.getConfiguration());
            try {
                marker.of(p.$());
                Assert.assertTrue("the epoch marker must load", marker.tryLoad());
                return marker.getGeneration();
            } finally {
                marker.close();
            }
        }
    }

    private int epochMetaSymbolColumns(TableToken tt, int generation) {
        try (Path p = new Path(); io.questdb.cairo.TableReaderMetadata meta =
                new io.questdb.cairo.TableReaderMetadata(engine.getConfiguration())) {
            p.of(engine.getConfiguration().getDbRoot()).concat(tt)
                    .concat(TableUtils.META_FILE_NAME).put(TableUtils.EPOCH_COPY_SUFFIX).put('.').put(generation);
            meta.loadMetadata(p.$());
            int n = 0;
            for (int i = 0, c = meta.getColumnCount(); i < c; i++) {
                final int type = meta.getColumnType(i);
                if (type > -1 && io.questdb.cairo.ColumnType.isSymbol(type)) {
                    n++;
                }
            }
            return n;
        }
    }

    private String stashLiveMeta(TableToken tt) {
        try (Path src = new Path(); Path dst = new Path()) {
            src.of(engine.getConfiguration().getDbRoot()).concat(tt).concat(TableUtils.META_FILE_NAME);
            dst.of(engine.getConfiguration().getDbRoot()).concat("stale_meta_one_symbol");
            Assert.assertTrue("could not stash the one-symbol _meta",
                    engine.getConfiguration().getFilesFacade().copy(src.$(), dst.$()) >= 0);
            return dst.toString();
        }
    }
}
