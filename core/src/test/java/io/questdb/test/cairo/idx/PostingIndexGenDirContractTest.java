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

package io.questdb.test.cairo.idx;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.idx.PostingIndexChainEntry;
import io.questdb.cairo.idx.PostingIndexChainHeader;
import io.questdb.cairo.idx.PostingIndexFwdReader;
import io.questdb.cairo.idx.PostingIndexUtils;
import io.questdb.cairo.idx.PostingIndexWriter;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.cairo.vm.MemoryCMARWImpl;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cairo.TableUtils.COLUMN_NAME_TXN_NONE;

/**
 * Pins the gen-dir {@code TXN_AT_SEAL} monotonicity contract, which spans a writer
 * and a reader half.
 * <p>
 * Writer half: {@code PostingIndexWriter.publishToChain} clamps a regressing slot
 * txnAtSeal up to its predecessor's, so a later gen can never become visible
 * earlier than an earlier one. The clamp only reaches the ONE slot each publish
 * writes, so {@code checkGenDirMonotonic} re-checks the whole gen-dir the publish
 * is about to expose and fails the publish -- BEFORE appendNewEntry/extendHead --
 * when a prefix off disk already regresses.
 * <p>
 * Reader half: {@code PostingGenLookup.snapshotMetadata} stops at the first drop in
 * the sequence and reports the truncated count, and
 * {@code AbstractPostingIndexReader.readIndexMetadataFromChain} fails the read when
 * that count falls short of the entry's own GEN_COUNT. Both seqlocks are held
 * across the snapshot at that point, so a shortfall is corruption at rest rather
 * than a torn read, and serving the monotonic prefix would return a partial index
 * scan with no signal.
 */
public class PostingIndexGenDirContractTest extends AbstractCairoTest {

    @Test
    public void testPublishToChainClampsRegressingGenTxnAtSeal() throws Exception {
        assertMemoryLeak(() -> {
            final String name = "posting_gen_dir_clamp";
            final FilesFacade ff = configuration.getFilesFacade();
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();
                try (PostingIndexWriter writer = new PostingIndexWriter(configuration)) {
                    writer.of(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, /* isInit */ true);
                    writer.setNextTxnAtSeal(7L);
                    writer.add(0, 0);
                    writer.setMaxValue(0);
                    writer.commit();

                    // A caller that re-arms with a LOWER txn drives the gen-dir
                    // backwards. publishToChain must clamp gen 1 up to gen 0's txn.
                    writer.setNextTxnAtSeal(3L);
                    writer.add(1, 1);
                    writer.setMaxValue(1);
                    writer.commit();
                }

                final LPSZ keyFile = PostingIndexUtils.keyFileName(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE);
                Assert.assertEquals("two commits must leave a two-gen head entry", 2, readHeadGenCount(ff, keyFile));
                Assert.assertEquals(7L, readGenDirTxnAtSeal(ff, keyFile, 0));
                Assert.assertEquals(
                        "the regressing gen must carry its predecessor's txn, not the caller's 3",
                        7L,
                        readGenDirTxnAtSeal(ff, keyFile, 1)
                );

                // Positive control for the reader half: the equal-txn gen-dir the
                // clamp just produced is a HEALTHY shape, so the reader must accept
                // it and serve both gens. snapshotMetadata compares with a strict
                // '<', so equal adjacent txns are not a drop; widening that to '<='
                // would truncate the snapshot here and fail every read with
                // INDEX_CORRUPT.
                try (PostingIndexFwdReader reader = new PostingIndexFwdReader(
                        configuration, path.trimTo(plen), name,
                        COLUMN_NAME_TXN_NONE, /* partitionTxn */ 0, /* columnTop */ 0)) {
                    assertOnlyRow("gen 0 must stay visible under the clamped gen", reader, 0, 0);
                    assertOnlyRow("the clamped gen must stay visible", reader, 1, 1);
                }
            }
        });
    }

    @Test
    public void testPublishToChainRejectsRegressingGenDirWithoutMutatingChain() throws Exception {
        assertMemoryLeak(() -> {
            final String name = "posting_gen_dir_publish_guard";
            final FilesFacade ff = configuration.getFilesFacade();
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();
                try (PostingIndexWriter writer = new PostingIndexWriter(configuration)) {
                    writer.of(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, /* isInit */ true);
                    for (int gen = 0; gen < 3; gen++) {
                        writer.setNextTxnAtSeal(gen + 1);
                        writer.add(gen, gen);
                        writer.setMaxValue(gen);
                        writer.commit();
                    }

                    final LPSZ keyFile = PostingIndexUtils.keyFileName(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE);
                    Assert.assertEquals("three commits must leave a three-gen head entry", 3, readHeadGenCount(ff, keyFile));

                    // A gen-dir prefix a pre-fix build left behind: gen 1 regresses
                    // below gen 0. publishToChain's clamp only constrains the ONE
                    // slot this commit writes, so it cannot repair an interior slot
                    // that came off disk.
                    stampGenDirTxnAtSeal(ff, keyFile, 1, 0L);

                    writer.setNextTxnAtSeal(4L);
                    writer.add(3, 3);
                    writer.setMaxValue(3);

                    // Snapshot AFTER setMaxValue: that republishes the header on
                    // its own (updateHeadMaxValue), so it must not be mistaken for
                    // the publish this test is pinning.
                    final long headEntryOffsetBefore = readHeadEntryOffset(ff, keyFile);
                    final int genCountBefore = readHeadGenCount(ff, keyFile);
                    final long sequenceBefore = readChainSequence(ff, keyFile);
                    try {
                        writer.commit();
                        Assert.fail("the writer must refuse to publish over a regressing gen-dir prefix");
                    } catch (CairoException e) {
                        // A diagnostic CairoException, NOT an AssertionError: production
                        // runs with -ea, and an Error escaping the commit path bypasses
                        // TableWriter's catch (CairoException) -> throwDistressException
                        // handling entirely.
                        // The reason names the WRITER and carries the index name, so this
                        // cannot pass on a reader-side detection of the same corruption.
                        TestUtils.assertContains(
                                e.getFlyweightMessage(),
                                "posting index is corrupt [reason=writer refused to publish a non-monotonic gen-dir"
                                        + ", index=" + name
                        );
                    }

                    // The failed publish must leave the chain exactly as a concurrent
                    // reader saw it before the commit: same head entry, same GEN_COUNT,
                    // same header sequence.
                    Assert.assertEquals(
                            "the failed publish must not extend the head entry",
                            genCountBefore,
                            readHeadGenCount(ff, keyFile)
                    );
                    Assert.assertEquals(
                            "the failed publish must not move the head entry",
                            headEntryOffsetBefore,
                            readHeadEntryOffset(ff, keyFile)
                    );
                    Assert.assertEquals(
                            "the failed publish must not republish the chain header",
                            sequenceBefore,
                            readChainSequence(ff, keyFile)
                    );
                }
            }
        });
    }

    @Test
    public void testReaderRejectsGenDirWithNonMonotonicTxnAtSeal() throws Exception {
        assertMemoryLeak(() -> {
            final String name = "posting_gen_dir_non_monotonic";
            final FilesFacade ff = configuration.getFilesFacade();
            try (Path path = new Path().of(configuration.getDbRoot())) {
                final int plen = path.size();
                try (PostingIndexWriter writer = new PostingIndexWriter(configuration)) {
                    writer.of(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE, /* isInit */ true);
                    for (int gen = 0; gen < 3; gen++) {
                        writer.setNextTxnAtSeal(gen + 1);
                        writer.add(gen, gen);
                        writer.setMaxValue(gen);
                        writer.commit();
                    }
                }

                final LPSZ keyFile = PostingIndexUtils.keyFileName(path.trimTo(plen), name, COLUMN_NAME_TXN_NONE);
                Assert.assertEquals("three commits must leave a three-gen head entry", 3, readHeadGenCount(ff, keyFile));

                // Corruption at rest: gen 1 reads back as an unpublished slot. It is
                // stable across every retry of the reader's seqlock loop, so the read
                // cannot absorb it as a torn read.
                final long headEntryOffset = readHeadEntryOffset(ff, keyFile);
                stampGenDirTxnAtSeal(ff, keyFile, 1, 0L);

                // publishedGenCount is the truncated count snapshotMetadata stopped at:
                // gen 0 only, NOT the monotonic-again gen 2 behind the drop.
                final String expected = "posting index is corrupt [reason=gen-dir TXN_AT_SEAL not monotonic"
                        + ", entryOffset=" + headEntryOffset + ", genCount=3, publishedGenCount=1]";
                try (PostingIndexFwdReader ignore = new PostingIndexFwdReader(
                        configuration, path.trimTo(plen), name,
                        COLUMN_NAME_TXN_NONE, /* partitionTxn */ 0, /* columnTop */ 0)) {
                    Assert.fail("the reader must refuse a gen-dir whose TXN_AT_SEAL sequence drops");
                } catch (CairoException e) {
                    TestUtils.assertEquals(expected, e.getFlyweightMessage());
                }
            }
        });
    }

    @Test
    public void testSelectFailsOnGenDirWithNonMonotonicTxnAtSeal() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE posting_gen_dir_select (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            // Two commits into the same partition leave a two-gen head entry: the
            // seal gen threshold (16) is well clear of two, so neither commit
            // collapses the chain into a single gen.
            execute("""
                    INSERT INTO posting_gen_dir_select
                    SELECT dateadd('s', x::INT, '2024-01-01T00:00:00Z'::TIMESTAMP), 'A'
                    FROM long_sequence(1_000)
                    """);
            execute("""
                    INSERT INTO posting_gen_dir_select
                    SELECT dateadd('s', (10_000 + x)::INT, '2024-01-01T00:00:00Z'::TIMESTAMP), 'B'
                    FROM long_sequence(1_000)
                    """);

            final String query = "SELECT count() FROM posting_gen_dir_select WHERE sym = 'A'";
            assertQuery(query).noLeakCheck().noRandomAccess().expectSize().returns("count\n1000\n");
            engine.releaseInactive();

            final TableToken token = engine.verifyTableName("posting_gen_dir_select");
            final long partitionTimestamp;
            final long partitionNameTxn;
            try (TableReader reader = engine.getReader(token)) {
                Assert.assertEquals(1, reader.getTxFile().getPartitionCount());
                partitionTimestamp = reader.getTxFile().getPartitionTimestampByIndex(0);
                partitionNameTxn = reader.getTxFile().getPartitionNameTxn(0);
            }
            engine.releaseInactive();

            final FilesFacade ff = configuration.getFilesFacade();
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(token);
                TableUtils.setPathForNativePartition(
                        path, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTimestamp, partitionNameTxn);
                final LPSZ keyFile = PostingIndexUtils.keyFileName(path, "sym", COLUMN_NAME_TXN_NONE);
                Assert.assertEquals("two inserts must leave a two-gen head entry", 2, readHeadGenCount(ff, keyFile));

                final long headEntryOffset = readHeadEntryOffset(ff, keyFile);
                final long saved = stampGenDirTxnAtSeal(ff, keyFile, 1, 0L);
                try {
                    assertQuery(query).noLeakCheck().failsWith(
                            "posting index is corrupt [reason=gen-dir TXN_AT_SEAL not monotonic"
                                    + ", entryOffset=" + headEntryOffset + ", genCount=2, publishedGenCount=1]"
                    );
                } finally {
                    // Leave the index readable so the suite's teardown checks run on a
                    // healthy table.
                    stampGenDirTxnAtSeal(ff, keyFile, 1, saved);
                    engine.releaseInactive();
                }
            }
        });
    }

    private static void assertOnlyRow(String message, PostingIndexFwdReader reader, int key, long expectedRowId) {
        try (RowCursor cursor = reader.getCursor(key, 0, Long.MAX_VALUE)) {
            Assert.assertTrue(message, cursor.hasNext());
            Assert.assertEquals(message, expectedRowId, cursor.next());
            Assert.assertFalse(message, cursor.hasNext());
        }
    }

    private static MemoryCMARWImpl openKeyFile(FilesFacade ff, LPSZ keyFile) {
        final long fileSize = ff.length(keyFile);
        Assert.assertTrue("the .pk must exist, path=" + keyFile, fileSize > 0);
        return new MemoryCMARWImpl(ff, keyFile, ff.getPageSize(), fileSize, MemoryTag.MMAP_DEFAULT, /* opts */ 0);
    }

    private static long readChainSequence(FilesFacade ff, LPSZ keyFile) {
        final MemoryCMARWImpl mem = openKeyFile(ff, keyFile);
        try {
            final PostingIndexChainHeader.Snapshot header = new PostingIndexChainHeader.Snapshot();
            Assert.assertTrue("chain header must be readable", PostingIndexChainHeader.readUnderSeqlock(mem, header));
            return header.sequence;
        } finally {
            mem.close(false);
        }
    }

    private static long readGenDirTxnAtSeal(FilesFacade ff, LPSZ keyFile, int genIndex) {
        final MemoryCMARWImpl mem = openKeyFile(ff, keyFile);
        try {
            final long slot = resolveGenDirSlot(mem, genIndex);
            return mem.getLong(slot + PostingIndexUtils.GEN_DIR_OFFSET_TXN_AT_SEAL);
        } finally {
            // close(false): reading must not resize the file.
            mem.close(false);
        }
    }

    private static PostingIndexChainEntry.Snapshot readHeadEntry(MemoryCMARWImpl mem) {
        final PostingIndexChainHeader.Snapshot header = new PostingIndexChainHeader.Snapshot();
        Assert.assertTrue("chain header must be readable", PostingIndexChainHeader.readUnderSeqlock(mem, header));
        Assert.assertFalse("chain must carry a head entry", header.isEmpty());
        final PostingIndexChainEntry.Snapshot entry = new PostingIndexChainEntry.Snapshot();
        PostingIndexChainEntry.read(mem, header.headEntryOffset, entry);
        return entry;
    }

    private static long readHeadEntryOffset(FilesFacade ff, LPSZ keyFile) {
        final MemoryCMARWImpl mem = openKeyFile(ff, keyFile);
        try {
            return readHeadEntry(mem).offset;
        } finally {
            mem.close(false);
        }
    }

    private static int readHeadGenCount(FilesFacade ff, LPSZ keyFile) {
        final MemoryCMARWImpl mem = openKeyFile(ff, keyFile);
        try {
            return readHeadEntry(mem).genCount;
        } finally {
            mem.close(false);
        }
    }

    private static long resolveGenDirSlot(MemoryCMARWImpl mem, int genIndex) {
        final PostingIndexChainEntry.Snapshot entry = readHeadEntry(mem);
        Assert.assertTrue("gen " + genIndex + " must exist, genCount=" + entry.genCount, genIndex < entry.genCount);
        return PostingIndexChainEntry.resolveGenDirOffset(
                entry.offset, genIndex, entry.coveringFormat, entry.coverCount);
    }

    /**
     * Overwrites a head gen-dir slot's TXN_AT_SEAL in place and returns the value it
     * replaced. Only the slot changes, so the chain header seqlock stays stable and
     * the reader observes the same bytes on every retry.
     */
    private static long stampGenDirTxnAtSeal(FilesFacade ff, LPSZ keyFile, int genIndex, long txnAtSeal) {
        final MemoryCMARWImpl mem = openKeyFile(ff, keyFile);
        try {
            final long slot = resolveGenDirSlot(mem, genIndex);
            final long previous = mem.getLong(slot + PostingIndexUtils.GEN_DIR_OFFSET_TXN_AT_SEAL);
            mem.putLong(slot + PostingIndexUtils.GEN_DIR_OFFSET_TXN_AT_SEAL, txnAtSeal);
            return previous;
        } finally {
            mem.close(false);
        }
    }
}
