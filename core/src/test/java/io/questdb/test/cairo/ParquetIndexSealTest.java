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

package io.questdb.test.cairo;

import io.questdb.MessageBus;
import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.IndexMetaFileReader;
import io.questdb.cairo.ParquetMetaFileReader;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.PostingSealPurgeJob;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TxWriter;
import io.questdb.cairo.idx.IndexReader;
import io.questdb.mp.MPSequence;
import io.questdb.mp.RingQueue;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.Os;
import io.questdb.std.str.Path;
import io.questdb.tasks.PostingSealPurgeTask;
import io.questdb.std.FindVisitor;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;

/**
 * Covers the covering-index seal that emits {@code <col>.pidx.<indexTxn>.parquet}
 * plus its {@code _im} sidecar instead of the native {@code .pv} / {@code .pc*}
 * files, selected by {@code cairo.posting.index.parquet.partition.format=parquet}.
 * <p>
 * The indexed partition deliberately carries a sparse key set: its symbols are a
 * subset of the table's, so the key space bound the {@code _im} records is far
 * larger than the count of distinct keys present. Passing the distinct count
 * instead makes every key above the first report as absent with no error at all,
 * which is why the two are asserted separately here.
 */
public class ParquetIndexSealTest extends AbstractCairoTest {

    // Symbol 's15' is the highest key id the indexed partition carries, so the
    // key space bound is its index key (15 + 1 for the null slot) plus one.
    // Both columns are covered and the key is a single symbol, so this is served
    // from the posting index rather than by scanning the parquet. Written out
    // rather than built from TABLE_NAME and INDEXED_PARTITION, which are declared
    // after it.
    private static final String COVERED_QUERY =
            "select price, qty from t_pidx where sym = 's0' and ts in '2024-01-01'";
    private static final int EXPECTED_KEY_SPACE_SIZE = 17;
    private static final String INDEXED_PARTITION = "2024-01-01";
    // Index keys of 's0', 's7' and 's15', that is the symbol id plus one for the
    // null slot. These three of the table's sixteen symbols are all the indexed
    // partition holds, which is what makes its key ids sparse.
    // Row group first keys and row counts the packed fixture must produce. Read
    // the createPackedKeyTable javadoc for how each boundary arises; between them
    // the five groups exercise every branch of planRowGroups.
    private static final int NULLS_ROW_COUNT = 20_000;
    private static final String NULLS_TABLE_NAME = "t_pidx_nulls";
    private static final int[] PACKED_GROUP_FIRST_KEYS = {1, 201, 343, 344, 344};
    private static final long[] PACKED_GROUP_ROW_COUNTS = {100_000, 99_400, 700, 100_000, 50_000};
    // Key id of the packed fixture's row id, as SQL. Symbol ids are handed out in
    // first-appearance order and an index key is the symbol id plus one for the
    // null slot, so 'q0' is key 1, 'q342' is key 343 and 'q999' is key 344.
    private static final String PACKED_KEY_OF_ROW_ID =
            "case when row_id < 100000 then row_id / 500 + 1" +
                    " when row_id < 200100 then 201 + (row_id - 100000) / 700" +
                    " else 344 end";
    private static final int PACKED_ROW_COUNT = 350_100;
    private static final String PACKED_TABLE_NAME = "t_pidx_packed";
    private static final int[] PRESENT_KEYS = {1, 8, 16};
    private static final int SKEWED_ROW_COUNT = 300_000;
    // Writer index of the indexed SYMBOL column in every fixture: ts is 0, sym
    // is 1. The _pm covering-index entry is keyed by writer index, which is what
    // the _pm records as a column id.
    private static final int SYM_COLUMN_ID = 1;
    private static final String SWITCH_INDEXED_TABLE_NAME = "t_pidx_switch_idx";
    private static final String SWITCH_TABLE_NAME = "t_pidx_switch";
    private static final String TABLE_NAME = "t_pidx";

    @Test
    public void testSwitchToParquetCopiesTheParquetMetaRatherThanSharingItsInode() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_PARQUET_PARTITION_FORMAT, "parquet");
        assertMemoryLeak(() -> {
            execute("CREATE TABLE " + SWITCH_TABLE_NAME + " (ts TIMESTAMP, sym SYMBOL)" +
                    " TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO " + SWITCH_TABLE_NAME + " VALUES" +
                    " ('2024-01-01T00:00:00Z', 'a'), ('2024-01-02T00:00:00Z', 'b')");

            final TableToken token = engine.verifyTableName(SWITCH_TABLE_NAME);
            final long partitionTs;
            final long sourceNameTxn;
            // Pinned across the switch: this is the reader the shared-inode
            // hazard is about, and holding it also keeps the source directory
            // from being purged before the assertions below can look at it.
            try (TableReader pinned = engine.getReader(token);
                 TableWriter writer = engine.getWriter(token, "test")) {
                Assert.assertTrue(pinned.size() > 0);
                final TxWriter tx = writer.getTxWriter();
                partitionTs = tx.getPartitionTimestampByIndex(0);
                final int partitionIndex = tx.getPartitionIndex(partitionTs);
                sourceNameTxn = tx.getPartitionNameTxn(partitionIndex);
                // Empty stubs: the switch only checks that data.parquet and _pm
                // exist and then carries them into the new directory, which is
                // the decision this test is about. Nothing here reads them.
                try (Path p = new Path()) {
                    p.of(configuration.getDbRoot()).concat(token);
                    TableUtils.setPathForParquetPartition(p, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTs, sourceNameTxn);
                    Assert.assertTrue("could not create the data.parquet stub", configuration.getFilesFacade().touch(p.$()));
                    p.of(configuration.getDbRoot()).concat(token);
                    TableUtils.setPathForParquetPartitionMetadata(p, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTs, sourceNameTxn);
                    Assert.assertTrue("could not create the _pm stub", configuration.getFilesFacade().touch(p.$()));
                }
                tx.setPartitionParquetGenerated(partitionIndex, true);
                Assert.assertEquals(TableWriter.SWITCH_OK, writer.switchNativePartitionWithParquet(partitionTs, 0L));

                // data.parquet is immutable and is hard-linked; the _pm is not.
                // It grows: publishing a covering-index token appends a footer
                // and patches the header at offset 0. A shared inode would make
                // that append mutate the file the source directory also names,
                // and with data.parquet byte-identical both footers resolve for
                // the same parquet size, so the pinned reader would resolve the
                // new index_txn and name an _im that is not there.
                try (Path p = new Path()) {
                    p.of(configuration.getDbRoot()).concat(token);
                    TableUtils.setPathForParquetPartitionMetadata(p, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTs, sourceNameTxn);
                    Assert.assertEquals(
                            "the source _pm must not share its inode with the switched-in copy",
                            1L,
                            hardLinkCount(p.toString())
                    );
                    p.of(configuration.getDbRoot()).concat(token);
                    TableUtils.setPathForParquetPartition(p, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTs, sourceNameTxn);
                    Assert.assertEquals(
                            "data.parquet is immutable and must still be hard-linked, or this test would pass for the wrong reason",
                            2L,
                            hardLinkCount(p.toString())
                    );
                }
            }
        });
    }

    @Test
    public void testConvertBackToNativeRebuildsAWorkingIndex() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_PARQUET_PARTITION_FORMAT, "parquet");
        assertMemoryLeak(() -> {
            inputRoot = root;
            createIndexedSparseKeyTable();
            execute("ALTER TABLE " + TABLE_NAME + " CONVERT PARTITION TO NATIVE LIST '" + INDEXED_PARTITION + "'");
            drainWalQueue();
            engine.releaseInactive();

            // restoreIndexFilesAfterParquetToNative prefers hard-linking the
            // existing index files and falls back to rebuildColumnIndex when the
            // key file is absent. The parquet seal leaves a .pk behind, so the
            // link branch would fire and carry over a key file whose chain has
            // no visible generation; the fallback is forced instead, and it is
            // complete because rebuildColumnIndex calls configureCoveringIfNeeded.
            try (Path path = new Path()) {
                assertNoFileNamed(partitionPath(path), "sym.pidx.");
                assertAnyFileNamed(partitionPath(path), "sym.pc0.");
                Assert.assertTrue(
                        "the rebuilt native index must publish a sealed .pv generation",
                        hasSealedValueFile(partitionPath(path))
                );
            }
            // Served from the rebuilt covering index, and the same rows the
            // partition returned before it was ever converted.
            assertQuery("select count() from (" + COVERED_QUERY + ')')
                    .inferRandomAccess()
                    .expectSize()
                    .returns("count\n75000\n");
        });
    }

    @Test
    public void testConvertBackToNativeRebuildsAWorkingIndexAfterTheFormatIsFlippedBack() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_PARQUET_PARTITION_FORMAT, "parquet");
        assertMemoryLeak(() -> {
            inputRoot = root;
            createIndexedSparseKeyTable();

            // The direction a configuration-keyed gate gets wrong. The partition
            // is already sealed as parquet; flipping the property back says
            // nothing about that, but it makes a format-keyed test answer "not
            // sealed as parquet", so the link branch fires, carries over the .pk
            // the parquet seal left behind -- a key file whose chain has no
            // visible generation -- and the converted native partition answers
            // "no keys, no rows". No refusal can see it: the partition is native
            // by then, and the reader's probe returns early for those.
            node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_PARQUET_PARTITION_FORMAT, "native");
            engine.releaseInactive();
            execute("ALTER TABLE " + TABLE_NAME + " CONVERT PARTITION TO NATIVE LIST '" + INDEXED_PARTITION + "'");
            drainWalQueue();
            engine.releaseInactive();

            try (Path path = new Path()) {
                assertNoFileNamed(partitionPath(path), "sym.pidx.");
                assertAnyFileNamed(partitionPath(path), "sym.pc0.");
                Assert.assertTrue(
                        "the rebuilt native index must publish a sealed .pv generation",
                        hasSealedValueFile(partitionPath(path))
                );
            }
            assertQuery("select count() from (" + COVERED_QUERY + ')')
                    .inferRandomAccess()
                    .expectSize()
                    .returns("count\n75000\n");
        });
    }

    @Test
    public void testDropIndexRetiresTheTokenAndItsArtifacts() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_PARQUET_PARTITION_FORMAT, "parquet");
        assertMemoryLeak(() -> {
            inputRoot = root;
            createIndexedSparseKeyTable();
            final String indexMeta;
            final String indexParquet;
            try (Path path = new Path()) {
                indexMeta = onlyFileNamed(partitionPath(path), "sym.pidx.", "._im");
                indexParquet = onlyFileNamed(partitionPath(path), "sym.pidx.", ".parquet");
            }

            execute("ALTER TABLE " + TABLE_NAME + " ALTER COLUMN sym DROP INDEX");
            drainWalQueue();
            engine.releaseInactive();

            try (Path path = new Path()) {
                final FilesFacade ff = configuration.getFilesFacade();
                // The token must go. It is not enough that the index is gone from
                // the metadata: publishParquetIndexTokens copies forward every
                // entry no pass reseals, so a surviving entry is restated into
                // every later footer for this partition, outliving the index that
                // produced it for good.
                assertNoCoveringIndexToken(path, TABLE_NAME, currentPartitionNameTxn(TABLE_NAME));

                // And the pair it named must be reclaimed. The orphan sweep does
                // not cover it -- its _im is committed -- so the only route is the
                // reader-gated purge the drop hands it to.
                runPostingSealPurgeJob();
                Assert.assertFalse(
                        "DROP INDEX must retire the parquet index _im",
                        ff.exists(partitionPath(path).concat(indexMeta).$())
                );
                Assert.assertFalse(
                        "DROP INDEX must retire the index parquet",
                        ff.exists(partitionPath(path).concat(indexParquet).$())
                );
            }
        });
    }

    /**
     * I1: the {@code _pm} token retirement must not become durable before the
     * drop that authorises it is committed.
     * <p>
     * The {@code _pm} header patch is durable the instant it lands, so with the
     * retirement running first, a crash in that window leaves a column metadata
     * still calls POSTING-indexed, on a partition that is still parquet, with no
     * token in the {@code _pm}. {@code checkPostingIndexIsReadable} answers a
     * missing token by returning, which hands the query to a native chain that
     * has no visible generation for a parquet-sealed column, and the query
     * silently answers nothing. That is the silent empty result this whole task
     * exists to close, reached through a crash rather than a configuration flip.
     * <p>
     * The crash is simulated at exactly that point: the header patch is
     * performed for real and the write then throws, so the on-disk state is
     * byte-for-byte what a power loss one instruction later would leave.
     * Everything up to it is production -- a real {@code ALTER ... DROP INDEX}
     * through the WAL apply path driving the real retirement.
     * <p>
     * With the retirement last, the same crash leaves the mirror state: the drop
     * is committed, the column is not indexed, the query is a plain scan and
     * answers correctly. A stale token can survive it, which is a leak rather
     * than a wrong answer, and the next publish for the partition drops it.
     */
    @Test
    public void testDropIndexDoesNotRetireTheTokenBeforeTheDropIsCommitted() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_PARQUET_PARTITION_FORMAT, "parquet");
        final boolean[] armed = {false};
        final boolean[] fired = {false};
        final long[] pmFd = {-1};
        ff = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                final long fd = super.openRW(name, opts);
                if (armed[0] && fd > -1 && Utf8s.endsWithAscii(name, TableUtils.PARQUET_METADATA_FILE_NAME)) {
                    pmFd[0] = fd;
                }
                return fd;
            }

            @Override
            public long write(long fd, long address, long len, long offset) {
                final long written = super.write(fd, address, len, offset);
                if (armed[0] && fd == pmFd[0] && offset == 0 && len == Long.BYTES) {
                    // The patch is on disk. Stop the world here, exactly as a
                    // power loss one instruction later would.
                    armed[0] = false;
                    fired[0] = true;
                    throw CairoException.critical(0).put("simulated crash right after the _pm header patch");
                }
                return written;
            }
        };
        assertMemoryLeak(ff, () -> {
            inputRoot = root;
            createIndexedSparseKeyTable();
            // The count cannot be taken before the drop: while the column is
            // POSTING-indexed on a parquet-sealed partition the reader refuses
            // the read outright. 75000 is the fixture's own arithmetic --
            // SKEWED_ROW_COUNT / 4 rows carry 's0' in the indexed partition --
            // and it is the number a silent empty result turns into 0.
            armed[0] = true;
            try {
                execute("ALTER TABLE " + TABLE_NAME + " ALTER COLUMN sym DROP INDEX");
                drainWalQueue();
            } finally {
                armed[0] = false;
            }
            Assert.assertTrue(
                    "the crash injection never fired, so the test proves nothing about the ordering",
                    fired[0]
            );
            engine.releaseInactive();

            Assert.assertEquals(
                    "a crash between the _pm retirement and the drop's commit must not silently empty the column",
                    SKEWED_ROW_COUNT / 4,
                    countIndexedRows()
            );
        });
    }

    /**
     * I2: a crash between an {@code _im} commit and the {@code _pm} header patch
     * strands the whole seal batch, and nothing else can ever reclaim it.
     * <p>
     * The old orphan sweep keyed on {@code IM_FILE_SIZE == 0}, so a
     * committed-but-unreferenced pair never matched -- and no later publish
     * supersedes it either, because the committed footer still names the
     * pre-batch {@code index_txn}. The sweep's own argument for removing an
     * uncommitted pair on sight ("no footer, live or superseded, names it, so no
     * reader can reach it") applies word for word to this state, and the
     * predicate contradicted it.
     * <p>
     * Simulated at exactly the boundary: the {@code pidx.parquet} and the
     * {@code _im} are written and committed for real, then the {@code _pm}
     * header patch is refused. The alter fails, the batch is stranded, and the
     * fixture asserts the {@code _im} really is committed -- without that the
     * old predicate would match and the test would prove nothing.
     */
    @Test
    public void testASealStrandedBeforeItsTokenIsPublishedIsSwept() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_PARQUET_PARTITION_FORMAT, "parquet");
        final boolean[] armed = {false};
        final boolean[] fired = {false};
        final long[] pmFd = {-1};
        // Set once the stranded pair's name is known; the counter is the test's
        // observable, because the file state afterwards is identical whether the
        // sweep ran or not -- see the comment at the resume below.
        final String[] watchedParquet = {null};
        final int[] watchedUnlinks = {0};
        ff = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                final long fd = super.openRW(name, opts);
                if (armed[0] && fd > -1 && Utf8s.endsWithAscii(name, TableUtils.PARQUET_METADATA_FILE_NAME)) {
                    pmFd[0] = fd;
                }
                return fd;
            }

            @Override
            public boolean removeQuiet(LPSZ name) {
                if (watchedParquet[0] != null && name != null && Utf8s.endsWithAscii(name, watchedParquet[0])) {
                    watchedUnlinks[0]++;
                }
                return super.removeQuiet(name);
            }

            @Override
            public long write(long fd, long address, long len, long offset) {
                if (armed[0] && fd == pmFd[0] && offset == 0 && len == Long.BYTES) {
                    // Refuse the header patch. The appended footer is already on
                    // disk as an invisible dead tail; the _im beside the pidx is
                    // committed. That is the stranded state.
                    armed[0] = false;
                    fired[0] = true;
                    return 0;
                }
                return super.write(fd, address, len, offset);
            }
        };
        assertMemoryLeak(ff, () -> {
            inputRoot = root;
            createSparseKeyTable();
            execute("ALTER TABLE " + TABLE_NAME + " CONVERT PARTITION TO PARQUET LIST '" + INDEXED_PARTITION + "'");
            drainWalQueue();
            engine.releaseInactive();

            armed[0] = true;
            try {
                execute("ALTER TABLE " + TABLE_NAME + " ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, qty)");
                drainWalQueue();
            } finally {
                armed[0] = false;
            }
            Assert.assertTrue("the _pm header patch was never refused, so nothing is stranded", fired[0]);
            engine.releaseInactive();

            final String strandedMeta;
            try (Path path = new Path()) {
                strandedMeta = onlyFileNamed(partitionPath(path), "sym.pidx.", "._im");
                // The half that makes this a different leak from the one the old
                // predicate covered: the _im committed, so IM_FILE_SIZE is not
                // zero and the old sweep would pass straight over it.
                Assert.assertTrue(
                        "the stranded _im must be committed, or this is the already-covered case",
                        imFileSizeField(partitionPath(path).concat(strandedMeta).$()) > 0
                );
                assertNoCoveringIndexToken(path, TABLE_NAME, currentPartitionNameTxn(TABLE_NAME));
            }

            // Resume the suspended apply so the retried ADD INDEX opens a fresh
            // seal batch on that partition, which is what runs the sweep.
            //
            // The retry names its artifacts with the same index txn -- the failed
            // batch never committed, so the writer's txn did not move -- and so it
            // overwrites the stranded files whether they were swept first or not.
            // The file state afterwards is therefore identical under both
            // behaviours and cannot be the assertion. The count of unlinks of
            // that exact name can: the seal always removes the file it is about
            // to write, so one unlink is the retry alone and two means the sweep
            // reclaimed the stranded pair at the batch head first.
            watchedParquet[0] = strandedMeta.substring(0, strandedMeta.length() - "._im".length()) + ".parquet";
            watchedUnlinks[0] = 0;
            execute("ALTER TABLE " + TABLE_NAME + " RESUME WAL");
            drainWalQueue();
            watchedParquet[0] = null;
            Assert.assertEquals(
                    "the stranded pair must be unlinked twice -- once by the sweep at the seal batch's head"
                            + " and once by the retried seal reusing the name; one unlink means the sweep"
                            + " passed over it [stranded=" + strandedMeta + ']',
                    2,
                    watchedUnlinks[0]
            );
            engine.releaseInactive();

            try (Path path = new Path()) {
                final File[] metas = new File(partitionPath(path).toString())
                        .listFiles((_, name) -> name.startsWith("sym.pidx.") && name.endsWith("._im"));
                Assert.assertNotNull(metas);
                Assert.assertEquals(
                        "the retried seal must leave exactly one pair behind",
                        1,
                        metas.length
                );
                assertCoveringIndexToken(
                        path,
                        TABLE_NAME,
                        SYM_COLUMN_ID,
                        Numbers.parseLong(metas[0].getName().substring(
                                "sym.pidx.".length(), metas[0].getName().length() - "._im".length())),
                        imFileSizeField(partitionPath(path).concat(metas[0].getName()).$())
                );
            }
        });
    }

    /**
     * I4: the orphan sweep is a directory listing plus a read per artifact it
     * finds, and it sits at the head of every seal batch -- which is the
     * per-commit O3 path. Under the default {@code native} format no
     * parquet-form seal ever runs, so every one of those listings is guaranteed
     * to find nothing.
     * <p>
     * Counted rather than timed: the cost is one {@code iterateDir} per
     * O3-touched parquet partition per commit, and the count is what regresses.
     */
    @Test
    public void testTheOrphanSweepDoesNotRunOnTheDefaultFormat() throws Exception {
        final int[] partitionListings = {0};
        final boolean[] counting = {false};
        ff = new TestFilesFacadeImpl() {
            @Override
            public void iterateDir(LPSZ path, FindVisitor func) {
                if (counting[0] && Utf8s.containsAscii(path, INDEXED_PARTITION)) {
                    partitionListings[0]++;
                }
                super.iterateDir(path, func);
            }
        };
        assertMemoryLeak(ff, () -> {
            inputRoot = root;
            createSparseKeyTable();
            execute("ALTER TABLE " + TABLE_NAME + " ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, qty)");
            drainWalQueue();
            execute("ALTER TABLE " + TABLE_NAME + " CONVERT PARTITION TO PARQUET LIST '" + INDEXED_PARTITION + "'");
            drainWalQueue();
            engine.releaseInactive();

            counting[0] = true;
            try {
                for (int i = 0; i < 8; i++) {
                    execute("INSERT INTO " + TABLE_NAME + " VALUES ('2024-01-01T00:00:00.0000" + (10 + i)
                            + "Z', 's0', 1.0, 1)");
                    drainWalQueue();
                }
            } finally {
                counting[0] = false;
            }

            Assert.assertEquals(
                    "a table on the default index format must not list the partition directory on the"
                            + " per-commit seal path",
                    0,
                    partitionListings[0]
            );
        });
    }

    /**
     * I3: the covering-index token publish bumps the partition table version so
     * a reloading reader drops its {@code _pm} mapping, and that bump also fires
     * {@code checkSchedulePurgeO3Partitions} on every reader release afterwards.
     * <p>
     * That task means "the partition list moved on while I held this txn, so
     * directories I pinned may be removable". A token publish creates no
     * removable directory -- same partitions, same name txns -- so every task it
     * schedules is a no-op, and under queue pressure it also logs "could not
     * queue purge partition task, queue is full" for work that would find
     * nothing.
     * <p>
     * The reconciliation the bump exists to force is asserted separately by
     * {@code testAReloadingReaderDropsItsParquetMetaMappingAcrossATokenPublish};
     * this pins the other half, that the scheduling stops.
     */
    @Test
    public void testATokenPublishSchedulesNoO3PartitionPurge() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_PARQUET_PARTITION_FORMAT, "parquet");
        assertMemoryLeak(() -> {
            inputRoot = root;
            createIndexedSparseKeyTable();

            final MessageBus bus = engine.getMessageBus();
            drainO3PurgeDiscoveryQueue(bus);

            final TableToken token = engine.verifyTableName(TABLE_NAME);
            // DROP INDEX / ADD INDEX rather than an O3 insert: both publish a
            // token into the partition's _pm and neither touches a single row, so
            // the token publish is the ONLY thing that could schedule a purge. An
            // O3 insert would not isolate it -- the O3 commit is itself a
            // partition change and schedules purges of its own, which is exactly
            // the work this task is for.
            //
            // The reader must be open across the publish and released after it,
            // or checkSchedulePurgeO3Partitions never runs.
            for (int i = 0; i < 4; i++) {
                try (TableReader ignored = engine.getReader(token)) {
                    execute("ALTER TABLE " + TABLE_NAME + " ALTER COLUMN sym DROP INDEX");
                    drainWalQueue();
                }
                engine.releaseInactive();
                try (TableReader ignored = engine.getReader(token)) {
                    execute("ALTER TABLE " + TABLE_NAME
                            + " ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, qty)");
                    drainWalQueue();
                }
                engine.releaseInactive();
            }

            Assert.assertEquals(
                    "a token publish changes no partition directory, so it must schedule no O3 partition purge",
                    0,
                    drainO3PurgeDiscoveryQueue(bus)
            );
        });
    }

    /**
     * I5: the opposite arm of
     * {@link #testTheRefusalProbeResolvesThePinnedReadersOwnIndexTxn}. That one
     * proves a reader which never reloads keeps its own snapshot's answer; this
     * one proves a reader which DOES reload stops giving the old one.
     * <p>
     * The two together are what makes the superseded artifacts' purge window
     * sound. The window is expressed in table txns, so it can only cover a
     * reader that is still pinned below the publish. A token publish moves
     * nothing else in {@code _txn} -- same partition name txn, same row count,
     * same {@code data.parquet} size, untouched column version -- so without a
     * restamp {@code reconcileOpenPartitions} takes its fast path and a reader
     * can advance its txn past the publish while still holding the mapping it
     * took at partition-open time. It would then resolve the old
     * {@code index_txn} from outside the window that protects the files that
     * token names.
     * <p>
     * Asserted through both the reader's own probe -- which is what a query
     * reaches -- and the mapping size directly.
     * <p>
     * <b>This test does not discriminate the bump.</b> Removing
     * {@code txWriter.bumpPartitionTableVersion()} from
     * {@code publishParquetIndexTokens} leaves it green, and that is not a gap
     * in the fixture: no production path reached from here publishes a token
     * without also moving something the reader already reconciles on. Measured
     * over six consecutive O3 token publishes into a non-last parquet partition,
     * the {@code _pm} was re-mapped every time both with and without the bump,
     * in the five of six where the partition name txn did not move either. The
     * bump is therefore insurance rather than the operative signal today, and it
     * is kept because the invariant it protects -- a reader must not advance its
     * txn past a publish while holding the pre-publish mapping -- is what the
     * purge window's soundness rests on. What this test pins is the invariant,
     * so a future change that makes the bump operative cannot silently lose it.
     */
    @Test
    public void testAReloadingReaderDropsItsParquetMetaMappingAcrossATokenPublish() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_PARQUET_PARTITION_FORMAT, "parquet");
        assertMemoryLeak(() -> {
            inputRoot = root;
            createIndexedSparseKeyTable();
            final long firstIndexTxn;
            try (Path path = new Path()) {
                final String firstMeta = onlyFileNamed(partitionPath(path), "sym.pidx.", "._im");
                firstIndexTxn = Numbers.parseLong(
                        firstMeta.substring("sym.pidx.".length(), firstMeta.length() - "._im".length())
                );
            }

            try (TableReader reader = engine.getReader(engine.verifyTableName(TABLE_NAME))) {
                // Take the mapping first, at the size this snapshot's header
                // names. The second seal appends past that tail.
                Assert.assertTrue(reader.openPartition(0) > 0);
                final long mappedSize = reader.getParquetMetadataSize(0);
                Assert.assertTrue("the reader must hold a _pm mapping", mappedSize > 0);
                final int symColumnIndex = reader.getMetadata().getColumnIndex("sym");

                execute("ALTER TABLE " + TABLE_NAME + " ALTER COLUMN sym DROP INDEX");
                drainWalQueue();
                execute("ALTER TABLE " + TABLE_NAME + " ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, qty)");
                drainWalQueue();

                final long secondIndexTxn;
                try (Path path = new Path()) {
                    final String secondMeta = newestFileNamed(
                            partitionPath(path), "sym.pidx.", "._im", "sym.pidx." + firstIndexTxn + "._im");
                    secondIndexTxn = Numbers.parseLong(
                            secondMeta.substring("sym.pidx.".length(), secondMeta.length() - "._im".length())
                    );
                }
                Assert.assertTrue(
                        "the fixture must actually supersede the first token, or the two answers coincide"
                                + " and this test cannot fail [first=" + firstIndexTxn + ", second=" + secondIndexTxn + ']',
                        secondIndexTxn > firstIndexTxn
                );

                // Advance this reader's txn past the publish. From here on it is
                // outside the purge window that protects the first token's
                // artifacts, so it must no longer be able to name them.
                Assert.assertTrue("the reader must have something to reload", reader.reload());
                Assert.assertTrue(reader.openPartition(0) > 0);
                Assert.assertNotEquals(
                        "the reloading reader must re-map the _pm, not keep the mapping it took"
                                + " at partition-open time",
                        mappedSize,
                        reader.getParquetMetadataSize(0)
                );

                try {
                    reader.getIndexReader(0, symColumnIndex, IndexReader.DIR_FORWARD);
                    Assert.fail("the parquet-form posting index must be refused");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "indexTxn=" + secondIndexTxn);
                }
            }
        });
    }

    /**
     * I8: a token publish changes the partition's directory -- it gains a
     * {@code <col>.pidx} pair and its {@code _pm} grows a footer -- while
     * leaving every field of the partition's own {@code _txn} record alone:
     * same name txn, same row count, same {@code data.parquet} size, so the
     * offset-3 value word does not move either.
     * <p>
     * That is precisely the state {@code squashSplitPartitions} documents the
     * squash counter for -- "even when the partition has the same version and
     * row count it will be included in a backup" -- and without stamping it a
     * per-partition consumer, incremental backup in particular, cannot tell
     * that the directory changed at all.
     * <p>
     * The test asserts both halves: the fields that must not move, so the
     * premise is real rather than assumed, and the counter that must.
     */
    @Test
    public void testATokenPublishRestampsThePartitionsChangeToken() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_PARQUET_PARTITION_FORMAT, "parquet");
        assertMemoryLeak(() -> {
            inputRoot = root;
            createIndexedSparseKeyTable();

            final TableToken token = engine.verifyTableName(TABLE_NAME);
            final long nameTxnBefore;
            final long rowCountBefore;
            final long parquetSizeBefore;
            final int squashCountBefore;
            try (TableReader reader = engine.getReader(token)) {
                nameTxnBefore = reader.getTxFile().getPartitionNameTxn(0);
                rowCountBefore = reader.getTxFile().getPartitionSize(0);
                parquetSizeBefore = reader.getTxFile().getPartitionParquetFileSize(0);
                squashCountBefore = reader.getTxFile().getPartitionSquashCount(0);
            }

            // DROP + ADD INDEX publishes into the _pm twice and writes no row, so
            // nothing about the partition's data changes.
            execute("ALTER TABLE " + TABLE_NAME + " ALTER COLUMN sym DROP INDEX");
            drainWalQueue();
            execute("ALTER TABLE " + TABLE_NAME + " ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, qty)");
            drainWalQueue();
            engine.releaseInactive();

            try (TableReader reader = engine.getReader(token)) {
                Assert.assertEquals("premise: the partition name txn must not move",
                        nameTxnBefore, reader.getTxFile().getPartitionNameTxn(0));
                Assert.assertEquals("premise: the partition row count must not move",
                        rowCountBefore, reader.getTxFile().getPartitionSize(0));
                Assert.assertEquals("premise: the data.parquet size must not move",
                        parquetSizeBefore, reader.getTxFile().getPartitionParquetFileSize(0));
                Assert.assertNotEquals(
                        "a token publish must restamp the partition's own change token, or a per-partition"
                                + " consumer cannot see that the directory changed",
                        squashCountBefore,
                        reader.getTxFile().getPartitionSquashCount(0)
                );
            }
        });
    }

    @Test
    public void testANativePurgeDoesNotUnlinkALiveParquetIndexOfTheSameNumber() throws Exception {
        assertMemoryLeak(() -> {
            inputRoot = root;
            // A directory carrying BOTH index forms at once, which is what makes
            // the two numbering spaces collide: index under the native format,
            // CONVERT PARTITION TO PARQUET (which links the native sealed
            // sidecars in), flip the format property, then write O3 into that
            // partition so the covering reseal writes the parquet form in place.
            // The result is sym.pv.<generation> and sym.pidx.<indexTxn> side by
            // side -- the first counted by PostingIndexChainWriter's per-column
            // genCounter, the second by the table txn.
            createSparseKeyTable();
            execute("ALTER TABLE " + TABLE_NAME + " ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, qty)");
            drainWalQueue();
            execute("ALTER TABLE " + TABLE_NAME + " CONVERT PARTITION TO PARQUET LIST '" + INDEXED_PARTITION + "'");
            drainWalQueue();
            node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_PARQUET_PARTITION_FORMAT, "parquet");
            engine.releaseInactive();
            execute("INSERT INTO " + TABLE_NAME + " VALUES ('2024-01-01T00:00:00.000005Z', 's0', 1.0, 1)");
            drainWalQueue();
            engine.releaseInactive();

            try (Path path = new Path()) {
                final FilesFacade ff = configuration.getFilesFacade();
                final String indexMeta = onlyFileNamed(partitionPath(path), "sym.pidx.", "._im");
                final String indexParquet = onlyFileNamed(partitionPath(path), "sym.pidx.", ".parquet");
                final long liveIndexTxn = Numbers.parseLong(
                        indexMeta.substring("sym.pidx.".length(), indexMeta.length() - "._im".length())
                );
                Assert.assertTrue(
                        "the fixture must leave both forms in one directory, or the collision cannot arise",
                        hasSealedValueFile(partitionPath(path))
                );

                // A routine native purge for a chain generation that happens to
                // carry the same number. PostingSealPurgeTask's sealTxn alone
                // cannot tell the two namespaces apart, so the task carries the
                // artifact form it was produced for and the operator acts only on
                // that form.
                //
                // LIMITATION, stated rather than implied: the task is published
                // straight into the ring queue. The numeric collision itself
                // cannot be forced through production SQL -- a native chain
                // generation is counted by PostingIndexChainWriter's per-column
                // genCounter and a covering index txn is a table txn, and nothing
                // in the SQL surface steers either to a chosen value -- so the
                // arithmetic coincidence has to be arranged. What the fixture
                // above DOES establish through production SQL is the part that
                // could otherwise be doubted: that one partition directory really
                // does carry both forms for one column at once, which is what
                // makes the coincidence reachable at all.
                final TableToken token = engine.verifyTableName(TABLE_NAME);
                final long partitionTs;
                final long partitionNameTxn;
                try (TableReader reader = engine.getReader(token)) {
                    partitionTs = reader.getTxFile().getPartitionTimestampByIndex(0);
                    partitionNameTxn = reader.getTxFile().getPartitionNameTxn(0);
                }
                final MessageBus bus = engine.getMessageBus();
                final MPSequence pubSeq = bus.getPostingSealPurgePubSeq();
                final RingQueue<PostingSealPurgeTask> queue = bus.getPostingSealPurgeQueue();
                long cursor;
                while ((cursor = pubSeq.next()) == -2) {
                    Os.pause();
                }
                Assert.assertTrue("purge queue must accept the task", cursor >= 0);
                try {
                    queue.get(cursor).of(
                            token, "sym", TableUtils.COLUMN_NAME_TXN_NONE, liveIndexTxn,
                            PostingSealPurgeTask.ARTIFACT_FORM_NATIVE,
                            partitionTs, partitionNameTxn, PartitionBy.DAY, ColumnType.TIMESTAMP,
                            0L, committedTxn(TABLE_NAME)
                    );
                } finally {
                    pubSeq.done(cursor);
                }
                runPostingSealPurgeJob();

                Assert.assertTrue(
                        "a native purge numbered like a live parquet index txn must not unlink its _im",
                        ff.exists(partitionPath(path).concat(indexMeta).$())
                );
                Assert.assertTrue(
                        "a native purge numbered like a live parquet index txn must not unlink its parquet",
                        ff.exists(partitionPath(path).concat(indexParquet).$())
                );
            }
        });
    }

    @Test
    public void testSwitchToParquetResealsTheIndexIntoTheNewDirectory() throws Exception {
        assertMemoryLeak(() -> {
            // switchNativePartitionWithParquet swaps a parquet generated beside a
            // native partition in as the partition itself. Under the parquet index
            // format linkPartitionIndexFiles deliberately carries no POSTING
            // sidecar over, so resealParquetIndexesAfterSwitch is the only thing
            // that puts an index in the new directory: without it the switch
            // publishes a parquet partition whose indexed column has no index at
            // all. The existing switch test cannot see any of this -- its table is
            // unindexed and its parquet is an empty stub.
            execute("CREATE TABLE " + SWITCH_INDEXED_TABLE_NAME + " (" +
                    "ts TIMESTAMP, sym SYMBOL, price DOUBLE, qty LONG" +
                    ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO " + SWITCH_INDEXED_TABLE_NAME + " SELECT" +
                    " dateadd('u', x::INT, '" + INDEXED_PARTITION + "T00:00:00Z'::TIMESTAMP)," +
                    " CASE WHEN x % 4 = 0 THEN 's0' WHEN x % 4 = 1 THEN 's7' ELSE 's15' END," +
                    " x::DOUBLE," +
                    " x" +
                    " FROM long_sequence(20000)");
            // A later partition, so the one under test is not the active one: a
            // non-WAL table cannot hold a parquet active partition, and CONVERT
            // silently leaves it native.
            execute("INSERT INTO " + SWITCH_INDEXED_TABLE_NAME + " VALUES" +
                    " ('2024-01-02T00:00:00Z', 's0', 1.0, 1)");
            execute("ALTER TABLE " + SWITCH_INDEXED_TABLE_NAME + " ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, qty)");

            // A real data.parquet + _pm for this partition, produced the only way
            // a test can produce one, then put back beside the native partition --
            // which is the state the switch expects to find and what a background
            // parquet generator would leave.
            final FilesFacade ff = configuration.getFilesFacade();
            final String[] carried = {"data.parquet", TableUtils.PARQUET_METADATA_FILE_NAME};
            execute("ALTER TABLE " + SWITCH_INDEXED_TABLE_NAME + " CONVERT PARTITION TO PARQUET LIST '" + INDEXED_PARTITION + "'");
            final long parquetNameTxn = currentPartitionNameTxn(SWITCH_INDEXED_TABLE_NAME);
            // Staged out before the convert back, which removes the parquet
            // directory as soon as it commits.
            try (Path src = new Path(); Path stage = new Path()) {
                for (String name : carried) {
                    partitionPathAt(src, SWITCH_INDEXED_TABLE_NAME, parquetNameTxn).concat(name).$();
                    stage.of(root).concat("stage_" + name).$();
                    Assert.assertTrue("the generated " + name + " must exist", ff.exists(src.$()));
                    Assert.assertTrue("could not stage " + name, ff.copy(src.$(), stage.$()) >= 0);
                }
            }
            execute("ALTER TABLE " + SWITCH_INDEXED_TABLE_NAME + " CONVERT PARTITION TO NATIVE LIST '" + INDEXED_PARTITION + "'");
            final long nativeNameTxn = currentPartitionNameTxn(SWITCH_INDEXED_TABLE_NAME);
            long generatedParquetSize = -1;
            try (Path stage = new Path(); Path dst = new Path()) {
                for (String name : carried) {
                    stage.of(root).concat("stage_" + name).$();
                    partitionPathAt(dst, SWITCH_INDEXED_TABLE_NAME, nativeNameTxn).concat(name).$();
                    Assert.assertTrue("could not place " + name, ff.copy(stage.$(), dst.$()) >= 0);
                    if ("data.parquet".equals(name)) {
                        generatedParquetSize = ff.length(dst.$());
                    }
                    ff.removeQuiet(stage.$());
                }
            }
            Assert.assertTrue("the staged parquet must have a size", generatedParquetSize > 0);

            node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_PARQUET_PARTITION_FORMAT, "parquet");
            engine.releaseInactive();

            final TableToken token = engine.verifyTableName(SWITCH_INDEXED_TABLE_NAME);
            final long switchedNameTxn;
            try (TableWriter writer = engine.getWriter(token, "test")) {
                final TxWriter tx = writer.getTxWriter();
                final long partitionTs = tx.getPartitionTimestampByIndex(0);
                tx.setPartitionParquetGenerated(tx.getPartitionIndex(partitionTs), true);
                Assert.assertEquals(TableWriter.SWITCH_OK, writer.switchNativePartitionWithParquet(partitionTs, generatedParquetSize));
                switchedNameTxn = tx.getPartitionNameTxn(tx.getPartitionIndex(partitionTs));
            }
            engine.releaseInactive();

            try (Path path = new Path()) {
                final Path partition = partitionPathAt(path, SWITCH_INDEXED_TABLE_NAME, switchedNameTxn);
                // Not the native sidecars: the switch must not have carried them
                // over, and the seal must have written the parquet form instead.
                assertNoFileNamed(partition, "sym.pc0.");
                Assert.assertFalse(
                        "the switch must publish no sealed .pv generation under the parquet format",
                        hasSealedValueFile(partitionPathAt(path, SWITCH_INDEXED_TABLE_NAME, switchedNameTxn))
                );
                final String indexMeta = onlyFileNamed(
                        partitionPathAt(path, SWITCH_INDEXED_TABLE_NAME, switchedNameTxn), "sym.pidx.", "._im");
                onlyFileNamed(partitionPathAt(path, SWITCH_INDEXED_TABLE_NAME, switchedNameTxn), "sym.pidx.", ".parquet");
                final long indexTxn = Numbers.parseLong(
                        indexMeta.substring("sym.pidx.".length(), indexMeta.length() - "._im".length())
                );
                final long imFileSize = imFileSizeField(
                        partitionPathAt(path, SWITCH_INDEXED_TABLE_NAME, switchedNameTxn).concat(indexMeta).$());
                // And the new directory's own _pm names it, so the artifacts are
                // referenced rather than merely present.
                assertCoveringIndexToken(path, SWITCH_INDEXED_TABLE_NAME, SYM_COLUMN_ID, indexTxn, imFileSize);
            }
        });
    }

    /**
     * A second seal replaces the token, and the pair the previous token named
     * must outlive every reader that can still resolve it.
     * <p>
     * Driven through DROP INDEX + ADD INDEX. That is deliberate and not an
     * accident of convenience: of the two supersession branches in
     * {@code publishParquetIndexTokens}, only the drop branch is reachable
     * through production today. The reseal branch needs the partition's
     * committed footer to still name the previous {@code index_txn} when the
     * reseal publishes, and the only production trigger for a reseal --
     * an out-of-order write into a sealed parquet partition -- goes through O3
     * update mode, which rewrites the footer with an empty covering-index
     * section first, so the supersession loop finds nothing to supersede. That
     * is the deferred O3 update-mode leak, and until it is fixed the reseal
     * branch cannot be reached with a live prior token. Verified by driving a
     * single out-of-order INSERT here: it leaves both pairs on disk and queues
     * no purge task at all.
     * <p>
     * What this pins instead of the branch is the bound itself, read back from
     * the purge log the job persists. {@code isRangeAvailable(from, to)} blocks
     * only on readers inside the half-open {@code [from, to)}, so a window that
     * stopped at the writer's committed txn would exclude every reader that
     * opened before the retirement -- precisely the population at risk. The
     * assertion is that the persisted upper bound is the txn the retirement
     * commits at, not the one before it, which is the difference between the
     * two candidate expressions and does not depend on how many txns the drop
     * happens to take.
     */
    @Test
    public void testSecondSealSupersedesTheFirst() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_PARQUET_PARTITION_FORMAT, "parquet");
        assertMemoryLeak(() -> {
            inputRoot = root;
            createIndexedSparseKeyTable();
            final String firstMeta;
            final String firstParquet;
            final long firstIndexTxn;
            try (Path path = new Path()) {
                firstMeta = onlyFileNamed(partitionPath(path), "sym.pidx.", "._im");
                firstParquet = onlyFileNamed(partitionPath(path), "sym.pidx.", ".parquet");
                firstIndexTxn = Numbers.parseLong(
                        firstMeta.substring("sym.pidx.".length(), firstMeta.length() - "._im".length())
                );
            }

            try (Path path = new Path()) {
                final FilesFacade ff = configuration.getFilesFacade();
                // The reader the purge window is about, opened before the token
                // that names the first pair stops being the committed one and
                // held across the whole reindex. Its _pm mapping is the
                // pre-retirement one, so it still resolves the first index_txn
                // and the files that token names must outlive the retirement for
                // as long as it lives.
                try (TableReader pinned = engine.getReader(engine.verifyTableName(TABLE_NAME))) {
                    Assert.assertTrue("the pinned reader must hold a txn", pinned.getTxn() >= 0);

                    execute("ALTER TABLE " + TABLE_NAME + " ALTER COLUMN sym DROP INDEX");
                    drainWalQueue();
                    final long retirementTxn = committedTxn(TABLE_NAME);

                    execute("ALTER TABLE " + TABLE_NAME + " ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, qty)");
                    drainWalQueue();
                    engine.releaseInactive();

                    final String secondMeta = newestFileNamed(partitionPath(path), "sym.pidx.", "._im", firstMeta);
                    final long secondIndexTxn = Numbers.parseLong(
                            secondMeta.substring("sym.pidx.".length(), secondMeta.length() - "._im".length())
                    );
                    Assert.assertTrue(
                            "the second seal must publish a later index txn [first=" + firstIndexTxn
                                    + ", second=" + secondIndexTxn + ']',
                            secondIndexTxn > firstIndexTxn
                    );
                    final long imFileSize = imFileSizeField(partitionPath(path).concat(secondMeta).$());
                    assertCoveringIndexToken(path, TABLE_NAME, SYM_COLUMN_ID, secondIndexTxn, imFileSize);

                    // Replacing the token unlinks nothing by itself.
                    Assert.assertTrue(
                            "the superseded _im must outlive the supersession",
                            ff.exists(partitionPath(path).concat(firstMeta).$())
                    );
                    Assert.assertTrue(
                            "the superseded index parquet must outlive the supersession",
                            ff.exists(partitionPath(path).concat(firstParquet).$())
                    );

                    // Nor does the purge job, while that reader is alive.
                    runPostingSealPurgeJob();
                    Assert.assertTrue(
                            "the purge must not unlink the superseded _im while a reader pinned at the"
                                    + " pre-retirement txn still resolves the token that names it",
                            ff.exists(partitionPath(path).concat(firstMeta).$())
                    );
                    Assert.assertTrue(
                            "the purge must not unlink the superseded index parquet while a reader pinned"
                                    + " at the pre-retirement txn still resolves the token that names it",
                            ff.exists(partitionPath(path).concat(firstParquet).$())
                    );

                    // The bound itself, as the job persisted it. A window that
                    // stopped one txn short would exclude every reader that
                    // opened before the retirement.
                    Assert.assertEquals(
                            "the retirement's purge window must reach the txn the retirement commits at,"
                                    + " not the one before it",
                            retirementTxn,
                            persistedPurgeWindowUpperBound(firstIndexTxn)
                    );
                }

                // Reader gone: the same job now retires the pair, from the entry
                // the drop point handed it -- same decision point, so the pointer
                // and the files it named cannot part company.
                runPostingSealPurgeJob();
                Assert.assertFalse(
                        "the purge must unlink the superseded _im",
                        ff.exists(partitionPath(path).concat(firstMeta).$())
                );
                Assert.assertFalse(
                        "the purge must unlink the superseded index parquet",
                        ff.exists(partitionPath(path).concat(firstParquet).$())
                );
            }
        });
    }

    @Test
    public void testConvertToParquetLinksTheNativeSidecarsUnderTheNativeFormat() throws Exception {
        assertMemoryLeak(() -> {
            inputRoot = root;
            createSparseKeyTable();
            execute("ALTER TABLE " + TABLE_NAME + " ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, qty)");
            drainWalQueue();
            execute("ALTER TABLE " + TABLE_NAME + " CONVERT PARTITION TO PARQUET LIST '" + INDEXED_PARTITION + "'");
            drainWalQueue();
            engine.releaseInactive();

            // The negative control for the gate: on the default format the same
            // switch carries the native sealed sidecars over and writes no pidx
            // artifact at all, so the gate cannot be reached by a user who has
            // not set the property.
            try (Path path = new Path()) {
                assertNoFileNamed(partitionPath(path), "sym.pidx.");
                assertAnyFileNamed(partitionPath(path), "sym.pc0.");
                Assert.assertTrue(
                        "the native format must carry a sealed .pv generation over",
                        hasSealedValueFile(partitionPath(path))
                );
                assertNoCoveringIndexToken(path, TABLE_NAME, currentPartitionNameTxn(TABLE_NAME));
            }
            assertQuery("select count() from (" + COVERED_QUERY + ')')
                    .inferRandomAccess()
                    .expectSize()
                    .returns("count\n75000\n");
        });
    }

    @Test
    public void testConvertToParquetSealsTheIndexInsteadOfLinkingTheNativeSidecars() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_PARQUET_PARTITION_FORMAT, "parquet");
        assertMemoryLeak(() -> {
            inputRoot = root;
            createSparseKeyTable();
            // Indexed while the partition is still native, so the switch is the
            // path that decides between carrying the native sealed sidecars
            // over and sealing a parquet index into the new directory.
            execute("ALTER TABLE " + TABLE_NAME + " ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, qty)");
            drainWalQueue();
            execute("ALTER TABLE " + TABLE_NAME + " CONVERT PARTITION TO PARQUET LIST '" + INDEXED_PARTITION + "'");
            drainWalQueue();
            engine.releaseInactive();

            try (Path path = new Path()) {
                // The sealed native artifacts are what the link path used to
                // carry over: the .pc<N> cover data and a .pv under a published
                // seal txn. The .pk and the unsealed .pv.0 are the index
                // writer's own working files, created fresh by the reseal rather
                // than linked, and the parquet seal still needs them to count
                // keys, so they are not what this asserts on.
                assertNoFileNamed(partitionPath(path), "sym.pc0.");
                Assert.assertFalse(
                        "the parquet format must publish no sealed .pv generation",
                        hasSealedValueFile(partitionPath(path))
                );
                final String indexMeta = onlyFileNamed(partitionPath(path), "sym.pidx.", "._im");
                onlyFileNamed(partitionPath(path), "sym.pidx.", ".parquet");
                final long indexTxn = Numbers.parseLong(
                        indexMeta.substring("sym.pidx.".length(), indexMeta.length() - "._im".length())
                );
                final long imFileSize = imFileSizeField(partitionPath(path).concat(indexMeta).$());
                assertCoveringIndexToken(path, TABLE_NAME, SYM_COLUMN_ID, indexTxn, imFileSize);
            }
        });
    }

    @Test
    public void testCoveredColumnAddedAfterThePartitionIsSealedAsNulls() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_PARQUET_PARTITION_FORMAT, "parquet");
        assertMemoryLeak(() -> {
            inputRoot = root;
            execute("CREATE TABLE " + NULLS_TABLE_NAME + " (" +
                    "ts TIMESTAMP, sym SYMBOL, price DOUBLE" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO " + NULLS_TABLE_NAME + " SELECT" +
                    " dateadd('u', x::INT, '" + INDEXED_PARTITION + "T00:00:00Z'::TIMESTAMP)," +
                    " 'n' || (x % 2)," +
                    " x::DOUBLE" +
                    " FROM long_sequence(" + NULLS_ROW_COUNT + ")");
            drainWalQueue();
            execute("INSERT INTO " + NULLS_TABLE_NAME + " VALUES ('2024-01-02T00:00:00Z', 'n0', 1.0)");
            drainWalQueue();
            execute("ALTER TABLE " + NULLS_TABLE_NAME + " CONVERT PARTITION TO PARQUET LIST '" + INDEXED_PARTITION + "'");
            drainWalQueue();
            // Added after the partition became parquet, so it is not in the
            // parquet at all and its top in that partition is the partition size.
            // The native seal covers this by emitting nulls; so must this one.
            execute("ALTER TABLE " + NULLS_TABLE_NAME + " ADD COLUMN qty LONG");
            drainWalQueue();
            execute("ALTER TABLE " + NULLS_TABLE_NAME + " ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, qty)");
            drainWalQueue();
            engine.releaseInactive();

            final String indexParquetPath;
            try (Path path = new Path()) {
                final String indexParquet = onlyFileNamed(partitionPath(path, NULLS_TABLE_NAME), "sym.pidx.", ".parquet");
                indexParquetPath = partitionPath(path, NULLS_TABLE_NAME).concat(indexParquet).toString();
            }

            assertPostingsCoverEveryRow(indexParquetPath, NULLS_ROW_COUNT);
            assertPostingKeysAgree(indexParquetPath, "case when (row_id + 1) % 2 = 1 then 1 else 2 end");
            assertIndexParquetQuery(
                    "select count() from read_parquet('" + indexParquetPath + "')" +
                            " where qty is not null",
                    "count\n0\n"
            );
            // The covered columns that do exist are unaffected by the all-null one.
            assertIndexParquetQuery(
                    "select count() from read_parquet('" + indexParquetPath + "')" +
                            " where price is null or price <> row_id + 1" +
                            " or ts <> dateadd('u', (row_id + 1)::int, '" + INDEXED_PARTITION + "T00:00:00Z'::timestamp)",
                    "count\n0\n"
            );
        });
    }

    @Test
    public void testPostingIndexReadIsRefusedWhileTheParquetFormatIsSelected() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_PARQUET_PARTITION_FORMAT, "parquet");
        assertMemoryLeak(() -> {
            createIndexedSparseKeyTable();
            // The seal wrote the index as parquet and discarded the native chain,
            // which a reader would otherwise read as "no keys, no rows" and answer
            // with an empty cursor. Nothing reads the parquet form yet, so the
            // read must fail rather than answer.
            assertQuery(COVERED_QUERY).failsWith("has no reader yet");
        });
    }

    @Test
    public void testPostingIndexReadIsRefusedAfterTheFormatIsFlippedBackToNative() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_PARQUET_PARTITION_FORMAT, "parquet");
        assertMemoryLeak(() -> {
            createIndexedSparseKeyTable();
            // The hole a format-keyed refusal leaves open. The partition is
            // already sealed as parquet and its token is published; flipping the
            // property back says nothing about what is on disk. A check keyed on
            // the configured format would wave this read through and answer it
            // with an empty cursor.
            node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_PARQUET_PARTITION_FORMAT, "native");
            engine.releaseInactive();
            assertQuery(COVERED_QUERY).failsWith("has no reader yet");
        });
    }

    @Test
    public void testTheRefusalProbeResolvesThePinnedReadersOwnIndexTxn() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_PARQUET_PARTITION_FORMAT, "parquet");
        assertMemoryLeak(() -> {
            inputRoot = root;
            createIndexedSparseKeyTable();
            final long firstIndexTxn;
            try (Path path = new Path()) {
                final String firstMeta = onlyFileNamed(partitionPath(path), "sym.pidx.", "._im");
                firstIndexTxn = Numbers.parseLong(
                        firstMeta.substring("sym.pidx.".length(), firstMeta.length() - "._im".length())
                );
            }

            try (TableReader pinned = engine.getReader(engine.verifyTableName(TABLE_NAME))) {
                // Map the partition's _pm at the size this snapshot's header
                // names. Everything published after this point lands past that
                // tail and is invisible to this mapping. Pinned before the
                // reindex, so this reader's metadata still carries the POSTING
                // index and the probe is the thing that decides.
                Assert.assertTrue(pinned.openPartition(0) > 0);
                final int symColumnIndex = pinned.getMetadata().getColumnIndex("sym");

                execute("ALTER TABLE " + TABLE_NAME + " ALTER COLUMN sym DROP INDEX");
                drainWalQueue();
                execute("ALTER TABLE " + TABLE_NAME + " ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, qty)");
                drainWalQueue();

                // The second seal restated the same data.parquet size, so its
                // footer shadows the first for anything that maps the _pm now.
                // This reader must still get its own: the artifacts it can reach
                // are the ones the first token names, and reporting the writer's
                // latest index_txn would be an answer about files this snapshot
                // has no claim on.
                final long secondIndexTxn;
                try (Path path = new Path()) {
                    final String secondMeta = newestFileNamed(
                            partitionPath(path), "sym.pidx.", "._im", "sym.pidx." + firstIndexTxn + "._im");
                    secondIndexTxn = Numbers.parseLong(
                            secondMeta.substring("sym.pidx.".length(), secondMeta.length() - "._im".length())
                    );
                }
                Assert.assertTrue(
                        "the fixture must actually supersede the first token, or the two answers coincide"
                                + " and this test cannot fail [first=" + firstIndexTxn + ", second=" + secondIndexTxn + ']',
                        secondIndexTxn > firstIndexTxn
                );

                try {
                    pinned.getIndexReader(0, symColumnIndex, IndexReader.DIR_FORWARD);
                    Assert.fail("the parquet-form posting index must be refused");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "indexTxn=" + firstIndexTxn);
                }
            }
        });
    }

    @Test
    public void testPostingIndexReadIsServedWhileTheNativeFormatIsSelected() throws Exception {
        assertMemoryLeak(() -> {
            createIndexedSparseKeyTable();
            // The negative control for the refusal above: on the default format
            // the same query over the same partition is served, so the refusal
            // cannot be reached by a user who has not set the property.
            assertQuery("select count() from (" + COVERED_QUERY + ')')
                    .inferRandomAccess()
                    .expectSize()
                    .returns("count\n75000\n");
        });
    }

    @Test
    public void testSealPacksManySmallKeysIntoSharedRowGroups() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_PARQUET_PARTITION_FORMAT, "parquet");
        assertMemoryLeak(() -> {
            inputRoot = root;
            createPackedKeyTable();

            execute("ALTER TABLE " + PACKED_TABLE_NAME + " CONVERT PARTITION TO PARQUET LIST '" + INDEXED_PARTITION + "'");
            drainWalQueue();
            execute("ALTER TABLE " + PACKED_TABLE_NAME + " ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, qty)");
            drainWalQueue();
            engine.releaseInactive();

            final String indexParquetPath;
            try (Path path = new Path()) {
                final String indexParquet = onlyFileNamed(partitionPath(path, PACKED_TABLE_NAME), "sym.pidx.", ".parquet");
                final String indexMeta = onlyFileNamed(partitionPath(path, PACKED_TABLE_NAME), "sym.pidx.", "._im");
                indexParquetPath = partitionPath(path, PACKED_TABLE_NAME).concat(indexParquet).toString();

                final IndexMetaFileReader reader = new IndexMetaFileReader();
                IndexMetaFileReader.openAndMapRO(
                        configuration.getFilesFacade(),
                        partitionPath(path, PACKED_TABLE_NAME).concat(indexMeta).$(),
                        reader
                );
                try {
                    assertRowGroupsAreKeyAligned(reader);
                    assertRowGroupsArePacked(reader);
                } finally {
                    reader.close();
                }
            }

            assertPostingsCoverEveryRow(indexParquetPath, PACKED_ROW_COUNT);
            assertPostingKeysAgree(indexParquetPath, PACKED_KEY_OF_ROW_ID);
            assertCoveredValuesAgree(indexParquetPath, INDEXED_PARTITION);
        });
    }

    @Test
    public void testSealPublishesTheCoveringIndexTokenIntoTheParquetMeta() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_PARQUET_PARTITION_FORMAT, "parquet");
        assertMemoryLeak(() -> {
            inputRoot = root;
            createIndexedSparseKeyTable();

            try (Path path = new Path()) {
                final String indexMeta = onlyFileNamed(partitionPath(path), "sym.pidx.", "._im");
                final long indexTxn = Numbers.parseLong(
                        indexMeta.substring("sym.pidx.".length(), indexMeta.length() - "._im".length())
                );
                final long imFileSize = imFileSizeField(partitionPath(path).concat(indexMeta).$());
                assertCoveringIndexToken(path, TABLE_NAME, SYM_COLUMN_ID, indexTxn, imFileSize);
            }
        });
    }

    @Test
    public void testSealWritesAKeyAlignedParquetIndex() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_PARQUET_PARTITION_FORMAT, "parquet");
        assertMemoryLeak(() -> {
            inputRoot = root;
            createSparseKeyTable();

            execute("ALTER TABLE " + TABLE_NAME + " CONVERT PARTITION TO PARQUET LIST '" + INDEXED_PARTITION + "'");
            drainWalQueue();
            execute("ALTER TABLE " + TABLE_NAME + " ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, qty)");
            drainWalQueue();
            engine.releaseInactive();

            final String indexParquetPath;
            try (Path path = new Path()) {
                final String indexParquet = onlyFileNamed(partitionPath(path), "sym.pidx.", ".parquet");
                final String indexMeta = onlyFileNamed(partitionPath(path), "sym.pidx.", "._im");
                Assert.assertEquals(
                        "the index parquet and its _im must share one index txn",
                        indexParquet.substring(0, indexParquet.length() - ".parquet".length()),
                        indexMeta.substring(0, indexMeta.length() - "._im".length())
                );
                indexParquetPath = partitionPath(path).concat(indexParquet).toString();

                final FilesFacade ff = configuration.getFilesFacade();
                final IndexMetaFileReader reader = new IndexMetaFileReader();
                IndexMetaFileReader.openAndMapRO(ff, partitionPath(path).concat(indexMeta).$(), reader);
                try {
                    assertKeySpaceIsTheKeyIdBound(reader);
                    assertRowGroupsAreKeyAligned(reader);
                    assertEveryPresentKeyResolves(reader);
                } finally {
                    reader.close();
                }
            }

            assertPostingsCoverEveryRow(indexParquetPath, SKEWED_ROW_COUNT);
            // The whole point of the payload assertion: the _im says nothing
            // about what the parquet holds, so the key of every posting is
            // checked against the row id it is filed under. 's0' takes the rows
            // whose x is divisible by 4, 's7' those with x % 4 == 1 and 's15'
            // the rest, and x is row id + 1.
            assertPostingKeysAgree(
                    indexParquetPath,
                    "case when (row_id + 1) % 4 = 0 then 1 when (row_id + 1) % 4 = 1 then 8 else 16 end"
            );
            assertCoveredValuesAgree(indexParquetPath, INDEXED_PARTITION);
            assertIndexParquetQuery(
                    "select key_id, count() from read_parquet('" + indexParquetPath + "') order by key_id",
                    "key_id\tcount\n1\t75000\n8\t75000\n16\t150000\n"
            );
        });
    }

    /**
     * Every key present in the partition resolves to a non-empty row group range.
     */
    private static void assertEveryPresentKeyResolves(IndexMetaFileReader reader) {
        for (int key : PRESENT_KEYS) {
            Assert.assertNotEquals(
                    "key " + key + " must resolve",
                    IndexMetaFileReader.KEY_ABSENT,
                    reader.getRowGroupRangeForKey(key)
            );
            final int lo = reader.getRowGroupLoForKey(key);
            final int hi = reader.getRowGroupHiForKey(key);
            Assert.assertTrue("key " + key + " range [" + lo + ", " + hi + ']', lo >= 0 && hi >= lo);
            for (int rg = lo; rg <= hi; rg++) {
                Assert.assertTrue("row group " + rg + " must hold rows", reader.getRowGroupNumRows(rg) > 0);
            }
        }
        // The hot key has more postings than a row group targets, so it must
        // occupy consecutive dedicated groups. Without this the alignment check
        // above would pass on a single-group file that proves nothing.
        final int hotKey = PRESENT_KEYS[PRESENT_KEYS.length - 1];
        Assert.assertTrue(
                "key " + hotKey + " must span consecutive dedicated row groups",
                reader.getRowGroupHiForKey(hotKey) > reader.getRowGroupLoForKey(hotKey)
        );
    }

    /**
     * KEY_SPACE_SIZE is the exclusive upper bound on key ids, not a count of
     * distinct keys present. The partition's key ids are sparse, so the two
     * differ, and a reader built on the wrong one resolves the wrong rows.
     */
    private static void assertKeySpaceIsTheKeyIdBound(IndexMetaFileReader reader) {
        Assert.assertEquals(EXPECTED_KEY_SPACE_SIZE, reader.getKeySpaceSize());
        Assert.assertTrue(
                "the partition's key set must be sparse for this test to tell the key space"
                        + " bound apart from a distinct-key count",
                reader.getKeySpaceSize() > PRESENT_KEYS.length
        );
    }

    /**
     * No row group boundary falls mid-key, read back from the {@code key_id}
     * chunk's own statistics rather than taken on the writer's word. A key that
     * ends one row group and starts the next has its earlier postings silently
     * dropped by the reader's exact-match resolution, and nothing at read time
     * can detect it.
     */
    private static void assertRowGroupsAreKeyAligned(IndexMetaFileReader reader) {
        final int keyIdColumn = reader.getKeyIdColumn();
        final int rowGroupCount = reader.getIndexRowGroupCount();
        Assert.assertTrue("a single row group would make this check vacuous", rowGroupCount > 1);
        long previousMax = Long.MIN_VALUE;
        for (int i = 0; i < rowGroupCount; i++) {
            Assert.assertTrue("row group " + i + " has no key id min stat", reader.hasChunkMinStat(i, keyIdColumn));
            Assert.assertTrue("row group " + i + " has no key id max stat", reader.hasChunkMaxStat(i, keyIdColumn));
            final long min = reader.getChunkMinStat(i, keyIdColumn);
            final long max = reader.getChunkMaxStat(i, keyIdColumn);
            Assert.assertEquals("row group " + i + " first key must be its key id min", min, reader.getRowGroupFirstKey(i));
            Assert.assertTrue("row group " + i + " key id range [" + min + ", " + max + ']', min <= max);
            if (i > 0) {
                if (min == previousMax) {
                    // A key larger than the target occupies consecutive dedicated
                    // groups, so a group may repeat the previous group's single
                    // key - but only if it holds nothing else.
                    Assert.assertEquals(
                            "row group " + i + " shares key " + min + " with row group " + (i - 1)
                                    + ", so it must be dedicated to that key",
                            min,
                            max
                    );
                } else {
                    Assert.assertTrue("row group " + i + " starts below row group " + (i - 1), min > previousMax);
                }
            }
            previousMax = max;
        }
    }

    /**
     * The packed fixture's row groups are the ones the key-alignment invariant
     * exists for: two of them hold hundreds of keys apiece. The skewed fixture
     * produces only single-key groups, which makes every statistics assertion
     * above trivially true, so the exact layout is pinned here.
     */
    private static void assertRowGroupsArePacked(IndexMetaFileReader reader) {
        Assert.assertEquals("row group count", PACKED_GROUP_FIRST_KEYS.length, reader.getIndexRowGroupCount());
        final int keyIdColumn = reader.getKeyIdColumn();
        int sharedGroups = 0;
        for (int i = 0; i < PACKED_GROUP_FIRST_KEYS.length; i++) {
            Assert.assertEquals("row group " + i + " first key", PACKED_GROUP_FIRST_KEYS[i], reader.getRowGroupFirstKey(i));
            Assert.assertEquals("row group " + i + " row count", PACKED_GROUP_ROW_COUNTS[i], reader.getRowGroupNumRows(i));
            if (reader.getChunkMaxStat(i, keyIdColumn) > reader.getChunkMinStat(i, keyIdColumn)) {
                sharedGroups++;
            }
        }
        Assert.assertEquals("groups holding more than one key", 2, sharedGroups);
        // Group 0 is closed by the branch that fires when a key boundary lands
        // exactly on the target, group 1 by the branch that closes an open shared
        // group before a key that would overflow it. Neither runs on the skewed
        // fixture.
        Assert.assertEquals(
                "the group closed on the exact target must hold 200 keys",
                200,
                reader.getChunkMaxStat(0, keyIdColumn) - reader.getChunkMinStat(0, keyIdColumn) + 1
        );
        Assert.assertEquals(
                "the group closed before an overflowing key must hold 142 keys",
                142,
                reader.getChunkMaxStat(1, keyIdColumn) - reader.getChunkMinStat(1, keyIdColumn) + 1
        );
    }

    private static void assertAnyFileNamed(Path partitionPath, String prefix) {
        final File[] files = new File(partitionPath.toString())
                .listFiles((_, name) -> name.startsWith(prefix));
        Assert.assertNotNull("no directory at " + partitionPath, files);
        Assert.assertTrue("expected at least one " + prefix + "* under " + partitionPath, files.length > 0);
    }

    private static void assertNoFileNamed(Path partitionPath, String prefix) {
        final File[] files = new File(partitionPath.toString())
                .listFiles((_, name) -> name.startsWith(prefix));
        Assert.assertNotNull("no directory at " + partitionPath, files);
        Assert.assertEquals(
                "expected no " + prefix + "* under " + partitionPath + " but found " + files.length,
                0,
                files.length
        );
    }

    /**
     * True when the partition directory holds a {@code .pv} under a published
     * seal txn, as opposed to the writer's own unsealed {@code .pv.0}. The
     * unsealed file exists under both formats -- the parquet seal still feeds
     * the native index writer to count keys -- so only a sealed generation
     * tells the two formats apart.
     */
    private static boolean hasSealedValueFile(Path partitionPath) {
        final File[] files = new File(partitionPath.toString())
                .listFiles((_, name) -> name.startsWith("sym.pv.") && !name.equals("sym.pv.0"));
        Assert.assertNotNull("no directory at " + partitionPath, files);
        return files.length > 0;
    }

    /**
     * Number of directory entries pointing at the file's inode. Two means the
     * two partition directories name one file, so a write through either is a
     * write to both.
     */
    private static long hardLinkCount(String file) throws Exception {
        return ((Number) java.nio.file.Files.getAttribute(java.nio.file.Path.of(file), "unix:nlink")).longValue();
    }

    /**
     * The one file matching {@code prefix}/{@code suffix} that is not
     * {@code exclude}. Used where a superseded artifact is deliberately still on
     * disk, so "exactly one" is the wrong assertion.
     */
    private static String newestFileNamed(Path partitionPath, String prefix, String suffix, String exclude) {
        final File[] files = new File(partitionPath.toString())
                .listFiles((_, name) -> name.startsWith(prefix) && name.endsWith(suffix) && !name.equals(exclude));
        Assert.assertNotNull("no directory at " + partitionPath, files);
        Assert.assertEquals(
                "expected exactly one " + prefix + "*" + suffix + " other than " + exclude + " under " + partitionPath,
                1,
                files.length
        );
        return files[0].getName();
    }

    private static String onlyFileNamed(Path partitionPath, String prefix, String suffix) {
        final File[] files = new File(partitionPath.toString())
                .listFiles((_, name) -> name.startsWith(prefix) && name.endsWith(suffix));
        Assert.assertNotNull("no directory at " + partitionPath, files);
        Assert.assertEquals(
                "expected exactly one " + prefix + "*" + suffix + " under " + partitionPath,
                1,
                files.length
        );
        return files[0].getName();
    }

    /**
     * The partition's {@code _pm} footer names the sealed index. Without this
     * entry the artifacts exist but nothing references them, so a reader has no
     * way to find them and no way to tell a committed index from an orphan a
     * failed seal left behind.
     * <p>
     * The footer is resolved through the partition's committed
     * {@code data.parquet} size, which is the same MVCC walk a reader makes, so
     * this reads exactly the snapshot a reader pinned to the current txn would.
     */
    private void assertCoveringIndexToken(Path path, String tableName, int columnId, long indexTxn, long imFileSize) {
        final FilesFacade ff = configuration.getFilesFacade();
        final TableToken token = engine.verifyTableName(tableName);
        final long parquetFileSize;
        try (TableReader reader = engine.getReader(token)) {
            parquetFileSize = reader.getTxFile().getPartitionParquetFileSize(0);
        }
        final ParquetMetaFileReader reader = new ParquetMetaFileReader();
        final long addr = ParquetMetaFileReader.openAndMapRO(
                ff,
                partitionPath(path, tableName).concat(TableUtils.PARQUET_METADATA_FILE_NAME).$(),
                reader
        );
        Assert.assertTrue("_pm must be readable", addr != 0);
        final long fileSize = reader.getFileSize();
        try {
            Assert.assertTrue("_pm footer must resolve for the committed parquet size", reader.resolveFooter(parquetFileSize));
            Assert.assertEquals("covering index entry count", 1, reader.getCoveringIndexCount());
            Assert.assertEquals("covering index column id", columnId, reader.getCoveringIndexColumnId(0));
            Assert.assertEquals("covering index txn", indexTxn, reader.getCoveringIndexTxn(0));
            Assert.assertEquals("covering index _im file size", imFileSize, reader.getCoveringIndexImFileSize(0));
        } finally {
            reader.clear();
            ff.munmap(addr, fileSize, MemoryTag.MMAP_PARQUET_METADATA_READER);
        }
    }

    /**
     * The partition directory's {@code _pm} carries no covering-index entry at
     * all.
     */
    private void assertNoCoveringIndexToken(Path path, String tableName, long partitionNameTxn) {
        final FilesFacade ff = configuration.getFilesFacade();
        final ParquetMetaFileReader reader = new ParquetMetaFileReader();
        final long addr = ParquetMetaFileReader.openAndMapRO(
                ff,
                partitionPathAt(path, tableName, partitionNameTxn).concat(TableUtils.PARQUET_METADATA_FILE_NAME).$(),
                reader
        );
        Assert.assertTrue("_pm must be readable", addr != 0);
        final long fileSize = reader.getFileSize();
        try {
            Assert.assertTrue("_pm last footer must resolve", reader.resolveLastFooter());
            Assert.assertEquals("covering index entry count", 0, reader.getCoveringIndexCount());
        } finally {
            reader.clear();
            ff.munmap(addr, fileSize, MemoryTag.MMAP_PARQUET_METADATA_READER);
        }
    }

    /**
     * Drives the reader-gated purge to quiescence. The operator returns false
     * and re-queues while the scoreboard says a reader can still be inside the
     * retired version's window, and the job backs off between attempts, so a
     * single {@code run()} proves nothing either way.
     */
    private void runPostingSealPurgeJob() throws Exception {
        try (PostingSealPurgeJob job = new PostingSealPurgeJob(engine)) {
            for (int i = 0; i < 8; i++) {
                setCurrentMicros(Math.max(currentMicros, 0) + 10_000_000L);
                job.run();
            }
        }
    }

    /**
     * The {@code _im}'s own {@code IM_FILE_SIZE} header field, which is what
     * {@code docs/index-metadata.md} defines the third field of the {@code _pm}
     * covering-index entry to be. The file's length on disk agrees with it today,
     * but it is the field the format names, and it is also the commit signal --
     * zero until the seal patches it last -- so asserting on the length would
     * pass over an {@code _im} that was never committed.
     */
    private long imFileSizeField(io.questdb.std.str.LPSZ imFile) {
        final FilesFacade ff = configuration.getFilesFacade();
        final long fd = ff.openRO(imFile);
        Assert.assertTrue("_im must be readable [file=" + imFile + ']', fd > -1);
        try {
            final long imFileSize = ff.readNonNegativeLong(fd, 0);
            Assert.assertTrue("_im must be committed [file=" + imFile + ']', imFileSize > 0);
            return imFileSize;
        } finally {
            ff.close(fd);
        }
    }

    /**
     * The table's committed txn, read the way a reader pins it.
     */
    /**
     * Rows the POSTING-indexed column holds for one key in the sealed
     * partition. The count is what a silent empty result destroys.
     */
    private long countIndexedRows() throws Exception {
        try (RecordCursorFactory factory = select(
                "SELECT count() FROM " + TABLE_NAME + " WHERE sym = 's0' AND ts IN '" + INDEXED_PARTITION + "'");
             RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            Assert.assertTrue(cursor.hasNext());
            return cursor.getRecord().getLong(0);
        }
    }

    /**
     * The {@code to_table_txn} the purge job persisted for the task that retires
     * {@code sealTxn}. This is the scoreboard window's upper bound as the
     * producer computed it, read back rather than inferred from behaviour.
     */
    private long persistedPurgeWindowUpperBound(long sealTxn) throws Exception {
        try (RecordCursorFactory factory = select(
                "SELECT to_table_txn FROM \"" + configuration.getSystemTableNamePrefix()
                        + "posting_seal_purge_log\" WHERE column_name = 'sym' AND seal_txn = " + sealTxn);
             RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            Assert.assertTrue("no purge log row for sealTxn=" + sealTxn, cursor.hasNext());
            final long bound = cursor.getRecord().getLong(0);
            Assert.assertFalse("more than one purge log row for sealTxn=" + sealTxn, cursor.hasNext());
            return bound;
        }
    }

    /**
     * Empties the O3 partition purge discovery queue and returns how many tasks
     * it held.
     */
    private static int drainO3PurgeDiscoveryQueue(MessageBus bus) {
        int drained = 0;
        while (true) {
            long cursor = bus.getO3PurgeDiscoverySubSeq().next();
            if (cursor == -2) {
                Os.pause();
                continue;
            }
            if (cursor < 0) {
                return drained;
            }
            bus.getO3PurgeDiscoveryQueue().get(cursor);
            bus.getO3PurgeDiscoverySubSeq().done(cursor);
            drained++;
        }
    }

    private long committedTxn(String tableName) {
        try (TableReader reader = engine.getReader(engine.verifyTableName(tableName))) {
            return reader.getTxn();
        }
    }

    private long currentPartitionNameTxn(String tableName) {
        try (TableReader reader = engine.getReader(engine.verifyTableName(tableName))) {
            return reader.getTxFile().getPartitionNameTxn(0);
        }
    }

    /**
     * The fixtures write {@code price}, {@code qty} and the row's timestamp from
     * the row's own {@code x}, which is its row id plus one, so a covered value
     * that does not satisfy that arithmetic was gathered from the wrong row.
     * This is what a wrong cover stride or a wrong {@code firstRowId} offset in
     * {@code sortPostingsByKey} looks like from outside; nothing in the
     * {@code _im} can see it. The designated timestamp is a covered column too,
     * because {@code cairo.posting.index.auto.include.timestamp} defaults on.
     */
    private void assertCoveredValuesAgree(String indexParquetPath, String partition) throws Exception {
        assertIndexParquetQuery(
                "select count() from read_parquet('" + indexParquetPath + "')" +
                        " where price is null or qty is null or ts is null" +
                        " or price <> row_id + 1 or qty <> row_id + 1" +
                        " or ts <> dateadd('u', (row_id + 1)::int, '" + partition + "T00:00:00Z'::timestamp)",
                "count\n0\n"
        );
    }

    /**
     * Runs a query over the emitted index parquet on the non-parallel
     * {@code read_parquet} path.
     * <p>
     * The parallel page-frame path cannot read this file: it projects columns by
     * QuestDB column id and, in
     * {@code ReadParquetRecordCursor.canProjectMetadata}, substitutes the
     * parquet column index for any negative id. The index parquet's synthetic
     * {@code key_id} and {@code row_id} carry id -1 (the {@code _im} writer
     * requires exactly that to tell them from the covered columns), so
     * {@code key_id} is remapped to id 0 and collides with any covered column
     * whose writer index is 0 - the designated timestamp, here. {@code key_id}
     * then serves the timestamp's page truncated to 32 bits. The file itself is
     * correct; only that projection is wrong, and it is out of this task's
     * scope.
     */
    private void assertIndexParquetQuery(String sql, String expected) throws Exception {
        final boolean wasParallel = sqlExecutionContext.isParallelReadParquetEnabled();
        try {
            sqlExecutionContext.setParallelReadParquetEnabled(false);
            assertQuery(sql).inferRandomAccess().expectSize().returns(expected);
        } finally {
            sqlExecutionContext.setParallelReadParquetEnabled(wasParallel);
        }
    }

    /**
     * Every posting is filed under the key the row actually holds.
     * {@code keyIdOfRowId} is the fixture's own row id to key id mapping, so a
     * counting sort that placed a row under a neighbouring key is caught even
     * though the per-key counts would still add up.
     */
    private void assertPostingKeysAgree(String indexParquetPath, String keyIdOfRowId) throws Exception {
        assertIndexParquetQuery(
                "select count() from read_parquet('" + indexParquetPath + "')" +
                        " where key_id <> (" + keyIdOfRowId + ")",
                "count\n0\n"
        );
    }

    /**
     * The index carries exactly one posting per row of the partition, and the
     * row ids it carries are the partition's own {@code [0, rowCount)} with no
     * duplicate and no gap. A dropped row, a duplicated row or a shifted row id
     * base all fail here.
     */
    private void assertPostingsCoverEveryRow(String indexParquetPath, long partitionRowCount) throws Exception {
        assertIndexParquetQuery(
                "select count() postings, count_distinct(row_id) distinctRowIds," +
                        " min(row_id) minRowId, max(row_id) maxRowId" +
                        " from read_parquet('" + indexParquetPath + "')",
                "postings\tdistinctRowIds\tminRowId\tmaxRowId\n"
                        + partitionRowCount + '\t' + partitionRowCount + "\t0\t" + (partitionRowCount - 1) + '\n'
        );
    }

    /**
     * The sparse-key fixture with its parquet partition indexed, that is
     * everything the two format tests share.
     */
    private void createIndexedSparseKeyTable() throws Exception {
        createSparseKeyTable();
        execute("ALTER TABLE " + TABLE_NAME + " CONVERT PARTITION TO PARQUET LIST '" + INDEXED_PARTITION + "'");
        drainWalQueue();
        execute("ALTER TABLE " + TABLE_NAME + " ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, qty)");
        drainWalQueue();
        engine.releaseInactive();
    }

    /**
     * The table's symbols are inserted into a later partition first, so the
     * symbol map assigns ids 0..15 there; the indexed partition then uses only
     * 's0', 's7' and 's15', leaving its key ids sparse.
     */
    private void createSparseKeyTable() throws Exception {
        execute("CREATE TABLE " + TABLE_NAME + " (" +
                "ts TIMESTAMP, sym SYMBOL, price DOUBLE, qty LONG" +
                ") TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("INSERT INTO " + TABLE_NAME + " SELECT" +
                " dateadd('m', x::INT, '2024-01-02T00:00:00Z'::TIMESTAMP)," +
                " 's' || (x - 1)," +
                " x::DOUBLE," +
                " x" +
                " FROM long_sequence(16)");
        drainWalQueue();
        // 75_000 rows for 's0', 75_000 for 's7' and 150_000 for 's15'. The skew
        // is deliberate: 's15' has more postings than a row group targets, so it
        // must land in consecutive dedicated groups rather than sharing one.
        execute("INSERT INTO " + TABLE_NAME + " SELECT" +
                " dateadd('u', x::INT, '2024-01-01T00:00:00Z'::TIMESTAMP)," +
                " CASE WHEN x % 4 = 0 THEN 's0' WHEN x % 4 = 1 THEN 's7' ELSE 's15' END," +
                " x::DOUBLE," +
                " x" +
                " FROM long_sequence(" + SKEWED_ROW_COUNT + ")");
        drainWalQueue();
    }

    /**
     * Builds a partition whose keys are mostly small, so row groups pack many
     * keys into one. Over the row's own {@code x}, which is its row id plus one:
     * <ul>
     *     <li>{@code x} in [1, 100000]: 200 keys of 500 rows. The 200th key ends
     *     exactly on the 100000-row target, closing a 200-key shared group.</li>
     *     <li>{@code x} in [100001, 200100]: 143 keys of 700 rows. The 143rd would
     *     take the open group to 100100 rows, so the group is closed before it
     *     with 142 keys in it.</li>
     *     <li>{@code x} above 200100: one key of 150000 rows, which exceeds the
     *     target on its own and so takes consecutive dedicated groups.</li>
     * </ul>
     */
    private void createPackedKeyTable() throws Exception {
        execute("CREATE TABLE " + PACKED_TABLE_NAME + " (" +
                "ts TIMESTAMP, sym SYMBOL, price DOUBLE, qty LONG" +
                ") TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("INSERT INTO " + PACKED_TABLE_NAME + " SELECT" +
                " dateadd('u', x::INT, '" + INDEXED_PARTITION + "T00:00:00Z'::TIMESTAMP)," +
                " CASE WHEN x <= 100000 THEN 'q' || ((x - 1) / 500)" +
                " WHEN x <= 200100 THEN 'q' || (200 + (x - 100001) / 700)" +
                " ELSE 'q999' END," +
                " x::DOUBLE," +
                " x" +
                " FROM long_sequence(" + PACKED_ROW_COUNT + ")");
        drainWalQueue();
        // A later partition, so the indexed one is not the active partition and
        // can be converted to parquet. Its symbol already exists, so it moves no
        // key id.
        execute("INSERT INTO " + PACKED_TABLE_NAME + " VALUES ('2024-01-02T00:00:00Z', 'q0', 1.0, 1)");
        drainWalQueue();
    }

    private Path partitionPath(Path path) {
        return partitionPath(path, TABLE_NAME);
    }

    private Path partitionPath(Path path, String tableName) {
        return partitionPathAt(path, tableName, Long.MIN_VALUE);
    }

    /**
     * The partition directory named by {@code nameTxn}, or by the committed name
     * txn when {@code nameTxn} is {@link Long#MIN_VALUE}. Naming an older txn is
     * how a test reaches the directory a reader pinned to an older snapshot
     * still reads.
     */
    private Path partitionPathAt(Path path, String tableName, long nameTxn) {
        final TableToken token = engine.verifyTableName(tableName);
        final long partitionTs;
        long dirNameTxn = nameTxn;
        try (TableReader reader = engine.getReader(token)) {
            partitionTs = reader.getTxFile().getPartitionTimestampByIndex(0);
            if (dirNameTxn == Long.MIN_VALUE) {
                dirNameTxn = reader.getTxFile().getPartitionNameTxn(0);
            }
        }
        path.of(configuration.getDbRoot()).concat(token);
        TableUtils.setPathForNativePartition(path, ColumnType.TIMESTAMP, PartitionBy.DAY, partitionTs, dirNameTxn);
        return path;
    }
}
