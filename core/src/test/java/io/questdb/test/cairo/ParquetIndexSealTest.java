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
import io.questdb.cairo.IndexType;
import io.questdb.cairo.ParquetMetaFileReader;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.PostingSealPurgeJob;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.TxWriter;
import io.questdb.cairo.idx.AbstractParquetPostingIndexReader;
import io.questdb.cairo.idx.IndexReader;
import io.questdb.cairo.idx.PostingIndexUtils;
import io.questdb.mp.MPSequence;
import io.questdb.mp.RingQueue;
import io.questdb.std.Chars;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
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
    // Writer index of the second SYMBOL column in the two-symbol fixture below:
    // ts 0, sym 1, sym2 2.
    private static final int SYM2_COLUMN_ID = 2;
    private static final String CHAIN_COST_TABLE_NAME = "t_pidx_chain_cost";
    private static final String CHAIN_TABLE_NAME = "t_pidx_chain";
    private static final String RESIDUE_TABLE_NAME = "t_pidx_residue";
    private static final String ROLLBACK_TABLE_NAME = "t_pidx_rollback";
    private static final String SWITCH_INDEXED_TABLE_NAME = "t_pidx_switch_idx";
    private static final String DROP_COLUMN_TABLE_NAME = "t_pidx_dropcol";
    private static final String SWITCH_TABLE_NAME = "t_pidx_switch";
    private static final String TABLE_NAME = "t_pidx";
    // Two-symbol fixtures below. Rows are only ever counted per key, so a small
    // partition is enough and the parquet convert stays cheap.
    private static final String TORN_TABLE_NAME = "t_pidx_torn";
    private static final int TWO_SYMBOL_ROW_COUNT = 20_000;

    /**
     * W4-I1: the cost of a {@code _pm} chain walk must not grow with the chain.
     * <p>
     * Every {@code resolvePrevFooter} step used to re-verify the footer's CRC,
     * and a footer's CRC covers the whole {@code _pm} prefix beneath it, so a
     * walk over {@code N} footers hashed {@code N^2 * F / 2} bytes. Measured on
     * this fixture before the fix: 202 footers / 6.5 ms, 402 / 25.1 ms, 602 /
     * 55.3 ms, 802 / 96.9 ms -- a clean quadratic, paid inside
     * {@code beginParquetIndexTokenBatch} on the O3 commit path, and paid on
     * EVERY commit once a pair the O3 in-place update dropped from the covering
     * section arms the escalation permanently.
     * <p>
     * Asserted as a ceiling on verifications rather than as a stopwatch: one
     * verification hashes one prefix, so "one per walk" and "one per footer" is
     * exactly the difference between linear and quadratic, and the count does
     * not vary with the machine.
     * <p>
     * The O3 rewrite trigger is turned off for the duration, because it is the
     * only thing that resets the chain today and this test is about what happens
     * when it does not fire. That is not a synthetic state: the trigger is dead
     * bytes in {@code data.parquet}, and a late append into a non-last parquet
     * partition adds only the old parquet footer plus a small tail row group per
     * commit.
     */
    @Test
    public void testAChainWalkVerifiesOneChecksumHoweverLongTheChainIs() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_PARQUET_PARTITION_FORMAT, "parquet");
        // Four row groups over 20k rows: a single-row-group parquet always takes
        // the O3 REWRITE branch, which lands in a new directory with a fresh _pm.
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 5000);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_RATIO, 1000000);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_MAX_BYTES, 1000000000000L);
        assertMemoryLeak(() -> {
            inputRoot = root;
            execute("CREATE TABLE " + CHAIN_COST_TABLE_NAME + " (" +
                    "ts TIMESTAMP, sym SYMBOL, price DOUBLE, qty LONG" +
                    ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO " + CHAIN_COST_TABLE_NAME + " SELECT" +
                    " dateadd('u', x::INT, '" + INDEXED_PARTITION + "T00:00:00Z'::TIMESTAMP)," +
                    " CASE WHEN x % 4 = 0 THEN 's0' WHEN x % 4 = 1 THEN 's7' ELSE 's15' END," +
                    " x::DOUBLE," +
                    " x" +
                    " FROM long_sequence(20000)");
            // A later partition, so the indexed one is not the active partition:
            // a non-WAL table cannot hold a parquet active partition.
            execute("INSERT INTO " + CHAIN_COST_TABLE_NAME + " VALUES ('2024-01-02T00:00:00Z', 's0', 1.0, 1)");
            execute("ALTER TABLE " + CHAIN_COST_TABLE_NAME + " CONVERT PARTITION TO PARQUET LIST '" + INDEXED_PARTITION + "'");
            execute("ALTER TABLE " + CHAIN_COST_TABLE_NAME + " ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price)");
            engine.releaseInactive();

            // Each O3 commit appends two footers: the in-place update's, which
            // drops the covering section, and the reseal publish's.
            for (int i = 1; i <= 100; i++) {
                execute("INSERT INTO " + CHAIN_COST_TABLE_NAME + " VALUES ('" + INDEXED_PARTITION
                        + "T00:00:00.03" + (i < 10 ? "000" : i < 100 ? "00" : "0") + i
                        + "Z', 's0', 1.0, 1)");
            }
            engine.releaseInactive();

            final FilesFacade facade = configuration.getFilesFacade();
            try (Path path = new Path()) {
                final ParquetMetaFileReader reader = new ParquetMetaFileReader();
                final long addr = ParquetMetaFileReader.openAndMapRO(
                        facade,
                        partitionPath(path, CHAIN_COST_TABLE_NAME).concat(TableUtils.PARQUET_METADATA_FILE_NAME).$(),
                        reader
                );
                Assert.assertTrue("_pm must be readable", addr != 0);
                final long fileSize = reader.getFileSize();
                try {
                    int footers = 0;
                    Assert.assertTrue(reader.resolveLastFooter());
                    do {
                        footers++;
                    } while (reader.resolvePrevFooter());
                    Assert.assertTrue(
                            "premise: the chain must actually be long, or a ceiling on the walk's cost"
                                    + " proves nothing [footers=" + footers + ']',
                            footers >= 150
                    );
                    Assert.assertEquals(
                            "a chain walk must verify exactly one checksum: each verification hashes the"
                                    + " whole _pm prefix up to its footer, so one per step makes the walk"
                                    + " quadratic in the chain length, on a commit path, with nothing but"
                                    + " the O3 rewrite trigger bounding that length [footers=" + footers
                                    + ", pmBytes=" + fileSize + ']',
                            1,
                            reader.getChecksumVerifications()
                    );
                } finally {
                    reader.clear();
                    facade.munmap(addr, fileSize, MemoryTag.MMAP_PARQUET_METADATA_READER);
                }
            }
        });
    }

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
     * I8a: the squash stamp a token publish makes must not be on a per-commit
     * trigger, because the counter it stamps is 16 bits.
     * <p>
     * {@code TxWriter.incrementPartitionSquashCounter} saturates at
     * {@code PARTITION_SQUASH_COUNTER_MAX} (0xFFFF) and is reset only by
     * {@code updatePartitionSizeAndTxnByRawIndex}, i.e. by a partition-version
     * rewrite. O3 into a parquet partition that runs IN PLACE takes
     * {@code updatePartitionSizeByRawIndex} instead and never resets it, so a
     * stamp on every O3 reseal saturates after 65 535 commits into one
     * partition and every publish after that takes the {@code .squash_ts} file
     * write, forever, with nothing to reset it.
     * <p>
     * The stamp is skipped exactly where it is redundant: an O3 commit has
     * already moved the partition's own {@code _txn} record, so a per-partition
     * consumer sees the change without it. This asserts both halves per commit
     * -- that the counter does NOT move, and that something else in the record
     * DOES -- because "the counter did not move" alone would also pass if the
     * publish had not run at all.
     * <p>
     * The complementary case, a token-only DDL commit where the stamp IS the
     * only signal, is {@link #testATokenPublishRestampsThePartitionsChangeToken}.
     */
    @Test
    public void testARepeatedO3PublishDoesNotWalkThePartitionsSquashCounter() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_PARQUET_PARTITION_FORMAT, "parquet");
        assertMemoryLeak(() -> {
            inputRoot = root;
            createIndexedSparseKeyTable();

            final TableToken token = engine.verifyTableName(TABLE_NAME);
            int previousSquashCount;
            long previousRecord;
            try (TableReader reader = engine.getReader(token)) {
                previousSquashCount = reader.getTxFile().getPartitionSquashCount(0);
                previousRecord = partitionRecordDigest(reader);
            }

            for (int i = 0; i < 6; i++) {
                // Out of order into the sealed parquet partition, which is what
                // drives resealParquetCoveringForPartition and its publish.
                execute("INSERT INTO " + TABLE_NAME + " VALUES ('2024-01-01T00:00:00.0000"
                        + (20 + i) + "Z', 's0', 1.0, 1)");
                drainWalQueue();
                engine.releaseInactive();
                try (TableReader reader = engine.getReader(token)) {
                    final long record = partitionRecordDigest(reader);
                    Assert.assertNotEquals(
                            "premise: the O3 commit must move the partition's own _txn record, or skipping"
                                    + " the stamp would leave a per-partition consumer with no signal at all"
                                    + " [commit=" + i + ']',
                            previousRecord,
                            record
                    );
                    previousRecord = record;
                    // Not equality: a parquet O3 rewrite resets the counter to
                    // zero (updatePartitionSizeAndTxnByRawIndex), which is a
                    // legitimate move in the other direction. What must never
                    // happen is an advance, because that is the per-commit
                    // increment that walks a 16-bit field toward saturation.
                    final int squashCount = reader.getTxFile().getPartitionSquashCount(0);
                    Assert.assertTrue(
                            "a per-commit publish must not advance the partition's 16-bit squash counter:"
                                    + " it saturates at 65535 and every publish after that takes the"
                                    + " .squash_ts file write forever [commit=" + i
                                    + ", before=" + previousSquashCount + ", after=" + squashCount + ']',
                            squashCount <= previousSquashCount
                    );
                    previousSquashCount = squashCount;
                }
            }
        });
    }

    /**
     * I2b: a rollback must re-apply the reader-facing marks a token publish made,
     * and the trigger for that is "a publish happened", not "a purge task is
     * pending".
     * <p>
     * {@code publishParquetIndexTokens} makes the {@code _pm} append durable
     * before the {@code _txn} commit and marks the partition in {@code txWriter}
     * -- a partition table version bump plus a squash-counter stamp -- so a
     * reloading reader drops its pre-publish mapping. {@code rollback()}'s
     * {@code unsafeLoadAll()} throws both marks away while the append stays on
     * disk, so they have to be re-applied. The old gate asked whether a
     * parquet-form purge task was pending, which is true only for a publish that
     * SUPERSEDES something: a first seal on a partition queues no task at all,
     * and its marks were silently dropped.
     * <p>
     * Driven on the first seal, which is exactly the case the old gate missed,
     * and through {@code TableWriter.rollback()} itself. {@code BYPASS WAL}
     * because a WAL apply would suspend and then retry the failed alter, and the
     * retry republishes the token -- restoring the very mark this test has to
     * observe the loss of.
     * <p>
     * The premise is asserted, not assumed: a plain in-order append is measured
     * first and must NOT move the partition table version, or the final
     * comparison would be satisfied by the append alone.
     */
    @Test
    public void testARollbackReappliesTheMarksOfAFirstSealThatSupersededNothing() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_PARQUET_PARTITION_FORMAT, "parquet");
        final boolean[] armedMetaSwap = {false};
        final boolean[] metaSwapRefused = {false};
        ff = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (armedMetaSwap[0] && name != null && Utf8s.containsAscii(name, TableUtils.META_SWAP_FILE_NAME)) {
                    metaSwapRefused[0] = true;
                    return -1;
                }
                return super.openRW(name, opts);
            }
        };
        assertMemoryLeak(ff, () -> {
            inputRoot = root;
            execute("CREATE TABLE " + ROLLBACK_TABLE_NAME + " (" +
                    "ts TIMESTAMP, sym SYMBOL, price DOUBLE, qty LONG" +
                    ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO " + ROLLBACK_TABLE_NAME + " SELECT" +
                    " dateadd('u', x::INT, '" + INDEXED_PARTITION + "T00:00:00Z'::TIMESTAMP)," +
                    " CASE WHEN x % 4 = 0 THEN 's0' WHEN x % 4 = 1 THEN 's7' ELSE 's15' END," +
                    " x::DOUBLE," +
                    " x" +
                    " FROM long_sequence(20000)");
            execute("INSERT INTO " + ROLLBACK_TABLE_NAME + " VALUES ('2024-01-02T00:00:00Z', 's0', 1.0, 1)");
            execute("ALTER TABLE " + ROLLBACK_TABLE_NAME + " CONVERT PARTITION TO PARQUET LIST '" + INDEXED_PARTITION + "'");
            engine.releaseInactive();

            final long versionBeforeAppend = committedPartitionTableVersion(ROLLBACK_TABLE_NAME);
            execute("INSERT INTO " + ROLLBACK_TABLE_NAME + " VALUES ('2024-01-02T00:00:01Z', 's0', 1.0, 1)");
            engine.releaseInactive();
            final long versionAfterAppend = committedPartitionTableVersion(ROLLBACK_TABLE_NAME);
            Assert.assertEquals(
                    "premise: a plain in-order append must not move the partition table version, or the"
                            + " final comparison is satisfied by the append rather than by the re-applied mark",
                    versionBeforeAppend,
                    versionAfterAppend
            );

            final TableToken token = engine.verifyTableName(ROLLBACK_TABLE_NAME);
            try (TableWriter writer = engine.getWriter(token, "test")) {
                final ObjList<CharSequence> covering = new ObjList<>();
                covering.add("price");
                covering.add("qty");
                armedMetaSwap[0] = true;
                try {
                    writer.addIndex("sym", configuration.getIndexValueBlockSize(), IndexType.POSTING, covering);
                    Assert.fail("the _meta.swp open was not refused");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "Cannot open indexed file");
                } finally {
                    armedMetaSwap[0] = false;
                }
                Assert.assertTrue("the _meta.swp open was never refused", metaSwapRefused[0]);
                // The first seal published its token and fsynced the _pm header
                // patch before the failure. Nothing was superseded, so there is
                // no parquet-form purge task for the old gate to see.
                writer.rollback();
                // A commit that moves nothing in the partition table on its own,
                // measured above, so the version can only move if the rollback
                // re-applied the publish's mark.
                final TableWriter.Row row = writer.newRow(writer.getMaxTimestamp() + 1);
                row.putSym(1, "s0");
                row.putDouble(2, 1.0);
                row.putLong(3, 1);
                row.append();
                writer.commit();
            }
            engine.releaseInactive();

            final long versionAfterRollback = committedPartitionTableVersion(ROLLBACK_TABLE_NAME);
            Assert.assertTrue(
                    "the rollback dropped the partition table version bump a durable _pm append had"
                            + " already earned, so a reloading reader keeps its pre-publish mapping"
                            + " [before=" + versionAfterAppend + ", after=" + versionAfterRollback + ']',
                    versionAfterRollback > versionAfterAppend
            );
        });
    }

    /**
     * I6a: the reseal supersession branch in {@code publishParquetIndexTokens}
     * was commented as unreachable in production, and therefore untested. It is
     * reachable, and this drives the route that reaches it.
     * <p>
     * {@code DROP INDEX} commits the metadata first and retires the {@code _pm}
     * token in a second transaction. A crash between the two leaves the footer
     * naming {@code (sym, T1)} for a column metadata no longer calls indexed,
     * and nothing reclaims that residue -- the merge loop has no such rule. A
     * later {@code ADD INDEX TYPE POSTING} on the same column then seals
     * {@code T2}, and the publish sees a footer entry for a column this batch
     * also staged, with a different index txn: the branch.
     * <p>
     * Asserted on the persisted purge window rather than on behaviour, because
     * what the branch decides is the window's upper bound: it must be the txn
     * the reseal commits at, so a reader pinned below it can still resolve
     * {@code T1}. {@code BYPASS WAL} so the injected failure surfaces to the
     * caller instead of suspending an apply that would then retry the drop.
     */
    @Test
    public void testAResealSupersedesATokenTheDropIndexRetirementNeverRemoved() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_PARQUET_PARTITION_FORMAT, "parquet");
        final boolean[] armedRetire = {false};
        final boolean[] retireRefused = {false};
        ff = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (armedRetire[0] && name != null && Utf8s.endsWithAscii(name, TableUtils.PARQUET_METADATA_FILE_NAME)) {
                    // The only _pm opened read-write during a DROP INDEX is the
                    // retirement's own footer append, which runs after the
                    // metadata commit. Refusing it is the crash window.
                    armedRetire[0] = false;
                    retireRefused[0] = true;
                    return -1;
                }
                return super.openRW(name, opts);
            }
        };
        assertMemoryLeak(ff, () -> {
            inputRoot = root;
            execute("CREATE TABLE " + RESIDUE_TABLE_NAME + " (" +
                    "ts TIMESTAMP, sym SYMBOL, price DOUBLE, qty LONG" +
                    ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO " + RESIDUE_TABLE_NAME + " SELECT" +
                    " dateadd('u', x::INT, '" + INDEXED_PARTITION + "T00:00:00Z'::TIMESTAMP)," +
                    " CASE WHEN x % 4 = 0 THEN 's0' WHEN x % 4 = 1 THEN 's7' ELSE 's15' END," +
                    " x::DOUBLE," +
                    " x" +
                    " FROM long_sequence(20000)");
            // A later partition so the indexed one is not the active partition:
            // a non-WAL table cannot hold a parquet active partition.
            execute("INSERT INTO " + RESIDUE_TABLE_NAME + " VALUES ('2024-01-02T00:00:00Z', 's0', 1.0, 1)");
            execute("ALTER TABLE " + RESIDUE_TABLE_NAME + " CONVERT PARTITION TO PARQUET LIST '" + INDEXED_PARTITION + "'");
            execute("ALTER TABLE " + RESIDUE_TABLE_NAME + " ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, qty)");
            engine.releaseInactive();

            final String firstMeta;
            final String firstParquet;
            final long firstIndexTxn;
            try (Path path = new Path()) {
                firstMeta = onlyFileNamed(partitionPath(path, RESIDUE_TABLE_NAME), "sym.pidx.", "._im");
                firstParquet = onlyFileNamed(partitionPath(path, RESIDUE_TABLE_NAME), "sym.pidx.", ".parquet");
                firstIndexTxn = Numbers.parseLong(
                        firstMeta.substring("sym.pidx.".length(), firstMeta.length() - "._im".length()));
            }

            armedRetire[0] = true;
            try {
                execute("ALTER TABLE " + RESIDUE_TABLE_NAME + " ALTER COLUMN sym DROP INDEX");
                Assert.fail("the retirement's _pm open was not refused");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "cannot remove index");
            } finally {
                armedRetire[0] = false;
            }
            Assert.assertTrue("the retirement's _pm open was never refused", retireRefused[0]);
            engine.releaseInactive();

            try (TableReader reader = engine.getReader(engine.verifyTableName(RESIDUE_TABLE_NAME))) {
                Assert.assertFalse(
                        "premise: the drop must have committed the metadata before the retirement failed",
                        reader.getMetadata().isColumnIndexed(reader.getMetadata().getColumnIndex("sym"))
                );
            }
            try (Path path = new Path()) {
                // The residue: a token for a column that is no longer indexed.
                assertCoveringIndexToken(
                        path,
                        RESIDUE_TABLE_NAME,
                        SYM_COLUMN_ID,
                        firstIndexTxn,
                        imFileSizeField(partitionPath(path, RESIDUE_TABLE_NAME).concat(firstMeta).$())
                );
            }

            execute("ALTER TABLE " + RESIDUE_TABLE_NAME + " ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, qty)");
            engine.releaseInactive();

            final long resealTxn = committedTxn(RESIDUE_TABLE_NAME);
            try (Path path = new Path()) {
                final String secondMeta = newestFileNamed(
                        partitionPath(path, RESIDUE_TABLE_NAME), "sym.pidx.", "._im", firstMeta);
                final long secondIndexTxn = Numbers.parseLong(
                        secondMeta.substring("sym.pidx.".length(), secondMeta.length() - "._im".length()));
                Assert.assertNotEquals(
                        "premise: the reseal must carry a different index txn, or the branch never fires",
                        firstIndexTxn,
                        secondIndexTxn
                );
                assertCoveringIndexToken(
                        path,
                        RESIDUE_TABLE_NAME,
                        SYM_COLUMN_ID,
                        secondIndexTxn,
                        imFileSizeField(partitionPath(path, RESIDUE_TABLE_NAME).concat(secondMeta).$())
                );
                Assert.assertTrue(
                        "the superseded pair must still be on disk until its readers drain",
                        configuration.getFilesFacade().exists(
                                partitionPath(path, RESIDUE_TABLE_NAME).concat(firstParquet).$())
                );
            }

            runPostingSealPurgeJob();
            Assert.assertEquals(
                    "the reseal must hand the superseded pair to the reader-gated purge with a window"
                            + " reaching the txn the reseal commits at",
                    resealTxn,
                    persistedPurgeWindowUpperBound(firstIndexTxn)
            );
        });
    }

    /**
     * C1, the inverse of {@link #testASealStrandedBeforeItsTokenIsPublishedIsSwept}
     * and the control that predicate was missing: a pair the committed
     * {@code _pm} DOES name must survive the sweep, even though its index txn is
     * above the committed txn.
     * <p>
     * That combination is not exotic, it is the window the write ordering
     * creates on purpose. A seal names its artifacts {@code getTxn() + 1} and
     * {@code publishParquetIndexTokens} fsyncs the {@code _pm} header patch
     * before the {@code _txn} commit, so a crash or a rollback between the two
     * leaves a committed footer naming an index txn one above the committed
     * txn -- for good, since the rollback moves the txn back.
     * <p>
     * Reached through the production entry path in two injected steps:
     * <ol>
     *     <li>{@code ADD INDEX} seals, publishes the token and makes the header
     *     patch durable, then the {@code _meta.swp} it needs next is refused.
     *     The alter throws and the transaction rolls back, leaving exactly that
     *     state -- asserted, not assumed, before step 2 runs.</li>
     *     <li>The retried alter opens a fresh seal batch, which runs the sweep,
     *     and is then failed at the {@code data.parquet} mapping -- after the
     *     sweep and before the reseal could recreate what the sweep removed. So
     *     the pair's presence afterwards is the sweep's verdict alone, with no
     *     recreate to mask it.</li>
     * </ol>
     * A file-existence assertion is only available because of that second
     * injection; without it the retry rewrites the same name either way, which
     * is why the sibling test has to count unlinks instead.
     */
    @Test
    public void testAPairTheCommittedFooterNamesIsNotSwept() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_PARQUET_PARTITION_FORMAT, "parquet");
        final boolean[] armedMetaSwap = {false};
        final boolean[] metaSwapRefused = {false};
        final boolean[] armedParquetMap = {false};
        final boolean[] parquetMapRefused = {false};
        final long[] parquetFd = {-1};
        ff = new TestFilesFacadeImpl() {
            @Override
            public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
                if (armedParquetMap[0] && fd > -1 && fd == parquetFd[0]) {
                    armedParquetMap[0] = false;
                    parquetMapRefused[0] = true;
                    return FilesFacade.MAP_FAILED;
                }
                return super.mmap(fd, len, offset, flags, memoryTag);
            }

            @Override
            public long openRO(LPSZ name) {
                final long fd = super.openRO(name);
                if (armedParquetMap[0] && fd > -1 && Utf8s.endsWithAscii(name, TableUtils.PARQUET_PARTITION_NAME)) {
                    parquetFd[0] = fd;
                }
                return fd;
            }

            @Override
            public long openRW(LPSZ name, int opts) {
                if (armedMetaSwap[0] && name != null && Utf8s.containsAscii(name, TableUtils.META_SWAP_FILE_NAME)) {
                    // The seal has already published its token and fsynced the
                    // _pm header patch by now; refusing the metadata swap file
                    // aborts the alter before the _txn commit.
                    metaSwapRefused[0] = true;
                    return -1;
                }
                return super.openRW(name, opts);
            }
        };
        assertMemoryLeak(ff, () -> {
            inputRoot = root;
            createSparseKeyTable();
            execute("ALTER TABLE " + TABLE_NAME + " CONVERT PARTITION TO PARQUET LIST '" + INDEXED_PARTITION + "'");
            drainWalQueue();
            engine.releaseInactive();

            final long committedTxnBefore = committedTxn(TABLE_NAME);
            armedMetaSwap[0] = true;
            try {
                execute("ALTER TABLE " + TABLE_NAME + " ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, qty)");
                drainWalQueue();
            } finally {
                armedMetaSwap[0] = false;
            }
            Assert.assertTrue("the _meta.swp was never refused, so nothing rolled back", metaSwapRefused[0]);
            engine.releaseInactive();

            final String publishedMeta;
            final String publishedParquet;
            final long publishedIndexTxn;
            try (Path path = new Path()) {
                publishedMeta = onlyFileNamed(partitionPath(path), "sym.pidx.", "._im");
                publishedParquet = onlyFileNamed(partitionPath(path), "sym.pidx.", ".parquet");
                publishedIndexTxn = Numbers.parseLong(
                        publishedMeta.substring("sym.pidx.".length(), publishedMeta.length() - "._im".length())
                );
                // The premise, both halves. The committed footer names the pair,
                // so it is referenced; and its index txn is above the committed
                // txn, so the rule under test is the one that fires.
                assertCoveringIndexToken(
                        path,
                        TABLE_NAME,
                        SYM_COLUMN_ID,
                        publishedIndexTxn,
                        imFileSizeField(partitionPath(path).concat(publishedMeta).$())
                );
                Assert.assertEquals(
                        "premise: the rollback must leave the committed txn where it was",
                        committedTxnBefore,
                        committedTxn(TABLE_NAME)
                );
                Assert.assertTrue(
                        "premise: the published index txn must be ABOVE the committed txn, or the"
                                + " committed-txn rule never fires and this test proves nothing"
                                + " [indexTxn=" + publishedIndexTxn + ", committedTxn=" + committedTxn(TABLE_NAME) + ']',
                        publishedIndexTxn > committedTxn(TABLE_NAME)
                );
            }

            armedParquetMap[0] = true;
            parquetFd[0] = -1;
            try {
                execute("ALTER TABLE " + TABLE_NAME + " RESUME WAL");
                drainWalQueue();
            } finally {
                armedParquetMap[0] = false;
            }
            Assert.assertTrue(
                    "the retried alter must be failed at the data.parquet mapping, after the sweep and"
                            + " before any reseal, or the reseal masks the sweep's verdict",
                    parquetMapRefused[0]
            );
            engine.releaseInactive();

            try (Path path = new Path()) {
                final FilesFacade facade = configuration.getFilesFacade();
                Assert.assertTrue(
                        "the sweep unlinked a pair the committed _pm still names -- silent deletion of"
                                + " referenced data [file=" + publishedParquet + ']',
                        facade.exists(partitionPath(path).concat(publishedParquet).$())
                );
                Assert.assertTrue(
                        "the sweep unlinked the _im of a pair the committed _pm still names"
                                + " [file=" + publishedMeta + ']',
                        facade.exists(partitionPath(path).concat(publishedMeta).$())
                );
                assertCoveringIndexToken(
                        path,
                        TABLE_NAME,
                        SYM_COLUMN_ID,
                        publishedIndexTxn,
                        imFileSizeField(partitionPath(path).concat(publishedMeta).$())
                );
            }
        });
    }

    /**
     * W3-C1: the footer the sweep reads is not the footer a reader resolves, and
     * an ordinary O3 write into the partition is enough to separate the two.
     * <p>
     * The sweep read the physically-last footer. An in-place O3 update writes
     * ITS footer with the covering section dropped -- {@code updateFileMetadata(0, 0, 0)},
     * the explicit "drop the section" answer -- and patches the {@code _pm}
     * header on the O3 worker, BEFORE the {@code _txn} commit. A reader is
     * unaffected: it matches on the committed {@code data.parquet} size from its
     * own snapshot and walks {@code prev} back to the footer that names the
     * tokens. The sweep, reading the tail, saw an empty section, fell through to
     * "no token for this column" and unlinked a pair that footer still names.
     * <p>
     * The state is built through production entry paths only:
     * <ol>
     *     <li>{@code sym} takes a covering posting index that survives. It is
     *     what arms {@code resealParquetCoveringForPartition}, and with it the
     *     sweep, on the insert in step 3 -- the gate is "the table has a
     *     covering posting column", not "this column".</li>
     *     <li>{@code sym2}'s {@code ADD INDEX} seals, publishes its token and
     *     fsyncs the {@code _pm} header patch, and is then refused its
     *     {@code _meta.swp}. The transaction rolls back, so the committed txn
     *     stays put and the footer a reader resolves names
     *     {@code (sym2, committedTxn + 1)} -- asserted, not assumed.</li>
     *     <li>A single out-of-order row lands in the parquet partition. That is
     *     an ordinary successful commit; nothing about it fails.</li>
     * </ol>
     * The verdict is asserted from the reader's side, with
     * {@code resolveFooter(parquetFileSizeBeforeTheInsert)}: the pair a reader
     * pinned to the pre-insert snapshot resolves must still be on disk. Both
     * halves of the premise are asserted first -- the update really ran in place
     * (a rewrite would move the partition directory and the sweep would find
     * nothing to do), and the post-insert footer really dropped {@code sym2}'s
     * token (or the cheap read alone would have protected the pair and this test
     * would prove nothing).
     */
    @Test
    public void testAPairOnlyAFooterBelowTheTailNamesIsNotSwept() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_PARQUET_PARTITION_FORMAT, "parquet");
        // Four row groups over the 20k rows below: a single-row-group parquet
        // file always takes the O3 REWRITE branch, and the rewrite moves the
        // partition directory, which is not the state under test.
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 5000);
        final boolean[] armedMetaSwap = {false};
        final boolean[] metaSwapRefused = {false};
        ff = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (armedMetaSwap[0] && name != null && Utf8s.containsAscii(name, TableUtils.META_SWAP_FILE_NAME)) {
                    // By now the seal has published its token and fsynced the _pm
                    // header patch; refusing the metadata swap aborts the alter
                    // before the _txn commit.
                    metaSwapRefused[0] = true;
                    return -1;
                }
                return super.openRW(name, opts);
            }
        };
        assertMemoryLeak(ff, () -> {
            inputRoot = root;
            execute("CREATE TABLE " + CHAIN_TABLE_NAME + " (" +
                    "ts TIMESTAMP, sym SYMBOL, sym2 SYMBOL, price DOUBLE, qty LONG" +
                    ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO " + CHAIN_TABLE_NAME + " SELECT" +
                    " dateadd('u', x::INT, '" + INDEXED_PARTITION + "T00:00:00Z'::TIMESTAMP)," +
                    " CASE WHEN x % 4 = 0 THEN 's0' WHEN x % 4 = 1 THEN 's7' ELSE 's15' END," +
                    " 'o' || (x % 4)," +
                    " x::DOUBLE," +
                    " x" +
                    " FROM long_sequence(20000)");
            // A later partition so the indexed one is not the active partition:
            // a non-WAL table cannot hold a parquet active partition.
            execute("INSERT INTO " + CHAIN_TABLE_NAME + " VALUES ('2024-01-02T00:00:00Z', 's0', 'o0', 1.0, 1)");
            execute("ALTER TABLE " + CHAIN_TABLE_NAME + " CONVERT PARTITION TO PARQUET LIST '" + INDEXED_PARTITION + "'");
            execute("ALTER TABLE " + CHAIN_TABLE_NAME + " ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price)");
            engine.releaseInactive();

            final long committedTxnBeforeStrand = committedTxn(CHAIN_TABLE_NAME);
            final TableToken token = engine.verifyTableName(CHAIN_TABLE_NAME);
            try (TableWriter writer = engine.getWriter(token, "test")) {
                final ObjList<CharSequence> covering = new ObjList<>();
                covering.add("price");
                armedMetaSwap[0] = true;
                try {
                    writer.addIndex("sym2", configuration.getIndexValueBlockSize(), IndexType.POSTING, covering);
                    Assert.fail("the _meta.swp open was not refused");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "Cannot open indexed file");
                } finally {
                    armedMetaSwap[0] = false;
                }
                writer.rollback();
            }
            Assert.assertTrue("the _meta.swp open was never refused, so nothing rolled back", metaSwapRefused[0]);
            engine.releaseInactive();

            final String strandedMeta;
            final String strandedParquet;
            final long strandedIndexTxn;
            final long parquetFileSizeBefore = committedParquetFileSize(CHAIN_TABLE_NAME);
            final long partitionNameTxnBefore = currentPartitionNameTxn(CHAIN_TABLE_NAME);
            try (Path path = new Path()) {
                strandedMeta = onlyFileNamed(partitionPath(path, CHAIN_TABLE_NAME), "sym2.pidx.", "._im");
                strandedParquet = onlyFileNamed(partitionPath(path, CHAIN_TABLE_NAME), "sym2.pidx.", ".parquet");
                strandedIndexTxn = Numbers.parseLong(
                        strandedMeta.substring("sym2.pidx.".length(), strandedMeta.length() - "._im".length())
                );
                Assert.assertEquals(
                        "premise: the rollback must leave the committed txn where it was",
                        committedTxnBeforeStrand,
                        committedTxn(CHAIN_TABLE_NAME)
                );
                Assert.assertTrue(
                        "premise: the stranded index txn must be ABOVE the committed txn, or the"
                                + " committed-txn fallback never fires and this test proves nothing"
                                + " [indexTxn=" + strandedIndexTxn + ", committedTxn=" + committedTxnBeforeStrand + ']',
                        strandedIndexTxn > committedTxnBeforeStrand
                );
                Assert.assertEquals(
                        "premise: the footer a reader resolves must name the stranded pair",
                        strandedIndexTxn,
                        publishedIndexTxnAt(path, CHAIN_TABLE_NAME, parquetFileSizeBefore, SYM2_COLUMN_ID)
                );
            }

            // An ordinary O3 commit. Nothing about it fails.
            execute("INSERT INTO " + CHAIN_TABLE_NAME + " VALUES ('" + INDEXED_PARTITION + "T00:00:00.007777Z', 's0', 'o0', 1.0, 1)");
            engine.releaseInactive();

            Assert.assertEquals(
                    "premise: the O3 update must have run IN PLACE -- a rewrite moves the partition"
                            + " directory and leaves the sweep nothing to look at",
                    partitionNameTxnBefore,
                    currentPartitionNameTxn(CHAIN_TABLE_NAME)
            );
            final long parquetFileSizeAfter = committedParquetFileSize(CHAIN_TABLE_NAME);
            Assert.assertNotEquals(
                    "premise: the in-place update must have moved the committed parquet size, or the"
                            + " two resolutions coincide and there is nothing to tell apart",
                    parquetFileSizeBefore,
                    parquetFileSizeAfter
            );
            try (Path path = new Path()) {
                // Asserted against the O3 update's OWN footer, which is the one
                // the sweep read: the sweep runs at beginParquetIndexTokenBatch,
                // after the in-place update has patched the _pm header and
                // before the reseal's publish appends anything, so the physical
                // tail then was the update's footer. After the commit that
                // footer is one prev step below the tail -- the publish anchors
                // its prev at the committed head it merged from, which is
                // exactly that footer. Resolving at parquetFileSizeAfter instead
                // would land on the publish's footer, one footer later than the
                // state under test; both drop sym2, so the old assertion held,
                // but it pinned the wrong footer.
                final int tailCoveringCount = coveringIndexCountAtTail(path, CHAIN_TABLE_NAME, 0);
                final int updateFooterCoveringCount = coveringIndexCountAtTail(path, CHAIN_TABLE_NAME, 1);
                Assert.assertTrue(
                        "premise: the reseal's publish must have named the surviving covering column, or"
                                + " the two footers below are not being told apart [tailCount="
                                + tailCoveringCount + ']',
                        tailCoveringCount > 0
                );
                Assert.assertEquals(
                        "premise: the O3 update's own footer -- the one the sweep read -- must have"
                                + " dropped the covering section outright (updateFileMetadata's (0,0,0)"
                                + " contract), or the cheap single-footer read protects the pair by"
                                + " itself and this test proves nothing",
                        0,
                        updateFooterCoveringCount
                );
                // The verdict, from the reader's side: a reader pinned to the
                // pre-insert snapshot resolves the footer that names the pair.
                Assert.assertEquals(
                        "the footer a pinned reader resolves must still name the stranded pair",
                        strandedIndexTxn,
                        publishedIndexTxnAt(path, CHAIN_TABLE_NAME, parquetFileSizeBefore, SYM2_COLUMN_ID)
                );
                final FilesFacade facade = configuration.getFilesFacade();
                Assert.assertTrue(
                        "the sweep unlinked a pair the footer a pinned reader resolves still names --"
                                + " silent deletion of referenced data [file=" + strandedParquet + ']',
                        facade.exists(partitionPath(path, CHAIN_TABLE_NAME).concat(strandedParquet).$())
                );
                Assert.assertTrue(
                        "the sweep unlinked the _im of a pair the footer a pinned reader resolves still"
                                + " names [file=" + strandedMeta + ']',
                        facade.exists(partitionPath(path, CHAIN_TABLE_NAME).concat(strandedMeta).$())
                );
            }
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

                // Having re-mapped, this reader is entitled to the SECOND token
                // and nothing else: the first token's artifacts are now outside
                // the purge window protecting them, so a reader still bound to
                // them could be reading files the purge is free to unlink.
                assertBoundParquetReader(reader, symColumnIndex, secondIndexTxn);
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

    /**
     * A partition slot outlives the partition it describes, so the cached index
     * forms must not.
     * <p>
     * A stale form here is a silent wrong answer rather than an error in both
     * directions. Left saying PARQUET over an incarnation that is now natively
     * indexed, it refuses a read that would have been served; left naming the
     * PREVIOUS incarnation's {@code index_txn}, it names an artifact pair that
     * incarnation's reseal superseded and the purge may already have unlinked.
     * <p>
     * Three incarnations of one partition, each written into a new directory
     * under a new name txn while the slot index stays put: parquet-sealed,
     * converted to native, converted back to parquet and resealed. The last
     * assertion goes through {@code getIndexReader}, which binds the reader it
     * dispatches to the {@code index_txn} it decided on, so the check is on the
     * production path rather than on an accessor only a test calls. It also
     * crosses the form twice in one reader, so the cached native reader of
     * incarnation 2 has to be dropped and rebuilt for incarnation 3.
     */
    @Test
    public void testAPartitionIncarnationChangeDropsTheCachedIndexForm() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_PARQUET_PARTITION_FORMAT, "parquet");
        assertMemoryLeak(() -> {
            createIndexedSparseKeyTable();
            try (TableReader reader = engine.getReader(engine.verifyTableName(TABLE_NAME))) {
                final int symColumnIndex = reader.getMetadata().getColumnIndex("sym");
                Assert.assertTrue(reader.openPartition(0) > 0);

                // Incarnation 1: sealed as parquet, so the _pm publishes a token.
                Assert.assertEquals(
                        "the fixture must seal the index as parquet, or nothing is cached to go stale",
                        PostingIndexUtils.PARQUET_INDEX_FORMAT_PARQUET,
                        reader.getPartitionIndexForm(0, symColumnIndex)
                );
                final long firstIndexTxn = reader.getPartitionIndexTxn(0, symColumnIndex);
                Assert.assertTrue("a published token names a real index txn", firstIndexTxn >= 0);
                Assert.assertTrue(
                        "a published token names a real _im size",
                        reader.getPartitionIndexImFileSize(0, symColumnIndex) > 0
                );
                final long firstNameTxn = reader.getTxFile().getPartitionNameTxn(0);

                // Incarnation 2: native. The new directory has no _pm at all, so
                // there is nothing to publish and the index is native again.
                execute("ALTER TABLE " + TABLE_NAME + " CONVERT PARTITION TO NATIVE LIST '" + INDEXED_PARTITION + "'");
                drainWalQueue();
                Assert.assertTrue("the reader must have something to reload", reader.reload());
                Assert.assertTrue(reader.openPartition(0) > 0);
                Assert.assertEquals(
                        "a partition with no _pm publishes no covering index",
                        PostingIndexUtils.PARQUET_INDEX_FORMAT_NATIVE,
                        reader.getPartitionIndexForm(0, symColumnIndex)
                );
                Assert.assertEquals(-1, reader.getPartitionIndexTxn(0, symColumnIndex));
                Assert.assertEquals(0, reader.getPartitionIndexImFileSize(0, symColumnIndex));
                Assert.assertNotNull(
                        "the rebuilt native index must be readable",
                        reader.getIndexReader(0, symColumnIndex, IndexReader.DIR_FORWARD)
                );

                // Incarnation 3: parquet again, resealed under a NEW index txn.
                execute("ALTER TABLE " + TABLE_NAME + " CONVERT PARTITION TO PARQUET LIST '" + INDEXED_PARTITION + "'");
                drainWalQueue();
                Assert.assertTrue("the reader must have something to reload", reader.reload());
                Assert.assertTrue(reader.openPartition(0) > 0);

                // Fixture guards: a new incarnation is exactly a new name txn, and
                // the two index txns must differ or the assertion below cannot
                // tell a refreshed cache from a retained one.
                Assert.assertNotEquals(
                        "the fixture must change the partition incarnation",
                        firstNameTxn,
                        reader.getTxFile().getPartitionNameTxn(0)
                );
                final long thirdIndexTxn = reader.getPartitionIndexTxn(0, symColumnIndex);
                Assert.assertTrue(
                        "the fixture must reseal under a new index txn, or the stale and fresh answers coincide"
                                + " [first=" + firstIndexTxn + ", third=" + thirdIndexTxn + ']',
                        thirdIndexTxn > firstIndexTxn
                );
                Assert.assertEquals(
                        PostingIndexUtils.PARQUET_INDEX_FORMAT_PARQUET,
                        reader.getPartitionIndexForm(0, symColumnIndex)
                );

                final IndexReader thirdReader = reader.getIndexReader(0, symColumnIndex, IndexReader.DIR_FORWARD);
                Assert.assertTrue(
                        "the resealed partition must dispatch to the parquet reader, got "
                                + thirdReader.getClass().getName(),
                        thirdReader instanceof AbstractParquetPostingIndexReader
                );
                Assert.assertEquals(
                        "the reader must be bound to the index txn this incarnation published",
                        thirdIndexTxn,
                        ((AbstractParquetPostingIndexReader) thirdReader).getIndexTxn()
                );
            }
        });
    }

    /**
     * The cost the index-form cache exists to remove. The refusal the dispatch
     * replaced used to resolve the partition's {@code _pm} footer and scan its
     * covering-index section on EVERY {@code getIndexReader} call, and that call
     * is made per page frame, per column and per KEY by the covering factory.
     * The dispatch reads the same three values, so it must not reintroduce it.
     * <p>
     * Asserted as a resolve count, not a duration: a stopwatch passes on a fast
     * machine whatever the call count is. The count also does not depend on the
     * memo the cache replaces -- that memo only ever recorded the EMPTY answer,
     * so on this fixture, whose section is not empty, every one of the calls
     * below used to miss it and resolve.
     */
    @Test
    public void testTheOnDiskIndexFormIsResolvedOncePerPartitionOpenNotOncePerCall() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_PARQUET_PARTITION_FORMAT, "parquet");
        assertMemoryLeak(() -> {
            createIndexedSparseKeyTable();
            try (TableReader reader = engine.getReader(engine.verifyTableName(TABLE_NAME))) {
                final int symColumnIndex = reader.getMetadata().getColumnIndex("sym");
                Assert.assertTrue(reader.openPartition(0) > 0);
                // Fixture guard: without a published token the loop below takes
                // the "nothing to refuse" exit and resolves nothing for a reason
                // that has nothing to do with the cache.
                Assert.assertEquals(
                        PostingIndexUtils.PARQUET_INDEX_FORMAT_PARQUET,
                        reader.getPartitionIndexForm(0, symColumnIndex)
                );

                final long before = reader.getParquetMetaReaderForTest().getFooterResolveCount();
                // Fixture guard: the counter must be the one the partition open
                // moved, or a zero delta below would prove only that it is dead.
                Assert.assertTrue(
                        "opening the partition must itself resolve a footer [before=" + before + ']',
                        before > 0
                );
                for (int i = 0; i < 64; i++) {
                    Assert.assertTrue(
                            reader.getIndexReader(0, symColumnIndex, IndexReader.DIR_FORWARD)
                                    instanceof AbstractParquetPostingIndexReader
                    );
                }
                Assert.assertEquals(
                        "the on-disk index form must be resolved at partition-open time,"
                                + " not once per getIndexReader call",
                        0,
                        reader.getParquetMetaReaderForTest().getFooterResolveCount() - before
                );
            }
        });
    }

    /**
     * The index form cache must not survive the mapping it was resolved from,
     * across a reseal that changes the answer without changing anything else
     * about the partition.
     * <p>
     * The first seal is native, so the {@code _pm} names no covering index and
     * the cached answer for {@code sym} is "native". The reseal is a token-only
     * append: same directory, same name txn, same row count, same
     * {@code data.parquet}. Only the {@code _pm} grows. A cache that outlived
     * its mapping would be a silent wrong answer rather than an error -- the
     * read would be served by the native reader off a chain the parquet seal
     * left with no visible generation, i.e. answered "no keys, no rows" instead
     * of refused.
     * <p>
     * It must be the SAME reader instance on both sides of the publish, which is
     * why this drives {@code TableReader} directly rather than running two
     * queries. Two queries do not test the cache at all: the DDL between them
     * evicts the pooled reader, and the second query gets a fresh reader with an
     * empty cache -- that shape passes with the invalidation removed, which is
     * how this test was first written and how the negative control caught it.
     */
    @Test
    public void testTheRefusalProbeIsNotSuppressedByAnEarlierNativeFormAnswer() throws Exception {
        assertMemoryLeak(() -> {
            // Default format: the seal is native and its sidecars are linked
            // into the parquet partition directory, so the _pm names no covering
            // index and the partition open caches exactly that.
            createIndexedSparseKeyTable();
            try (TableReader reader = engine.getReader(engine.verifyTableName(TABLE_NAME))) {
                Assert.assertTrue(reader.openPartition(0) > 0);
                final long mappedSize = reader.getParquetMetadataSize(0);
                Assert.assertTrue("the reader must hold a _pm mapping", mappedSize > 0);
                final int symColumnIndex = reader.getMetadata().getColumnIndex("sym");
                // Answers, off the form the partition open resolved.
                Assert.assertNotNull(reader.getIndexReader(0, symColumnIndex, IndexReader.DIR_FORWARD));

                // Re-seal the same partition in parquet form. A token-only
                // append: the partition directory, its name txn, its row count
                // and its data.parquet size are all unchanged, so the _pm's own
                // growth is the only thing that distinguishes the two answers.
                node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_PARQUET_PARTITION_FORMAT, "parquet");
                execute("ALTER TABLE " + TABLE_NAME + " ALTER COLUMN sym DROP INDEX");
                drainWalQueue();
                execute("ALTER TABLE " + TABLE_NAME + " ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, qty)");
                drainWalQueue();

                Assert.assertTrue("the reader must have something to reload", reader.reload());
                Assert.assertTrue(reader.openPartition(0) > 0);
                Assert.assertNotEquals(
                        "the fixture must move the mapped _pm size, or the reseal appended nothing",
                        mappedSize,
                        reader.getParquetMetadataSize(0)
                );

                final IndexReader resealed = reader.getIndexReader(0, symColumnIndex, IndexReader.DIR_FORWARD);
                Assert.assertTrue(
                        "the resealed partition must dispatch to the parquet reader, got "
                                + resealed.getClass().getName(),
                        resealed instanceof AbstractParquetPostingIndexReader
                );
            }
        });
    }

    @Test
    public void testPostingIndexReadDispatchesToParquetWhileTheParquetFormatIsSelected() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_PARQUET_PARTITION_FORMAT, "parquet");
        assertMemoryLeak(() -> {
            createIndexedSparseKeyTable();
            // The seal wrote the index as parquet and discarded the native
            // chain, which a NATIVE reader would read as "no keys, no rows" and
            // answer with an empty cursor. The parquet reader serves it instead,
            // and now serves it for real: the same rows the query returns over a
            // natively sealed partition.
            //
            // Comparing against the unindexed scan rather than a literal is what
            // makes this an oracle. A literal would still pass if the cursor and
            // the seal drifted together; the scan reads the parquet partition
            // without consulting the index at all.
            // Task 4 gave the reader a working row cursor, but the covering
            // path also needs the covered values, which Task 7 projects. Until
            // then this refuses rather than returning the rows with no price
            // and qty -- or, worse, no rows at all.
            assertQuery(COVERED_QUERY)
                    .failsWith("parquet-form covering index cannot project covered columns yet");
        });
    }

    @Test
    public void testPostingIndexReadDispatchesToParquetAfterTheFormatIsFlippedBackToNative() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_PARQUET_PARTITION_FORMAT, "parquet");
        assertMemoryLeak(() -> {
            createIndexedSparseKeyTable();
            // The hole a format-keyed dispatch leaves open. The partition is
            // already sealed as parquet and its token is published; flipping the
            // property back says nothing about what is on disk. A dispatch keyed
            // on the configured format would send this to the native reader and
            // answer it with an empty cursor.
            node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_PARQUET_PARTITION_FORMAT, "native");
            engine.releaseInactive();
            // Dispatch keys on the published token, so the parquet reader is
            // still the one that serves this -- proved by WHICH refusal comes
            // back. A format-keyed dispatch would have sent it to the native
            // reader, which answers an empty cursor with no error at all.
            assertQuery(COVERED_QUERY)
                    .failsWith("parquet-form covering index cannot project covered columns yet");
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

                final IndexReader reader = pinned.getIndexReader(0, symColumnIndex, IndexReader.DIR_FORWARD);
                Assert.assertTrue(
                        "dispatch must bind a parquet-form reader, not a native one [reader="
                                + reader.getClass().getSimpleName() + ']',
                        reader instanceof AbstractParquetPostingIndexReader
                );
                Assert.assertEquals(
                        "the pinned reader must be bound to the token ITS OWN snapshot publishes,"
                                + " not the writer's latest",
                        firstIndexTxn,
                        ((AbstractParquetPostingIndexReader) reader).getIndexTxn()
                );
            }
        });
    }

    /**
     * A partition open that TEARS leaves the partition slot marked closed while
     * this reader still holds the {@code _pm} mapping and the index forms
     * resolved from it -- and nothing collects that state before the next open.
     * <p>
     * {@code openMissingColumnsInPartition}'s catch block sets the slot's SIZE
     * word to -1 and its ACTIVE_COLUMNS_OPEN word to 0 WITHOUT going through
     * {@code closeParquetPartition}, and neither {@code closeExcessPartitions}
     * nor {@code reconcileOpenPartitions0} collects such a slot afterwards --
     * both guard on {@code openPartitionSize > -1}. So the next
     * {@code openPartition} re-enters {@code openParquetMetadata} with the
     * previous mapping's answer still in the list. That matters because
     * {@code cacheParquetIndexForms} appends and {@code indexFormEntryOffset}
     * returns the FIRST match: a survivor outranks the entry just resolved, and
     * the reader hands out an {@code index_txn} naming an artifact pair a later
     * seal superseded and the purge may already have unlinked.
     * <p>
     * Driven entirely through production. {@code setActiveColumns} is the
     * production lever that clears ACTIVE_COLUMNS_OPEN on an open partition
     * (goPassive takes the same route through {@code resetAllColumnsOpenFlag}),
     * and the throw is a real {@code openRO} refusal of {@code sym2}'s posting
     * key file inside {@code reloadColumnAt}, which is the only I/O
     * {@code reloadColumnAt} performs for a PARQUET partition. Hence the
     * fixture's two indexed columns: {@code sym} carries the parquet-form token
     * that can go stale, {@code sym2} is sealed natively so it has a native
     * index reader to fail on.
     * <p>
     * The reopen is made to resolve a DIFFERENT answer -- {@code DROP INDEX}
     * retires the token, so the fresh answer is "no covering index at all". That
     * is the {@code n == 0} shape, and it is the worse of the two: a partition
     * that stops publishing takes {@code cacheParquetIndexForms}' early return,
     * so a stale entry survives WHOLE rather than merely being shadowed by a
     * fresh one.
     * <p>
     * <b>What this test does and does not discriminate.</b> Two independent
     * clears stand between this shape and a stale answer:
     * {@code invalidateIndexFormCache} at the top of {@code openParquetMetadata}
     * and the {@code existing.clear()} inside {@code cacheParquetIndexForms}.
     * The latter is the ONLY call site of the former's successor -- the
     * invalidate runs unconditionally, immediately above the remap, with nothing
     * between them -- so removing the clear alone cannot fail this or any other
     * test: it is dominated. Removing the invalidate alone, or both, does fail
     * it, with the stale {@code index_txn} of the retired token. The clear is
     * defence in depth for a route added later that skips the invalidate; this
     * test is what closes the gap the invalidate itself had, which no test
     * covered.
     */
    @Test
    public void testATornPartitionOpenDoesNotStrandTheCachedIndexForm() throws Exception {
        final boolean[] armed = {false};
        final boolean[] fired = {false};
        ff = new TestFilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                if (armed[0] && name != null && Utf8s.containsAscii(name, "sym2.pk")) {
                    fired[0] = true;
                    return -1;
                }
                return super.openRO(name);
            }
        };
        assertMemoryLeak(ff, () -> {
            inputRoot = root;
            createTwoIndexFormsTable();
            try (TableReader reader = engine.getReader(engine.verifyTableName(TORN_TABLE_NAME))) {
                final int symColumnIndex = reader.getMetadata().getColumnIndex("sym");
                final int sym2ColumnIndex = reader.getMetadata().getColumnIndex("sym2");
                Assert.assertTrue(reader.openPartition(0) > 0);

                // Fixture guards. sym must carry a parquet-form token, or there
                // is nothing that can go stale; sym2 must NOT, or its index
                // reader -- the throw this test needs -- is refused instead of
                // created.
                Assert.assertEquals(
                        "the fixture must seal sym as parquet",
                        PostingIndexUtils.PARQUET_INDEX_FORMAT_PARQUET,
                        reader.getPartitionIndexForm(0, symColumnIndex)
                );
                final long firstIndexTxn = reader.getPartitionIndexTxn(0, symColumnIndex);
                Assert.assertTrue("a published token names a real index txn", firstIndexTxn >= 0);
                Assert.assertEquals(
                        "the fixture must seal sym2 natively",
                        PostingIndexUtils.PARQUET_INDEX_FORMAT_NATIVE,
                        reader.getPartitionIndexForm(0, sym2ColumnIndex)
                );
                // Populates indexes[primaryIndex] for sym2, which is what makes
                // reloadColumnAt open a file at all on a parquet partition. Only
                // the BACKWARD direction lands in the slot reloadColumnAt reads.
                Assert.assertNotNull(reader.getIndexReader(0, sym2ColumnIndex, IndexReader.DIR_BACKWARD));

                // Tear the open. setActiveColumns clears ACTIVE_COLUMNS_OPEN
                // while the slot stays open, so openPartition routes into
                // openMissingColumnsInPartition rather than openPartition0.
                reader.setActiveColumns(null);
                armed[0] = true;
                try {
                    reader.openPartition(0);
                    Assert.fail("the partition open must fail while sym2's key file cannot be opened");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "sym2.pk");
                } finally {
                    armed[0] = false;
                }
                Assert.assertTrue("the injected open failure never fired", fired[0]);

                // The state the catch block leaves, asserted rather than assumed:
                // the slot says closed, the _pm mapping is still held, and the
                // forms resolved from it are still in the list.
                Assert.assertEquals(
                        "the catch block must mark the partition closed",
                        -1,
                        reader.getPartitionRowCount(0)
                );
                Assert.assertTrue(
                        "the torn open must leave the _pm mapping in place -- that is what makes"
                                + " the next open re-enter with a populated list",
                        reader.getParquetMetadataSize(0) > 0
                );
                Assert.assertEquals(
                        "the torn open must leave the cached form in place, or the reopen below"
                                + " has nothing to be stale about",
                        PostingIndexUtils.PARQUET_INDEX_FORMAT_PARQUET,
                        reader.getPartitionIndexForm(0, symColumnIndex)
                );

                // The partition stops publishing a token. Same directory, same
                // name txn, same data.parquet: only the _pm grows a footer whose
                // covering section is empty.
                final long nameTxnBefore = reader.getTxFile().getPartitionNameTxn(0);
                execute("ALTER TABLE " + TORN_TABLE_NAME + " ALTER COLUMN sym DROP INDEX");
                drainWalQueue();
                Assert.assertEquals(
                        "premise: the retirement must not move the partition directory, or this"
                                + " reader's stale name txn sends the reopen at the OLD _pm and the"
                                + " two answers coincide",
                        nameTxnBefore,
                        currentPartitionNameTxn(TORN_TABLE_NAME)
                );
                try (Path path = new Path()) {
                    assertNoCoveringIndexToken(path, TORN_TABLE_NAME, nameTxnBefore);
                }

                // Deliberately no reload(): a reload would close the partition
                // through reconcileOpenPartitions0 and invalidate the cache the
                // ordinary way, which is the path that is already covered.
                Assert.assertTrue(reader.openPartition(0) > 0);
                Assert.assertEquals(
                        "the reopen must resolve the CURRENT _pm, which publishes nothing --"
                                + " a stale entry from the torn open's mapping is a superseded"
                                + " index txn [stale=" + firstIndexTxn + ']',
                        PostingIndexUtils.PARQUET_INDEX_FORMAT_NATIVE,
                        reader.getPartitionIndexForm(0, symColumnIndex)
                );
                Assert.assertEquals(-1, reader.getPartitionIndexTxn(0, symColumnIndex));
                Assert.assertEquals(0, reader.getPartitionIndexImFileSize(0, symColumnIndex));

                // And on the production path: this reader's metadata still calls
                // sym POSTING-indexed, so the dispatch still runs, and a stale
                // entry would send the read to a parquet reader over a retired
                // artifact pair.
                Assert.assertFalse(
                        "the dispatch keyed on a form resolved from a mapping this reopen replaced",
                        reader.getIndexReader(0, symColumnIndex, IndexReader.DIR_BACKWARD)
                                instanceof AbstractParquetPostingIndexReader
                );
            }
        });
    }

    /**
     * Why the cache is keyed by column id inside a partition rather than laid
     * out densely over the {@code (partition, column)} grid that {@code columns},
     * {@code columnTops} and {@code indexes} use.
     * <p>
     * {@code ALTER TABLE ... DROP COLUMN} shifts every later READER column index
     * down. It does not shift writer indexes: QuestDB keeps the dropped column's
     * {@code _meta} slot with a negated type, so the ids the {@code _pm} records
     * -- {@code TableWriter} stages the token with {@code getWriterIndex()} and
     * retires with {@code writerIndex} -- are exactly the ids that survive.
     * A dense per-reader-index cache is shifted by nothing:
     * {@code reshuffleColumns} moves {@code columns} / {@code columnTops} /
     * {@code indexes} and never touches {@code parquetMetadataPartitions}, so
     * every entry above the drop would re-point at its neighbour's -- a silently
     * wrong {@code index_txn}, or a silent "no covering index" for a column that
     * has one.
     * <p>
     * So this drops {@code doomed}, which sits between the two indexed columns,
     * and asserts each survivor still resolves ITS OWN token. Under a dense
     * layout {@code sym2} would move from reader index 3 to 2 and read
     * {@code doomed}'s empty slot.
     * <p>
     * <b>The masking question, answered in the assertions below.</b> A dense
     * layout is only observably wrong if the partition is still open, with the
     * cache it had before the drop, when the shifted index is used to read it:
     * a close rebuilds either layout correctly. {@code reconcileOpenPartitions0}
     * closes every open parquet partition it visits, so the question is whether
     * the drop reaches it. It does not for this shape -- the fast path in
     * {@code reconcileOpenPartitions} fires whenever the partition table version
     * and the column version are both unchanged, and it examines only the LAST
     * partition, which the indexed one is not. That fact is asserted rather than
     * described, so the day it changes this test says so.
     */
    @Test
    public void testDroppingAColumnBeforeAnIndexedOneKeepsEveryTokenWithItsOwnColumn() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_PARQUET_PARTITION_FORMAT, "parquet");
        assertMemoryLeak(() -> {
            inputRoot = root;
            createTwoIndexedColumnsTable();
            try (TableReader reader = engine.getReader(engine.verifyTableName(DROP_COLUMN_TABLE_NAME))) {
                final int symIndexBefore = reader.getMetadata().getColumnIndex("sym");
                final int sym2IndexBefore = reader.getMetadata().getColumnIndex("sym2");
                final int symWriterIndex = reader.getMetadata().getWriterIndex(symIndexBefore);
                final int sym2WriterIndex = reader.getMetadata().getWriterIndex(sym2IndexBefore);
                Assert.assertTrue(reader.openPartition(0) > 0);

                final long symIndexTxn = reader.getPartitionIndexTxn(0, symIndexBefore);
                final long sym2IndexTxn = reader.getPartitionIndexTxn(0, sym2IndexBefore);
                // Fixture guards: both columns must publish a token, and the two
                // tokens must differ, or an entry read off the wrong column is
                // indistinguishable from the right one.
                Assert.assertEquals(
                        PostingIndexUtils.PARQUET_INDEX_FORMAT_PARQUET,
                        reader.getPartitionIndexForm(0, symIndexBefore)
                );
                Assert.assertEquals(
                        PostingIndexUtils.PARQUET_INDEX_FORMAT_PARQUET,
                        reader.getPartitionIndexForm(0, sym2IndexBefore)
                );
                Assert.assertNotEquals(
                        "the fixture must seal the two columns under different index txns",
                        symIndexTxn,
                        sym2IndexTxn
                );

                final long columnVersionBefore = reader.getTxFile().getColumnVersion();
                final long partitionTableVersionBefore = reader.getTxFile().getPartitionTableVersion();

                execute("ALTER TABLE " + DROP_COLUMN_TABLE_NAME + " DROP COLUMN doomed");
                drainWalQueue();
                Assert.assertTrue("the reader must have something to reload", reader.reload());

                // Premise 1: the reader indexes really did shift, and the writer
                // indexes really did not. Without both, nothing distinguishes the
                // two layouts.
                final int symIndexAfter = reader.getMetadata().getColumnIndex("sym");
                final int sym2IndexAfter = reader.getMetadata().getColumnIndex("sym2");
                Assert.assertEquals(
                        "premise: sym sits before the drop, so its reader index must NOT move",
                        symIndexBefore,
                        symIndexAfter
                );
                Assert.assertEquals(
                        "premise: sym2 sits after the drop, so its reader index must shift down"
                                + " -- that shift is the whole hazard [before=" + sym2IndexBefore + ']',
                        sym2IndexBefore - 1,
                        sym2IndexAfter
                );
                Assert.assertEquals(
                        "premise: the writer index -- the id the _pm records -- must survive the drop",
                        sym2WriterIndex,
                        reader.getMetadata().getWriterIndex(sym2IndexAfter)
                );
                Assert.assertEquals(symWriterIndex, reader.getMetadata().getWriterIndex(symIndexAfter));

                // Premise 2: the partition is still open, with the cache and the
                // mapping it had before the drop. A close would rebuild either
                // layout from the _pm and mask the difference outright, which is
                // what makes this the negative control for the keying choice
                // rather than a test that passes for the wrong reason.
                Assert.assertEquals(
                        "premise: the drop must not move the column version, or reconcileOpenPartitions"
                                + " takes its slow path and closes the partition",
                        columnVersionBefore,
                        reader.getTxFile().getColumnVersion()
                );
                Assert.assertEquals(
                        "premise: the drop must not move the partition table version, for the same reason",
                        partitionTableVersionBefore,
                        reader.getTxFile().getPartitionTableVersion()
                );
                Assert.assertTrue(
                        "premise: the drop must leave the partition open, or a dense layout"
                                + " would be rebuilt by the reopen and the two layouts are"
                                + " indistinguishable through production",
                        reader.getPartitionRowCount(0) > -1
                );

                // The verdict. A dense (partition, column) cache answers these
                // with doomed's slot for sym2.
                Assert.assertEquals(
                        "sym2 must still resolve its own token after the shift",
                        sym2IndexTxn,
                        reader.getPartitionIndexTxn(0, sym2IndexAfter)
                );
                Assert.assertEquals(
                        PostingIndexUtils.PARQUET_INDEX_FORMAT_PARQUET,
                        reader.getPartitionIndexForm(0, sym2IndexAfter)
                );
                Assert.assertEquals(
                        "sym must still resolve its own token after the shift",
                        symIndexTxn,
                        reader.getPartitionIndexTxn(0, symIndexAfter)
                );

                // And on the production path, where dispatch binds each reader to
                // the token its own column publishes. This is the assertion the
                // dense (partition, column) layout fails: after the shift, sym2's
                // reader index addresses what sym's used to, so a cache keyed by
                // reader index hands sym2 the wrong token -- or none at all.
                assertBoundParquetReader(reader, sym2IndexAfter, sym2IndexTxn);
                assertBoundParquetReader(reader, symIndexAfter, symIndexTxn);
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
     * Asserts that {@code getIndexReader} dispatched a PARQUET-form reader for
     * {@code columnIndex} and bound it to {@code expectedIndexTxn}.
     * <p>
     * The txn is the whole point. A parquet-form reader that binds the wrong
     * generation reads a superseded artifact pair -- one the purge is entitled
     * to unlink, because the scoreboard window protects only the generation the
     * partition currently publishes -- and does so without any error. So a test
     * that asserted only "a parquet reader came back" would pass against a
     * dispatch that resolves the writer's latest token instead of this
     * snapshot's, which is the failure the whole cache exists to prevent.
     */
    private static void assertBoundParquetReader(TableReader reader, int columnIndex, long expectedIndexTxn) {
        final IndexReader indexReader = reader.getIndexReader(0, columnIndex, IndexReader.DIR_FORWARD);
        Assert.assertTrue(
                "dispatch must bind a parquet-form reader for a column whose token is published"
                        + " [column=" + reader.getMetadata().getColumnName(columnIndex)
                        + ", reader=" + indexReader.getClass().getSimpleName() + ']',
                indexReader instanceof AbstractParquetPostingIndexReader
        );
        Assert.assertEquals(
                "the reader must be bound to the token this column publishes"
                        + " [column=" + reader.getMetadata().getColumnName(columnIndex) + ']',
                expectedIndexTxn,
                ((AbstractParquetPostingIndexReader) indexReader).getIndexTxn()
        );
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

    /**
     * The number of covering-index entries carried by the footer {@code stepsBack}
     * {@code prev} links below the {@code _pm}'s physical tail. {@code 0} is the
     * tail itself. Walks with the same {@code resolveLastFooter} /
     * {@code resolvePrevFooter} pair the orphan sweep's chain walk uses, so a
     * test can pin the footer the sweep saw rather than the one that happens to
     * resolve for the current committed parquet size.
     */
    private int coveringIndexCountAtTail(Path path, String tableName, int stepsBack) {
        final FilesFacade ff = configuration.getFilesFacade();
        final ParquetMetaFileReader reader = new ParquetMetaFileReader();
        final long addr = ParquetMetaFileReader.openAndMapRO(
                ff,
                partitionPath(path, tableName).concat(TableUtils.PARQUET_METADATA_FILE_NAME).$(),
                reader
        );
        Assert.assertTrue("_pm must be readable", addr != 0);
        final long fileSize = reader.getFileSize();
        try {
            Assert.assertTrue(reader.resolveLastFooter());
            for (int i = 0; i < stepsBack; i++) {
                Assert.assertTrue(
                        "the _pm chain must be at least " + (stepsBack + 1) + " footers deep",
                        reader.resolvePrevFooter()
                );
            }
            return reader.getCoveringIndexCount();
        } finally {
            reader.clear();
            ff.munmap(addr, fileSize, MemoryTag.MMAP_PARQUET_METADATA_READER);
        }
    }

    private long committedTxn(String tableName) {
        try (TableReader reader = engine.getReader(engine.verifyTableName(tableName))) {
            return reader.getTxn();
        }
    }

    private long committedParquetFileSize(String tableName) {
        try (TableReader reader = engine.getReader(engine.verifyTableName(tableName))) {
            return reader.getTxFile().getPartitionParquetFileSize(0);
        }
    }

    /**
     * The index txn the {@code _pm} publishes for {@code columnId} to a reader
     * whose committed {@code data.parquet} size is {@code parquetFileSize}, or
     * {@link Long#MIN_VALUE} when that footer names no token for the column.
     * <p>
     * Resolves the way every reader does -- {@code resolveFooter(committedSize)},
     * as in {@code TableReader}, {@code ParquetPartitionDecoder} and
     * {@code O3PartitionJob} -- so passing an older committed size is how a test
     * asks what a reader pinned to an older snapshot still sees.
     */
    private long publishedIndexTxnAt(Path path, String tableName, long parquetFileSize, int columnId) {
        final FilesFacade ff = configuration.getFilesFacade();
        final ParquetMetaFileReader reader = new ParquetMetaFileReader();
        final long addr = ParquetMetaFileReader.openAndMapRO(
                ff,
                partitionPath(path, tableName).concat(TableUtils.PARQUET_METADATA_FILE_NAME).$(),
                reader
        );
        Assert.assertTrue("_pm must be readable", addr != 0);
        final long fileSize = reader.getFileSize();
        try {
            Assert.assertTrue(
                    "_pm footer must resolve for parquet size " + parquetFileSize,
                    reader.resolveFooter(parquetFileSize)
            );
            for (int i = 0, n = reader.getCoveringIndexCount(); i < n; i++) {
                if (reader.getCoveringIndexColumnId(i) == columnId) {
                    return reader.getCoveringIndexTxn(i);
                }
            }
            return Long.MIN_VALUE;
        } finally {
            reader.clear();
            ff.munmap(addr, fileSize, MemoryTag.MMAP_PARQUET_METADATA_READER);
        }
    }

    /**
     * The fields of the partition's own {@code _txn} record a per-partition
     * consumer can compare, folded together: row count, name txn and the
     * offset-3 value word (the {@code data.parquet} size for a parquet
     * partition). Deliberately excludes the squash counter, which is the thing
     * under test.
     */
    private static long partitionRecordDigest(TableReader reader) {
        final TxReader tx = reader.getTxFile();
        long h = tx.getPartitionSize(0);
        h = h * 31 + tx.getPartitionNameTxn(0);
        h = h * 31 + tx.getPartitionParquetFileSize(0);
        return h;
    }

    private long committedPartitionTableVersion(String tableName) {
        try (TableReader reader = engine.getReader(engine.verifyTableName(tableName))) {
            return reader.getTxFile().getPartitionTableVersion();
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
     * Runs a query over the emitted index parquet on BOTH {@code read_parquet}
     * paths, which project the file by different means: the serial cursor by
     * parquet index, the parallel page-frame cursor by field id through a
     * {@code ColumnMapping}.
     * <p>
     * Only the second can be wrong about the synthetic {@code key_id} and
     * {@code row_id}, whose field id is -1 because the {@code _im} writer
     * requires exactly that to tell them from the covered columns, and it once
     * was: both it and the map it resolves ids through substituted the parquet
     * POSITION for a negative id, so {@code key_id} (parquet column 0) took id 0
     * and aliased onto the covered column whose writer index is 0 - the
     * designated timestamp, here - serving its page truncated to 32 bits. These
     * oracles ran on the serial path alone to route around that, which made them
     * depend on {@code cairo.sql.parallel.read.parquet.enabled}, a config a user
     * can flip. Both paths are asserted now.
     * <p>
     * Which arm failed is the whole point of the label, so everything an arm
     * can throw is labelled, not only {@code AssertionError} - a
     * {@code CairoException} raised on one path and not the other is exactly
     * the asymmetry worth naming. The label is built from {@code toString()}
     * rather than {@code getMessage()}: a message-less {@code AssertionError}
     * would otherwise report a line ending in "null". The original is chained
     * either way, so nothing is lost.
     */
    private void assertIndexParquetQuery(String sql, String expected) throws Exception {
        final boolean wasParallel = sqlExecutionContext.isParallelReadParquetEnabled();
        try {
            for (int i = 0; i < 2; i++) {
                final boolean parallel = i == 0;
                sqlExecutionContext.setParallelReadParquetEnabled(parallel);
                try {
                    assertQuery(sql).inferRandomAccess().expectSize().returns(expected);
                } catch (Throwable th) {
                    throw new AssertionError("[parallelReadParquet=" + parallel + "] " + th, th);
                }
            }
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
     * A parquet partition whose {@code _pm} publishes a covering-index token for
     * TWO columns, sealed in separate passes so the two tokens name different
     * index txns, with an unindexed {@code doomed} column between them: writer
     * ids ts 0, sym 1, doomed 2, sym2 3.
     */
    private void createTwoIndexedColumnsTable() throws Exception {
        execute("CREATE TABLE " + DROP_COLUMN_TABLE_NAME + " (" +
                "ts TIMESTAMP, sym SYMBOL, doomed SYMBOL, sym2 SYMBOL, price DOUBLE, qty LONG" +
                ") TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("INSERT INTO " + DROP_COLUMN_TABLE_NAME + " SELECT" +
                " dateadd('u', x::INT, '" + INDEXED_PARTITION + "T00:00:00Z'::TIMESTAMP)," +
                " CASE WHEN x % 4 = 0 THEN 's0' WHEN x % 4 = 1 THEN 's7' ELSE 's15' END," +
                " 'd' || (x % 4)," +
                " 'o' || (x % 4)," +
                " x::DOUBLE," +
                " x" +
                " FROM long_sequence(" + TWO_SYMBOL_ROW_COUNT + ")");
        drainWalQueue();
        // A later partition, so the indexed one is not the active partition.
        execute("INSERT INTO " + DROP_COLUMN_TABLE_NAME + " VALUES ('2024-01-02T00:00:00Z', 's0', 'd0', 'o0', 1.0, 1)");
        drainWalQueue();
        execute("ALTER TABLE " + DROP_COLUMN_TABLE_NAME + " CONVERT PARTITION TO PARQUET LIST '" + INDEXED_PARTITION + "'");
        drainWalQueue();
        execute("ALTER TABLE " + DROP_COLUMN_TABLE_NAME + " ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price)");
        drainWalQueue();
        execute("ALTER TABLE " + DROP_COLUMN_TABLE_NAME + " ALTER COLUMN sym2 ADD INDEX TYPE POSTING INCLUDE (qty)");
        drainWalQueue();
        engine.releaseInactive();
    }

    /**
     * A parquet partition carrying ONE covering index of each on-disk form:
     * {@code sym} sealed as parquet, so the {@code _pm} publishes a token for
     * it, and {@code sym2} sealed natively, so its {@code .pk} / {@code .pv} /
     * {@code .pc} sidecars are linked into the same directory and a native index
     * reader can be opened over it.
     * <p>
     * The order is load-bearing: the format property is read at seal time, so
     * {@code sym2} must be indexed while it still says {@code native}. Sealing
     * {@code sym} afterwards touches only {@code sym}'s entry --
     * {@code publishParquetIndexTokens} copies forward whatever it does not
     * reseal, and {@code sym2} has nothing to copy.
     */
    private void createTwoIndexFormsTable() throws Exception {
        execute("CREATE TABLE " + TORN_TABLE_NAME + " (" +
                "ts TIMESTAMP, sym SYMBOL, sym2 SYMBOL, price DOUBLE, qty LONG" +
                ") TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("INSERT INTO " + TORN_TABLE_NAME + " SELECT" +
                " dateadd('u', x::INT, '" + INDEXED_PARTITION + "T00:00:00Z'::TIMESTAMP)," +
                " CASE WHEN x % 4 = 0 THEN 's0' WHEN x % 4 = 1 THEN 's7' ELSE 's15' END," +
                " 'o' || (x % 4)," +
                " x::DOUBLE," +
                " x" +
                " FROM long_sequence(" + TWO_SYMBOL_ROW_COUNT + ")");
        drainWalQueue();
        // A later partition, so the indexed one is not the active partition.
        execute("INSERT INTO " + TORN_TABLE_NAME + " VALUES ('2024-01-02T00:00:00Z', 's0', 'o0', 1.0, 1)");
        drainWalQueue();
        execute("ALTER TABLE " + TORN_TABLE_NAME + " CONVERT PARTITION TO PARQUET LIST '" + INDEXED_PARTITION + "'");
        drainWalQueue();
        execute("ALTER TABLE " + TORN_TABLE_NAME + " ALTER COLUMN sym2 ADD INDEX TYPE POSTING INCLUDE (price)");
        drainWalQueue();
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_PARQUET_PARTITION_FORMAT, "parquet");
        execute("ALTER TABLE " + TORN_TABLE_NAME + " ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price)");
        drainWalQueue();
        engine.releaseInactive();
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
