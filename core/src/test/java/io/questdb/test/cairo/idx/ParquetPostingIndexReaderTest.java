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

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.idx.AbstractParquetPostingIndexReader;
import io.questdb.cairo.idx.CoveringRowCursor;
import io.questdb.cairo.idx.IndexReader;
import io.questdb.cairo.idx.ParquetPostingIndexFwdReader;
import io.questdb.cairo.idx.PostingIndexFwdReader;
import io.questdb.cairo.idx.PostingIndexReader;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.std.Chars;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Dispatch: a partition whose {@code _pm} publishes a covering-index token must
 * be served by the parquet-form reader, and one whose index is a native sidecar
 * set must not be -- whatever
 * {@code cairo.posting.index.parquet.partition.format} currently says, since
 * that property describes the NEXT seal rather than what is on disk.
 * <p>
 * Both fixtures below convert the SAME partition to parquet and index the same
 * column; they differ only in the format the seal ran under. So the on-disk
 * index form is the only thing that can distinguish them, which is what makes
 * the pair a control for each other.
 */
public class ParquetPostingIndexReaderTest extends AbstractCairoTest {

    private static final String INDEXED_PARTITION = "2024-01-01";
    private static final int ROW_COUNT = 50_000;

    @Test
    public void testANativeSealedParquetPartitionStillDispatchesToTheNativeReader() throws Exception {
        assertMemoryLeak(() -> {
            // Format left at the default 'native': the partition converts to
            // parquet but its index stays a native sidecar set hard-linked into
            // the parquet partition directory. Dispatch must follow the ON-DISK
            // form, so this must NOT reach the parquet reader.
            createNativeSealedParquetTable("y");
            try (TableReader reader = engine.getReader(engine.verifyTableName("y"))) {
                final int columnIndex = reader.getMetadata().getColumnIndex("sym");
                final IndexReader indexReader = reader.getIndexReader(0, columnIndex, IndexReader.DIR_FORWARD);
                Assert.assertTrue(
                        "a natively sealed partition must dispatch to the native reader, got "
                                + indexReader.getClass().getName(),
                        indexReader instanceof PostingIndexFwdReader
                );
            }
        });
    }

    @Test
    public void testAParquetSealedPartitionDispatchesToTheParquetReader() throws Exception {
        assertMemoryLeak(() -> {
            createIndexedParquetTable("x");
            try (TableReader reader = engine.getReader(engine.verifyTableName("x"))) {
                final int columnIndex = reader.getMetadata().getColumnIndex("sym");
                final IndexReader indexReader = reader.getIndexReader(0, columnIndex, IndexReader.DIR_FORWARD);
                Assert.assertTrue(
                        "a parquet-sealed partition must dispatch to the parquet reader, got "
                                + indexReader.getClass().getName(),
                        indexReader instanceof ParquetPostingIndexFwdReader
                );
                Assert.assertTrue(indexReader instanceof PostingIndexReader);
                Assert.assertTrue(indexReader.isOpen());
                // The token is what the reader was bound with, and it is not
                // derivable from anything else on the call path.
                final AbstractParquetPostingIndexReader parquetReader = (AbstractParquetPostingIndexReader) indexReader;
                Assert.assertEquals(reader.getPartitionIndexTxn(0, columnIndex), parquetReader.getIndexTxn());
                Assert.assertEquals(reader.getPartitionIndexImFileSize(0, columnIndex), parquetReader.getImFileSize());
                // KEY_SPACE_SIZE, the exclusive upper bound on key ids, not a
                // distinct-key count. The fixture's three symbols are ids 0, 7
                // and 15, so the bound is 15 + 1 for the null slot, plus one.
                Assert.assertEquals(17, indexReader.getKeyCount());
                // The five mmap-shaped methods have no meaning for a
                // parquet-backed reader. They return 0: LatestByAllIndexed is
                // built only for IndexType.BITMAP so a POSTING reader never
                // reaches it, and TouchTableFunctionFactory.touchMemory guards
                // baseAddress == 0, so a 0 degrades touch_table() to a no-op.
                Assert.assertEquals(0, indexReader.getKeyBaseAddress());
                Assert.assertEquals(0, indexReader.getValueBaseAddress());
                Assert.assertEquals(0, indexReader.getKeyMemorySize());
                Assert.assertEquals(0, indexReader.getValueMemorySize());
                Assert.assertEquals(0, indexReader.getValueBlockCapacity());
            }
        });
    }

    /**
     * A partition close/reopen inside one reader must not leave the parquet-form
     * reader bound to a mapping the reopen replaced, and must not route it
     * through {@code reloadColumnAt}'s nine-argument {@code of()}, which cannot
     * name an {@code index_txn}-suffixed artifact pair and therefore throws.
     * <p>
     * {@code goPassive} / {@code closeExcessPartitions} do exactly this within
     * one txn, so the shape is not hypothetical. DIR_BACKWARD specifically: that
     * is the only direction whose reader is cached in the slot
     * {@code reloadColumnAt} rebinds.
     */
    @Test
    public void testAParquetReaderSurvivesAPartitionCloseAndReopen() throws Exception {
        assertMemoryLeak(() -> {
            createIndexedParquetTable("x");
            try (TableReader reader = engine.getReader(engine.verifyTableName("x"))) {
                final int columnIndex = reader.getMetadata().getColumnIndex("sym");
                Assert.assertTrue(
                        reader.getIndexReader(0, columnIndex, IndexReader.DIR_BACKWARD)
                                instanceof AbstractParquetPostingIndexReader
                );
                reader.goPassive();
                reader.goActive();
                Assert.assertTrue(reader.openPartition(0) > 0);
                final IndexReader reopened = reader.getIndexReader(0, columnIndex, IndexReader.DIR_BACKWARD);
                Assert.assertTrue(
                        "a reopened parquet partition must still dispatch to the parquet reader, got "
                                + reopened.getClass().getName(),
                        reopened instanceof AbstractParquetPostingIndexReader
                );
                Assert.assertTrue(reopened.isOpen());
                Assert.assertEquals(
                        reader.getPartitionIndexTxn(0, columnIndex),
                        ((AbstractParquetPostingIndexReader) reopened).getIndexTxn()
                );
            }
        });
    }

    /**
     * {@code LatestByAllIndexedRecordCursor} is the one caller that consumes
     * {@code getKeyBaseAddress} / {@code getValueBaseAddress} with no guard at
     * all, handing both straight to native code -- so a reader answering 0 there
     * would feed it a null pointer. It cannot receive one: its factory is built
     * only when the column's index type is {@code IndexType.BITMAP}, at both
     * construction sites in {@code SqlCodeGenerator}, and only a POSTING seal
     * publishes a covering-index token. Pinned here on the plan, so a later
     * change that relaxes that gate fails this rather than crashing the JVM.
     */
    @Test
    public void testLatestOnOverAParquetSealedIndexNeverReachesTheUnguardedNativeConsumer() throws Exception {
        assertMemoryLeak(() -> {
            createIndexedParquetTable("x");
            printSql("EXPLAIN SELECT * FROM x LATEST ON ts PARTITION BY sym");
            Assert.assertFalse(
                    "LATEST ON over a POSTING index must not plan LatestByAllIndexed,"
                            + " whose cursor dereferences getKeyBaseAddress unguarded [plan=" + sink + ']',
                    Chars.contains(sink, "LatestByAllIndexed")
            );
        });
    }

    /**
     * {@code touch_table()} walks every indexed column of every page frame and
     * pre-touches the index key and value mappings. A parquet-form reader has
     * neither, and reports both base addresses as 0; {@code touchMemory} returns
     * zero pages for a 0 address, so the call succeeds and reports no index
     * pages rather than dereferencing anything.
     */
    @Test
    public void testTouchTableOverAParquetBackedCoveringIndexSucceeds() throws Exception {
        assertMemoryLeak(() -> {
            createIndexedParquetTable("x");
            // The function is spelled touch(cursor), not touch_table(name).
            // Scoped to the parquet partition: the fixture's other partition is
            // native and its index reader touches real pages, which would make
            // a whole-table count non-zero for a reason unrelated to this.
            printSql("SELECT touch(SELECT * FROM x WHERE ts IN '" + INDEXED_PARTITION + "')");
            TestUtils.assertContains(sink, "\"index_key_pages\":0");
            TestUtils.assertContains(sink, "\"index_values_pages\": 0");
        });
    }

    /**
     * The placeholder cursor. It throws rather than answering empty, because an
     * empty cursor would turn every indexed query over a parquet-sealed
     * partition into a silent empty result -- the exact failure the refusal this
     * dispatch replaced existed to prevent. Phase 2C Task 4 replaces it, and
     * this test is what makes an unfinished 2C impossible to ship silently.
     */
    /**
     * The backward cursor must be the forward one reversed, exactly -- same
     * postings, opposite order.
     * <p>
     * Comparing against the forward cursor's output rather than against a
     * literal is what makes this an oracle: the two share the directory lookup
     * and the zone-map skip but not the traversal, so a defect in either
     * traversal shows up as a disagreement. A test asserting only "descending"
     * would pass on a cursor that silently dropped a group.
     */
    @Test
    public void testTheBackwardCursorMirrorsTheForwardOne() throws Exception {
        assertMemoryLeak(() -> {
            createIndexedParquetTable("x");
            try (TableReader reader = engine.getReader(engine.verifyTableName("x"))) {
                final int columnIndex = reader.getMetadata().getColumnIndex("sym");
                final int key = reader.getSymbolMapReader(columnIndex).keyOf("s15") + 1;

                final LongList forward = new LongList();
                try (RowCursor fwd = reader.getIndexReader(0, columnIndex, IndexReader.DIR_FORWARD)
                        .getCursor(key, 0, Long.MAX_VALUE)) {
                    while (fwd.hasNext()) {
                        forward.add(fwd.next());
                    }
                }
                Assert.assertTrue("the key must have postings", forward.size() > 0);

                int i = forward.size();
                try (RowCursor bwd = reader.getIndexReader(0, columnIndex, IndexReader.DIR_BACKWARD)
                        .getCursor(key, 0, Long.MAX_VALUE)) {
                    while (bwd.hasNext()) {
                        Assert.assertTrue("backward cursor emitted more rows than forward", i > 0);
                        Assert.assertEquals(forward.getQuick(--i), bwd.next());
                    }
                }
                Assert.assertEquals("backward cursor emitted fewer rows than forward", 0, i);
            }
        });
    }

    /**
     * The backward cursor honours the same row-id window as the forward one,
     * including the zone-map skip -- a window is not a forward-only concept.
     */
    @Test
    public void testTheBackwardCursorClipsToTheRowIdWindow() throws Exception {
        assertMemoryLeak(() -> {
            createIndexedParquetTable("x");
            try (TableReader reader = engine.getReader(engine.verifyTableName("x"))) {
                final int columnIndex = reader.getMetadata().getColumnIndex("sym");
                final int key = reader.getSymbolMapReader(columnIndex).keyOf("s15") + 1;

                final LongList all = new LongList();
                try (RowCursor fwd = reader.getIndexReader(0, columnIndex, IndexReader.DIR_FORWARD)
                        .getCursor(key, 0, Long.MAX_VALUE)) {
                    while (fwd.hasNext()) {
                        all.add(fwd.next());
                    }
                }
                Assert.assertTrue("the fixture needs at least four postings", all.size() >= 4);

                final long lo = all.getQuick(1);
                final long hi = all.getQuick(all.size() - 2);
                final LongList clipped = new LongList();
                try (RowCursor bwd = reader.getIndexReader(0, columnIndex, IndexReader.DIR_BACKWARD)
                        .getCursor(key, lo, hi)) {
                    while (bwd.hasNext()) {
                        clipped.add(bwd.next());
                    }
                }
                Assert.assertEquals(all.size() - 2, clipped.size());
                // Relative to minValue, as above.
                Assert.assertEquals(
                        "descending: the window's high end comes first",
                        hi - lo, clipped.getQuick(0)
                );
                Assert.assertEquals(0, clipped.getQuick(clipped.size() - 1));
            }
        });
    }

    /**
     * The forward cursor's postings must agree with the rows the table actually
     * holds -- same count, and every row id in ascending order.
     * <p>
     * The count is compared against a SQL aggregate over the same symbol rather
     * than a literal, so the assertion cannot drift from the fixture. Ascending
     * order is asserted rather than assumed: the emitted sequence depends on the
     * seal writing postings ascending within a key AND on the cursor visiting
     * row groups in ascending order, and a regression in either would be
     * invisible to a count.
     */
    @Test
    public void testTheForwardCursorReturnsAKeysPostingsInAscendingOrder() throws Exception {
        assertMemoryLeak(() -> {
            createIndexedParquetTable("x");
            long count = 0;
            try (TableReader reader = engine.getReader(engine.verifyTableName("x"))) {
                final int columnIndex = reader.getMetadata().getColumnIndex("sym");
                final int key = reader.getSymbolMapReader(columnIndex).keyOf("s7") + 1; // +1: key 0 is NULL
                final IndexReader indexReader = reader.getIndexReader(0, columnIndex, IndexReader.DIR_FORWARD);
                long previous = -1;
                try (RowCursor cursor = indexReader.getCursor(key, 0, Long.MAX_VALUE)) {
                    while (cursor.hasNext()) {
                        final long rowId = cursor.next();
                        Assert.assertTrue(
                                "row ids must ascend, got " + rowId + " after " + previous,
                                rowId > previous
                        );
                        previous = rowId;
                        count++;
                    }
                }
            }
            // Outside the reader's scope: the assertion opens its own, and a
            // held one leaves the table behind on pool shutdown.
            Assert.assertTrue("the key must have postings", count > 0);
            assertQuery("select count() from x where sym = 's7' and ts in '" + INDEXED_PARTITION + "'")
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n" + count + "\n");
        });
    }

    /**
     * Pruning level 2. A narrow row-id window must decode fewer row groups than
     * the key's whole run.
     * <p>
     * The assertion counts row groups DECODED, not elapsed time: a latency
     * assertion passes on warm-up while the skip misses entirely, and this
     * branch has already shipped one perf claim that a repeat run inverted.
     * <p>
     * The count is also checked against the rows the window actually contains,
     * so a skip that pruned too much -- dropping a group that did hold matching
     * postings -- fails here rather than silently returning fewer rows.
     */
    @Test
    public void testANarrowRowIdRangeDecodesFewerRowGroupsThanTheKeysWholeRun() throws Exception {
        assertMemoryLeak(() -> {
            createHotKeyParquetTable("x", 400_000, "hot");
            try (TableReader reader = engine.getReader(engine.verifyTableName("x"))) {
                final int columnIndex = reader.getMetadata().getColumnIndex("sym");
                final int key = reader.getSymbolMapReader(columnIndex).keyOf("hot") + 1;
                final AbstractParquetPostingIndexReader indexReader =
                        (AbstractParquetPostingIndexReader) reader.getIndexReader(0, columnIndex, IndexReader.DIR_FORWARD);

                final long wholeRows = drain(indexReader.getCursor(key, 0, Long.MAX_VALUE));
                final long whole = indexReader.getDecodedRowGroupCount();
                Assert.assertTrue(
                        "the fixture must give the hot key more than one row group, got " + whole,
                        whole > 1
                );

                final long before = indexReader.getDecodedRowGroupCount();
                final long narrowRows = drain(indexReader.getCursor(key, 0, 999));
                final long narrow = indexReader.getDecodedRowGroupCount() - before;
                Assert.assertTrue(
                        "a narrow row-id range must decode fewer row groups: narrow=" + narrow
                                + " whole=" + whole,
                        narrow < whole
                );

                // The skip must not prune a group that did hold matching rows.
                // Row ids 0..999 cover the first 1000 rows of the partition, of
                // which the hot symbol takes all but every 16th.
                Assert.assertTrue("the narrow window must still return rows", narrowRows > 0);
                Assert.assertTrue(
                        "the narrow window cannot return more than the whole run",
                        narrowRows < wholeRows
                );
                Assert.assertEquals(
                        "every posting in [0, 999] must survive the skip",
                        1000 - (1000 / 16),
                        narrowRows
                );
            }
        });
    }

    /**
     * Two cursors drawn from ONE reader must each walk their own answer.
     * <p>
     * This is not hypothetical. {@code CoveringIndexRecordCursorFactory} asks a
     * reader for the next key's cursor BEFORE freeing the one it is holding --
     * {@code tryOpenKey} and {@code findLatestRow} both call {@code getCursor}
     * and only then {@code Misc.free(currentRowCursor)} -- and an interval scan
     * hands the same partition, and so the same reader, to that loop more than
     * once. A reader with a single re-{@code of()}-ed instance answers both
     * calls with the same object: the second call resets the first's traversal
     * mid-iteration, and the free that follows closes the cursor just handed
     * out.
     * <p>
     * The first cursor is deliberately left part-walked across the second
     * cursor's whole life, because a reset is invisible to a test that drains
     * them one after the other.
     */
    @Test
    public void testTwoCursorsFromOneReaderDoNotShareTraversalState() throws Exception {
        assertMemoryLeak(() -> {
            createIndexedParquetTable("x");
            try (TableReader reader = engine.getReader(engine.verifyTableName("x"))) {
                final int columnIndex = reader.getMetadata().getColumnIndex("sym");
                final int keyA = reader.getSymbolMapReader(columnIndex).keyOf("s15") + 1;
                final int keyB = reader.getSymbolMapReader(columnIndex).keyOf("s7") + 1;
                Assert.assertNotEquals("the fixture needs two distinct keys", keyA, keyB);
                final IndexReader indexReader = reader.getIndexReader(0, columnIndex, IndexReader.DIR_FORWARD);

                final LongList expectedA = new LongList();
                try (RowCursor c = indexReader.getCursor(keyA, 0, Long.MAX_VALUE)) {
                    while (c.hasNext()) {
                        expectedA.add(c.next());
                    }
                }
                final LongList expectedB = new LongList();
                try (RowCursor c = indexReader.getCursor(keyB, 0, Long.MAX_VALUE)) {
                    while (c.hasNext()) {
                        expectedB.add(c.next());
                    }
                }
                Assert.assertTrue("both keys must have postings", expectedA.size() > 8 && expectedB.size() > 8);

                final LongList actualA = new LongList();
                try (RowCursor a = indexReader.getCursor(keyA, 0, Long.MAX_VALUE)) {
                    Assert.assertTrue(a.hasNext());
                    actualA.add(a.next());
                    Assert.assertTrue(a.hasNext());
                    actualA.add(a.next());

                    final LongList actualB = new LongList();
                    try (RowCursor b = indexReader.getCursor(keyB, 0, Long.MAX_VALUE)) {
                        while (b.hasNext()) {
                            actualB.add(b.next());
                        }
                    }
                    Assert.assertEquals("the second cursor's answer changed", expectedB.size(), actualB.size());
                    for (int i = 0, n = expectedB.size(); i < n; i++) {
                        Assert.assertEquals(expectedB.getQuick(i), actualB.getQuick(i));
                    }

                    while (a.hasNext()) {
                        actualA.add(a.next());
                    }
                }
                Assert.assertEquals(
                        "the first cursor was reset by the second", expectedA.size(), actualA.size()
                );
                for (int i = 0, n = expectedA.size(); i < n; i++) {
                    Assert.assertEquals(
                            "the first cursor's row id changed at " + i,
                            expectedA.getQuick(i), actualA.getQuick(i)
                    );
                }
                // Asserted last, and on purpose: identity is the mechanism, the
                // corrupted sequences above are the symptom, and a control that
                // trips on the mechanism first proves only that the mechanism
                // changed.
                try (
                        RowCursor a = indexReader.getCursor(keyA, 0, Long.MAX_VALUE);
                        RowCursor b = indexReader.getCursor(keyB, 0, Long.MAX_VALUE)
                ) {
                    Assert.assertNotSame("a second cursor must not be the first one back again", a, b);
                }
            }
        });
    }

    /**
     * Closing the READER must release everything the reader owns, including the
     * key probe its pooled cursor created.
     * <p>
     * The probe is a second {@code CountingCursor}, and it owns a second parquet
     * decoder, its own row-group buffers and its own projection -- three native
     * allocations. It is created lazily, on the first row group the cursor
     * bounds, so it exists exactly when the cursor has been iterated. A reader
     * whose {@code close()} reaches only the cursor's own buffers leaks all
     * three, and it does so on the ordinary path: nothing obliges a caller to
     * close a POOLED cursor, which is the reader's to recycle.
     * <p>
     * The cursor is deliberately left open here. Asserting through
     * {@code assertMemoryLeak} rather than through an accessor is what makes
     * this catch the leak by its size rather than by a flag the fix would also
     * have to set.
     */
    @Test
    public void testAReaderCloseReleasesItsPooledCursorsKeyProbe() throws Exception {
        assertMemoryLeak(() -> {
            createIndexedParquetTable("x");
            try (TableReader reader = engine.getReader(engine.verifyTableName("x"))) {
                final int columnIndex = reader.getMetadata().getColumnIndex("sym");
                final int key = reader.getSymbolMapReader(columnIndex).keyOf("s15") + 1;
                for (int direction : new int[]{IndexReader.DIR_FORWARD, IndexReader.DIR_BACKWARD}) {
                    final IndexReader indexReader = reader.getIndexReader(0, columnIndex, direction);
                    final RowCursor cursor = indexReader.getCursor(key, 0, Long.MAX_VALUE);
                    long n = 0;
                    while (cursor.hasNext()) {
                        cursor.next();
                        n++;
                    }
                    // Without postings no row group is bounded, so no probe is
                    // built and the fixture would prove nothing.
                    Assert.assertTrue("the key must have postings [direction=" + direction + ']', n > 0);
                }
            }
        });
    }

    /**
     * The pruning instrumentation must count every decode N concurrent detached
     * cursors perform, not most of them.
     * <p>
     * {@code decodedRowGroupCount} and {@code decodedRowCount} are reader state
     * written by every cursor, and {@code getDetachedCursor} exists precisely so
     * that N workers can decode over ONE reader at once. A plain {@code long++}
     * is a read-modify-write, so concurrent decodes lose updates -- and they
     * lose them DOWNWARDS, which is the dangerous direction: these two counters
     * are what the pruning assertions read, so an under-count makes a pruning
     * test pass by destroying its own evidence rather than by pruning.
     * <p>
     * Each worker walks the identical cursor, so the expected totals are exact
     * multiples of the serial one rather than a bound.
     */
    @Test
    public void testConcurrentDetachedDecodesAreAllCounted() throws Exception {
        assertMemoryLeak(() -> {
            createIndexedParquetTable("x");
            try (TableReader reader = engine.getReader(engine.verifyTableName("x"))) {
                final int columnIndex = reader.getMetadata().getColumnIndex("sym");
                final int key = reader.getSymbolMapReader(columnIndex).keyOf("s15") + 1;
                final AbstractParquetPostingIndexReader indexReader =
                        (AbstractParquetPostingIndexReader) reader.getIndexReader(0, columnIndex, IndexReader.DIR_FORWARD);

                // One serial pass establishes what a single walk costs.
                final long groupsBefore = indexReader.getDecodedRowGroupCount();
                final long rowsBefore = indexReader.getDecodedRowCount();
                try (RowCursor c = indexReader.getDetachedCursor(key, 0, Long.MAX_VALUE, null)) {
                    while (c.hasNext()) {
                        c.next();
                    }
                }
                final long groupsPerWalk = indexReader.getDecodedRowGroupCount() - groupsBefore;
                final long rowsPerWalk = indexReader.getDecodedRowCount() - rowsBefore;
                Assert.assertTrue("a walk must decode something", groupsPerWalk > 0 && rowsPerWalk > 0);

                final int workers = 8;
                final int repeats = 40;
                final long groupsAtStart = indexReader.getDecodedRowGroupCount();
                final long rowsAtStart = indexReader.getDecodedRowCount();
                indexReader.setFrozen(true);
                final CountDownLatch start = new CountDownLatch(1);
                final CountDownLatch done = new CountDownLatch(workers);
                final AtomicReference<Throwable> failure = new AtomicReference<>();
                for (int w = 0; w < workers; w++) {
                    new Thread(() -> {
                        try {
                            start.await();
                            for (int i = 0; i < repeats; i++) {
                                try (RowCursor c = indexReader.getDetachedCursor(key, 0, Long.MAX_VALUE, null)) {
                                    while (c.hasNext()) {
                                        c.next();
                                    }
                                }
                            }
                        } catch (Throwable th) {
                            failure.compareAndSet(null, th);
                        } finally {
                            done.countDown();
                        }
                    }).start();
                }
                start.countDown();
                Assert.assertTrue("workers did not finish", done.await(120, TimeUnit.SECONDS));
                indexReader.setFrozen(false);
                if (failure.get() != null) {
                    throw new AssertionError(failure.get());
                }

                Assert.assertEquals(
                        "row group decodes were lost",
                        groupsAtStart + (long) workers * repeats * groupsPerWalk,
                        indexReader.getDecodedRowGroupCount()
                );
                Assert.assertEquals(
                        "decoded rows were lost",
                        rowsAtStart + (long) workers * repeats * rowsPerWalk,
                        indexReader.getDecodedRowCount()
                );
            }
        });
    }

    /**
     * N detached cursors over ONE frozen reader must each return the whole
     * answer, concurrently.
     * <p>
     * This is the property the parallel covered decode needs and the one that
     * shared decode state silently breaks: with buffers or a cover slot-to-chunk
     * map shared, two workers interleave one allocation and each sees the
     * other's group. Running them on real threads and comparing every cursor's
     * output to the serial answer is what catches that -- a single-threaded
     * "does getDetachedCursor return something" test would not.
     */
    @Test
    public void testDetachedCursorsCanRunConcurrentlyOverOneReader() throws Exception {
        assertMemoryLeak(() -> {
            createIndexedParquetTable("x");
            try (TableReader reader = engine.getReader(engine.verifyTableName("x"))) {
                final int columnIndex = reader.getMetadata().getColumnIndex("sym");
                final int key = reader.getSymbolMapReader(columnIndex).keyOf("s15") + 1;
                final IndexReader indexReader = reader.getIndexReader(0, columnIndex, IndexReader.DIR_FORWARD);
                final int[] covers = new int[]{0, 1};

                final LongList expected = new LongList();
                try (RowCursor c = indexReader.getCursor(key, 0, Long.MAX_VALUE, covers)) {
                    while (c.hasNext()) {
                        expected.add(c.next());
                    }
                }
                Assert.assertTrue("the key must have postings", expected.size() > 0);

                // Frozen: the mappings must not move under the workers.
                indexReader.setFrozen(true);
                final int workers = 4;
                final CountDownLatch start = new CountDownLatch(1);
                final CountDownLatch done = new CountDownLatch(workers);
                final AtomicReference<Throwable> failure = new AtomicReference<>();
                for (int w = 0; w < workers; w++) {
                    new Thread(() -> {
                        try {
                            start.await();
                            final LongList mine = new LongList();
                            try (RowCursor c = indexReader.getDetachedCursor(key, 0, Long.MAX_VALUE, covers)) {
                                while (c.hasNext()) {
                                    final CoveringRowCursor crc = (CoveringRowCursor) c;
                                    final long rowId = c.next();
                                    mine.add(rowId);
                                    // Touch a covered value too: a shared chunk
                                    // map shows up here rather than in the ids.
                                    crc.getCoveredDouble(0);
                                }
                            }
                            if (mine.size() != expected.size()) {
                                throw new AssertionError("detached cursor saw " + mine.size()
                                        + " postings, serial saw " + expected.size());
                            }
                            for (int i = 0, n = mine.size(); i < n; i++) {
                                if (mine.getQuick(i) != expected.getQuick(i)) {
                                    throw new AssertionError("detached cursor diverged at " + i
                                            + ": " + mine.getQuick(i) + " != " + expected.getQuick(i));
                                }
                            }
                        } catch (Throwable t) {
                            failure.compareAndSet(null, t);
                        } finally {
                            done.countDown();
                        }
                    }).start();
                }
                start.countDown();
                Assert.assertTrue("workers did not finish", done.await(60, TimeUnit.SECONDS));
                indexReader.setFrozen(false);
                if (failure.get() != null) {
                    throw new AssertionError(failure.get());
                }
            }
        });
    }

    /**
     * The metadata primitives must agree with the cursor that walks the same
     * postings, and with SQL.
     * <p>
     * Both sentinels are asserted explicitly. Returning {@code -1} instead of
     * {@code LONG_NULL} is not a near miss: the sole caller tests
     * {@code != LONG_NULL} and then does {@code total += c}, so a {@code -1}
     * silently subtracts one from a {@code count(*)}, and from
     * {@code selectKthMatch} it is consumed as an absolute row id.
     */
    @Test
    public void testTheMetadataPrimitivesAgreeWithTheCursor() throws Exception {
        assertMemoryLeak(() -> {
            createIndexedParquetTable("x");
            long counted;
            try (TableReader reader = engine.getReader(engine.verifyTableName("x"))) {
                final int columnIndex = reader.getMetadata().getColumnIndex("sym");
                final int key = reader.getSymbolMapReader(columnIndex).keyOf("s15") + 1;
                final PostingIndexReader indexReader =
                        (PostingIndexReader) reader.getIndexReader(0, columnIndex, IndexReader.DIR_FORWARD);

                counted = indexReader.countMatchesClamped(key, 0, Long.MAX_VALUE, Long.MAX_VALUE);
                Assert.assertNotEquals(
                        "the metadata path must answer rather than fall back",
                        Numbers.LONG_NULL, counted
                );
                Assert.assertNotEquals("-1 is consumed as a count, never as a sentinel", -1, counted);

                final LongList walked = new LongList();
                try (RowCursor c = indexReader.getCursor(key, 0, Long.MAX_VALUE)) {
                    while (c.hasNext()) {
                        walked.add(c.next());
                    }
                }
                Assert.assertEquals("the count must equal what the cursor walks", walked.size(), counted);

                // Every k, not just the ends: an off-by-one in the group-skip
                // arithmetic only shows up at a group boundary.
                for (int i = 0; i < walked.size(); i++) {
                    Assert.assertEquals(
                            "selectKthMatch disagreed at k=" + i,
                            walked.getQuick(i),
                            indexReader.selectKthMatch(key, 0, Long.MAX_VALUE, Long.MAX_VALUE, i)
                    );
                }
                Assert.assertEquals(
                        "k past the end must be LONG_NULL, never -1",
                        Numbers.LONG_NULL,
                        indexReader.selectKthMatch(key, 0, Long.MAX_VALUE, Long.MAX_VALUE, walked.size())
                );

                // getEntryMaxValue is the reader-level clamp and must be the
                // highest row id the index covers, not the key's.
                Assert.assertTrue(
                        "entry max must cover the key's last posting",
                        indexReader.getEntryMaxValue() >= walked.getQuick(walked.size() - 1)
                );
                indexReader.populateCacheForKey(key);
            }
            assertQuery("select count() from x where sym = 's15' and ts in '" + INDEXED_PARTITION + "'")
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n" + counted + "\n");
        });
    }

    /**
     * Pruning level 3: inside a PACKED row group, only the key's own rows have
     * their values decoded.
     * <p>
     * Asserted on rows decoded rather than row groups, because narrowing
     * inside a group leaves the group count unchanged -- level 2's metric
     * cannot see this and would pass against no narrowing at all. The fixture's
     * three symbols share one row group, so the key's slice is a strict subset
     * and the two numbers must differ.
     * <p>
     * This is level 3's EFFECT, not the spec's mechanism. The seal writes
     * neither {@code ColumnIndex} nor {@code OffsetIndex} -- its writer takes
     * a statistics flag and nothing for a page index -- so page skipping is
     * reached through {@code decodeRowGroup}'s row bounds instead. Phase 3 must
     * add the indexes if the stated mechanism is wanted.
     */
    @Test
    public void testAPackedRowGroupDecodesOnlyTheKeysRows() throws Exception {
        assertMemoryLeak(() -> {
            createIndexedParquetTable("x");
            try (TableReader reader = engine.getReader(engine.verifyTableName("x"))) {
                final int columnIndex = reader.getMetadata().getColumnIndex("sym");
                final int key = reader.getSymbolMapReader(columnIndex).keyOf("s0") + 1;
                final AbstractParquetPostingIndexReader indexReader =
                        (AbstractParquetPostingIndexReader) reader.getIndexReader(0, columnIndex, IndexReader.DIR_FORWARD);

                long walked = 0;
                try (RowCursor c = indexReader.getCursor(key, 0, Long.MAX_VALUE)) {
                    while (c.hasNext()) {
                        c.next();
                        walked++;
                    }
                }
                Assert.assertTrue("the key must have postings", walked > 0);

                final long decodedRows = indexReader.getDecodedRowCount();
                final long groupRows = ROW_COUNT;
                Assert.assertTrue(
                        "the fixture must pack several keys into the group, or this cannot fail:"
                                + " walked=" + walked + " groupRows=" + groupRows,
                        walked < groupRows
                );
                Assert.assertEquals(
                        "only the key's own rows may have their values decoded",
                        walked,
                        decodedRows
                );
                Assert.assertTrue(
                        "decoding the whole group would be " + groupRows + " rows, got " + decodedRows,
                        decodedRows < groupRows
                );
            }
        });
    }

    /**
     * SELECT DISTINCT over a parquet-sealed partition must return the same
     * symbols the table holds.
     * <p>
     * The interface documents {@code -1} as "not supported, caller falls back",
     * but the only caller does {@code foundCount += collectDistinctKeys(...)},
     * so declining does not fall back -- it silently shortens the answer by one
     * per partition. Comparing against a scan that cannot use the index is what
     * catches that; asserting "returned something" would not.
     */
    @Test
    public void testSelectDistinctOverAParquetSealedPartitionMatchesTheTable() throws Exception {
        assertMemoryLeak(() -> {
            createIndexedParquetTable("x");
            // The indexed partition holds exactly s0, s7 and s15. Dropping a
            // key -- which is what returning -1 does, one per partition --
            // shows up as a missing row here.
            assertQuery("select distinct sym from x where ts in '" + INDEXED_PARTITION + "' order by 1")
                    .expectSize()
                    .returns("sym\ns0\ns15\ns7\n");
            // And the count agrees with a scan that cannot use the index.
            assertSqlCursors(
                    "select count_distinct(cast(sym as varchar)) c from x where ts in '" + INDEXED_PARTITION + "'",
                    "select count_distinct(sym) c from x where ts in '" + INDEXED_PARTITION + "'"
            );
        });
    }

    /**
     * A key the directory does not resolve is an ordinary answer, not an error:
     * a query for a symbol this partition never carried must return no rows.
     * <p>
     * The key used is above {@code KEY_SPACE_SIZE}, so it is absent by the
     * directory's own bound rather than by occupancy -- the one absence that
     * needs no row-group decode to establish.
     */
    @Test
    public void testAnAbsentKeyReturnsAnEmptyCursorRatherThanThrowing() throws Exception {
        assertMemoryLeak(() -> {
            createIndexedParquetTable("x");
            try (TableReader reader = engine.getReader(engine.verifyTableName("x"))) {
                final int columnIndex = reader.getMetadata().getColumnIndex("sym");
                final IndexReader indexReader = reader.getIndexReader(0, columnIndex, IndexReader.DIR_FORWARD);
                try (RowCursor cursor = indexReader.getCursor(indexReader.getKeyCount() + 7, 0, Long.MAX_VALUE)) {
                    Assert.assertFalse(cursor.hasNext());
                }
            }
        });
    }

    /**
     * The row-id bound is the caller's page-frame window and does not align with
     * row-group boundaries, so the cursor must clip within a decoded group as
     * well as between groups.
     * <p>
     * The window is taken from the key's own postings -- second to
     * second-to-last -- so it necessarily cuts inside whatever groups the key
     * spans, whatever the fixture's row-group size turns out to be.
     */
    @Test
    public void testTheForwardCursorClipsToTheRowIdWindow() throws Exception {
        assertMemoryLeak(() -> {
            createIndexedParquetTable("x");
            try (TableReader reader = engine.getReader(engine.verifyTableName("x"))) {
                final int columnIndex = reader.getMetadata().getColumnIndex("sym");
                final int key = reader.getSymbolMapReader(columnIndex).keyOf("s7") + 1;
                final IndexReader indexReader = reader.getIndexReader(0, columnIndex, IndexReader.DIR_FORWARD);

                final LongList all = new LongList();
                try (RowCursor cursor = indexReader.getCursor(key, 0, Long.MAX_VALUE)) {
                    while (cursor.hasNext()) {
                        all.add(cursor.next());
                    }
                }
                Assert.assertTrue("the fixture needs at least four postings to clip both ends", all.size() >= 4);

                final long lo = all.getQuick(1);
                final long hi = all.getQuick(all.size() - 2);
                final LongList clipped = new LongList();
                try (RowCursor cursor = indexReader.getCursor(key, lo, hi)) {
                    while (cursor.hasNext()) {
                        clipped.add(cursor.next());
                    }
                }
                Assert.assertEquals(
                        "the window must drop exactly the first and last posting",
                        all.size() - 2,
                        clipped.size()
                );
                // Row ids come back RELATIVE to minValue -- the contract
                // IndexReader.getCursor states and what the native readers do.
                // So the window's own lower bound is emitted as 0.
                Assert.assertEquals(0, clipped.getQuick(0));
                Assert.assertEquals(hi - lo, clipped.getQuick(clipped.size() - 1));
            }
        });
    }

    /**
     * A key whose postings span several dedicated row groups, so a narrow
     * row-id window can exclude most of them.
     * <p>
     * The seal targets {@code TARGET_ROW_GROUP_ROWS} rows per group and splits
     * a key that exceeds it, so the hot symbol needs comfortably more than that
     * many postings. With fewer, the whole index is one row group and the two
     * arms of the pruning test cannot differ -- it would pass against no
     * pruning at all.
     */
    private void createHotKeyParquetTable(String tableName, int rows, String hotSymbol) throws Exception {
        execute("CREATE TABLE " + tableName + " (" +
                "ts TIMESTAMP, sym SYMBOL, price DOUBLE, qty LONG" +
                ") TIMESTAMP(ts) PARTITION BY DAY WAL");
        // 15 of every 16 rows carry the hot symbol, so at `rows` = 400k it holds
        // ~375k postings: four row groups at the 100k target, against one for
        // everything else.
        execute("INSERT INTO " + tableName + " SELECT" +
                " dateadd('u', x::INT, '" + INDEXED_PARTITION + "T00:00:00Z'::TIMESTAMP)," +
                " CASE WHEN x % 16 = 0 THEN 'cold' ELSE '" + hotSymbol + "' END," +
                " x::DOUBLE," +
                " x" +
                " FROM long_sequence(" + rows + ")");
        drainWalQueue();
        execute("ALTER TABLE " + tableName + " CONVERT PARTITION TO PARQUET LIST '" + INDEXED_PARTITION + "'");
        drainWalQueue();
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_PARQUET_PARTITION_FORMAT, "parquet");
        execute("ALTER TABLE " + tableName + " ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, qty)");
        drainWalQueue();
        engine.releaseInactive();
    }

    private static long drain(RowCursor cursor) {
        long n = 0;
        try (RowCursor c = cursor) {
            while (c.hasNext()) {
                c.next();
                n++;
            }
        }
        return n;
    }

    /**
     * The sparse-key fixture, sealed in the parquet form. The partition's three
     * symbols are ids 0, 7 and 15 out of the table's sixteen, so its key ids are
     * sparse and KEY_SPACE_SIZE is distinguishable from a distinct-key count.
     */
    private void createIndexedParquetTable(String tableName) throws Exception {
        createParquetTable(tableName);
        node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_PARQUET_PARTITION_FORMAT, "parquet");
        execute("ALTER TABLE " + tableName + " ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, qty)");
        drainWalQueue();
        engine.releaseInactive();
    }

    /**
     * The same fixture sealed under the DEFAULT format, so the partition is
     * parquet but its index is not.
     */
    private void createNativeSealedParquetTable(String tableName) throws Exception {
        createParquetTable(tableName);
        execute("ALTER TABLE " + tableName + " ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, qty)");
        drainWalQueue();
        engine.releaseInactive();
    }

    private void createParquetTable(String tableName) throws Exception {
        execute("CREATE TABLE " + tableName + " (" +
                "ts TIMESTAMP, sym SYMBOL, price DOUBLE, qty LONG" +
                ") TIMESTAMP(ts) PARTITION BY DAY WAL");
        // Assigns symbol ids 0..15 in a later partition, so the indexed
        // partition below uses only three of them and its key ids stay sparse.
        execute("INSERT INTO " + tableName + " SELECT" +
                " dateadd('m', x::INT, '2024-01-02T00:00:00Z'::TIMESTAMP)," +
                " 's' || (x - 1)," +
                " x::DOUBLE," +
                " x" +
                " FROM long_sequence(16)");
        drainWalQueue();
        execute("INSERT INTO " + tableName + " SELECT" +
                " dateadd('u', x::INT, '" + INDEXED_PARTITION + "T00:00:00Z'::TIMESTAMP)," +
                " CASE WHEN x % 4 = 0 THEN 's0' WHEN x % 4 = 1 THEN 's7' ELSE 's15' END," +
                " x::DOUBLE," +
                " x" +
                " FROM long_sequence(" + ROW_COUNT + ")");
        drainWalQueue();
        execute("ALTER TABLE " + tableName + " CONVERT PARTITION TO PARQUET LIST '" + INDEXED_PARTITION + "'");
        drainWalQueue();
    }
}
