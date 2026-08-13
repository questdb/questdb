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
import io.questdb.cairo.idx.IndexReader;
import io.questdb.cairo.idx.ParquetPostingIndexFwdReader;
import io.questdb.cairo.idx.PostingIndexFwdReader;
import io.questdb.cairo.idx.PostingIndexReader;
import io.questdb.std.Chars;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

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
    @Test
    public void testTheParquetCursorRefusesLoudlyRatherThanAnsweringEmpty() throws Exception {
        assertMemoryLeak(() -> {
            createIndexedParquetTable("x");
            try (TableReader reader = engine.getReader(engine.verifyTableName("x"))) {
                final int columnIndex = reader.getMetadata().getColumnIndex("sym");
                final IndexReader indexReader = reader.getIndexReader(0, columnIndex, IndexReader.DIR_FORWARD);
                try {
                    indexReader.getCursor(1, 0, Long.MAX_VALUE);
                    Assert.fail("the placeholder cursor must throw");
                } catch (CairoException e) {
                    TestUtils.assertContains(
                            e.getFlyweightMessage(),
                            "parquet-form posting index cursor is not implemented yet"
                    );
                }
            }
        });
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
