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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnVersionWriter;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.SymbolCountProvider;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TxWriter;
import io.questdb.std.FilesFacade;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cairo.TableUtils.COLUMN_VERSION_FILE_NAME;
import static io.questdb.cairo.TableUtils.TXN_FILE_NAME;

/**
 * Plan 3 (composite partitioning), Tasks 6-7: {@link TableReader}'s fresh-open path ({@code
 * initOpenPartitionInfo}) must carry each partition's {@code cellKey} in {@code openPartitionInfo}
 * (the previously-padding slot 6), and every {@code columnVersionReader.getMaxPartitionVersion(ts)}
 * call made while resolving a partition's column-version must pass that partition's own {@code
 * cellKey} -- otherwise two cells sharing a timestamp alias onto the same column-version, silently
 * corrupting one cell's resolved state with the other's.
 * <p>
 * This is the reader-side counterpart of Tasks 1-5 ({@code _txn}/{@code _cv} writer+reader storage).
 * Production never creates a multi-cell partition set in this plan (write-routing is Plan 4), so this
 * test synthesizes one directly through the {@code *ForTest} seams established by {@link
 * CompositeTxCellTest} and {@link CompositeColumnVersionCellTest}, mirroring real {@code
 * TableWriter}/{@code ColumnVersionWriter} commit sequencing -- commit {@code _cv} first, thread its
 * resulting version into the standalone {@code TxWriter} via {@code setColumnVersion} (exactly {@code
 * columnVersionWriter.commit(); txWriter.setColumnVersion(columnVersionWriter.getVersion());} in real
 * {@code TableWriter} code), then commit {@code _txn} -- so a genuinely fresh {@link TableReader}'s
 * {@code reloadColumnVersion()} converges instead of spinning to a timeout.
 * <p>
 * {@link #testReconcileOpenPartitionsInsertsSameTimestampCellNotAliased} and {@link
 * #testReconcileOpenPartitionsDeletesSameTimestampCellNotAliased} below are Task 7: they open a reader,
 * then perform a SECOND raw {@code _cv}/{@code _txn} surgery pass while it stays open, and call {@code
 * reader.reload()} directly, to exercise {@code reconcileOpenPartitions0} (the reload-diff path) itself
 * -- the first test above only exercises the initial, straight per-physical-index load ({@code
 * initOpenPartitionInfo}), never a reload of an already-open reader.
 */
public class CompositeReaderCellTest extends AbstractCairoTest {

    @Test
    public void testOpenPartitionInfoCarriesCellKeyAndResolvesColumnVersionPerCell() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table c (ts timestamp, exchange symbol, x double) " +
                    "timestamp(ts) partition by day, exchange");
            engine.releaseInactive(); // no pooled writer/reader may keep _txn/_cv open under our direct use

            final long day1 = 0L;
            final int col = 3;

            FilesFacade ff = engine.getConfiguration().getFilesFacade();
            TableToken tableToken = engine.verifyTableName("c");

            // 1. Raw _cv surgery: two cells at day1, distinct column-versions (100 vs 200) -- this is
            // exactly the signal TableReader's PARTITIONS_SLOT_OFFSET_COLUMN_VERSION slot is populated
            // from (via columnVersionReader.getMaxPartitionVersion), so it discriminates aliasing.
            long cvVersion;
            try (
                    Path path = new Path();
                    ColumnVersionWriter cvWriter = new ColumnVersionWriter(
                            configuration,
                            path.of(configuration.getDbRoot()).concat(tableToken).concat(COLUMN_VERSION_FILE_NAME).$(),
                            true
                    )
            ) {
                cvWriter.upsert(day1, 0, col, 100, 0);
                cvWriter.upsert(day1, 1, col, 200, 0);
                cvWriter.commit();
                cvVersion = cvWriter.getVersion();
            }

            // 2. Raw _txn surgery: two partitions at day1 -- cell0 and cell1 -- via the tail-append seam
            // (already in (ts, cellKey) order; distinct partitionNameTxn values 900/901, unrelated to the
            // _cv-derived column-version being tested). Threads the _cv version above into the TxWriter.
            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(tableToken).concat(TXN_FILE_NAME).$();

                try (TxWriter txWriter = new TxWriter(ff, configuration)) {
                    txWriter.setCompositeForTest(true);
                    txWriter.ofRW(path.$(), ColumnType.TIMESTAMP_MICRO, PartitionBy.DAY);

                    // ofRW()'s unsafeLoadAll() has already loaded the real, current per-symbol value
                    // counts (the "exchange" column, plus this composite table's dedicated-dict/registry
                    // interner slots -- see CompositeInternerLayout). commit() rewrites this whole region
                    // from whatever SymbolCountProvider list it's handed; passing an empty list here would
                    // zero the symbol map writer count and break a subsequent real TableReader's
                    // openSymbolMaps(), which indexes into it. Pass through the real counts unchanged.
                    final int symbolColumnCount = txWriter.getSymbolColumnCount();
                    ObjList<SymbolCountProvider> symbolCountProviders = new ObjList<>();
                    for (int i = 0; i < symbolColumnCount; i++) {
                        final int count = txWriter.getSymbolValueCount(i);
                        symbolCountProviders.add(() -> count);
                    }

                    txWriter.appendPartitionForTest(day1, 5L, 900L, 0);
                    txWriter.appendPartitionForTest(day1, 5L, 901L, 1);
                    txWriter.updateMaxTimestamp(day1 + 1);
                    txWriter.finishPartitionSizeUpdate();
                    txWriter.setColumnVersion(cvVersion);
                    txWriter.commit(symbolCountProviders);
                }
            }

            // 3. Open a genuinely FRESH TableReader (never seen this _txn/_cv content before) and assert
            // its openPartitionInfo carries the real per-partition cellKey, and resolves each partition's
            // own column-version -- no aliasing across the two cells sharing day1.
            try (TableReader reader = getReader("c")) {
                Assert.assertEquals(2, reader.getPartitionCount());

                Assert.assertEquals("physical index 0 must be cell0", 0, reader.getPartitionCellKey(0));
                Assert.assertEquals("physical index 1 must be cell1", 1, reader.getPartitionCellKey(1));

                Assert.assertEquals(
                        "cell0's column-version must be its own (100), not aliased",
                        100, reader.getPartitionColumnVersion(0)
                );
                Assert.assertEquals(
                        "cell1's column-version must be its own (200), not aliased onto cell0's",
                        200, reader.getPartitionColumnVersion(1)
                );
            }
        });
    }

    /**
     * Task 7: {@code reconcileOpenPartitions0}'s merge-diff must classify a newly-appeared
     * same-timestamp cell as an INSERT of a new physical partition -- not a refresh of the timestamp's
     * pre-existing cell -- and the newly-inserted record's cellKey (slot 6) must be its own, not stale
     * data left behind by {@code LongList.insert}'s non-zeroing arraycopy-shift (T7-a).
     * <p>
     * The scenario is engineered so the T7-a bug is unmissable without any extra seeding: starting from
     * (day1,cell0) and (day2,cell0), inserting (day1,cell1) between them makes {@code
     * openPartitionInfo.insert} shift (day2,cell0)'s record one slot to the right. {@code
     * LongList.insert}'s arraycopy only moves the SUFFIX (the source range is never itself overwritten
     * by a copy into a disjoint destination), so the vacated slot -- where (day1,cell1)'s record will
     * live -- is left holding (day2,cell0)'s OLD, unerased bytes, including its cellKey (0), until
     * {@code insertPartition} explicitly overwrites every slot. A pre-fix {@code insertPartition} does
     * not touch slot 6, so the inserted cell would silently read back cellKey 0 -- a different, wrong,
     * but entirely plausible-looking value (0 is also a valid cellKey) -- instead of the correct 1.
     * <p>
     * Column-version is not asserted for the newly-inserted (day1,cell1) itself: {@code insertPartition}
     * always leaves a fresh slot's column-version at the -1 "pending" sentinel (resolved lazily, only
     * when a caller later opens the partition for real, which needs a backing directory this synthetic
     * test never creates) -- that is unrelated, pre-existing, lazy-resolution behaviour, not something
     * Task 7 changes. (day1,cell0) and (day2,cell0), by contrast, were part of the initial fresh-open
     * population, which resolves column-version eagerly (see the test above) -- so asserting their
     * column-version stayed put across the reload IS a meaningful proof that the sibling insert didn't
     * disturb them.
     */
    @Test
    public void testReconcileOpenPartitionsInsertsSameTimestampCellNotAliased() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table c (ts timestamp, exchange symbol, x double) " +
                    "timestamp(ts) partition by day, exchange");
            engine.releaseInactive();

            final long day1 = 0L;
            final long day2 = Micros.DAY_MICROS;
            final int col = 3;

            FilesFacade ff = engine.getConfiguration().getFilesFacade();
            TableToken tableToken = engine.verifyTableName("c");

            // 1. Initial _cv + _txn surgery: (day1,cell0) and (day2,cell0), each with its own distinct
            // column-version -- same pattern as the fresh-open test above.
            long cvVersion1;
            try (
                    Path path = new Path();
                    ColumnVersionWriter cvWriter = new ColumnVersionWriter(
                            configuration,
                            path.of(configuration.getDbRoot()).concat(tableToken).concat(COLUMN_VERSION_FILE_NAME).$(),
                            true
                    )
            ) {
                cvWriter.upsert(day1, 0, col, 100, 0);
                cvWriter.upsert(day2, 0, col, 300, 0);
                cvWriter.commit();
                cvVersion1 = cvWriter.getVersion();
            }

            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(tableToken).concat(TXN_FILE_NAME).$();
                try (TxWriter txWriter = new TxWriter(ff, configuration)) {
                    txWriter.setCompositeForTest(true);
                    txWriter.ofRW(path.$(), ColumnType.TIMESTAMP_MICRO, PartitionBy.DAY);

                    final int symbolColumnCount = txWriter.getSymbolColumnCount();
                    ObjList<SymbolCountProvider> symbolCountProviders = new ObjList<>();
                    for (int i = 0; i < symbolColumnCount; i++) {
                        final int count = txWriter.getSymbolValueCount(i);
                        symbolCountProviders.add(() -> count);
                    }

                    txWriter.appendPartitionForTest(day1, 5L, 900L, 0);
                    txWriter.appendPartitionForTest(day2, 5L, 800L, 0);
                    txWriter.updateMaxTimestamp(day2 + 1);
                    txWriter.finishPartitionSizeUpdate();
                    txWriter.setColumnVersion(cvVersion1);
                    txWriter.commit(symbolCountProviders);
                }
            }

            // 2. Open a genuinely fresh reader over the 2-partition state, and keep it open across the
            // reload below (this is the reload-diff path, unlike the fresh-open-only test above).
            try (TableReader reader = getReader("c")) {
                Assert.assertEquals(2, reader.getPartitionCount());
                Assert.assertEquals(0, reader.getPartitionCellKey(0));
                Assert.assertEquals(0, reader.getPartitionCellKey(1));
                Assert.assertEquals(100, reader.getPartitionColumnVersion(0));
                Assert.assertEquals(300, reader.getPartitionColumnVersion(1));

                // 3. Second _cv + _txn surgery pass: seam-insert (day1,cell1) via the (ts, cellKey)
                // ordered-insert seam, landing it BETWEEN (day1,cell0) and (day2,cell0), and bump the txn.
                long cvVersion2;
                try (
                        Path path = new Path();
                        ColumnVersionWriter cvWriter = new ColumnVersionWriter(
                                configuration,
                                path.of(configuration.getDbRoot()).concat(tableToken).concat(COLUMN_VERSION_FILE_NAME).$(),
                                true
                        )
                ) {
                    cvWriter.upsert(day1, 1, col, 200, 0);
                    cvWriter.commit();
                    cvVersion2 = cvWriter.getVersion();
                }

                try (Path path = new Path()) {
                    path.of(configuration.getDbRoot()).concat(tableToken).concat(TXN_FILE_NAME).$();
                    try (TxWriter txWriter = new TxWriter(ff, configuration)) {
                        txWriter.setCompositeForTest(true);
                        txWriter.ofRW(path.$(), ColumnType.TIMESTAMP_MICRO, PartitionBy.DAY);

                        final int symbolColumnCount = txWriter.getSymbolColumnCount();
                        ObjList<SymbolCountProvider> symbolCountProviders = new ObjList<>();
                        for (int i = 0; i < symbolColumnCount; i++) {
                            final int count = txWriter.getSymbolValueCount(i);
                            symbolCountProviders.add(() -> count);
                        }

                        txWriter.insertPartitionForTest(day1, 5L, 901L, 1);
                        txWriter.finishPartitionSizeUpdate();
                        txWriter.setColumnVersion(cvVersion2);
                        txWriter.commit(symbolCountProviders);
                    }
                }

                Assert.assertTrue("reload() must report a change", reader.reload());

                Assert.assertEquals(3, reader.getPartitionCount());

                Assert.assertEquals(day1, reader.getPartitionTimestampByIndex(0));
                Assert.assertEquals("(day1,cell0) keeps its own identity", 0, reader.getPartitionCellKey(0));

                Assert.assertEquals(day1, reader.getPartitionTimestampByIndex(1));
                Assert.assertEquals(
                        "T7-a lock: inserted (day1,cell1) must carry its own cellKey, not stale slot-6 " +
                                "garbage shifted in from (day2,cell0)'s old record",
                        1, reader.getPartitionCellKey(1)
                );

                Assert.assertEquals(day2, reader.getPartitionTimestampByIndex(2));
                Assert.assertEquals("(day2,cell0) shifted right but keeps its own cellKey", 0, reader.getPartitionCellKey(2));

                // (day1,cell0) and (day2,cell0)'s own cached column-versions are undisturbed by the
                // sibling insert -- neither aliased onto the other nor onto (day1,cell1)'s.
                Assert.assertEquals(100, reader.getPartitionColumnVersion(0));
                Assert.assertEquals(300, reader.getPartitionColumnVersion(2));
            }
        });
    }

    /**
     * Task 7, delete-direction counterpart of {@link
     * #testReconcileOpenPartitionsInsertsSameTimestampCellNotAliased}: {@code reconcileOpenPartitions0}
     * must classify a same-timestamp cell that disappeared from {@code _txn} as a DELETE of that exact
     * physical partition -- not a timestamp-only "refresh" that aliases a surviving sibling cell's
     * identity onto the deleted one's stale cached slot.
     * <p>
     * Starting from (day1,cell0), (day1,cell1) and (day2,cell0), removing (day1,cell0) leaves
     * (day1,cell1) as the sole day1 entry. Under a timestamp-only comparator, the FIRST reader/txn
     * comparison (cached (day1,cell0) vs. reloaded dense index 0, which is now (day1,cell1)) sees equal
     * timestamps and takes the "refresh" branch -- concluding this is the SAME physical partition, just
     * with a changed nameTxn -- rather than recognising that cell0 was deleted and cell1 needs to slide
     * into its place. That branch closes the (never-forced-open) slot and refreshes only its FORMAT,
     * leaving cellKey and column-version stuck at cell0's stale (0, 110) instead of cell1's real (1,
     * 210); the tail cleanup loop then deletes the real (day1,cell1) record outright, since the
     * timestamp-only walk believes it has already accounted for every day1 entry.
     * <p>
     * (day1,cell1) and (day2,cell0) are part of the initial fresh-open population (eager column-version
     * resolution -- see the first test above), so their column-versions staying correct across the
     * delete-of-a-sibling reload is a meaningful proof that the survivor was refreshed in place, not
     * aliased or rebuilt from a different cell's stale state.
     */
    @Test
    public void testReconcileOpenPartitionsDeletesSameTimestampCellNotAliased() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table c (ts timestamp, exchange symbol, x double) " +
                    "timestamp(ts) partition by day, exchange");
            engine.releaseInactive();

            final long day1 = 0L;
            final long day2 = Micros.DAY_MICROS;
            final int col = 3;

            FilesFacade ff = engine.getConfiguration().getFilesFacade();
            TableToken tableToken = engine.verifyTableName("c");

            // 1. Initial _cv + _txn surgery: THREE partitions -- (day1,cell0), (day1,cell1),
            // (day2,cell0) -- each with its own distinct column-version, so aliasing can't hide behind a
            // coincidentally-matching value.
            long cvVersion1;
            try (
                    Path path = new Path();
                    ColumnVersionWriter cvWriter = new ColumnVersionWriter(
                            configuration,
                            path.of(configuration.getDbRoot()).concat(tableToken).concat(COLUMN_VERSION_FILE_NAME).$(),
                            true
                    )
            ) {
                cvWriter.upsert(day1, 0, col, 110, 0);
                cvWriter.upsert(day1, 1, col, 210, 0);
                cvWriter.upsert(day2, 0, col, 310, 0);
                cvWriter.commit();
                cvVersion1 = cvWriter.getVersion();
            }

            try (Path path = new Path()) {
                path.of(configuration.getDbRoot()).concat(tableToken).concat(TXN_FILE_NAME).$();
                try (TxWriter txWriter = new TxWriter(ff, configuration)) {
                    txWriter.setCompositeForTest(true);
                    txWriter.ofRW(path.$(), ColumnType.TIMESTAMP_MICRO, PartitionBy.DAY);

                    final int symbolColumnCount = txWriter.getSymbolColumnCount();
                    ObjList<SymbolCountProvider> symbolCountProviders = new ObjList<>();
                    for (int i = 0; i < symbolColumnCount; i++) {
                        final int count = txWriter.getSymbolValueCount(i);
                        symbolCountProviders.add(() -> count);
                    }

                    // appendPartitionForTest is a raw tail-append: cell0 then cell1 at day1 already
                    // yields (ts, cellKey) order without needing the ordered-insert seam.
                    txWriter.appendPartitionForTest(day1, 7L, 910L, 0);
                    txWriter.appendPartitionForTest(day1, 9L, 911L, 1);
                    txWriter.appendPartitionForTest(day2, 5L, 810L, 0);
                    txWriter.updateMaxTimestamp(day2 + 1);
                    txWriter.finishPartitionSizeUpdate();
                    txWriter.setColumnVersion(cvVersion1);
                    txWriter.commit(symbolCountProviders);
                }
            }

            // 2. Open a genuinely fresh reader over the 3-partition state, and keep it open across the
            // reload below.
            try (TableReader reader = getReader("c")) {
                Assert.assertEquals(3, reader.getPartitionCount());
                Assert.assertEquals(0, reader.getPartitionCellKey(0));
                Assert.assertEquals(1, reader.getPartitionCellKey(1));
                Assert.assertEquals(0, reader.getPartitionCellKey(2));
                Assert.assertEquals(210, reader.getPartitionColumnVersion(1));
                Assert.assertEquals(310, reader.getPartitionColumnVersion(2));

                // 3. Seam: remove (day1,cell0) and bump the txn. (day1,cell1)'s own _txn/_cv records are
                // completely untouched by this -- only its dense tx-index shifts down (2 -> ... no, 1 -> 0).
                try (Path path = new Path()) {
                    path.of(configuration.getDbRoot()).concat(tableToken).concat(TXN_FILE_NAME).$();
                    try (TxWriter txWriter = new TxWriter(ff, configuration)) {
                        txWriter.setCompositeForTest(true);
                        txWriter.ofRW(path.$(), ColumnType.TIMESTAMP_MICRO, PartitionBy.DAY);

                        final int symbolColumnCount = txWriter.getSymbolColumnCount();
                        ObjList<SymbolCountProvider> symbolCountProviders = new ObjList<>();
                        for (int i = 0; i < symbolColumnCount; i++) {
                            final int count = txWriter.getSymbolValueCount(i);
                            symbolCountProviders.add(() -> count);
                        }

                        txWriter.removeAttachedPartitions(day1, 0);
                        txWriter.finishPartitionSizeUpdate();
                        txWriter.commit(symbolCountProviders);
                    }
                }

                Assert.assertTrue("reload() must report a change", reader.reload());

                Assert.assertEquals(2, reader.getPartitionCount());

                Assert.assertEquals(day1, reader.getPartitionTimestampByIndex(0));
                Assert.assertEquals(
                        "only (day1,cell1) remains at day1 -- must not be aliased with the deleted (day1,cell0)",
                        1, reader.getPartitionCellKey(0)
                );

                Assert.assertEquals(day2, reader.getPartitionTimestampByIndex(1));
                Assert.assertEquals(0, reader.getPartitionCellKey(1));

                // (day1,cell1) was refreshed in place (shifted down to index 0), not aliased onto
                // (day1,cell0)'s stale state, and (day2,cell0) is untouched.
                Assert.assertEquals(210, reader.getPartitionColumnVersion(0));
                Assert.assertEquals(310, reader.getPartitionColumnVersion(1));
            }
        });
    }
}
