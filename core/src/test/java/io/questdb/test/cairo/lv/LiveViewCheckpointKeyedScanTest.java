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

package io.questdb.test.cairo.lv;

import io.questdb.PropertyKey;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.lv.LiveViewCheckpointKeyProjector;
import io.questdb.cairo.lv.LiveViewCheckpointKeyedScanCost;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.table.PageFrameRecordCursorFactory;
import io.questdb.std.IntList;

import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Coverage for the pieces a keyed repair is made of: the shared partition identity a view
 * names its keys through, the forward index-backed scan that follows those keys' rows, and
 * the cost model that decides whether following them is cheaper than reading the segment.
 * <p>
 * None of it changes what a repair does. A closed segment still replays whole, because a
 * keyed replay's <b>publication</b> is the piece that is missing: {@code REPLACE_RANGE}
 * deletes the segment's range wholesale, so a replay emitting only the affected keys' rows
 * would drop every unaffected key's stored row inside it. What these cases pin is that the
 * inputs to that decision are right, and that the measurement they produce is the real
 * comparison rather than a model of it.
 * <p>
 * The view is the same reported customer shape the per-segment repair cases use: an
 * anchored WINDOW carrying an unbounded cumulative sum per account, over a base whose
 * timestamps span several anchor days so closed segments exist at all.
 */
public class LiveViewCheckpointKeyedScanTest extends AbstractLiveViewTest {

    @Test
    public void testACorrectionInOneClosedSegmentPricesItsKeyedScan() throws Exception {
        // The measurement the stage exists to take: one account corrected inside one closed
        // day, against a day holding every account's rows. The keyed side has to be the
        // smaller of the two, and the verdict has to say so.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        // The default prices one index open at 256 base rows, which is what a real
        // hourly-partitioned base against a daily segment is worth - and at forty rows a
        // day it would (correctly) prefer the whole segment whatever the key domain. The
        // verdict this case is about is the row comparison, so the setup term is priced at
        // the scale the fixture actually has.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_KEYED_SCAN_INDEX_OPEN_ROWS, 1);
        assertMemoryLeak(() -> {
            createView(seedFourAccountsOverThreeDays(), true);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit(row(5, 1, "acct-1"), job);
                Assert.assertEquals(0, job.keyedScanPricedCountForTest());

                commit(row(2, 3, "acct-1"), job);

                Assert.assertEquals(
                        "the corrected closed day must be priced exactly once",
                        1,
                        job.keyedScanPricedCountForTest()
                );
                Assert.assertEquals(0, job.keyedScanUnpricedCountForTest());
                Assert.assertEquals(
                        "one account of four is less to read than the whole day",
                        1,
                        job.keyedScanCheaperCountForTest()
                );
                Assert.assertTrue(
                        "the keyed scan must read fewer rows than the whole segment: posting="
                                + job.keyedScanPostingRowsForTest()
                                + " whole=" + job.keyedScanWholeRangeRowsForTest(),
                        job.keyedScanPostingRowsForTest() < job.keyedScanWholeRangeRowsForTest()
                );
                // The corrected account holds ten seeded rows in that day plus the
                // correction, so anything below eleven means the key never resolved to the
                // rows it names - which reads as a spectacular saving rather than as a bug.
                Assert.assertEquals(
                        "the priced keyed scan must find the corrected account's rows",
                        11,
                        job.keyedScanPostingRowsForTest()
                );
                Assert.assertEquals(
                        "the repair itself is unchanged - the segment still replays whole",
                        1,
                        job.segmentRepairCountForTest()
                );
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testPricingLeavesTheReadersIndexUsableForALaterQuery() throws Exception {
        // The estimate reads through the repair's own pinned reader, which is a pooled one.
        // TableReader hands out an index reader per partition and, for a partition whose
        // columns it has not mapped yet, hands out AND CACHES one that yields no row at all -
        // so an estimate that skipped the open would report the keyed scan as free and leave
        // that cached no-op reader behind for the next index-driven query on the same reader.
        // Both halves are asserted here: the count the estimate reaches, and the count a
        // query reaches afterwards.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            createView(seedFourAccountsOverThreeDays(), true);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit(row(5, 1, "acct-1"), job);
                // Lands in an anchor day that is closed, and in an hour partition of its own
                // that the pinned reader has never opened.
                commit(row(2, 3, "acct-1"), job);

                final String indexed = "select count() from tx "
                        + "where created_at in '2026-01-02' and cod_acct_no = 'acct-1'";
                final String scanned = "select count() from tx "
                        + "where created_at in '2026-01-02' and cod_acct_no::string = 'acct-1'";
                Assert.assertEquals("the whole-scan oracle", 11, count(scanned));
                Assert.assertEquals(
                        "an index-driven query after a priced repair must still find every row",
                        11,
                        count(indexed)
                );

                final LiveViewCheckpointKeyedScanCost cost = new LiveViewCheckpointKeyedScanCost();
                try (TableReader reader = engine.getReader(engine.getTableTokenIfExists("tx"))) {
                    cost.of(reader);
                    final IntList keys = new IntList();
                    keys.add(reader.getSymbolMapReader(1).keyOf("acct-1"));
                    Assert.assertEquals(
                            "the estimate must count the rows in a partition it has not opened yet",
                            11,
                            cost.estimateKeyedScanRows(
                                    ts("2026-01-02T00:00:00.000000Z"),
                                    ts("2026-01-02T23:59:59.999999Z"),
                                    1,
                                    keys,
                                    Long.MAX_VALUE
                            )
                    );
                }
                Assert.assertEquals(
                        "pricing must leave the reader's index cache usable",
                        11,
                        count(indexed)
                );
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testAKeyDomainOverTheBudgetLeavesTheSegmentUnpriced() throws Exception {
        // A budget of one key against a correction carrying two: the segment keeps the keys
        // it collected and reports the domain incomplete, which is not a denial - it reads
        // whole, which is what it does anyway.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SCAN_MAX_KEYS, 1);
        assertMemoryLeak(() -> {
            createView(seedFourAccountsOverThreeDays(), true);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit(row(5, 1, "acct-1"), job);

                commit(row(2, 3, "acct-1") + ", " + row(2, 4, "acct-2"), job);

                Assert.assertEquals(0, job.keyedScanPricedCountForTest());
                Assert.assertEquals(
                        "a segment past its key budget must be reported unpriced",
                        1,
                        job.keyedScanUnpricedCountForTest()
                );
                Assert.assertEquals(1, job.segmentRepairCountForTest());
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testAnUnindexedKeyIsNeverPriced() throws Exception {
        // Without an index there is nothing to name one key's rows with, so the question is
        // not asked at all - and the repair is exactly the one this view has today.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            createView(seedFourAccountsOverThreeDays(), false);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit(row(5, 1, "acct-1"), job);

                commit(row(2, 3, "acct-1"), job);

                Assert.assertEquals(0, job.keyedScanPricedCountForTest());
                Assert.assertEquals(0, job.keyedScanUnpricedCountForTest());
                Assert.assertEquals(1, job.segmentRepairCountForTest());
                Assert.assertEquals(
                        "an unindexed SYMBOL key names no column a repair could seek through",
                        -1,
                        keyProjector().getIndexedSymbolColumnIndex()
                );
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testHotKeyPricingPrefersTheWholeSegment() throws Exception {
        // The case the cost model exists for. Pricing off affectedKeys * averageRowsPerKey
        // would call one key of four a quarter of the segment; the posting lists say this
        // key holds most of it, and the merge and setup terms put it over the top.
        Assert.assertFalse(LiveViewCheckpointKeyedScanCost.isKeyedScanCheaper(900, 4, 1, 1_000, 256));
        // A sparse domain the index opens dominate: 40 rows behind 200 index opens is
        // 51,240 row-equivalents against a 1,000-row segment.
        Assert.assertFalse(LiveViewCheckpointKeyedScanCost.isKeyedScanCheaper(40, 200, 20, 1_000, 256));
        // And the shape it is there to admit: a few keys, few rows, few opens.
        Assert.assertTrue(LiveViewCheckpointKeyedScanCost.isKeyedScanCheaper(40, 2, 2, 10_000, 256));
        // An unpriceable estimate reads as expensive rather than as free.
        Assert.assertFalse(LiveViewCheckpointKeyedScanCost.isKeyedScanCheaper(
                LiveViewCheckpointKeyedScanCost.UNPRICEABLE, 0, 1, Long.MAX_VALUE, 256));
        // Neither term may wrap: a saturated count has to stay the most expensive answer.
        Assert.assertEquals(
                Long.MAX_VALUE,
                LiveViewCheckpointKeyedScanCost.keyedScanCostRows(Long.MAX_VALUE, Long.MAX_VALUE, 4096, 256)
        );
        // One key costs one sift per row and nothing more, so the merge term never charges a
        // single-key scan for a heap it does not build.
        Assert.assertEquals(100 + 2 * 256, LiveViewCheckpointKeyedScanCost.keyedScanCostRows(100, 2, 1, 256));
    }

    @Test
    public void testTheForwardIndexedCursorYieldsExactlyTheNamedKeysRows() throws Exception {
        // The cursor's own contract: the subsequence of the full forward scan whose key is
        // one of the named ones, in the same order, over the same inclusive bounds - and the
        // other keys' rows not at all.
        assertMemoryLeak(() -> {
            execute("create table tx (created_at timestamp, cod_acct_no symbol nocache index capacity 4, "
                    + "amt_txn double) timestamp(created_at) partition by hour wal");
            final StringBuilder rows = new StringBuilder();
            for (int hour = 0; hour < 12; hour++) {
                for (int account = 1; account <= 4; account++) {
                    if (rows.length() > 0) {
                        rows.append(", ");
                    }
                    rows.append(row(2, hour, "acct-" + account));
                }
            }
            execute("insert into tx values " + rows);
            drainWalQueue();

            final IntList keys = new IntList();
            try (TableReader reader = engine.getReader(engine.getTableTokenIfExists("tx"))) {
                keys.add(reader.getSymbolMapReader(1).keyOf("acct-2"));
                keys.add(reader.getSymbolMapReader(1).keyOf("acct-4"));
            }
            final long lowTs = ts("2026-01-02T02:00:00.000000Z");
            final long highTs = ts("2026-01-02T09:00:00.000000Z");

            final StringSink actual = new StringSink();
            try (RecordCursorFactory factory = select("tx")) {
                // SqlCompiler wraps every compiled query in a QueryProgress factory for
                // registry tracking; the scan underneath is what carries the substitution.
                RecordCursorFactory scan = factory;
                while (scan != null && !(scan instanceof PageFrameRecordCursorFactory)) {
                    scan = scan.getBaseFactory();
                }
                Assert.assertTrue(
                        "a plain full scan is what the substitution needs",
                        scan instanceof PageFrameRecordCursorFactory
                );
                final PageFrameRecordCursorFactory pageFrameFactory = (PageFrameRecordCursorFactory) scan;
                Assert.assertTrue(pageFrameFactory.isIndexedForwardTimestampRangeSupported(1));
                try (RecordCursor cursor = pageFrameFactory.getCursorInTimestampRangeForwardIndexed(
                        sqlExecutionContext, lowTs, highTs, 1, keys)) {
                    final Record record = cursor.getRecord();
                    while (cursor.hasNext()) {
                        actual.putISODate(record.getTimestamp(0)).putAscii('\t').put(record.getSymA(1)).putAscii('\n');
                    }
                }
            }

            final StringSink expected = new StringSink();
            try (
                    RecordCursorFactory factory = select("select created_at, cod_acct_no from tx "
                            + "where cod_acct_no in ('acct-2', 'acct-4') "
                            + "and created_at between '2026-01-02T02:00:00.000000Z' "
                            + "and '2026-01-02T09:00:00.000000Z' order by created_at");
                    RecordCursor cursor = factory.getCursor(sqlExecutionContext)
            ) {
                final Record record = cursor.getRecord();
                while (cursor.hasNext()) {
                    expected.putISODate(record.getTimestamp(0)).putAscii('\t').put(record.getSymA(1)).putAscii('\n');
                }
            }
            TestUtils.assertEquals(expected, actual);
            Assert.assertEquals(16, countLines(actual));

            // And the estimate has to name that same number. The interval covers eight
            // whole hourly partitions, so nothing here is interpolated and the count is
            // exact - which is what makes it an assertion rather than a sanity check. It is
            // also what catches the index's key space: the postings are keyed by
            // symbolKey + 1, so a table-local key passed straight through would count the
            // neighbouring account's rows and still look plausible.
            final LiveViewCheckpointKeyedScanCost cost = new LiveViewCheckpointKeyedScanCost();
            try (TableReader reader = engine.getReader(engine.getTableTokenIfExists("tx"))) {
                cost.of(reader);
                Assert.assertEquals(16, cost.estimateKeyedScanRows(lowTs, highTs, 1, keys, Long.MAX_VALUE));
                Assert.assertEquals(16, cost.getPostingRows());
                // Two keys across eight partitions, which is what HeapRowCursorFactory
                // rebuilds per page frame and what the setup term is charged for.
                Assert.assertEquals(16, cost.getIndexOpens());
            }
        });
    }

    @Test
    public void testTheSharedKeyProjectorNamesTheIndexedSymbolColumn() throws Exception {
        // The identity every function on the view shares, and the one thing a keyed repair
        // needs off it that the two sinks do not carry: which base column the index is on.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            createView(seedFourAccountsOverThreeDays(), true);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                final LiveViewCheckpointKeyProjector projector = keyProjector();
                Assert.assertEquals(1, projector.getPartitionByColumnCount());
                Assert.assertEquals(1, projector.getPartitionByColumnIndex(0));
                Assert.assertEquals(1, projector.getIndexedSymbolColumnIndex());
                Assert.assertNotNull(projector.getKeySink());
                Assert.assertNotNull(projector.getCheckpointKeySink());
                Assert.assertNotSame(
                        "a SYMBOL key column needs a second sink writing its resolved string",
                        projector.getKeySink(),
                        projector.getCheckpointKeySink()
                );
            }
        });
    }

    private long count(String sql) throws Exception {
        try (
                RecordCursorFactory factory = select(sql);
                RecordCursor cursor = factory.getCursor(sqlExecutionContext)
        ) {
            Assert.assertTrue(cursor.hasNext());
            return cursor.getRecord().getLong(0);
        }
    }

    private static int countLines(StringSink sink) {
        int lines = 0;
        for (int i = 0, n = sink.length(); i < n; i++) {
            if (sink.charAt(i) == '\n') {
                lines++;
            }
        }
        return lines;
    }

    private void assertViewMatchesRecompute() throws Exception {
        final String bucket = "timestamp_floor('1d', created_at, '1970-01-01T00:00:00.000000Z'::timestamp)";
        final String recompute = "select created_at, cod_acct_no, "
                + "sum(amt_txn) over (partition by cod_acct_no, bucket order by created_at "
                + "rows between unbounded preceding and current row) as cumulative_sum "
                + "from (select created_at, cod_acct_no, amt_txn, " + bucket + " as bucket from tx)";
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(" + recompute + ") order by 2, 1",
                "(lv) order by 2, 1",
                LOG,
                true
        );
        assertNoRefreshFaults("lv");
    }

    private void commit(String values, LiveViewRefreshJob job) throws Exception {
        execute("insert into tx values " + values);
        drainWalQueue();
        driveRefreshToQuiescence(job);
    }

    private void createView(String seedRows, boolean isKeyIndexed) throws Exception {
        execute("create table tx (created_at timestamp, cod_acct_no symbol nocache"
                + (isKeyIndexed ? " index capacity 4" : "") + ", "
                + "amt_txn double) timestamp(created_at) partition by hour wal");
        execute("insert into tx values " + seedRows);
        drainWalQueue();
        execute("create live view lv flush every 100ms start from beginning as "
                + "select created_at, cod_acct_no, sum(amt_txn) over w as cumulative_sum "
                + "from tx window w as (partition by cod_acct_no order by created_at anchor daily '00:00')");
    }

    private LiveViewCheckpointKeyProjector keyProjector() {
        final LiveViewCheckpointKeyProjector projector = viewInstance()
                .getCompiledPlan()
                .getWindowFactory()
                .getCheckpointKeyProjector();
        Assert.assertNotNull("a single-identity view must compile a shared key projector", projector);
        return projector;
    }

    /**
     * One row of {@code account} at {@code hour} on 2026-01-{@code day}, as an INSERT tuple.
     * The day is what carries the case: with a daily anchor it is also the segment.
     */
    private String row(int day, int hour, String account) {
        return row(day, hour, 0, account);
    }

    private String row(int day, int hour, int minute, String account) {
        return "('2026-01-" + String.format("%02d", day) + "T" + String.format("%02d", hour)
                + ":" + String.format("%02d", minute) + ":00.000000Z', '" + account + "', 1.0)";
    }

    /**
     * Ten rows of each of four accounts on each of 2026-01-02, 2026-01-03 and 2026-01-04,
     * all inside one hour of their day. A correction touching one account therefore leaves
     * three quarters of the segment the keyed scan must not read, and the whole day sits in
     * one partition so the per-key-per-frame setup does not dominate the comparison at this
     * scale the way it does on a real hourly-partitioned base.
     */
    private String seedFourAccountsOverThreeDays() {
        final StringBuilder rows = new StringBuilder();
        for (int day = 2; day <= 4; day++) {
            for (int minute = 0; minute < 10; minute++) {
                for (int account = 1; account <= 4; account++) {
                    if (rows.length() > 0) {
                        rows.append(", ");
                    }
                    rows.append(row(day, 1, minute * 4 + account, "acct-" + account));
                }
            }
        }
        return rows.toString();
    }

    private LiveViewInstance viewInstance() {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull("live view 'lv' must be registered", instance);
        return instance;
    }
}
