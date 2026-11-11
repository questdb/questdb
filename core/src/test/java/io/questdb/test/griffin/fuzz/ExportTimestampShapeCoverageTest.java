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

package io.questdb.test.griffin.fuzz;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cutlass.parquet.ParquetExportMode;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.LimitRecordCursorFactory;
import io.questdb.griffin.engine.orderby.EncodedSortLightRecordCursorFactory;
import io.questdb.griffin.engine.orderby.EncodedSortLimitedLightRecordCursorFactory;
import io.questdb.griffin.engine.orderby.EncodedSortRecordCursorFactory;
import io.questdb.griffin.engine.orderby.LimitedSizeSortedLightRecordCursorFactory;
import io.questdb.griffin.engine.orderby.SortedLightRecordCursorFactory;
import io.questdb.griffin.engine.orderby.SortedRecordCursorFactory;
import io.questdb.griffin.engine.table.AdaptiveSymbolPatternRecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncFilteredRecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncJitFilteredRecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncTopKRecordCursorFactory;
import io.questdb.griffin.engine.table.DeferredSingleSymbolFilterPageFrameRecordCursorFactory;
import io.questdb.griffin.engine.table.FilterOnExcludedValuesRecordCursorFactory;
import io.questdb.griffin.engine.table.FilterOnValuesRecordCursorFactory;
import io.questdb.griffin.engine.table.FilteredRecordCursorFactory;
import io.questdb.griffin.engine.table.PageFrameRecordCursorFactory;
import io.questdb.griffin.engine.table.SelectedRecordCursorFactory;
import io.questdb.griffin.engine.table.VirtualRecordCursorFactory;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.griffin.fuzz.types.DoubleType;
import io.questdb.test.griffin.fuzz.types.IntKeyType;
import io.questdb.test.griffin.fuzz.types.LongKeyType;
import io.questdb.test.griffin.fuzz.types.LongType;
import io.questdb.test.griffin.fuzz.types.StringType;
import io.questdb.test.griffin.fuzz.types.SymbolType;
import io.questdb.test.griffin.fuzz.types.TimestampType;
import io.questdb.test.griffin.fuzz.types.VarcharKeyType;
import org.junit.Assert;
import org.junit.Test;

/**
 * Pins the plans that QueryFuzzTest's generated queries hand to the streamed timestamp check of {@link QueryRunner}:
 * plans whose timestamp a CURSOR_BASED Parquet export of a lazily type-converted column writes as the file's
 * designated timestamp, because a table scan orders it, in each family whose factories answer
 * {@link RecordCursorFactory#hasTableScanTimestamp()}, and the plans whose timestamp it does not stream. Both tables
 * retype a column after their first days went to Parquet, so their plans report converted columns, which a
 * CURSOR_BASED export streams or a temp table materializes. A generator that stopped reaching one of them would leave
 * that answer unchecked while the fuzz run stays green. An index scan of several keys reports no order only under a
 * sort, or under a parent that drops the timestamp, so no generated plan answers true while it reports no order;
 * HasTableScanTimestampTest pins such scans.
 * <p>
 * The generator is random, so the assertions run over a corpus; the seed is fixed so a failure is reproducible.
 */
public class ExportTimestampShapeCoverageTest extends AbstractCairoTest {
    private static final int CORPUS_SIZE = 4_000;

    @Test
    public void testGeneratedQueriesReachStreamedTimestampPaths() throws Exception {
        assertMemoryLeak(() -> {
            // The columns of FuzzTableFactory's tables, with the join keys sym, k, lk and vk, other columns, and ts last.
            // sym is indexed for the index scans, and c1 and c3 turn from INT to LONG and from VARCHAR to STRING after
            // the first two days went to Parquet, as FuzzTableFactory's retype step does.
            final ObjList<FuzzTable> tables = new ObjList<>();
            for (int t = 0; t < 2; t++) {
                final String tableName = "fuzz_t" + t;
                execute("CREATE TABLE " + tableName + " (sym SYMBOL INDEX, k INT, lk LONG, vk VARCHAR, c0 SYMBOL, c1 INT, c2 DOUBLE, c3 VARCHAR, ts TIMESTAMP)"
                        + " TIMESTAMP(ts) PARTITION BY DAY WAL");
                execute("INSERT INTO " + tableName + " SELECT"
                        + " CASE WHEN x % 5 = 0 THEN NULL ELSE 'a' || (x % 4) END, (x % 16)::INT, x % 16, 'v' || (x % 16),"
                        + " 'b' || (x % 3), x::INT, x / 3.0, 'w' || x, timestamp_sequence('2024-01-01', 3_600_000_000L)"
                        + " FROM long_sequence(72)");
                drainWalQueue();
                execute("ALTER TABLE " + tableName + " CONVERT PARTITION TO PARQUET LIST '2024-01-01', '2024-01-02'");
                drainWalQueue();
                execute("ALTER TABLE " + tableName + " ALTER COLUMN c1 TYPE LONG");
                execute("ALTER TABLE " + tableName + " ALTER COLUMN c3 TYPE STRING");
                drainWalQueue();
                final ObjList<FuzzColumn> columns = new ObjList<>();
                columns.add(new FuzzColumn("sym", SymbolType.INSTANCE, new FuzzIndex(FuzzIndex.Kind.BITMAP, null)));
                columns.add(new FuzzColumn("k", IntKeyType.INSTANCE));
                columns.add(new FuzzColumn("lk", LongKeyType.INSTANCE));
                columns.add(new FuzzColumn("vk", VarcharKeyType.INSTANCE));
                columns.add(new FuzzColumn("c0", SymbolType.INSTANCE));
                columns.add(new FuzzColumn("c1", LongType.INSTANCE));
                columns.add(new FuzzColumn("c2", DoubleType.INSTANCE));
                columns.add(new FuzzColumn("c3", StringType.INSTANCE));
                columns.add(new FuzzColumn("ts", TimestampType.INSTANCE));
                tables.add(new FuzzTable(tableName, columns, "ts"));
            }

            final Rnd rnd = new Rnd(0x5eed, 0x7135);
            int forwardCount = 0;
            int backwardCount = 0;
            int otherCount = 0;
            int notStreamedCount = 0;
            int noTimestampCount = 0;
            int scanCount = 0;
            int filterCount = 0;
            int indexScanCount = 0;
            int projectionCount = 0;
            int limitCount = 0;
            int sortCount = 0;
            int cursorBasedCount = 0;
            int tempTableCount = 0;
            int checkedCount = 0;
            for (int i = 0; i < CORPUS_SIZE; i++) {
                final String sql = QueryGenerator.generate(rnd, tables, null, false, true, true, true, true, true, true).sql();
                try (RecordCursorFactory factory = select(sql)) {
                    final int timestampIndex = factory.getMetadata().getTimestampIndex();
                    // QueryRunner asks a plan for its direction only once the plan answered, as an export does.
                    final int direction = QueryRunner.streamedTimestampDirection(factory);
                    if (factory.hasParquetConvertedColumns(sqlExecutionContext)) {
                        switch (ParquetExportMode.determineExportMode(factory, false, sqlExecutionContext)) {
                            case CURSOR_BASED -> cursorBasedCount++;
                            case TEMP_TABLE -> tempTableCount++;
                            default -> Assert.fail("a plan that reads a retyped column exported its page frames: " + sql);
                        }
                    }
                    if (timestampIndex < 0) {
                        noTimestampCount++;
                    } else if (!factory.hasTableScanTimestamp()) {
                        notStreamedCount++;
                    } else if (direction == RecordCursorFactory.SCAN_DIRECTION_FORWARD) {
                        forwardCount++;
                    } else if (direction == RecordCursorFactory.SCAN_DIRECTION_BACKWARD) {
                        backwardCount++;
                    } else {
                        // a plan that promises no order does not stream its timestamp
                        Assert.assertFalse(sql, ParquetExportMode.hasStreamableTimestamp(factory));
                        otherCount++;
                    }
                    if (direction != RecordCursorFactory.SCAN_DIRECTION_OTHER) {
                        // every factory of the plan answered for the timestamp
                        boolean hasScan = false;
                        boolean hasFilter = false;
                        boolean hasIndexScan = false;
                        boolean hasProjection = false;
                        boolean hasLimit = false;
                        boolean hasSort = false;
                        for (RecordCursorFactory f = factory; f != null; f = f.getBaseFactory()) {
                            hasIndexScan |= f instanceof DeferredSingleSymbolFilterPageFrameRecordCursorFactory
                                    || (f instanceof PageFrameRecordCursorFactory pf && pf.usesIndex())
                                    || f instanceof FilterOnValuesRecordCursorFactory
                                    || f instanceof FilterOnExcludedValuesRecordCursorFactory
                                    || f instanceof AdaptiveSymbolPatternRecordCursorFactory;
                            hasScan |= f instanceof PageFrameRecordCursorFactory pf && !pf.usesIndex();
                            hasFilter |= f instanceof AsyncJitFilteredRecordCursorFactory
                                    || f instanceof AsyncFilteredRecordCursorFactory
                                    || f instanceof FilteredRecordCursorFactory;
                            hasProjection |= f instanceof SelectedRecordCursorFactory || f instanceof VirtualRecordCursorFactory;
                            hasLimit |= f instanceof LimitRecordCursorFactory;
                            hasSort |= isSort(f);
                        }
                        scanCount += hasScan ? 1 : 0;
                        filterCount += hasFilter ? 1 : 0;
                        indexScanCount += hasIndexScan ? 1 : 0;
                        projectionCount += hasProjection ? 1 : 0;
                        limitCount += hasLimit ? 1 : 0;
                        sortCount += hasSort ? 1 : 0;
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            Assert.assertEquals(sql, -1, QueryRunner.findStreamedTimestampViolation(cursor, timestampIndex, direction));
                        }
                        checkedCount++;
                    }
                } catch (SqlException | CairoException | ImplicitCastException e) {
                    // QueryRunner skips the generated queries the engine rejects, at compile or at run time, as well.
                }
            }
            final String counts = "forward: " + forwardCount + ", backward: " + backwardCount + ", other: " + otherCount
                    + ", not streamed: " + notStreamedCount + ", no timestamp: " + noTimestampCount
                    + ", scan: " + scanCount + ", filter: " + filterCount + ", index scan: " + indexScanCount
                    + ", projection: " + projectionCount + ", limit: " + limitCount + ", sort: " + sortCount
                    + ", cursor based: " + cursorBasedCount + ", temp table: " + tempTableCount + ", checked: " + checkedCount;
            Assert.assertTrue("a streamed timestamp that ascends, " + counts, forwardCount > 0);
            Assert.assertTrue("a streamed timestamp that descends, " + counts, backwardCount > 0);
            Assert.assertTrue("a timestamp that no table scan orders, " + counts, notStreamedCount > 0);
            Assert.assertTrue("a plan without a timestamp, " + counts, noTimestampCount > 0);
            Assert.assertTrue("a table scan, " + counts, scanCount > 0);
            Assert.assertTrue("a filter, " + counts, filterCount > 0);
            Assert.assertTrue("an index scan, " + counts, indexScanCount > 0);
            Assert.assertTrue("a projection, " + counts, projectionCount > 0);
            Assert.assertTrue("a LIMIT, " + counts, limitCount > 0);
            Assert.assertTrue("a sort on the timestamp, " + counts, sortCount > 0);
            Assert.assertTrue("a retyped column streamed, " + counts, cursorBasedCount > 0);
            Assert.assertTrue("a retyped column through a temp table, " + counts, tempTableCount > 0);
            Assert.assertEquals("the check runs on every streamed timestamp, " + counts, forwardCount + backwardCount, checkedCount);
        });
    }

    private static boolean isSort(RecordCursorFactory factory) {
        return factory instanceof EncodedSortLightRecordCursorFactory
                || factory instanceof EncodedSortLimitedLightRecordCursorFactory
                || factory instanceof EncodedSortRecordCursorFactory
                || factory instanceof LimitedSizeSortedLightRecordCursorFactory
                || factory instanceof SortedLightRecordCursorFactory
                || factory instanceof SortedRecordCursorFactory
                || factory instanceof AsyncTopKRecordCursorFactory;
    }
}
