/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.test.cutlass.suspend;

import io.questdb.cairo.DataUnavailableException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.pool.ReaderPool;
import io.questdb.network.DefaultIODispatcherConfiguration;
import io.questdb.network.SuspendEvent;
import io.questdb.network.SuspendEventFactory;
import io.questdb.std.ConcurrentHashMap;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;

public class TestCases {
    private static final String TABLE_X_DDL = "create table x as ( " +
            "  select " +
            "    cast(x as int) i, " +
            "    rnd_double(2) d, " +
            "    rnd_long() l, " +
            "    rnd_str('a','b','c') s, " +
            "    rnd_symbol('a','b','c') sym, " +
            "    rnd_symbol('a','b','c') isym, " +
            "    timestamp_sequence(0, 100000000) ts " +
            "   from long_sequence(100)" +
            "), index(isym) timestamp(ts) partition by hour";

    private static final String TABLE_Y_DDL = "create table y as (select * from x), index(isym) timestamp(ts) partition by hour";
    private final SuspendingReaderListener suspendingListener = new SuspendingReaderListener();
    private final ObjList<TestCase> testCases = new ObjList<>();

    public TestCases() {
        addTestCase("select * from x");
        addTestCase("select * from x order by ts desc");

        // AsyncFilteredRecordCursor
        addTestCase("select * from x where i = 42");
        addTestCase("select * from x where i = 42 limit 3");

        // AsyncFilteredNegativeLimitRecordCursor
        addTestCase("select * from x where i = 42 limit -3");

        // FilteredRecordCursor
        addTestCase("select * from (x union all y) where i = 42");

        // FilterOnSubQueryRecordCursorFactory
        addTestCase("select * from x where isym in (select s from y limit 3) and i != 42");

        // FilterOnExcludedValuesRecordCursorFactory
        addTestCase("select * from x where isym not in ('a','b') and i != 42");

        // FilterOnValuesRecordCursorFactory
        addTestCase("select * from x where isym in (?,?)", false, "d", "a");

        // DataFrameRecordCursorFactory
        addTestCase("select * from x where isym = ? and i != 42", false, "c");

        // DeferredSingleSymbolFilterDataFrameRecordCursorFactory
        addTestCase("select * from x where isym = ?", false, "b");

        // LimitRecordCursorFactory, FullFwdDataFrameCursor, FullBwdDataFrameCursor
        addTestCase("select * from x limit 1");
        addTestCase("select * from x limit 1,3");
        addTestCase("select * from x order by ts desc limit 1,3");
        addTestCase("select * from x limit 1,-1");
        addTestCase("select * from x limit 0,-1");
        addTestCase("select * from x limit -1");
        addTestCase("select * from x limit -4000");
        addTestCase("select * from x limit -3,-1");
        addTestCase("select * from x limit -3,-4", true);
        addTestCase("select * from (x union all y) limit 1");
        addTestCase("select * from (x union all y) limit 1,3");
        addTestCase("select * from (x union all y) limit 1,-1");
        addTestCase("select * from (x union all y) limit -1");
        addTestCase("select * from (x union all y) limit -3,-1");
        addTestCase("select * from (x union all (y where isym = 'a')) limit 1");
        addTestCase("select * from (x union all (y where isym = 'a')) limit 1,3");
        addTestCase("select * from (x union all (y where isym = 'a')) limit 1,-1");
        addTestCase("select * from (x union all (y where isym = 'a')) limit -1");
        addTestCase("select * from (x union all (y where isym = 'a')) limit -3,-1");
        addTestCase("select * from (x union all (y where isym = 'a')) limit -4000,-1");

        // SortedRecordCursorFactory
        addTestCase("select * from (x union all y) order by i");

        // SortedLightRecordCursorFactory
        addTestCase("select sym, min(i) imin from x where ts in '1970-01-01' order by imin");
        addTestCase("select * from x where ts in '1970-01-01' order by isym, ts desc");

        // SortedSymbolIndexRecordCursorFactory
        addTestCase("select * from x where ts in '1970-01-01T00' order by isym, ts desc");

        // LimitedSizeSortedLightRecordCursorFactory
        addTestCase("select * from x order by i limit 3");

        // CachedWindowRecordCursorFactory
        addTestCase("select i, row_number() over (partition by sym) from x");
        addTestCase("select i, row_number() over (partition by sym order by ts) from x");

        // InSymbolCursorFunctionFactory
        addTestCase("select * from x where sym in (select sym from y)");
        addTestCase("select * from x where cast(s as symbol) in (select sym from y)");
        addTestCase("select * from x where sym in (select sym from y where isym in (select isym from x limit 3))");

        // CountRecordCursorFactory
        addTestCase("select count() from x where isym = 'c'");

        // DistinctTimeSeriesRecordCursorFactory
        addTestCase("select distinct * from x");

        // DistinctRecordCursorFactory
        addTestCase("select distinct sym from (x union all y)");

        // DistinctKeyRecordCursorFactory
        addTestCase("select distinct sym from x order by sym");

        // GroupByNotKeyedVectorRecordCursor
        addTestCase("select max(i), min(i) from x");

        // vect/GroupByRecordCursorFactory
        // order by is added here to guarantee a deterministic order in the result set
        addTestCase("select sym, max(i), min(i) from x order by sym");

        // GroupByNotKeyedRecordCursorFactory
        addTestCase("select max(i), min(i) from (x union all y)");

        // GroupByRecordCursorFactory
        addTestCase("select sym, max(i), min(i) from (x union all y)");

        // SampleByFillNoneNotKeyedRecordCursor
        addTestCase("select max(i), min(i) from x sample by 1h");

        // SampleByFillNoneRecordCursor
        addTestCase("select sym, max(i), min(i) from x sample by 1h");

        // SampleByFillPrevNotKeyedRecordCursor
        addTestCase("select max(i), min(i) from x sample by 1h fill(prev)");

        // SampleByFillPrevRecordCursor
        addTestCase("select sym, max(i), min(i) from x sample by 1h fill(prev)");

        // SampleByFillValueNotKeyedRecordCursor
        addTestCase("select max(i), min(i) from x sample by 1h fill(42,42)");

        // SampleByFillValueRecordCursor
        addTestCase("select sym, max(i), min(i) from x sample by 1h fill(null)");
        addTestCase("select sym, max(i), min(i) from x sample by 1h fill(42,42)");

        // SampleByInterpolateRecordCursorFactory
        addTestCase("select max(i), min(i) from x sample by 1h fill(linear)");

        // SampleByFillNullNotKeyedRecordCursorFactory
        addTestCase("select sum(i) s, ts from x sample by 30m fill(null)");

        // SampleByFirstLastRecordCursorFactory
        addTestCase("select first(i) f, last(i) l, isym, ts from x where isym = 'a' sample by 2h");

        // LatestByValueListRecordCursor
        addTestCase("select * from x latest on ts partition by sym");
        addTestCase("select * from x where isym <> 'd' latest on ts partition by isym");
        addTestCase("select * from x where isym not in ('e','f') and i > 42 latest on ts partition by isym");

        // LatestByAllIndexedRecordCursor
        addTestCase("select * from x latest on ts partition by isym");

        // LatestByAllRecordCursor
        addTestCase("select * from x latest on ts partition by s");

        // LatestByAllFilteredRecordCursor
        addTestCase("select * from x where i != 42 latest on ts partition by s");

        // LatestByLightRecordCursorFactory
        addTestCase("select * from ((x union all y) order by ts asc) latest on ts partition by s");
        addTestCase("select * from ((x union all y) order by ts desc) latest on ts partition by s");

        // LatestByRecordCursorFactory
        addTestCase("with yy as (select ts, max(s) s from y sample by 1h) select * from yy latest on ts partition by s");

        // LatestByValueRecordCursor
        addTestCase("select * from x where sym in ('a') latest on ts partition by sym");

        // LatestByValueFilteredRecordCursor
        addTestCase("select * from x where sym in ('a') and i > 42 latest on ts partition by sym");

        // LatestByValueDeferredFilteredRecordCursorFactory
        addTestCase("select * from x where sym in ('d') and i > 42 latest on ts partition by sym", true);

        // LatestByAllSymbolsFilteredRecordCursor
        addTestCase("select * from x latest on ts partition by sym, isym");
        addTestCase("select * from x where i != 42 latest on ts partition by sym, isym");

        // LatestBySubQueryRecordCursorFactory, LatestByValuesRecordCursor
        addTestCase("select * from x where sym in (select sym from y limit 3) latest on ts partition by sym");

        // LatestBySubQueryRecordCursorFactory, LatestByValuesFilteredRecordCursor
        addTestCase("select * from x where sym in (select sym from y limit 3) and i%2 <> 1 latest on ts partition by sym");

        // LatestByValueIndexedFilteredRecordCursorFactory
        addTestCase("select * from x where isym = 'c' and i <> 13 latest on ts partition by isym");

        // DataFrameRecordCursorFactory, LatestByValueIndexedRowCursorFactory
        addTestCase("select * from x where isym = 'c' latest on ts partition by isym");

        // DataFrameRecordCursorFactory, LatestByValueDeferredIndexedRowCursorFactory
        addTestCase("select * from x where isym = ? latest on ts partition by isym", false, "a");

        // LatestByValueDeferredIndexedFilteredRecordCursorFactory
        addTestCase("select * from x where isym = ? and i <> 0 latest on ts partition by isym", false, "c");

        // LatestByValuesIndexedFilteredRecordCursor
        addTestCase("select * from x where isym in ('a','c') and i < 13 latest on ts partition by isym");

        // LatestByValuesIndexedRecordCursor
        addTestCase("select * from x where isym in ('b','c') latest on ts partition by isym");

        // HashJoinRecordCursorFactory
        addTestCase("select * from x join (x union all y) on (sym)");

        // HashOuterJoinRecordCursorFactory
        addTestCase("select * from x left join (x union all y) on (sym)");

        // HashOuterJoinFilteredRecordCursorFactory
        addTestCase("select * from x left join (x union all y) xy on x.sym = xy.sym and x.sym ~ 'a'");

        // HashJoinLightRecordCursorFactory
        addTestCase("select * from x join y on (sym)");

        // HashOuterJoinLightRecordCursorFactory
        addTestCase("select * from x left join y on (sym)");

        // HashOuterJoinFilteredLightRecordCursorFactory
        addTestCase("select * from x left join y on x.sym = y.sym and x.sym ~ 'a'");

        // NestedLoopLeftJoinRecordCursorFactory
        addTestCase("select * from x left join y on x.i + 42 = y.i");

        // CrossJoinRecordCursorFactory
        addTestCase("select * from x cross join y");

        // AsOfJoinNoKeyRecordCursorFactory
        addTestCase("select * from x asof join y");

        // AsOfJoinRecordCursorFactory
        addTestCase("with yy as (select ts, max(l) l from y sample by 1h) select * from x asof join (yy timestamp(ts)) on (l)");

        // AsOfJoinLightRecordCursorFactory
        addTestCase("select * from x asof join y on (sym)");

        // LtJoinNoKeyRecordCursorFactory
        addTestCase("select * from x lt join y");

        // LtJoinRecordCursorFactory
        addTestCase("with yy as (select ts, max(l) l from y sample by 1h) select * from x lt join (yy timestamp(ts)) on (l)");

        // LtJoinLightRecordCursorFactory
        addTestCase("select * from x lt join y on (l)");

        // SpliceJoinLightRecordCursorFactory
        addTestCase("select * from x splice join y");

        // UnionRecordCursorFactory
        addTestCase("x union y");

        // UnionAllRecordCursorFactory
        addTestCase("x union all y");

        // ExceptRecordCursor
        addTestCase("x except y", true);

        // ExceptCastRecordCursor
        addTestCase("(select s sym from x) except (select sym from y)", true);

        // ExceptAllRecordCursor
        addTestCase("x except all y", true);

        // ExceptAllCastRecordCursor
        addTestCase("(select s sym from x) except all (select sym from y)", true);

        // IntersectRecordCursor
        addTestCase("x intersect y");

        // IntersectCastRecordCursor
        addTestCase("(select s sym from x) intersect (select sym from y)");

        // IntersectAllRecordCursor
        addTestCase("x intersect all y");

        // IntersectAllCastRecordCursor
        addTestCase("(select s sym from x) intersect all (select sym from y)");
    }

    public String getDdlX() {
        return TABLE_X_DDL;
    }

    public String getDdlY() {
        return TABLE_Y_DDL;
    }

    public TestCase getQuick(int i) {
        return testCases.getQuick(i);
    }

    public ReaderPool.ReaderListener getSuspendingListener() {
        suspendingListener.clear();
        return suspendingListener;
    }

    public int size() {
        return testCases.size();
    }

    private void addTestCase(String query, boolean allowEmptyResultSet) {
        testCases.add(new TestCase(query, allowEmptyResultSet));
    }

    @SuppressWarnings("SameParameterValue")
    private void addTestCase(String query, boolean allowEmptyResultSet, String... bindVariableValues) {
        testCases.add(new TestCase(query, allowEmptyResultSet, bindVariableValues));
    }

    private void addTestCase(String query) {
        addTestCase(query, false);
    }

    /**
     * This listener varies DataUnavailableException and successful execution of TableReader#openPartition()
     * in the following sequence: exception, success, exception, success, etc.
     */
    private static class SuspendingReaderListener implements ReaderPool.ReaderListener, Mutable {

        private final ConcurrentHashMap<SuspendEvent> suspendedPartitions = new ConcurrentHashMap<>();

        @Override
        public void clear() {
            suspendedPartitions.forEach((charSequence, suspendEvent) -> suspendEvent.close());
        }

        @Override
        public void onOpenPartition(TableToken tableToken, int partitionIndex) {
            final String key = tableToken + "$" + partitionIndex;
            SuspendEvent computedEvent = suspendedPartitions.compute(key, (charSequence, prevEvent) -> {
                if (prevEvent != null) {
                    // Success case.
                    prevEvent.close();
                    return null;
                }
                // Exception case.
                SuspendEvent nextEvent = SuspendEventFactory.newInstance(DefaultIODispatcherConfiguration.INSTANCE);
                // Mark the event as immediately fulfilled.
                nextEvent.trigger();
                return nextEvent;
            });
            if (computedEvent != null) {
                throw DataUnavailableException.instance(tableToken, String.valueOf(partitionIndex), computedEvent);
            }
        }
    }
}
