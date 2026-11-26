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


package io.questdb.test.griffin;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.FunctionParser;
import io.questdb.griffin.IntervalAnalysis;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.TextPlanSink;
import io.questdb.griffin.engine.functions.MonotonicTimestampFunctionFactory;
import io.questdb.griffin.model.RuntimeIntervalModel;
import io.questdb.griffin.plan.logical.FilterPlan;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.ProjectPlan;
import io.questdb.griffin.plan.logical.ScanPlan;
import io.questdb.std.LongList;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Runs timestamp predicates through {@link IntervalAnalysis} and through codegen's interval extraction, whose
 * paranoia check compares the two models bit for bit, and asserts the analysis decided each of them.
 */
public class IntervalAnalysisTest extends AbstractCairoTest {
    private static final String[] DYNAMIC = {
            "ts > now()",
            "ts = now()",
            "ts != now()",
            "ts BETWEEN now() AND '2024-01-01'",
            "ts BETWEEN '2024-01-01' AND now()",
            "ts BETWEEN now() AND now()",
            "ts NOT BETWEEN now() AND dateadd('d', 1, now())",
            "ts IN '$now-1h..$now'",
            "ts NOT IN '$now-1h..$now'",
            "ts IN '2024-01' AND ts IN '$now-1h..$now'",
            "ts = $1",
            "ts > $1 AND ts < '2024-01-02'",
            "ts IN '2024-01' AND ts > now()",
            "ts > (SELECT min(ts) FROM %s)",
            "ts = (SELECT max(ts) FROM %s)",
            "ts BETWEEN (SELECT min(ts) FROM %s) AND '2024-01-02'",
    };
    // Static or dynamic depending on the table's precision.
    private static final String[] MIXED = {
            "ts = now() OR ts = '2024-01-01T00:00:00.000000Z'",
            "ts = '2024-01-01T00:00:00.000000Z' OR ts IN ('2024-02-01', now())",
            "ts = $1 OR ts = $1",
            "ts IN '$now-1h..$now' AND ts IN '2024-01-01T10:00:00.123456789Z'",
            "ts > now() AND ts = NULL",
            "ts = now() AND ts = '2024-01-01T10:00:00.123456789Z'",
            "ts IN (now(), '2024-01-01')",
            "ts IN ($1, '2024-01-01', '2024-01-02T10:00:00.000000Z'::timestamp)",
    };
    private static final String[] MONOTONIC = {
            "ts::long >= 1_704_067_200_000_000",
            "ts::long BETWEEN 1_704_067_200_000_000 AND 1_704_153_600_000_000",
            "ts::timestamp_ns > '2024-01-01'",
            "ts::timestamp < '2024-01-02'",
            "date_trunc('day', ts) = '2024-01-01'",
            "'2024-01-01' <= date_trunc('day', ts)",
            "date_trunc('month', ts) BETWEEN '2024-01-01' AND '2024-03-01'",
            "date_trunc('day', ts) BETWEEN '2024-01-04' AND '2024-01-02'",
            "date_trunc('microsecond', ts) > '2024-01-01'",
            "date_trunc('nanosecond', ts) > '2024-01-01'",
            "date_trunc('week', ts) >= '2024-01-01'",
            "date_trunc('day', ts) IN '2024-01'",
            "timestamp_floor('d', ts) = '2024-01-01'",
            "timestamp_floor('15m', ts) >= '2024-01-01T10:00'",
            "timestamp_floor('3d', ts) < '2024-01-10'",
            "timestamp_floor('5n', ts) > '2024-01-01'",
            "timestamp_floor('2M', ts) <= '2024-05-01'",
            "timestamp_floor('1h', ts, '2024-01-01T00:30:00.000000Z') >= '2024-01-01T10:00'",
            "timestamp_floor('1h', ts, '2024-01-01T00:30:00.000000Z') < '2023-12-01'",
            "timestamp_floor('1h', ts, null, '00:15', null) > '2024-01-01'",
            "timestamp_floor('1h', ts, null, '00:00', 'Europe/London') BETWEEN '2024-03-31' AND '2024-04-01'",
            "timestamp_floor('1d', ts, null, '00:00', '+02:00') = '2024-01-01'",
            "timestamp_floor('1d', ts, null, '00:00', 'UTC') = '2024-01-01'",
            "timestamp_floor('1M', ts, null, '00:00', 'Europe/Berlin') > '2024-01-01'",
            "timestamp_floor('30m', ts, '2024-01-01', '00:10', 'Asia/Kolkata') <= '2024-01-02'",
            "timestamp_floor_utc('1h', ts, null, '00:00', 'Europe/Berlin') > '2024-01-01'",
            "timestamp_floor_utc('1d', ts, null, '00:00', '-03:00') < '2024-01-05'",
            "timestamp_ceil('h', ts) <= '2024-01-01T10:00'",
            "timestamp_ceil('M', ts) > '2024-01-01'",
            "year(ts) = 2024",
            "year(ts) BETWEEN 2023 AND 2024",
            "year(ts) > 300_001",
            "extract(year FROM ts) = 2024",
            "extract(year FROM ts) BETWEEN 2023 AND 2024",
            "extract(year FROM to_timezone(ts, 'Asia/Tokyo')) < 2024",
            "year(ts) IN (2024)",
            "ts::long IN (1_704_103_200_000_000)",
            "date_trunc('microsecond', ts) IN ('2024-01-01T10:00:00.000000Z', '2024-01-03')",
            "date_trunc('nanosecond', ts) IN ('2024-01-01T10:00:00.000000Z'::timestamp)",
            "date_trunc('microsecond', ts) < (SELECT max(ts) FROM %s)",
            "ts::timestamp < (SELECT max(ts) FROM %s)",
            "ts::timestamp_ns = (SELECT max(ts) FROM %s)",
            "dateadd('h', 1, ts) > '2024-01-01'",
            "dateadd('M', 1, ts) < '2024-03-01'",
            "dateadd('d', -2, ts) BETWEEN '2024-01-01' AND '2024-01-03'",
            "dateadd('y', 1, ts) >= '2025-01-01'",
            "dateadd('h', 1, ts, 'Europe/Berlin') > '2024-01-01'",
            "dateadd('d', 1, ts, '+03:00') < '2024-01-05'",
            "dateadd('M', 1, ts, 'America/New_York') BETWEEN '2024-02-01' AND '2024-03-01'",
            "to_timezone(ts, 'Europe/Berlin') >= '2024-01-01'",
            "to_timezone(ts, '+02:00') < '2024-01-02'",
            "to_utc(ts, 'America/New_York') BETWEEN '2024-03-10' AND '2024-03-11'",
            "to_utc(ts, '-05:00') = '2024-01-01T10:00:00.000000Z'",
            "to_utc(ts, 'Europe/Berlin') IN '2024-01-01'",
            "ts + 1_000_000 > '2024-01-01'",
            "ts - 3_600_000_000 <= '2024-01-01'",
            "ts + 9_000_000_000_000_000_000 < '2022-06-01'",
            "ts + NULL > '2024-01-01'",
            "ts + v > '2024-01-01'",
            "date_trunc('day', to_utc(ts, 'Europe/Berlin')) = '2024-01-01'",
            "timestamp_floor('1h', dateadd('m', 30, ts)) >= '2024-01-01'",
            "dateadd('h', 1, dateadd('h', 1, ts)) > '2024-01-01'",
            "dateadd('h', 1, ts::timestamp_ns) > '2024-01-01'",
            "year(to_timezone(ts, 'Asia/Tokyo')) = 2024",
            "date_trunc('day', ts) > now()",
            "dateadd('h', 1, ts) BETWEEN $1 AND '2024-02-01'",
            "ts + 1_000_000 >= $1",
            "to_utc(ts, 'Europe/Berlin') < (SELECT max(ts) FROM %s)",
            "dateadd('h', v::int, ts) > '2024-01-01'",
            "to_timezone(ts, sym) > '2024-01-01'",
            "ts IN '2024-01' AND date_trunc('day', ts) > now()",
            "timestamp_floor('1d', ts, 0) = '2024-01-01'",
            "timestamp_floor('1d', ts, 0::long) = '2024-01-01'",
            "timestamp_floor('1d', ts, 0::date) = '2024-01-01'",
            "timestamp_floor('1d', ts, null) = '2024-01-01'",
            "timestamp_floor('1d', ts, 1_800_000_000) >= '2024-01-01T00:30'",
            "timestamp_floor('1d', ts, 1_800_000::date) >= '2024-01-01T00:30'",
            "timestamp_floor('1d', ts, 1_800_000_000_000::timestamp_ns) >= '2024-01-01T00:30'",
            "timestamp_floor('1d', ts, 0, '00:00', null) = '2024-01-01'",
            "timestamp_floor('1d', ts, 1_800_000::date, '06:00'::varchar, 'UTC'::varchar) = '2024-01-01T06:30'",
            "timestamp_floor('1d', ts, 0::long, '00:00', 'Europe/Berlin'::symbol) = '2024-01-01'",
            "timestamp_floor_utc('1h', ts, 0::date, null::varchar, '+02:00'::varchar) > '2024-01-01'",
            "timestamp_floor('1d'::varchar, ts) = '2024-01-01'",
            "timestamp_floor('1d'::symbol, ts, 0) = '2024-01-01'",
            "date_trunc('day'::varchar, ts) = '2024-01-01'",
            "date_trunc('day'::symbol, ts) = '2024-01-01'",
            "timestamp_ceil('h'::varchar, ts) <= '2024-01-01T10:00'",
            "dateadd('h'::varchar, 6::short, ts) > '2024-01-01'",
            "dateadd('h', '6', ts) > '2024-01-01'",
            "dateadd('h'::symbol, 6::byte, ts, 'Europe/Berlin'::varchar) > '2024-01-01'",
            "dateadd('h', '6'::varchar, ts, '+02:00'::symbol) > '2024-01-01'",
            "to_timezone(ts, 'Europe/Berlin'::varchar) >= '2024-01-01'",
            "to_timezone(ts, '+02:00'::symbol) >= '2024-01-01'",
            "to_utc(ts, 'America/New_York'::varchar) < '2024-01-02'",
            "ts + 3_600::short > '2024-01-01'",
            "ts + '5' > '2024-01-01'",
            "ts - 100::byte <= '2024-01-01'",
            "ts + null::long > '2024-01-01'",
    };
    private static final String[] OFFSET = {
            "SELECT * FROM (SELECT dateadd('h', -1, ts) ts2, v FROM %s) WHERE ts2 > '2024-01-01'",
            "SELECT * FROM (SELECT dateadd('h', -1, ts) ts2, v FROM %s) WHERE ts2 IN '2024-01'",
            "SELECT * FROM (SELECT dateadd('h', -1, ts) ts2, v FROM %s) WHERE ts2 != '2024-01-01T00:00:00.000000Z'",
            "SELECT * FROM (SELECT dateadd('h', -1, ts) ts2, v FROM %s) WHERE ts2 > NULL",
            "SELECT * FROM (SELECT dateadd('M', 1, ts) ts2, v FROM %s) WHERE ts2 < '2024-03-01'",
            "SELECT * FROM (SELECT dateadd('y', -1, ts) ts2, v FROM %s) WHERE ts2 BETWEEN '2023-01-01' AND '2023-06-01'",
            "SELECT * FROM (SELECT dateadd('d', 1, ts2) ts3 FROM (SELECT dateadd('h', -1, ts) ts2 FROM %s)) WHERE ts3 > '2024-01-01'",
    };
    private static final int[] PARTITIONS = {PartitionBy.DAY, PartitionBy.NONE, PartitionBy.DAY, PartitionBy.NONE};
    private static final String[] STATIC = {
            "ts = '2024-01-01T10:00:00.000000Z'",
            "ts = '2024-01-01T10:00:00.123456789Z'",
            "ts = '2024-01-01'",
            "'2024-01-01T10:00:00.000000Z' = ts",
            "ts = '2024-01-01T10:00:00.000000Z'::timestamp",
            "ts = '2024-01-01T10:00:00.000000001Z'::timestamp_ns",
            "ts = '2024-01-01T10:00:00.000001000Z'::timestamp_ns",
            "ts = 1_704_103_200_000_000",
            "ts = 1_704_103_200",
            "ts IN '2024-01-01'",
            "ts IN '2024-01'",
            "ts IN '2024'",
            "ts IN '2024-01-01T10:00;1h'",
            "ts IN '2024-01-01T10:00;30m;1d;3'",
            "ts IN ('2024-01-01', '2024-01-03')",
            "ts IN ('2024-01-01T10:00:00.000000Z', '2024-01', '2024-03-01T00:00:00.000000Z'::timestamp)",
            "ts NOT IN ('2024-01-01T10:00:00.000000Z')",
            "ts NOT IN '2024-01'",
            "ts NOT IN ('2024-01', '2024-03')",
            "ts BETWEEN '2024-01-01' AND '2024-01-02'",
            "ts BETWEEN '2024-01-02' AND '2024-01-01'",
            "ts NOT BETWEEN '2024-01-01' AND '2024-01-02'",
            "ts BETWEEN '2024-01-01T00:00:00.000000001Z'::timestamp_ns AND '2024-01-02T00:00:00.000000999Z'::timestamp_ns",
            "ts BETWEEN NULL AND '2024-01-02'",
            "ts > '2024-01-01'",
            "ts >= '2024-01-01'",
            "ts < '2024-01-01'",
            "ts <= '2024-01-01'",
            "'2024-01-01' < ts",
            "ts > 1_704_067_200_000_000",
            "ts < 1_704_067_200",
            "ts > '2024-01-01T00:00:00.000000001Z'::timestamp_ns",
            "ts > 9_223_372_036_854_775_807",
            "ts != '2024-01-01T10:00:00.000000Z'",
            "ts <> '2024-01-01T10:00:00.000000Z'",
            "ts > '2024-01-01' AND ts < '2024-01-02'",
            "ts > '2024-01-01' AND ts < '2024-01-02' AND v > 0",
            "sym = 'a' AND ts IN '2024-01-01' AND length(sym) > 0",
            "v > 1 AND abs(v) < 10",
            "ts = '2024-01-01' OR ts = '2024-01-03'",
            "ts IN '2024-01-01' OR ts IN '2024-01-05'",
            "ts IN ('2024-01-01', '2024-01-02') OR ts = '2024-02-01T00:00:00.000000Z'",
            "ts IN '2024-01' AND (ts = '2024-01-01' OR ts = '2024-02-03')",
            "ts = NULL",
            "ts > NULL",
            "ts = ts",
            "ts != ts",
            "ts > '2024-01-02' AND ts < '2024-01-01'",
            "ts IN '2024-01-01' AND ts NOT IN '2024-01-01T10'",
            "ts IN '2024-01-02' AND ts BETWEEN '2024-01-01' AND '2024-01-03'",
    };

    private static final Object[][] TIMESTAMP_ORDER = {
            // call, micro, nano; null where binding replaces the call by its argument
            {"timestamp_floor('1h', ts)", true, true},
            {"timestamp_floor('15m', ts)", true, true},
            {"timestamp_floor('1w', ts)", true, false},
            {"timestamp_floor('2w', ts)", true, false},
            {"timestamp_floor('3M', ts)", true, true},
            {"timestamp_floor('1y', ts)", true, true},
            {"timestamp_floor('10n', ts)", false, true},
            {"timestamp_floor('106751d', ts)", true, true},
            {"timestamp_floor('106752d', ts)", true, false},
            {"timestamp_floor('106751991d', ts)", true, false},
            {"timestamp_floor('106751992d', ts)", false, false},
            {"timestamp_floor('1h', ts, '2024-01-01T00:30:00.000000Z')", true, true},
            {"timestamp_floor('1h', ts, '1969-12-31T23:00:00.000000Z')", false, false},
            {"timestamp_floor('1w', ts, '2024-01-01T00:00:00.000000Z')", true, true},
            {"timestamp_floor('1w', ts, null)", true, false},
            {"timestamp_floor_utc('1h', ts, null, '00:00', null)", true, true},
            {"timestamp_floor_utc('1h', ts, null, '00:15', null)", true, true},
            {"timestamp_floor_utc('1h', ts, null, '-00:15', null)", false, false},
            {"timestamp_floor_utc('1h', ts, null, '00:00', '+01:00')", true, false},
            {"timestamp_floor_utc('1h', ts, null, '00:00', '-01:00')", true, true},
            {"timestamp_floor_utc('1M', ts, null, '00:00', '-01:00')", true, true},
            {"timestamp_floor_utc('2M', ts, null, '00:00', '-01:00')", false, false},
            {"timestamp_floor_utc('1h', ts, null, '00:15', '+01:00')", false, false},
            {"timestamp_floor_utc('1h', ts, null, '00:00', 'UTC')", true, true},
            {"timestamp_floor_utc('1h', ts, null, '00:00', 'America/New_York')", true, true},
            {"timestamp_floor_utc('1d', ts, null, '00:00', 'America/New_York')", false, false},
            {"timestamp_floor('1h', ts, null, '00:00', 'America/New_York')", false, false},
            {"timestamp_floor_utc('1h', ts, null, $1, null)", false, false},
            {"to_utc(ts, '+01:00')", true, true},
            {"to_timezone(ts, '+01:00')", true, false},
            {"to_utc(ts, 'Europe/London')", false, false},
            {"to_utc(ts, $1)", false, false},
            {"year(ts)", true, true},
            {"extract(year from ts)", true, true},
            {"extract(month from ts)", false, false},
            {"date_trunc('hour', ts)", true, true},
            {"date_trunc('week', ts)", true, false},
            {"date_trunc('millennium', ts)", true, false},
            {"date_trunc('microsecond', ts)", true, true},
            {"ts::long", true, true},
            {"ts::timestamp_ns", false, null},
            {"ts::timestamp", null, true},
            {"ts + 1", true, false},
            {"ts + 8969969736054775808", true, false},
            {"ts + 8969969736054775809", false, false},
            {"ts + null::long", false, false},
            {"ts - 1", true, true},
            {"ts - (-8969969736054775808)", true, false},
            {"ts - (-8969969736054775809)", false, false},
            {"ts + $1", false, false},
            {"dateadd('h', 1, ts)", true, false},
            {"dateadd('h', -1, ts)", true, true},
            {"dateadd('M', 1, ts)", false, false},
            {"dateadd('M', 0, ts)", true, true},
            {"dateadd('h', $1, ts)", false, false},
            {"dateadd('h', -1, ts, '+01:00')", true, true},
            {"dateadd('h', -1, ts, 'Europe/London')", false, false},
            {"timestamp_ceil('h', ts)", true, false},
            {"timestamp_ceil('n', ts)", true, true},
    };
    private static final String[] TABLES = {"ia_micro_day", "ia_micro_none", "ia_nano_day", "ia_nano_none"};

    @Test
    public void testDeclaredTimestampOrder() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final MonotonicTimestampFunctionFactory.ConstantArguments arguments = new MonotonicTimestampFunctionFactory.ConstantArguments();
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                for (Object[] row : TIMESTAMP_ORDER) {
                    for (int i = 0; i < 2; i++) {
                        final String table = i == 0 ? "ia_micro_none" : "ia_nano_none";
                        final String sql = "SELECT " + row[0] + " e FROM " + table;
                        bindVariableService.clear();
                        final String call = (String) row[0];
                        if (call.contains("ts + $1")) {
                            bindVariableService.setLong(0, 1);
                        } else if (call.contains("dateadd")) {
                            bindVariableService.setInt(0, 1);
                        } else {
                            bindVariableService.setStr(0, "+01:00");
                        }
                        try (RecordCursorFactory ignore = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                            final FunctionExpression projected = projectedCall(compiler.getPlanForTesting(), "e");
                            if (row[i + 1] == null) {
                                Assert.assertNull(sql, projected);
                                continue;
                            }
                            Assert.assertNotNull(sql, projected);
                            final MonotonicTimestampFunctionFactory factory = MonotonicTimestampFunctionFactory.of(projected);
                            Assert.assertNotNull(sql, factory);
                            Assert.assertEquals(sql, row[i + 1], factory.preservesTimestampOrder(projected, arguments));
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testDynamicPredicates() throws Exception {
        assertPredicates(DYNAMIC, false);
    }

    @Test
    public void testMixedPredicates() throws Exception {
        assertPredicates(MIXED, null);
    }

    @Test
    public void testMonotonicPredicates() throws Exception {
        assertPredicates(MONOTONIC, null);
    }

    @Test
    public void testOffsetPredicates() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            bindVariableService.clear();
            bindVariableService.setTimestamp(0, 1_704_067_200_000_000L);
            try (
                    SqlCompilerImpl compiler = new SqlCompilerImpl(engine);
                    FunctionBindingHarness harness = new FunctionBindingHarness(engine, new FunctionParser(configuration, engine.getFunctionFactoryCache()))
            ) {
                final IntervalAnalysis analysis = new IntervalAnalysis(configuration, new StringSink(), new LongList());
                for (int i = 0; i < TABLES.length; i++) {
                    for (String query : OFFSET) {
                        assertAnalysis(compiler, harness, analysis, String.format(query, TABLES[i]), PARTITIONS[i], true);
                    }
                }
            }
        });
    }

    @Test
    public void testStaticPredicates() throws Exception {
        assertPredicates(STATIC, true);
    }

    private static FunctionExpression projectedCall(LogicalPlan plan, String name) {
        if (plan instanceof ProjectPlan project) {
            final int index = project.getOutput().getColumnIndexQuiet(name);
            if (index >= 0 && project.getExpressions().getQuick(index) instanceof FunctionExpression call) {
                return call;
            }
        }
        for (int i = 0, n = plan.inputCount(); i < n; i++) {
            final FunctionExpression call = projectedCall(plan.inputAt(i), name);
            if (call != null) {
                return call;
            }
        }
        return null;
    }

    private static FilterPlan scanFilter(LogicalPlan plan) {
        if (plan instanceof FilterPlan filter && filter.inputAt(0) instanceof ScanPlan) {
            return filter;
        }
        for (int i = 0, n = plan.inputCount(); i < n; i++) {
            final FilterPlan filter = scanFilter(plan.inputAt(i));
            if (filter != null) {
                return filter;
            }
        }
        return null;
    }

    private void assertAnalysis(
            SqlCompilerImpl compiler,
            FunctionBindingHarness harness,
            IntervalAnalysis analysis,
            String sql,
            int partitionBy,
            Boolean isStatic
    ) throws Exception {
        try (RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
            final FilterPlan filter = scanFilter(compiler.getPlanForTesting());
            Assert.assertNotNull(sql, filter);
            final ScanPlan scan = (ScanPlan) filter.inputAt(0);
            final int timestampType = scan.getNativeTimestampType();
            analysis.analyse(filter.getPredicate(), scan.getNativeTimestampColumnId(), timestampType, 0, harness.getRewriter());
            if (isStatic != null) {
                Assert.assertEquals(sql, isStatic, analysis.isStatic());
            }
            if (!analysis.isStatic()) {
                Assert.assertEquals(sql, PartitionBy.NONE == partitionBy, analysis.allIntervalsHitOnePartition(partitionBy));
                return;
            }
            final LongList intervals = analysis.getStaticIntervals();
            final TextPlanSink plan = new TextPlanSink();
            plan.of(factory, sqlExecutionContext);
            final String explain = plan.getSink().toString();
            if (analysis.hasIntervalFilters() && intervals.size() > 0) {
                final TextPlanSink expected = new TextPlanSink();
                new RuntimeIntervalModel(ColumnType.getTimestampDriver(timestampType), partitionBy, new LongList(intervals)).toPlan(expected);
                Assert.assertTrue(sql + '\n' + explain + "\nexpected intervals: " + expected.getSink(),
                        explain.contains("intervals: " + expected.getSink()));
            } else if (!analysis.hasIntervalFilters()) {
                Assert.assertFalse(sql + '\n' + explain, explain.contains("intervals:"));
            }
            final TimestampDriver.TimestampFloorMethod floor = ColumnType.getTimestampDriver(timestampType).getPartitionFloorMethod(PartitionBy.DAY);
            final boolean isOnePartition = PartitionBy.NONE == partitionBy || intervals.size() == 0
                    || floor.floor(intervals.getQuick(0)) == floor.floor(intervals.getLast());
            Assert.assertEquals(sql, isOnePartition, analysis.allIntervalsHitOnePartition(partitionBy));
        }
    }

    private void assertPredicates(String[] predicates, Boolean isStatic) throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            bindVariableService.clear();
            bindVariableService.setTimestamp(0, 1_704_067_200_000_000L);
            try (
                    SqlCompilerImpl compiler = new SqlCompilerImpl(engine);
                    FunctionBindingHarness harness = new FunctionBindingHarness(engine, new FunctionParser(configuration, engine.getFunctionFactoryCache()))
            ) {
                final IntervalAnalysis analysis = new IntervalAnalysis(configuration, new StringSink(), new LongList());
                for (int i = 0; i < TABLES.length; i++) {
                    for (String predicate : predicates) {
                        final String sql = "SELECT * FROM " + TABLES[i] + " WHERE " + String.format(predicate, TABLES[i]);
                        assertAnalysis(compiler, harness, analysis, sql, PARTITIONS[i], isStatic);
                    }
                }
            }
        });
    }

    private void createTables() throws Exception {
        for (int i = 0; i < TABLES.length; i++) {
            execute("CREATE TABLE " + TABLES[i] + " (ts " + (i < 2 ? "TIMESTAMP" : "TIMESTAMP_NS") + ", sym SYMBOL, v LONG) TIMESTAMP(ts) PARTITION BY "
                    + PartitionBy.toString(PARTITIONS[i]));
            execute("INSERT INTO " + TABLES[i] + " VALUES ('2024-01-01T10:00:00.000000Z', 'a', 1), ('2024-01-03T00:00:00.000000Z', 'b', 2)");
        }
    }
}
