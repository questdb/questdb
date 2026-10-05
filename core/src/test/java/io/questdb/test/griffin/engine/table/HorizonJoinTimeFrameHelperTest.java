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

package io.questdb.test.griffin.engine.table;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.RecordSinkSPI;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.sql.AtomicBooleanCircuitBreaker;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SqlExecutionCircuitBreakerConfiguration;
import io.questdb.cairo.sql.SqlExecutionCircuitBreakerWrapper;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.TimeFrame;
import io.questdb.cairo.sql.TimeFrameCursor;
import io.questdb.griffin.DefaultSqlExecutionCircuitBreakerConfiguration;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.functions.constants.BooleanConstant;
import io.questdb.griffin.engine.table.HorizonJoinTimeFrameHelper;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.Rows;
import io.questdb.std.datetime.NanosecondClock;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.test.AbstractTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

public class HorizonJoinTimeFrameHelperTest extends AbstractTest {
    private static final RecordSink KEY_SINK = new RecordSink() {
        @Override
        public void copy(Record record, RecordSinkSPI sink) {
            sink.putInt(record.getInt(0));
        }

        @Override
        public void setFunctions(ObjList<Function> functions) {
        }
    };
    private static final Record MISSING_KEY = new Record() {
        @Override
        public int getInt(int columnIndex) {
            return 2;
        }
    };

    @Test
    public void testAllFrameNavigationLoops() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            State state = new State();
            try (PollingEngine engine = new PollingEngine(root, state)) {
                for (int branch = 0; branch < 7; branch++) {
                    for (int throttle : new int[]{0, 2048}) {
                        assertFrameBranch(engine, state, branch, throttle, false);
                        assertFrameBranch(engine, state, branch, throttle, true);
                    }
                }
            }
        });
    }

    @Test
    public void testAllRowScansBoundCancellation() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            State state = new State();
            try (PollingEngine engine = new PollingEngine(root, state)) {
                for (int kind = 0; kind < 4; kind++) {
                    for (int throttle : new int[]{0, 2048}) {
                        for (int cancelAt : new int[]{1, 64, 65}) {
                            Trace trace = new Trace();
                            trace.cancelAt = cancelAt;
                            trace.expectedMethod = switch (kind) {
                                case 0 -> "backwardScanForFilterMatch";
                                case 1 -> "backwardScanForKeyMatch";
                                case 2 -> "forwardScanToPosition";
                                default -> "linearScanAsOf";
                            };
                            trace.isTimestampTrace = kind == 3;
                            Cursor cursor = new Cursor(trace, kind == 3 ? 512 : 256);
                            HorizonJoinTimeFrameHelper helper = helper(cursor, kind == 3 ? null : filter(trace, false), 512);
                            try (TracingBreaker breaker = new TracingBreaker(engine, state, trace, throttle);
                                 Map map = newMap(engine)) {
                                breaker.setCancelledFlag(trace.signal);
                                breaker.resetTimer();
                                try {
                                    switch (kind) {
                                        case 0 -> helper.findNotKeyedAsOfMatch(255, breaker);
                                        case 1 ->
                                                helper.findKeyedAsOfMatch(255, MISSING_KEY, KEY_SINK, KEY_SINK, map, null, breaker);
                                        case 2 -> helper.forwardScanToPosition(255, KEY_SINK, map, breaker);
                                        default -> helper.findAsOfRow(300, breaker);
                                    }
                                    Assert.fail(trace.expectedMethod);
                                } catch (CairoException e) {
                                    Assert.assertEquals(SqlExecutionCircuitBreaker.STATE_CANCELLED, e.getInterruptionReason());
                                }
                                Assert.assertTrue(trace.signal.get());
                                Assert.assertEquals(cancelAt < 64 ? 63 : 127, trace.visits);
                                Assert.assertTrue(trace.visits - cancelAt <= 64);
                                Assert.assertTrue(trace.visits < 256);
                                for (int i = 0; i < trace.pollAtVisits.size(); i++) {
                                    Assert.assertEquals(i * 64 + 63, trace.pollAtVisits.getQuick(i));
                                }
                            }
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testBoundedBinaryTailAndLongLookahead() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            State state = new State();
            try (PollingEngine engine = new PollingEngine(root, state)) {
                for (int lookahead : new int[]{64, 512}) {
                    Trace trace = new Trace();
                    trace.isTimestampTrace = true;
                    Cursor cursor = new Cursor(trace, 512);
                    HorizonJoinTimeFrameHelper helper = helper(cursor, null, lookahead);
                    try (TracingBreaker breaker = new TracingBreaker(engine, state, trace, 2048)) {
                        breaker.setCancelledFlag(trace.signal);
                        breaker.resetTimer();
                        Assert.assertEquals(300, helper.findAsOfRow(300, breaker));
                        int polls = trace.pollAtVisits.size();
                        int visits = trace.visits;
                        Assert.assertEquals(300, helper.findAsOfRow(300, breaker));
                        Assert.assertEquals(visits, trace.visits);
                        Assert.assertEquals(polls, trace.pollAtVisits.size());
                        Assert.assertEquals(lookahead == 64 ? 1 : 4, polls);
                        if (lookahead == 64) {
                            Assert.assertTrue(trace.visits <= 64 + 10 + 65 + 1);
                            helper.toTop();
                            trace.visits = 0;
                            trace.cancelAt = 65;
                            trace.expectedMethod = "binarySearchAsOf";
                            Assert.assertEquals(300, helper.findAsOfRow(300, breaker));
                            Assert.assertTrue(trace.signal.get());
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testConstantFalseFilterScansNothing() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Trace trace = new Trace();
            trace.isTimestampTrace = true;
            Cursor cursor = new Cursor(trace, 256);
            HorizonJoinTimeFrameHelper helper = helper(cursor, BooleanConstant.FALSE, 64);
            Assert.assertEquals(Long.MIN_VALUE, helper.findAsOfRow(100, SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER));
            Assert.assertEquals(0, trace.opens);
            Assert.assertEquals(0, trace.visits);
        });
    }

    @Test
    public void testFastReturnsDoNotPoll() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            State state = new State();
            Trace trace = new Trace();
            Cursor cursor = new Cursor(trace, 256);
            try (PollingEngine engine = new PollingEngine(root, state);
                 TracingBreaker breaker = new TracingBreaker(engine, state, trace, 2048);
                 Map map = newMap(engine)) {
                breaker.resetTimer();
                HorizonJoinTimeFrameHelper noFilter = helper(cursor, null, 64);
                Assert.assertEquals(255, noFilter.findNotKeyedAsOfMatch(255, breaker));
                Assert.assertEquals(Long.MIN_VALUE, noFilter.findNotKeyedAsOfMatch(Long.MIN_VALUE, breaker));
                Assert.assertEquals(Long.MIN_VALUE, noFilter.findKeyedAsOfMatch(Long.MIN_VALUE, MISSING_KEY, KEY_SINK, KEY_SINK, map, null, breaker));
                noFilter.forwardScanToPosition(Long.MIN_VALUE, KEY_SINK, map, breaker);
                Assert.assertEquals(0, trace.pollAtVisits.size());
                Assert.assertEquals(0, trace.visits);
                Assert.assertEquals(0, trace.opens);
                HorizonJoinTimeFrameHelper filtered = helper(cursor, filter(trace, false), 64);
                Assert.assertEquals(Long.MIN_VALUE, filtered.findNotKeyedAsOfMatch(255, breaker));
                int polls = trace.pollAtVisits.size();
                int reads = state.millisReads;
                Assert.assertEquals(Long.MIN_VALUE, filtered.findNotKeyedAsOfMatch(255, breaker));
                Assert.assertEquals(Long.MIN_VALUE, filtered.findNotKeyedAsOfMatch(128, breaker));
                Assert.assertEquals(256, trace.visits);
                Assert.assertEquals(polls, trace.pollAtVisits.size());
                Assert.assertEquals(reads, state.millisReads);
            }
        });
    }

    @Test
    public void testFilteredDeepHitSwitchesToForwardScan() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Trace trace = new Trace();
            Cursor cursor = new Cursor(trace, 16_384);
            SqlExecutionCircuitBreaker breaker = SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER;
            Function filter = new BooleanFunction() {
                @Override
                public boolean getBool(Record record) {
                    trace.visit();
                    return record.getRowId() == 10;
                }
            };
            try (PollingEngine engine = new PollingEngine(root, new State());
                 Map map = newMap(engine);
                 HorizonJoinTimeFrameHelper helper = new HorizonJoinTimeFrameHelper(
                         engine.getConfiguration(), 64, 1, 131_072, 1_024, 8, filter, new ArrayColumnTypes().add(ColumnType.INT)
                 )) {
                // A hit below the cost floor rescans at the next position.
                helper.of(cursor, null);
                Assert.assertEquals(10, helper.findKeyedAsOfMatch(100, cursor.getRecord(), KEY_SINK, KEY_SINK, map, null, breaker));
                Assert.assertEquals(91, trace.visits);
                Assert.assertEquals(10, helper.findKeyedAsOfMatch(110, cursor.getRecord(), KEY_SINK, KEY_SINK, map, null, breaker));
                Assert.assertEquals(192, trace.visits);

                // A hit above it scans only the gap from then on.
                helper.of(cursor, null);
                map.clear();
                Assert.assertEquals(10, helper.findKeyedAsOfMatch(9_000, cursor.getRecord(), KEY_SINK, KEY_SINK, map, null, breaker));
                Assert.assertEquals(9_183, trace.visits);
                Assert.assertEquals(10, helper.findKeyedAsOfMatch(9_010, cursor.getRecord(), KEY_SINK, KEY_SINK, map, null, breaker));
                Assert.assertEquals(9_193, trace.visits);
                Assert.assertEquals(10, helper.findKeyedAsOfMatch(9_020, cursor.getRecord(), KEY_SINK, KEY_SINK, map, null, breaker));
                Assert.assertEquals(9_203, trace.visits);
            }
        });
    }

    @Test
    public void testFilteredKeyMissBoundsNextPosition() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            State state = new State();
            Trace trace = new Trace();
            Cursor cursor = new Cursor(trace, 4096);
            SqlExecutionCircuitBreaker breaker = SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER;
            try (PollingEngine engine = new PollingEngine(root, state);
                 Map map = newMap(engine);
                 HorizonJoinTimeFrameHelper helper = new HorizonJoinTimeFrameHelper(
                         engine.getConfiguration(), 64, 1, 131_072, 1_024, 8, filter(trace, true), new ArrayColumnTypes().add(ColumnType.INT)
                 )) {
                helper.of(cursor, null);
                Assert.assertEquals(Long.MIN_VALUE, helper.findKeyedAsOfMatch(1000, MISSING_KEY, KEY_SINK, KEY_SINK, map, null, breaker));
                Assert.assertEquals(1001, trace.visits);
                Assert.assertEquals(Long.MIN_VALUE, helper.findKeyedAsOfMatch(1010, MISSING_KEY, KEY_SINK, KEY_SINK, map, null, breaker));
                Assert.assertEquals(1010, helper.findKeyedAsOfMatch(1010, cursor.getRecord(), KEY_SINK, KEY_SINK, map, null, breaker));
                Assert.assertEquals(Long.MIN_VALUE, helper.findKeyedAsOfMatch(1020, MISSING_KEY, KEY_SINK, KEY_SINK, map, null, breaker));
                Assert.assertEquals(1021, trace.visits);
            }
        });
    }

    @Test
    public void testFilteredKeyMissSurvivesFrameReset() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Trace trace = new Trace();
            Cursor cursor = new Cursor(trace, 4096);
            SqlExecutionCircuitBreaker breaker = SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER;
            try (PollingEngine engine = new PollingEngine(root, new State());
                 Map map = newMap(engine);
                 HorizonJoinTimeFrameHelper helper = new HorizonJoinTimeFrameHelper(
                         engine.getConfiguration(), 64, 1, 131_072, 1_024, 8, filter(trace, true), new ArrayColumnTypes().add(ColumnType.INT)
                 )) {
                helper.of(cursor, null);
                Assert.assertEquals(Long.MIN_VALUE, helper.findKeyedAsOfMatch(1000, MISSING_KEY, KEY_SINK, KEY_SINK, map, null, breaker));
                Assert.assertEquals(1001, trace.visits);

                // A later master frame scans only the rows above the recorded miss.
                helper.toTop();
                map.clear();
                Assert.assertEquals(Long.MIN_VALUE, helper.findKeyedAsOfMatch(1010, MISSING_KEY, KEY_SINK, KEY_SINK, map, null, breaker));
                Assert.assertEquals(1011, trace.visits);
                Assert.assertEquals(1010, helper.findKeyedAsOfMatch(1010, cursor.getRecord(), KEY_SINK, KEY_SINK, map, null, breaker));

                // An out-of-order master frame below the recorded miss scans nothing for the key.
                helper.toTop();
                map.clear();
                Assert.assertEquals(Long.MIN_VALUE, helper.findKeyedAsOfMatch(500, MISSING_KEY, KEY_SINK, KEY_SINK, map, null, breaker));
                Assert.assertEquals(1011, trace.visits);
                Assert.assertEquals(500, helper.findKeyedAsOfMatch(500, cursor.getRecord(), KEY_SINK, KEY_SINK, map, null, breaker));
                Assert.assertEquals(1012, trace.visits);

                // A new cursor drops the recorded misses.
                helper.of(cursor, null);
                map.clear();
                Assert.assertEquals(Long.MIN_VALUE, helper.findKeyedAsOfMatch(100, MISSING_KEY, KEY_SINK, KEY_SINK, map, null, breaker));
                Assert.assertEquals(1113, trace.visits);
            }
        });
    }

    @Test
    public void testFilteredKeyMissDoesNotSwitchToForwardScan() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Trace trace = new Trace();
            Cursor cursor = new Cursor(trace, 4096);
            SqlExecutionCircuitBreaker breaker = SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER;
            final Record otherMissingKey = new Record() {
                @Override
                public int getInt(int columnIndex) {
                    return 3;
                }
            };
            try (PollingEngine engine = new PollingEngine(root, new State());
                 Map map = newMap(engine);
                 HorizonJoinTimeFrameHelper helper = new HorizonJoinTimeFrameHelper(
                         engine.getConfiguration(), 64, 1, 131_072, 1_024, 8, filter(trace, true), new ArrayColumnTypes().add(ColumnType.INT)
                 )) {
                helper.of(cursor, null);
                Assert.assertEquals(Long.MIN_VALUE, helper.findKeyedAsOfMatch(1000, MISSING_KEY, KEY_SINK, KEY_SINK, map, null, breaker));
                Assert.assertEquals(Long.MIN_VALUE, helper.findKeyedAsOfMatch(1000, otherMissingKey, KEY_SINK, KEY_SINK, map, null, breaker));
                Assert.assertEquals(1001, trace.visits);

                helper.toTop();
                map.clear();
                Assert.assertEquals(Long.MIN_VALUE, helper.findKeyedAsOfMatch(2000, MISSING_KEY, KEY_SINK, KEY_SINK, map, null, breaker));
                Assert.assertEquals(2001, trace.visits);
                // A bounded miss stays below the cost floor, so the second key rescans from its own miss.
                Assert.assertEquals(Long.MIN_VALUE, helper.findKeyedAsOfMatch(2001, otherMissingKey, KEY_SINK, KEY_SINK, map, null, breaker));
                Assert.assertEquals(3002, trace.visits);
                Assert.assertEquals(Long.MIN_VALUE, helper.findKeyedAsOfMatch(2002, otherMissingKey, KEY_SINK, KEY_SINK, map, null, breaker));
                Assert.assertEquals(3003, trace.visits);

                // A shallow miss then a one-row gap must not lock a far lookup into a forward scan.
                helper.of(cursor, null);
                map.clear();
                Assert.assertEquals(Long.MIN_VALUE, helper.findKeyedAsOfMatch(63, MISSING_KEY, KEY_SINK, KEY_SINK, map, null, breaker));
                Assert.assertEquals(3067, trace.visits);
                Assert.assertEquals(64, helper.findKeyedAsOfMatch(64, cursor.getRecord(), KEY_SINK, KEY_SINK, map, null, breaker));
                Assert.assertEquals(3068, trace.visits);
                Assert.assertEquals(4095, helper.findKeyedAsOfMatch(4095, cursor.getRecord(), KEY_SINK, KEY_SINK, map, null, breaker));
                Assert.assertEquals(3069, trace.visits);
            }
        });
    }

    @Test
    public void testFilteredKeyMissThenMatch() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Trace trace = new Trace();
            Cursor cursor = new Cursor(trace, 4096);
            SqlExecutionCircuitBreaker breaker = SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER;
            Function filter = new BooleanFunction() {
                @Override
                public boolean getBool(Record record) {
                    trace.visit();
                    return record.getRowId() == 1005;
                }
            };
            try (PollingEngine engine = new PollingEngine(root, new State());
                 Map map = newMap(engine);
                 HorizonJoinTimeFrameHelper helper = new HorizonJoinTimeFrameHelper(
                         engine.getConfiguration(), 64, 1, 131_072, 1_024, 8, filter, new ArrayColumnTypes().add(ColumnType.INT)
                 )) {
                helper.of(cursor, null);
                Assert.assertEquals(Long.MIN_VALUE, helper.findKeyedAsOfMatch(1000, cursor.getRecord(), KEY_SINK, KEY_SINK, map, null, breaker));
                Assert.assertEquals(1001, trace.visits);

                // A recorded miss must not hide a qualifying row above it.
                helper.toTop();
                map.clear();
                Assert.assertEquals(1005, helper.findKeyedAsOfMatch(1010, cursor.getRecord(), KEY_SINK, KEY_SINK, map, null, breaker));
                Assert.assertEquals(1007, trace.visits);

                // Positions between the recorded miss and the qualifying row still miss,
                // and each one raises the recorded miss.
                helper.toTop();
                map.clear();
                Assert.assertEquals(Long.MIN_VALUE, helper.findKeyedAsOfMatch(1003, cursor.getRecord(), KEY_SINK, KEY_SINK, map, null, breaker));
                Assert.assertEquals(1010, trace.visits);
                helper.toTop();
                map.clear();
                Assert.assertEquals(Long.MIN_VALUE, helper.findKeyedAsOfMatch(1004, cursor.getRecord(), KEY_SINK, KEY_SINK, map, null, breaker));
                Assert.assertEquals(1011, trace.visits);
                helper.toTop();
                map.clear();
                Assert.assertEquals(1005, helper.findKeyedAsOfMatch(1005, cursor.getRecord(), KEY_SINK, KEY_SINK, map, null, breaker));
                Assert.assertEquals(1012, trace.visits);

                // Within one master frame, the next position scans back only to the qualifying row.
                helper.of(cursor, null);
                map.clear();
                Assert.assertEquals(Long.MIN_VALUE, helper.findKeyedAsOfMatch(1000, cursor.getRecord(), KEY_SINK, KEY_SINK, map, null, breaker));
                Assert.assertEquals(1005, helper.findKeyedAsOfMatch(1010, cursor.getRecord(), KEY_SINK, KEY_SINK, map, null, breaker));
                Assert.assertEquals(2019, trace.visits);
            }
        });
    }

    @Test
    public void testNotKeyedMatchOutOfOrderLookups() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Trace trace = new Trace();
            Cursor cursor = new Cursor(trace, 256);
            SqlExecutionCircuitBreaker breaker = SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER;
            HorizonJoinTimeFrameHelper helper = helper(cursor, new BooleanFunction() {
                @Override
                public boolean getBool(Record record) {
                    trace.visit();
                    return record.getRowId() % 100 == 0;
                }
            }, 64);
            Assert.assertEquals(200, helper.findNotKeyedAsOfMatch(250, breaker));
            Assert.assertEquals(100, helper.findNotKeyedAsOfMatch(150, breaker));
            Assert.assertEquals(0, helper.findNotKeyedAsOfMatch(50, breaker));
            Assert.assertEquals(0, helper.findNotKeyedAsOfMatch(99, breaker));
            Assert.assertEquals(200, helper.findNotKeyedAsOfMatch(255, breaker));
            Assert.assertEquals(258, trace.visits);
            Assert.assertEquals(200, helper.findNotKeyedAsOfMatch(230, breaker));
            Assert.assertEquals(258, trace.visits);
        });
    }

    @Test
    public void testRowCadenceCrossesFrames() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            State state = new State();
            try (PollingEngine engine = new PollingEngine(root, state)) {
                for (int kind = 0; kind < 3; kind++) {
                    Trace trace = new Trace();
                    Cursor cursor = new Cursor(trace, 40, 40, 40, 40);
                    HorizonJoinTimeFrameHelper helper = helper(cursor, filter(trace, false), 64);
                    try (TracingBreaker breaker = new TracingBreaker(engine, state, trace, 2048);
                         Map map = newMap(engine)) {
                        breaker.resetTimer();
                        long last = Rows.toRowID(3, 39);
                        switch (kind) {
                            case 0 -> Assert.assertEquals(Long.MIN_VALUE, helper.findNotKeyedAsOfMatch(last, breaker));
                            case 1 ->
                                    Assert.assertEquals(Long.MIN_VALUE, helper.findKeyedAsOfMatch(last, MISSING_KEY, KEY_SINK, KEY_SINK, map, null, breaker));
                            default -> helper.forwardScanToPosition(last, KEY_SINK, map, breaker);
                        }
                        Assert.assertEquals(160, trace.visits);
                        Assert.assertEquals("[40,63,80,120,127]", trace.pollAtVisits.toString());
                    }
                }
            }
        });
    }

    @Test
    public void testScanCadencePreservesOuterCooperativePolling() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            State state = new State();
            try (PollingEngine engine = new PollingEngine(root, state)) {
                for (int kind = 0; kind < 4; kind++) {
                    for (int throttle : new int[]{0, 5, 1537, 2048}) {
                        IntList baseline = cooperativeTrace(engine, state, kind, throttle, false);
                        IntList withHelper = cooperativeTrace(engine, state, kind, throttle, true);
                        if (kind < 2) {
                            Assert.assertEquals(baseline, withHelper);
                            if (throttle == 2048) {
                                Assert.assertEquals("[1020,2044]", withHelper.toString());
                            }
                        } else if (throttle == 2048) {
                            Assert.assertEquals(2049, withHelper.size());
                            Assert.assertEquals(1, withHelper.getQuick(0));
                            Assert.assertEquals(2049, withHelper.getLast());
                        }
                    }
                }
            }
        });
    }

    private static void assertFrameBranch(PollingEngine engine, State state, int branch, int throttle, boolean isCancel) {
        Trace trace = new Trace();
        Cursor cursor = new Cursor(trace, branch == 3 ? 0 : 1, 0, 0, 0, 0, 0, 0, 0, 0, 1);
        cursor.seekIndex = -1;
        HorizonJoinTimeFrameHelper helper = helper(cursor, filter(trace, branch >= 3 && branch <= 5), 64);
        try (TracingBreaker breaker = new TracingBreaker(engine, state, trace, throttle);
             Map map = newMap(engine)) {
            breaker.setCancelledFlag(trace.signal);
            breaker.resetTimer();
            if (branch == 1) {
                Assert.assertEquals(Rows.toRowID(9, 0), helper.findAsOfRow(90_000, breaker));
                trace.opens = 0;
            } else if (branch == 4) {
                helper.initForwardWatermark(0);
            }
            trace.cancelOnOpen = isCancel ? 2 : 0;
            trace.pollAtVisits.clear();
            long result = Long.MIN_VALUE;
            try {
                switch (branch) {
                    case 0 ->
                            result = helper.findKeyedAsOfMatch(Rows.toRowID(9, 0), MISSING_KEY, KEY_SINK, KEY_SINK, map, null, breaker);
                    case 1 -> result = helper.findAsOfRow(0, breaker);
                    case 2 -> result = helper.findAsOfRow(90_000, breaker);
                    case 3, 4, 5 -> helper.forwardScanToPosition(Rows.toRowID(9, 0), KEY_SINK, map, breaker);
                    default -> result = helper.findNotKeyedAsOfMatch(Rows.toRowID(9, 0), breaker);
                }
                Assert.assertFalse("frame branch " + branch, isCancel);
            } catch (CairoException e) {
                Assert.assertTrue(isCancel);
                Assert.assertEquals(SqlExecutionCircuitBreaker.STATE_CANCELLED, e.getInterruptionReason());
            }
            if (isCancel) {
                Assert.assertTrue(trace.signal.get());
                Assert.assertEquals("frame branch " + branch, 2, trace.opens);
            } else {
                Assert.assertTrue(trace.opens >= 10);
                Assert.assertTrue(trace.pollAtVisits.size() >= 9);
                if (branch >= 3 && branch <= 5) {
                    MapKey key = map.withKey();
                    key.putInt(1);
                    Assert.assertEquals(Rows.toRowID(9, 0), key.findValue().getLong(0));
                } else {
                    Assert.assertEquals(branch == 1 ? 0 : branch == 2 ? Rows.toRowID(9, 0) : Long.MIN_VALUE, result);
                }
            }
        }
    }

    private static IntList cooperativeTrace(PollingEngine engine, State state, int kind, int throttle, boolean hasHelper) {
        state.hooks.clear();
        state.nanos = 0;
        Trace trace = new Trace();
        Cursor cursor = new Cursor(trace, 3_000_000);
        HorizonJoinTimeFrameHelper helper = helper(cursor, filter(trace, false), 64);
        SqlExecutionCircuitBreakerConfiguration config = configuration(state, throttle);
        try (TracingBreaker network = new TracingBreaker(engine, state, trace, throttle);
             SqlExecutionCircuitBreakerWrapper wrapper = new SqlExecutionCircuitBreakerWrapper(engine, config)) {
            SqlExecutionCircuitBreaker breaker = kind < 2 ? network : new AtomicBooleanCircuitBreaker(engine, throttle);
            if ((kind & 1) == 1) {
                wrapper.init(breaker);
                breaker = wrapper;
            }
            breaker.resetTimer();
            for (int i = 0; i < 4; i++) {
                breaker.statefulThrowExceptionIfTripped();
            }
            for (int outer = 0; outer < 2050; outer++) {
                state.outer = outer;
                state.nanos += SqlExecutionCircuitBreaker.COOPERATIVE_POLL_INTERVAL_NANOS;
                breaker.statefulThrowExceptionIfTrippedOrYield();
                if (hasHelper) {
                    int readsBefore = state.millisReads;
                    int visitsBefore = trace.visits;
                    trace.pollAtVisits.clear();
                    state.isInsideHelper = true;
                    try {
                        Assert.assertEquals(Long.MIN_VALUE, helper.findNotKeyedAsOfMatch((outer + 1L) * 1023 - 1, breaker));
                    } finally {
                        state.isInsideHelper = false;
                    }
                    Assert.assertEquals(1023, trace.visits - visitsBefore);
                    if (kind < 2) {
                        Assert.assertEquals(15, state.millisReads - readsBefore);
                    }
                    if (kind == 0) {
                        Assert.assertEquals(15, trace.pollAtVisits.size());
                        for (int i = 0; i < 15; i++) {
                            Assert.assertEquals(visitsBefore + i * 64 + 63, trace.pollAtVisits.getQuick(i));
                        }
                    }
                }
            }
            IntList hooks = new IntList();
            hooks.addAll(state.hooks);
            return hooks;
        }
    }

    private static SqlExecutionCircuitBreakerConfiguration configuration(State state, int throttle) {
        return new DefaultSqlExecutionCircuitBreakerConfiguration() {
            @Override
            public boolean checkConnection() {
                return false;
            }

            @Override
            public int getCircuitBreakerThrottle() {
                return throttle;
            }

            @Override
            public MillisecondClock getClock() {
                return () -> {
                    state.millisReads++;
                    return 1000;
                };
            }

            @Override
            public long getQueryTimeout() {
                return Long.MAX_VALUE;
            }
        };
    }

    private static Function filter(Trace trace, boolean isAccepted) {
        return new BooleanFunction() {
            @Override
            public boolean getBool(Record record) {
                trace.visit();
                return isAccepted;
            }
        };
    }

    private static HorizonJoinTimeFrameHelper helper(Cursor cursor, Function filter, long lookahead) {
        HorizonJoinTimeFrameHelper helper = new HorizonJoinTimeFrameHelper(null, lookahead, 1, 0, 0, 1, filter, null);
        helper.of(cursor, null);
        return helper;
    }

    private static Map newMap(CairoEngine engine) {
        return MapFactory.createUnorderedMap(engine.getConfiguration(), new ArrayColumnTypes().add(ColumnType.INT), new ArrayColumnTypes().add(ColumnType.LONG));
    }

    private static class Cursor implements TimeFrameCursor {
        private final TimeFrame frame = new TimeFrame();
        private final Row record = new Row();
        private final long[] sizes;
        private final Trace trace;
        private int seekIndex;

        Cursor(Trace trace, long... sizes) {
            this.trace = trace;
            this.sizes = sizes;
            toTop();
        }

        @Override
        public void close() {
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public StaticSymbolTable getSymbolTable(int columnIndex) {
            throw new UnsupportedOperationException();
        }

        @Override
        public TimeFrame getTimeFrame() {
            return frame;
        }

        @Override
        public int getTimestampIndex() {
            return 0;
        }

        @Override
        public void jumpTo(int index) {
            frame.ofEstimate(index, index * 10_000L, (index + 1L) * 10_000);
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean next() {
            int index = frame.getFrameIndex() + 1;
            if (index >= sizes.length) {
                return false;
            }
            jumpTo(index);
            return true;
        }

        @Override
        public long open() {
            int index = frame.getFrameIndex();
            if (++trace.opens == trace.cancelOnOpen) {
                trace.signal.set(true);
            }
            frame.ofOpen(index * 10_000L, index * 10_000L + sizes[index], 0, sizes[index]);
            return sizes[index];
        }

        @Override
        public boolean prev() {
            int index = frame.getFrameIndex() - 1;
            if (index < 0) {
                return false;
            }
            jumpTo(index);
            return true;
        }

        @Override
        public void recordAt(Record record, long rowId) {
            recordAt(record, Rows.toPartitionIndex(rowId), Rows.toLocalRowID(rowId));
        }

        @Override
        public void recordAt(Record record, int frameIndex, long rowIndex) {
            this.record.frameIndex = frameIndex;
            this.record.rowIndex = rowIndex;
        }

        @Override
        public void recordAtRowIndex(Record record, long rowIndex) {
            this.record.rowIndex = rowIndex;
        }

        @Override
        public void seekEstimate(long timestamp) {
            jumpTo(seekIndex);
        }

        @Override
        public void toTop() {
            frame.ofEstimate(-1, Long.MIN_VALUE, Long.MIN_VALUE);
        }

        private class Row implements Record {
            private int frameIndex;
            private long rowIndex;

            @Override
            public int getInt(int columnIndex) {
                return 1;
            }

            @Override
            public long getRowId() {
                return Rows.toRowID(frameIndex, rowIndex);
            }

            @Override
            public long getTimestamp(int columnIndex) {
                if (trace.isTimestampTrace) {
                    trace.visit();
                }
                return frameIndex * 10_000L + rowIndex;
            }
        }
    }

    private static class PollingEngine extends CairoEngine {
        private final State state;

        PollingEngine(CharSequence root, State state) {
            super(new DefaultCairoConfiguration(root) {
                @Override
                public NanosecondClock getNanosecondClock() {
                    return () -> state.nanos;
                }
            }, false);
            this.state = state;
            enableSqlExecutionCooperativePolling();
        }

        @Override
        public void onSqlExecutionCooperativePoll() {
            Assert.assertFalse("helper must not invoke a cooperative hook", state.isInsideHelper);
            state.hooks.add(state.outer);
        }
    }

    private static class State {
        private final IntList hooks = new IntList();
        private boolean isInsideHelper;
        private int millisReads;
        private long nanos;
        private int outer;
    }

    private static class Trace {
        private final IntList pollAtVisits = new IntList();
        private final AtomicBoolean signal = new AtomicBoolean();
        private int cancelAt;
        private int cancelOnOpen;
        private String expectedMethod;
        private boolean isTimestampTrace;
        private int opens;
        private int visits;

        void visit() {
            if (++visits == cancelAt) {
                Assert.assertTrue(expectedMethod, StackWalker.getInstance().walk(stream -> stream.anyMatch(frame -> frame.getMethodName().equals(expectedMethod))));
                signal.set(true);
            }
        }
    }

    private static class TracingBreaker extends NetworkSqlExecutionCircuitBreaker {
        private final Trace trace;

        TracingBreaker(PollingEngine engine, State state, Trace trace, int throttle) {
            super(engine, configuration(state, throttle));
            this.trace = trace;
        }

        @Override
        public void statefulThrowExceptionIfTrippedTimeThrottled() {
            trace.pollAtVisits.add(trace.visits);
            super.statefulThrowExceptionIfTrippedTimeThrottled();
        }
    }
}
