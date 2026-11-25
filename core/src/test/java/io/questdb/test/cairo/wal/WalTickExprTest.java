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

package io.questdb.test.cairo.wal;

import io.questdb.DefaultFactoryProvider;
import io.questdb.PropertyKey;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TickCalendarServiceFactory;
import io.questdb.cairo.sql.OperationFuture;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.engine.ops.UpdateOperation;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.str.Utf8String;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.TestTickCalendarService;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

/**
 * WAL apply runs an UPDATE, or an ALTER with a WHERE clause, after the client was told it
 * succeeded, against the commit time of the transaction. A tick expression must not fail there:
 * the table would stay suspended, as the same statement fails the same way on every RESUME WAL.
 * <ul>
 *     <li>a literal that does not parse, or that has too many intervals, fails when the statement
 *     compiles, before the client sees it succeed;</li>
 *     <li>a range that starts after it ends at the commit time adds no interval;</li>
 *     <li>a bind variable or a value of a row that does not parse acts as a null value, which IN
 *     matches no row with and NOT IN every row, as a row filter computes NOT IN as the negation
 *     of IN;</li>
 *     <li>date variables in a bind variable or in a value of a row resolve against the commit
 *     time, as they do in a literal, so every replay and every node computes the same rows.</li>
 * </ul>
 * A statement that a user runs directly keeps failing on all of these.
 */
public class WalTickExprTest extends AbstractCairoTest {
    private static final String BAD_ZONE = "2024-01-02T08:00@Bogus/Zone";
    private static final String COLUMNS = "(ts TIMESTAMP, ts2 TIMESTAMP, v INT, s STRING, vc VARCHAR) TIMESTAMP(ts) PARTITION BY DAY";
    private static final String INVERTED_AT_0300 = "$today + 570m..$now";
    // five rows: a valid point in time, a value that does not parse, a valid day, a row on the next
    // day, and a row with null values
    private static final String ROWS = """
            ('2024-01-02T08:00:00.000000Z', '2024-01-02T08:00:00.000000Z', 1, '2024-01-02T08:00', '2024-01-02T08:00'),
            ('2024-01-02T09:00:00.000000Z', '2024-01-02T09:00:00.000000Z', 1, 'garbage', 'garbage'),
            ('2024-01-02T10:00:00.000000Z', '2024-01-02T10:00:00.000000Z', 1, '2024-01-02', '2024-01-02'),
            ('2024-01-03T09:00:00.000000Z', '2024-01-03T09:00:00.000000Z', 1, '2024-01-03T09:00', '2024-01-03T09:00'),
            ('2024-01-04T09:00:00.000000Z', NULL, 1, NULL, NULL)
            """;
    private static final String[] TIMESTAMPS = {
            "2024-01-02T08:00:00.000000Z",
            "2024-01-02T09:00:00.000000Z",
            "2024-01-02T10:00:00.000000Z",
            "2024-01-03T09:00:00.000000Z",
            "2024-01-04T09:00:00.000000Z"
    };
    private int tableCount;

    @Test
    public void testAlterWherePastRangeOrderAtCommitTimeKeepsTableApplying() throws Exception {
        // The client compiles the ALTER at 12:00, when the range is valid, and the WAL commits it the
        // next day at 03:00, when the range starts after it ends. The apply drops nothing.
        assertMemoryLeak(() -> {
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-03T12:00:00.000000Z"));
            final TableToken token = createTable(true);
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                final CompiledQuery cq = compiler.compile(
                        "ALTER TABLE " + token.getTableName() + " DROP PARTITION WHERE ts IN '" + INVERTED_AT_0300 + "'",
                        sqlExecutionContext
                );
                setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-04T03:00:00.000000Z"));
                try (OperationFuture fut = cq.execute(null)) {
                    fut.await();
                }
            }
            assertApplied(token, "11111");
        });
    }

    @Test
    public void testBareDateVariableTickCalendarDurationInUpdate() throws Exception {
        // An UPDATE with a bare date variable expression with a tick calendar and a duration updates
        // what the tick calendar keeps of each time slot, extended by the duration once, as a query
        // selects it, applied from the WAL and run directly. On Friday 2025-01-24, XNYS trades from
        // 14:30 to 21:00Z: the minutes at 15:00 and 16:00, an hour longer, hold the 121 minutes from
        // 15:00 to 17:00, and the minute at 15:00 the 61 minutes from 15:00 to 16:00, which
        // dateadd('h', 1, ts) selects an hour earlier.
        factoryProvider = new DefaultFactoryProvider() {
            @Override
            public @NotNull TickCalendarServiceFactory getTickCalendarServiceFactory() {
                return () -> TestTickCalendarService.INSTANCE;
            }
        };
        assertMemoryLeak(() -> {
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2025-01-24T12:00:00.000000Z"));
            try {
                for (boolean isWal : new boolean[]{true, false}) {
                    final String name = isWal ? "w" : "nw";
                    execute("CREATE TABLE " + name + " AS ("
                            + "SELECT timestamp_sequence('2025-01-24T00:00:00.000000Z', 60_000_000L) ts, 1 v FROM long_sequence(2_880)"
                            + ") TIMESTAMP(ts) PARTITION BY DAY" + (isWal ? " WAL" : " BYPASS WAL"));
                    drainWalQueue();
                    bindVariableService.clear();
                    bindVariableService.setStr(0, "$todayT[15:00,16:00]#XNYS;1h");
                    try {
                        update("UPDATE " + name + " SET v = 2 WHERE ts IN $1");
                    } finally {
                        bindVariableService.clear();
                    }
                    drainWalQueue();
                    assertRowsWithValue(name, 2, "121\t2025-01-24T15:00:00.000000Z\t2025-01-24T17:00:00.000000Z");
                    update("UPDATE " + name + " SET v = 3 WHERE dateadd('h', 1, ts) IN '$todayT[15:00@UTC]#XNYS;1h'");
                    drainWalQueue();
                    assertRowsWithValue(name, 3, "61\t2025-01-24T14:00:00.000000Z\t2025-01-24T15:00:00.000000Z");
                }

                // dateadd('h', 1, ts) IN a literal whose time the IR rejects runs on the static
                // inversion of the parse as the statement compiles, which makes the minute at 15:00 an
                // hour-wide window and extends what the session keeps of it by an hour again, see
                // MonotonicTimestampPruningTest#testInBareTickCalendarLiteralRejectedByIrKeepsStaticInversion.
                // The UPDATE of the table without WAL runs on it; the one of the WAL table compiles the
                // filter for the apply, which in() cannot build, and fails before the client sees it
                // succeed. The WAL table then keeps applying.
                update("UPDATE nw SET v = 4 WHERE dateadd('h', 1, ts) IN '$todayT15:00:00Z#XNYS;1h'");
                assertRowsWithValue("nw", 4, "120\t2025-01-24T14:00:00.000000Z\t2025-01-24T15:59:00.000000Z");
                final TableToken walToken = engine.verifyTableName("w");
                final String rejected = "UPDATE w SET v = 4 WHERE dateadd('h', 1, ts) IN '$todayT15:00:00Z#XNYS;1h'";
                final long seqTxn = engine.getTableSequencerAPI().lastTxn(walToken);
                assertExceptionNoLeakCheck(rejected, rejected.indexOf("'$"), "Invalid time override: 15:00:00Z");
                Assert.assertEquals(seqTxn, engine.getTableSequencerAPI().lastTxn(walToken));

                // a duration that does not parse in a bind variable acts as a null value in the apply
                bindVariableService.clear();
                bindVariableService.setStr(0, "$today#XNYS;1x");
                try {
                    update("UPDATE w SET v = 5 WHERE ts IN $1");
                } finally {
                    bindVariableService.clear();
                }
                update("UPDATE w SET v = 6 WHERE ts = '2025-01-25T00:00:00.000000Z'");
                drainWalQueue();
                Assert.assertFalse("table is suspended", engine.getTableSequencerAPI().isSuspended(walToken));
                assertRowsWithValue("w", 5, "0\t\t");
                assertRowsWithValue("w", 6, "1\t2025-01-25T00:00:00.000000Z\t2025-01-25T00:00:00.000000Z");
            } finally {
                bindVariableService.clear();
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testBindVariableDateVariablesResolveAtCommitTime() throws Exception {
        // the UPDATE commits at 12:00 and applies at 03:00 the next day: a date variable in a bind
        // variable resolves against the commit time, as one in a literal does, and not against the
        // clock at the time of the apply, which differs on every replay and on every node
        assertMemoryLeak(() -> {
            final long noon = MicrosFormatUtils.parseTimestamp("2024-01-02T12:00:00.000000Z");
            final long nextDay = MicrosFormatUtils.parseTimestamp("2024-01-03T03:00:00.000000Z");
            // 09:30 to 12:00 on the day of the commit
            assertBindUpdateAppliedLater("ts IN $1", INVERTED_AT_0300, noon, nextDay, "11211");
            assertLiteralUpdateAppliedLater("ts IN '" + INVERTED_AT_0300 + "'", noon, nextDay, "11211");
            // the day of the commit
            assertBindUpdateAppliedLater("ts IN $1", "$today", noon, nextDay, "22211");
            assertBindUpdateAppliedLater("ts2 IN $1", "$today", noon, nextDay, "22211");
            assertBindUpdateAppliedLater("ts NOT IN $1", "$today", noon, nextDay, "11122");
            assertLiteralUpdateAppliedLater("ts IN '$today'", noon, nextDay, "22211");
        });
    }

    @Test
    public void testBindVariableInvertedRangeIsEmpty() throws Exception {
        // at 03:00 the range starts at 09:30 and ends at 03:00: it adds no interval, the same as the
        // literal, so IN matches nothing and NOT IN matches every row
        assertMemoryLeak(() -> {
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-02T03:00:00.000000Z"));
            assertBindUpdateApplied("ts IN $1", INVERTED_AT_0300, "11111");
            assertBindUpdateApplied("ts NOT IN $1", INVERTED_AT_0300, "22222");
            assertBindUpdateApplied("ts2 IN $1", INVERTED_AT_0300, "11111");
            assertBindUpdateApplied("ts2 NOT IN $1", INVERTED_AT_0300, "22222");
            // a list keeps its other elements
            assertBindUpdateApplied("ts IN $1", "[" + INVERTED_AT_0300 + ", $tomorrow]T09:00", "11121");
        });
    }

    @Test
    public void testBindVariableOverIntervalLimitIsNull() throws Exception {
        // nothing checked the value of a bind variable before the apply, so the interval limit holds
        // there, and a value over it acts as a null value
        assertMemoryLeak(() -> {
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-02T03:00:00.000000Z"));
            final String overLimit = "[$today..$today + 2000d]T09:00";
            for (String bindValue : new String[]{overLimit, null}) {
                assertBindUpdateApplied("ts IN $1", bindValue, "11111");
                assertBindUpdateApplied("ts NOT IN $1", bindValue, "22222");
                assertBindUpdateApplied("ts2 IN $1", bindValue, "11111");
                assertBindUpdateApplied("ts2 NOT IN $1", bindValue, "22222");
            }
        });
    }

    @Test
    public void testBindVariableThatDoesNotParseIsNull() throws Exception {
        // the value acts as null, in the interval scan and in the row filter alike, so the UPDATE
        // matches the same rows as with a null bind variable
        assertMemoryLeak(() -> {
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-02T03:00:00.000000Z"));
            // a range between two points in time ignores its duration, which still has to parse:
            // from 03:00 to 10:00 it would hold the rows of the first day. A range that starts
            // after it ends needs a timezone of its element that parses too: $today would hold
            // the rows of the first day. A point in time ignores the time part of the list, which
            // still has to parse: 08:00 would hold the first row.
            for (String bindValue : new String[]{
                    BAD_ZONE, null, "$now..$now + 7h;1h#Tue", "[$tomorrow..$today@Bogus/Zone, $today]", "[$today + 8h]T[25:00]"
            }) {
                for (boolean isVarchar : new boolean[]{false, true}) {
                    // the interval scan
                    assertBindUpdateApplied("ts IN $1", bindValue, isVarchar, "11111");
                    assertBindUpdateApplied("ts NOT IN $1", bindValue, isVarchar, "22222");
                    assertBindUpdateApplied("ts NOT IN $1 AND ts < '2024-01-03'", bindValue, isVarchar, "22211");
                    // the row filter, null timestamp included
                    assertBindUpdateApplied("ts2 IN $1", bindValue, isVarchar, "11111");
                    assertBindUpdateApplied("ts2 NOT IN $1", bindValue, isVarchar, "22222");
                    // the other side of an OR still decides
                    assertBindUpdateApplied("ts IN $1 OR ts = '2024-01-03T09:00:00.000000Z'", bindValue, isVarchar, "11121");
                    assertBindUpdateApplied("ts2 IN $1 OR v = 3", bindValue, isVarchar, "11111");
                }
            }
        });
    }

    @Test
    public void testBusinessDayRangeWithoutDayWithMalformedElementSuffixFails() throws Exception {
        // On Saturday 2024-01-06 the range of business days of the day holds none, and adds no
        // interval. A timezone or day filter of its element that does not parse fails the UPDATE
        // as it compiles all the same, before the client sees it succeed: the apply would run it
        // as a null value, so NOT IN would update every row. With a timezone that parses, NOT IN
        // updates every row, and IN none.
        assertMemoryLeak(() -> {
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-06T10:00:00.000000Z"));
            final TableToken token = createTable(true);
            assertUpdateRejected(token, "ts NOT IN '[$today..$today + 0bd@Bogus/Zone]'", "invalid timezone: Bogus/Zone");
            assertUpdateRejected(token, "ts2 NOT IN '[$today..$today + 0bd@Bogus/Zone]'", "invalid timezone: Bogus/Zone");
            assertUpdateRejected(token, "ts IN '[$today..$today + 0bd#Xyz]'", "Invalid day name: Xyz");
            assertApplied(token, "11111");
            assertUpdateApplied("ts NOT IN '[$today..$today + 0bd@+00:00]'", "22222");
            assertUpdateApplied("ts IN '[$today..$today + 0bd@+00:00]'", "11111");
        });
    }

    @Test
    public void testInvertedRangeIsEmptyAtCommitTime() throws Exception {
        // at 03:00 the literal range starts at 09:30 and ends at 03:00. The client compiles it,
        // since whether it starts after it ends depends on the time the statement runs, and the
        // apply runs it at the commit time, where it adds no interval.
        assertMemoryLeak(() -> {
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-02T03:00:00.000000Z"));
            assertUpdateApplied("ts IN '" + INVERTED_AT_0300 + "'", "11111");
            assertUpdateApplied("ts NOT IN '" + INVERTED_AT_0300 + "'", "22222");
            assertUpdateApplied("ts2 IN '" + INVERTED_AT_0300 + "'", "11111");
            assertUpdateApplied("ts2 NOT IN '" + INVERTED_AT_0300 + "'", "22222");
            assertUpdateApplied("dateadd('h', 1, ts) IN '" + INVERTED_AT_0300 + "'", "11111");
            // a range that starts after it ends whenever it runs
            assertUpdateApplied("ts IN '[$tomorrow..$today]'", "11111");
            // an expression that SQL parses on every evaluation keeps its other elements
            assertUpdateApplied("ts IN '[$tomorrow..$today, 2024-01-03]T[09:00,10:00]'", "11121");
            assertUpdateApplied("ts IN '[" + INVERTED_AT_0300 + ", $tomorrow]T09:00'", "11121");
            // a query that a user runs still fails on the range
            assertExceptionNoLeakCheck(
                    "SELECT count() FROM wt1 WHERE ts IN '" + INVERTED_AT_0300 + "'",
                    36,
                    "Invalid date range: start is after end"
            );
        });
    }

    @Test
    public void testLegacyLiteralWithElementTimePartUnderListTimeAppliesAsNull() throws Exception {
        // A WAL that an older build wrote can hold an UPDATE with a literal that SQL now parses on
        // every evaluation and rejects as it compiles: a static element with a time part of its own
        // that starts with a bracket, under a time of the list. The apply keeps the table applying,
        // and runs it as a null value. The literal selected 09:00 on every day of January, and of
        // $today.
        assertMemoryLeak(() -> {
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-03T03:00:00.000000Z"));
            assertLegacyLiteralAppliesAsNull("[2024-01T[09:00,10:00], $today]T09:00");
        });
    }

    @Test
    public void testLegacyLiteralWithMalformedTimeOfPointInTimeAppliesAsNull() throws Exception {
        // A WAL that an older build wrote can hold an UPDATE with a literal that SQL now parses on
        // every evaluation and rejects as it compiles: an element that keeps its own time, under a
        // time part of the list that does not parse. The apply keeps the table applying, and runs it
        // as a null value. The literals selected the row at 09:00 on the day of the commit.
        assertMemoryLeak(() -> {
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-03T03:00:00.000000Z"));
            for (String tickExpr : new String[]{"[$today + 9h@+00:00]T[25:00]", "[$now + 6h..$now + 6h@+00:00]T25:00"}) {
                assertLegacyLiteralAppliesAsNull(tickExpr);
            }
        });
    }

    @Test
    public void testLegacyLiteralWithUnknownDurationUnitAppliesAsNull() throws Exception {
        // A WAL that an older build wrote can hold an UPDATE with a literal that SQL now rejects as
        // it compiles, such as one with a duration unit that the timestamp drivers do not add. The
        // apply keeps the table applying, and runs it as a null value, as it runs
        // [$today@+00:00];1x, a literal without IR that does not parse. The literal selected every
        // row before $today, and the rows with a NULL ts2. A range between two points in time
        // ignores its duration, which still has to parse: it selected the rows of the first day.
        assertMemoryLeak(() -> {
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-03T03:00:00.000000Z"));
            for (String tickExpr : new String[]{"$today;1x", "[$today@+00:00];1x", "$now - 1d..$now;1x"}) {
                assertLegacyLiteralAppliesAsNull(tickExpr);
            }
        });
    }

    @Test
    public void testLiteralOverIntervalLimitFailsBeforeUpdateIsAcknowledged() throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-02T03:00:00.000000Z"));
            final TableToken token = createTable(true);
            // the compiled form
            assertUpdateRejected(token, "ts IN '[$today..$today + 2000d]T09:00'", "Date range expansion produces too many intervals (max 1024)");
            assertUpdateRejected(token, "ts NOT IN '[$today..$today + 2000d]T09:00'", "Date range expansion produces too many intervals (max 1024)");
            assertUpdateRejected(token, "ts2 IN '[$today..$today + 2000d]T09:00'", "Date range expansion produces too many intervals (max 1024)");
            // the form that SQL parses on every evaluation
            assertUpdateRejected(token, "ts IN '[$today..$today + 600d, 2024-01-01]T[09:00,10:00]'", "produces too many intervals (max 1024)");
            assertApplied(token, "11111");
        });
    }

    @Test
    public void testLiteralThatDoesNotParseFailsBeforeUpdateIsAcknowledged() throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-02T03:00:00.000000Z"));
            final TableToken token = createTable(true);
            // forms that SQL parses on every evaluation
            assertUpdateRejected(token, "ts IN '[$today,garbage]T[09:00,10:00]'", "Invalid date");
            assertUpdateRejected(token, "ts NOT IN '[$today,garbage]T[09:00,10:00]'", "Invalid date");
            assertUpdateRejected(token, "ts2 IN '[$today,garbage]T[09:00,10:00]'", "Invalid date");
            assertUpdateRejected(token, "ts IN '$todayT[09,14]:[00,99]'", "Invalid date: 2024-01-02T09:99");
            assertUpdateRejected(token, "ts IN '[$today]T[09,14]:[00,99]'", "Invalid date: 2024-01-02T09:99");
            // a duration unit that the timestamp drivers do not add
            assertUpdateRejected(token, "ts IN '$today;1x'", "Invalid duration unit: x");
            assertUpdateRejected(token, "ts NOT IN '$now;1H'", "Invalid duration unit: H");
            assertUpdateRejected(token, "ts2 IN '$today;2D'", "Invalid duration unit: D");
            // a function of the designated timestamp, and an ALTER that drops partitions
            final long seqTxn = engine.getTableSequencerAPI().lastTxn(token);
            for (String[] sql : new String[][]{
                    {"UPDATE " + token.getTableName() + " SET v = 2 WHERE dateadd('h', 1, ts) IN '$today;1x'", "x"},
                    {"ALTER TABLE " + token.getTableName() + " DROP PARTITION WHERE ts IN '$today;1D'", "D"}
            }) {
                assertExceptionNoLeakCheck(sql[0], sql[0].indexOf("'$today"), "Invalid duration unit: " + sql[1]);
            }
            Assert.assertEquals(seqTxn, engine.getTableSequencerAPI().lastTxn(token));
            // a range between two points in time ignores its duration, which still has to parse
            assertUpdateRejected(token, "ts IN '$now - 1h..$now;1x'", "Invalid duration unit: x");
            assertUpdateRejected(token, "ts NOT IN '[$now - 1h..$now@+00:00];1h#Tue'", "Expected number before unit '#'");
            assertUpdateRejected(token, "ts2 IN '$now - 1h..$now;1H'", "Invalid duration unit: H");
            for (String[] sql : new String[][]{
                    {"UPDATE " + token.getTableName() + " SET v = 2 WHERE dateadd('h', 1, ts) IN '$now - 1h..$now;1h#Tue'", "Expected number before unit '#'"},
                    {"ALTER TABLE " + token.getTableName() + " DROP PARTITION WHERE ts IN '$now - 1d..$now;1x'", "Invalid duration unit: x"}
            }) {
                assertExceptionNoLeakCheck(sql[0], sql[0].indexOf("'$now"), sql[1]);
            }
            Assert.assertEquals(seqTxn, engine.getTableSequencerAPI().lastTxn(token));
            // a static element with a time part of its own that starts with a bracket, under a time
            // of the list: the literal selected 09:00 on every day of January
            final String elementTimePart = "'[2024-01T[09:00,10:00], $today]T09:00'";
            assertUpdateRejected(token, "ts IN " + elementTimePart, "Invalid date: 2024-01-01T09:00T09:00");
            assertUpdateRejected(token, "ts NOT IN " + elementTimePart, "Invalid date: 2024-01-01T09:00T09:00");
            assertUpdateRejected(token, "ts2 IN " + elementTimePart, "Invalid date: 2024-01-01T09:00T09:00");
            // a range that starts after it ends at 03:00, with a timezone or day filter of its
            // element that does not parse: the apply would run it as a range that adds no
            // interval, so NOT IN would update every row
            assertUpdateRejected(token, "ts NOT IN '[$tomorrow..$today@Invalid/Zone]'", "invalid timezone: Invalid/Zone");
            assertUpdateRejected(token, "ts IN '[$tomorrow..$today#Xyz]T[09..10]:30'", "Invalid day name: Xyz");
            assertUpdateRejected(token, "ts2 NOT IN '[" + INVERTED_AT_0300 + "@Invalid/Zone]T[09..10]:30'", "invalid timezone: Invalid/Zone");
            // an element that keeps its own time, under a time part of the list that does not parse:
            // the literal selected the row at 08:00, or every other row
            assertUpdateRejected(token, "ts IN '[$today + 8h@+00:00]T[25:00]'", "Invalid date: 2024-01-02T25:00");
            assertUpdateRejected(token, "ts NOT IN '[$now + 5h..$now + 5h@+00:00]T25:00'", "Invalid date: 2024-01-02T25:00");
            assertUpdateRejected(token, "ts2 IN '[$now + 5h]T[08..09]:99'", "Invalid date: 2024-01-02T08:99");
            assertUpdateRejected(token, "ts NOT IN '[$today..$now - 4h#Xyz]'", "Invalid day name: Xyz");
            assertUpdateRejected(token, "ts NOT IN '[$tomorrow..$today@+02:00@UTC]'", "Invalid date: 2024-01-03@+02:00");
            for (String sql : new String[]{
                    "UPDATE " + token.getTableName() + " SET v = 2 WHERE dateadd('h', 1, ts) IN '[$tomorrow..$today@Invalid/Zone]'",
                    "ALTER TABLE " + token.getTableName() + " DROP PARTITION WHERE ts IN '[$tomorrow..$today@Invalid/Zone]'"
            }) {
                assertExceptionNoLeakCheck(sql, sql.indexOf("'[$tomorrow"), "invalid timezone: Invalid/Zone");
            }
            Assert.assertEquals(seqTxn, engine.getTableSequencerAPI().lastTxn(token));
            assertApplied(token, "11111");
        });
    }

    @Test
    public void testLiteralUnderIntervalLimitAtCompileTimeAppliesInFull() throws Exception {
        // A month has a different number of days on the day the UPDATE compiles and on the day it
        // commits: it compiles with 30 intervals, under the limit of 31, and the apply runs it with
        // 32. The limit held when the client could still see an error, so the apply runs it in full.
        setProperty(PropertyKey.CAIRO_SQL_INTERVAL_INCREMENTAL_MERGE_THRESHOLD, 8);
        setProperty(PropertyKey.CAIRO_SQL_INTERVAL_MAX_INTERVALS_AFTER_MERGE, 31);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE wm (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO wm VALUES
                    ('2024-02-01T09:00:00.000000Z', 1),
                    ('2024-02-29T09:00:00.000000Z', 1),
                    ('2024-03-01T09:00:00.000000Z', 1),
                    ('2024-03-31T09:00:00.000000Z', 1),
                    ('2024-04-01T09:00:00.000000Z', 1),
                    ('2024-04-02T09:00:00.000000Z', 1)
                    """);
            drainWalQueue();
            final String tickExpr = "'[$today..$today + 1M]T09:00'";
            // 2024-02-01 to 2024-03-01, 30 days
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-02-01T12:00:00.000000Z"));
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                final CompiledQuery cq = compiler.compile("UPDATE wm SET v = 2 WHERE ts IN " + tickExpr, sqlExecutionContext);
                // 2024-03-01 to 2024-04-01, 32 days
                setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-03-01T12:00:00.000000Z"));
                try (UpdateOperation ignore = cq.getUpdateOperation(); OperationFuture fut = cq.execute(null)) {
                    fut.await();
                }
            }
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("wm")));
            assertQuery("SELECT ts, v FROM wm")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            ts\tv
                            2024-02-01T09:00:00.000000Z\t1
                            2024-02-29T09:00:00.000000Z\t1
                            2024-03-01T09:00:00.000000Z\t2
                            2024-03-31T09:00:00.000000Z\t2
                            2024-04-01T09:00:00.000000Z\t2
                            2024-04-02T09:00:00.000000Z\t1
                            """);
            // a query that a user runs on that day fails on the limit
            assertExceptionNoLeakCheck(
                    "SELECT count() FROM wm WHERE ts IN " + tickExpr,
                    35,
                    "Date range expansion produces too many intervals (max 31)"
            );
        });
    }

    @Test
    public void testLiteralWithLaterDaysFirstAppliesInFull() throws Exception {
        // 200 later days, then the 401 days before them: from the 256th interval on, each day merges
        // into the intervals so far, in front of the 200 later ones. The apply evaluates the literal
        // without the interval limit, and updates the row at 09:30 of each of the 601 days, from
        // 2023-12-21 to 2025-08-12.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE wr (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY MONTH WAL");
            // a row at 09:30 and one at 10:30 of each day from 2023-12-11 to 2025-08-22
            execute("""
                    INSERT INTO wr
                    SELECT dateadd('h', ((x - 1) % 2)::INT, dateadd('d', ((x - 1) / 2)::INT, '2023-12-11T09:30:00.000000Z'::TIMESTAMP)), 1
                    FROM long_sequence(1_242)
                    """);
            drainWalQueue();
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2025-01-24T12:00:00.000000Z"));
            try {
                update("UPDATE wr SET v = 2 WHERE ts IN '[$today+1d..$today+200d, $today-400d..$today]T09:00;1h'");
                drainWalQueue();
                Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("wr")));
                assertQuery("SELECT v, count(), min(ts), max(ts) FROM wr ORDER BY v")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                v\tcount\tmin\tmax
                                1\t641\t2023-12-11T09:30:00.000000Z\t2025-08-22T10:30:00.000000Z
                                2\t601\t2023-12-21T09:30:00.000000Z\t2025-08-12T09:30:00.000000Z
                                """);
            } finally {
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testNonWalUpdateFailsOnTickExpression() throws Exception {
        // an UPDATE that runs when the client sends it keeps failing, as a query does
        assertMemoryLeak(() -> {
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-02T03:00:00.000000Z"));
            final TableToken token = createTable(false);
            final String name = token.getTableName();
            bindVariableService.clear();
            bindVariableService.setStr(0, BAD_ZONE);
            try {
                // the UPDATE reports the error of the interval scan inside its own, without a position
                assertExceptionNoLeakCheck("UPDATE " + name + " SET v = 2 WHERE ts IN $1", -1, "invalid timezone: Bogus/Zone");
                assertExceptionNoLeakCheck("UPDATE " + name + " SET v = 2 WHERE ts NOT IN $1", -1, "invalid timezone: Bogus/Zone");
                assertExceptionNoLeakCheck("UPDATE " + name + " SET v = 2 WHERE ts2 IN $1", -1, "invalid timezone: Bogus/Zone");
            } finally {
                bindVariableService.clear();
            }
            assertUpdateFails("UPDATE " + name + " SET v = 2 WHERE ts IN s", " s", "Invalid date: garbage");
            assertUpdateFails("UPDATE " + name + " SET v = 2 WHERE ts IN '" + INVERTED_AT_0300 + "'", "'", "Invalid date range: start is after end");
            assertUpdateFails("UPDATE " + name + " SET v = 2 WHERE ts2 IN '" + INVERTED_AT_0300 + "'", "'", "Invalid date range: start is after end");
            assertUpdateFails("UPDATE " + name + " SET v = 2 WHERE ts IN '$today;1x'", "'", "Invalid duration unit: x");
            assertUpdateFails("UPDATE " + name + " SET v = 2 WHERE ts IN '$now - 1h..$now;1x'", "'", "Invalid duration unit: x");
            // a timezone of the element of a range that starts after it ends fails the statement
            // as it compiles, before the order of the range when it runs
            assertUpdateFails("UPDATE " + name + " SET v = 2 WHERE ts IN '[$tomorrow..$today@Bogus/Zone]'", "'", "invalid timezone: Bogus/Zone");
            assertRows(name, "11111");
        });
    }

    @Test
    public void testPointInTimeRangeElementSuffixAppliesAtCommitTime() throws Exception {
        // The apply runs a range between two points in time with a day filter of its element, and
        // with a timezone of its element before it, against the commit time, as a literal and as a
        // bind variable, in the interval scan and in the row filter. 2024-01-02 is a Tuesday: at
        // 10:00 the range from 07:00 holds the rows of that morning with #Tue, and none with #Wed;
        // at 15:00, 13:00 to 15:00 at UTC+05 is 08:00 to 10:00Z.
        assertMemoryLeak(() -> {
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-02T10:00:00.000000Z"));
            assertUpdateApplied("ts IN '[$now - 3h..$now#Tue]'", "22211");
            assertUpdateApplied("ts IN '[$now - 3h..$now#Wed]'", "11111");
            assertUpdateApplied("ts NOT IN '[$now - 3h..$now#Wed]'", "22222");
            assertUpdateApplied("ts2 IN '[$now - 3h..$now#Tue]'", "22211");
            assertUpdateApplied("ts2 IN '[$now - 3h..$now#Wed]'", "11111");
            assertBindUpdateApplied("ts IN $1", "[$now - 3h..$now#Wed]", "11111");
            assertBindUpdateApplied("ts2 IN $1", "[$now - 3h..$now#Tue]", "22211");
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-02T15:00:00.000000Z"));
            assertUpdateApplied("ts IN '[$now - 2h..$now@UTC+05#Tue]'", "22211");
            assertUpdateApplied("ts2 IN '[$now - 2h..$now@UTC+05#Tue]'", "22211");
            assertBindUpdateApplied("ts IN $1", "[$now - 2h..$now@UTC+05#Tue]", "22211");
        });
    }

    @Test
    public void testPointInTimeRangeElementUnknownDayFilterFailsOrActsAsNull() throws Exception {
        // A day filter of the element of a range between two points in time that does not parse
        // fails a literal as the UPDATE compiles, before the client sees it succeed. The apply runs
        // a bind variable with it as a null value, and so a literal that an older build wrote to
        // the WAL, and the table keeps applying. At 10:00 the range holds the rows from 08:00.
        assertMemoryLeak(() -> {
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-02T10:00:00.000000Z"));
            final String tickExpr = "[$now - 2h..$now#Xyz]";
            final TableToken token = createTable(true);
            assertUpdateRejected(token, "ts IN '" + tickExpr + "'", "Invalid day name: Xyz");
            assertUpdateRejected(token, "ts NOT IN '" + tickExpr + "'", "Invalid day name: Xyz");
            assertUpdateRejected(token, "ts2 IN '" + tickExpr + "'", "Invalid day name: Xyz");
            assertUpdateRejected(token, "ts IN '[$now - 2h..$now@UTC+05#Xyz]'", "Invalid day name: Xyz");
            assertApplied(token, "11111");
            for (boolean isVarchar : new boolean[]{false, true}) {
                assertBindUpdateApplied("ts IN $1", tickExpr, isVarchar, "11111");
                assertBindUpdateApplied("ts NOT IN $1", tickExpr, isVarchar, "22222");
                assertBindUpdateApplied("ts2 IN $1", tickExpr, isVarchar, "11111");
                assertBindUpdateApplied("ts2 NOT IN $1", tickExpr, isVarchar, "22222");
            }
            assertLegacyLiteralAppliesAsNull(tickExpr);
        });
    }

    @Test
    public void testPreparedUpdateCompilesWhileRangeStartsAfterItEnds() throws Exception {
        // The UPDATE compiles at 03:00, when the range starts after it ends, and runs at 12:00, when
        // it does not: compiling checks only what does not depend on the time the statement runs.
        assertMemoryLeak(() -> {
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-02T03:00:00.000000Z"));
            final TableToken token = createTable(true);
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                final CompiledQuery cq = compiler.compile(
                        "UPDATE " + token.getTableName() + " SET v = 2 WHERE ts IN '" + INVERTED_AT_0300 + "'",
                        sqlExecutionContext
                );
                setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-02T12:00:00.000000Z"));
                try (UpdateOperation ignore = cq.getUpdateOperation(); OperationFuture fut = cq.execute(null)) {
                    fut.await();
                }
            }
            // 09:30 to 12:00
            assertApplied(token, "11211");
        });
    }

    @Test
    public void testPreparedUpdateWithMalformedDurationFailsWhileRangeStartsAfterItEnds() throws Exception {
        // The UPDATE compiles at 03:00, when the range from 09:30 to now starts after it ends, which
        // SQL lets through, as whether it does depends on the time the statement runs. A duration
        // that does not parse fails it as it compiles all the same, before the client sees it
        // succeed: the apply would run it at the commit time as a null value, so NOT IN would
        // update every row.
        assertMemoryLeak(() -> {
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-02T03:00:00.000000Z"));
            final TableToken token = createTable(true);
            assertUpdateRejected(token, "ts NOT IN '[" + INVERTED_AT_0300 + "];1x'", "Invalid duration unit: x");
            assertUpdateRejected(token, "ts IN '[" + INVERTED_AT_0300 + "];1x'", "Invalid duration unit: x");
            assertUpdateRejected(token, "ts2 NOT IN '[" + INVERTED_AT_0300 + "];1x'", "Invalid duration unit: x");
            // forms that SQL parses on every evaluation
            assertUpdateRejected(token, "ts NOT IN '[$today + 570m..$now@+00:00];1h#Tue'", "Expected number before unit '#'");
            assertUpdateRejected(token, "ts NOT IN '[$today + 570m..$now@+00:00];1h;1d'", "Invalid interval format: ;1h;1d");
            assertApplied(token, "11111");

            // The repeating interval of the parser, a duration, a period and a count, repeats
            // nothing after the range, which the UPDATE runs at 12:00, from 09:30 to 12:00
            final TableToken repeating = createTable(true);
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                final CompiledQuery cq = compiler.compile(
                        "UPDATE " + repeating.getTableName() + " SET v = 2 WHERE ts NOT IN '[$today + 570m..$now@+00:00];1h;1d;3'",
                        sqlExecutionContext
                );
                setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-02T12:00:00.000000Z"));
                try (UpdateOperation ignore = cq.getUpdateOperation(); OperationFuture fut = cq.execute(null)) {
                    fut.await();
                }
            }
            assertApplied(repeating, "22122");
        });
    }

    @Test
    public void testPreparedUpdateWithMalformedElementSuffixFailsWhileRangeStartsAfterItEnds() throws Exception {
        // The UPDATE compiles at 03:00, when the range starts after it ends, which SQL lets
        // through, as whether it does depends on the time the statement runs. A timezone or day
        // filter of its element that does not parse fails it as it compiles all the same, before
        // the client sees it succeed: the apply would run it as a range that adds no interval, or,
        // at a commit time when the range does not start after it ends, as a null value, so NOT IN
        // would update every row. With a timezone that parses, the UPDATE runs at 12:00: the range
        // from 09:30 to "now" holds the row at 10:00, and the one from the start of the day to 4
        // hours before "now" the row at 08:00.
        assertMemoryLeak(() -> {
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-02T03:00:00.000000Z"));
            final TableToken token = createTable(true);
            assertUpdateRejected(token, "ts NOT IN '[" + INVERTED_AT_0300 + "@Bogus/Zone]'", "invalid timezone: Bogus/Zone");
            assertUpdateRejected(token, "ts NOT IN '[$today..$now - 4h@Bogus/Zone]'", "invalid timezone: Bogus/Zone");
            assertUpdateRejected(token, "ts NOT IN '[$today..$now - 4h#Xyz]'", "Invalid day name: Xyz");
            assertApplied(token, "11111");

            for (String[] tickExpr : new String[][]{{"[" + INVERTED_AT_0300 + "@+00:00]", "11211"}, {"[$today..$now - 4h@+00:00]", "21111"}}) {
                setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-02T03:00:00.000000Z"));
                final TableToken prepared = createTable(true);
                try (SqlCompiler compiler = engine.getSqlCompiler()) {
                    final CompiledQuery cq = compiler.compile(
                            "UPDATE " + prepared.getTableName() + " SET v = 2 WHERE ts IN '" + tickExpr[0] + "'",
                            sqlExecutionContext
                    );
                    setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-02T12:00:00.000000Z"));
                    try (UpdateOperation ignore = cq.getUpdateOperation(); OperationFuture fut = cq.execute(null)) {
                        fut.await();
                    }
                }
                assertApplied(prepared, tickExpr[1]);
            }
        });
    }

    @Test
    public void testRowValueDateVariablesResolveAtCommitTime() throws Exception {
        assertMemoryLeak(() -> {
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-02T12:00:00.000000Z"));
            final TableToken token = createTable(true);
            final String name = token.getTableName();
            update("UPDATE " + name + " SET s = '$today', vc = '$today'");
            drainWalQueue();
            update("UPDATE " + name + " SET v = 2 WHERE ts IN s");
            update("UPDATE " + name + " SET v = v + 10 WHERE ts IN vc");
            // apply the next day
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-03T03:00:00.000000Z"));
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(token));
            assertQuery("SELECT ts, v FROM " + name)
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            ts\tv
                            2024-01-02T08:00:00.000000Z\t12
                            2024-01-02T09:00:00.000000Z\t12
                            2024-01-02T10:00:00.000000Z\t12
                            2024-01-03T09:00:00.000000Z\t1
                            2024-01-04T09:00:00.000000Z\t1
                            """);
        });
    }

    @Test
    public void testRowValueThatDoesNotParseIsNull() throws Exception {
        // the value of the second row acts as null, as the value of the fifth row is
        assertMemoryLeak(() -> {
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-02T03:00:00.000000Z"));
            for (String column : new String[]{"s", "vc"}) {
                assertUpdateApplied("ts IN " + column, "21221");
                assertUpdateApplied("ts NOT IN " + column, "12112");
                assertUpdateApplied("ts IN " + column + " OR v = 3", "21221");
            }
            // a query that a user runs still fails on the value
            assertExceptionNoLeakCheck("SELECT count() FROM wt1 WHERE ts IN s", 36, "Invalid date: garbage");

            // A range between two points in time ignores its duration, which still has to parse.
            // At 12:00 the range of the second row holds its 09:00.
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-02T12:00:00.000000Z"));
            for (String column : new String[]{"s", "vc"}) {
                assertUpdateAppliedWithSecondRowValue("$now - 4h..$now;1h#Tue", "ts IN " + column, "21221");
                assertUpdateAppliedWithSecondRowValue("$now - 4h..$now;1h#Tue", "ts NOT IN " + column, "12112");
                // a range that starts after it ends needs a timezone of its element that parses
                // too: $today would hold the 09:00 of the second row
                assertUpdateAppliedWithSecondRowValue("[$tomorrow..$today@Bogus/Zone, $today]", "ts IN " + column, "21221");
                assertUpdateAppliedWithSecondRowValue("[$tomorrow..$today@Bogus/Zone, $today]", "ts NOT IN " + column, "12112");
            }
        });
    }

    @Test
    public void testStaticDateTickCalendarDurationInUpdate() throws Exception {
        // An UPDATE with a literal with a fixed date, a date variable, a tick calendar and a duration
        // updates what the tick calendar keeps of the own extent of the fixed date, extended by the
        // duration once, as a query selects it, applied from the WAL and run directly: on Monday
        // 2025-01-27, the sessions of Friday 2025-01-24 and of the Monday, from 14:30 to 21:00Z, each an
        // hour longer, 900 minutes. A fixed date whose own extents alone exceed the interval limit, the
        // 1,440 seconds at the minutes of 2025-01-24, which nothing else in the list holds, fails the
        // UPDATE of the WAL table as it compiles, before the client sees it succeed, and the table keeps
        // applying.
        factoryProvider = new DefaultFactoryProvider() {
            @Override
            public @NotNull TickCalendarServiceFactory getTickCalendarServiceFactory() {
                return () -> TestTickCalendarService.INSTANCE;
            }
        };
        assertMemoryLeak(() -> {
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2025-01-27T12:00:00.000000Z"));
            try {
                for (boolean isWal : new boolean[]{true, false}) {
                    final String name = isWal ? "w" : "nw";
                    execute("CREATE TABLE " + name + " AS ("
                            + "SELECT timestamp_sequence('2025-01-24T00:00:00.000000Z', 60_000_000L) ts, 1 v FROM long_sequence(5_760)"
                            + ") TIMESTAMP(ts) PARTITION BY DAY" + (isWal ? " WAL" : " BYPASS WAL"));
                    drainWalQueue();
                    update("UPDATE " + name + " SET v = 2 WHERE ts IN '[$today, 2025-01-24]#XNYS;1h'");
                    drainWalQueue();
                    assertRowsWithValue(name, 2, "900\t2025-01-24T14:30:00.000000Z\t2025-01-27T21:59:00.000000Z");
                }

                final TableToken walToken = engine.verifyTableName("w");
                final String rejected = "UPDATE w SET v = 3 WHERE ts IN '[$today, 2025-01-24T[00..23]:[00..59]:00]#XNYS;1m'";
                final long seqTxn = engine.getTableSequencerAPI().lastTxn(walToken);
                assertExceptionNoLeakCheck(rejected, rejected.indexOf('\''), "Bracket expansion produces too many intervals (max 1024)");
                Assert.assertEquals(seqTxn, engine.getTableSequencerAPI().lastTxn(walToken));
                update("UPDATE w SET v = 4 WHERE ts = '2025-01-25T00:00:00.000000Z'");
                drainWalQueue();
                Assert.assertFalse("table is suspended", engine.getTableSequencerAPI().isSuspended(walToken));
                assertRowsWithValue("w", 3, "0\t\t");
                assertRowsWithValue("w", 4, "1\t2025-01-25T00:00:00.000000Z\t2025-01-25T00:00:00.000000Z");
            } finally {
                setCurrentMicros(-1);
            }
        });
    }

    @Test
    public void testTableKeepsApplyingAfterUpdate() throws Exception {
        // the shapes that suspended the table before this fix, followed by an INSERT, all apply
        assertMemoryLeak(() -> {
            setCurrentMicros(MicrosFormatUtils.parseTimestamp("2024-01-02T03:00:00.000000Z"));
            final TableToken token = createTable(true);
            final String name = token.getTableName();
            bindVariableService.clear();
            bindVariableService.setStr(0, BAD_ZONE);
            update("UPDATE " + name + " SET v = 2 WHERE ts IN $1");
            update("UPDATE " + name + " SET v = 3 WHERE ts2 IN $1");
            bindVariableService.clear();
            update("UPDATE " + name + " SET v = 4 WHERE ts IN s");
            update("UPDATE " + name + " SET v = 5 WHERE ts IN '" + INVERTED_AT_0300 + "'");
            execute("INSERT INTO " + name + " VALUES ('2024-01-05T00:00:00.000000Z', NULL, 7, NULL, NULL)");
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(token));
            assertQuery("SELECT writerTxn = sequencerTxn applied, suspended FROM wal_tables() WHERE name = '" + name + "'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            applied\tsuspended
                            true\tfalse
                            """);
            assertQuery("SELECT ts, v FROM " + name)
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            ts\tv
                            2024-01-02T08:00:00.000000Z\t4
                            2024-01-02T09:00:00.000000Z\t1
                            2024-01-02T10:00:00.000000Z\t4
                            2024-01-03T09:00:00.000000Z\t4
                            2024-01-04T09:00:00.000000Z\t1
                            2024-01-05T00:00:00.000000Z\t7
                            """);
        });
    }

    private static String expectedRows(String values) {
        Assert.assertEquals(TIMESTAMPS.length, values.length());
        final StringBuilder sb = new StringBuilder("ts\tv\n");
        for (int i = 0; i < TIMESTAMPS.length; i++) {
            sb.append(TIMESTAMPS[i]).append('\t').append(values.charAt(i)).append('\n');
        }
        return sb.toString();
    }

    private void assertApplied(TableToken token, String values) throws Exception {
        drainWalQueue();
        Assert.assertFalse("table is suspended", engine.getTableSequencerAPI().isSuspended(token));
        assertRows(token.getTableName(), values);
        // the table keeps applying what comes after
        execute("INSERT INTO " + token.getTableName() + " VALUES ('2024-01-05T00:00:00.000000Z', NULL, 7, NULL, NULL)");
        drainWalQueue();
        Assert.assertFalse("table is suspended", engine.getTableSequencerAPI().isSuspended(token));
        assertQuery("SELECT count() FROM " + token.getTableName() + " WHERE v = 7")
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .returns("""
                        count
                        1
                        """);
    }

    private void assertBindUpdateApplied(String predicate, String bindValue, String values) throws Exception {
        assertBindUpdateApplied(predicate, bindValue, false, values);
    }

    private void assertBindUpdateApplied(String predicate, String bindValue, boolean isVarchar, String values) throws Exception {
        final TableToken token = createTable(true);
        bindVariableService.clear();
        if (isVarchar) {
            bindVariableService.setVarchar(0, bindValue == null ? null : new Utf8String(bindValue));
        } else {
            bindVariableService.setStr(0, bindValue);
        }
        try {
            update("UPDATE " + token.getTableName() + " SET v = 2 WHERE " + predicate);
        } finally {
            bindVariableService.clear();
        }
        assertApplied(token, values);
    }

    private void assertBindUpdateAppliedLater(String predicate, String bindValue, long commitTime, long applyTime, String values) throws Exception {
        setCurrentMicros(commitTime);
        final TableToken token = createTable(true);
        bindVariableService.clear();
        bindVariableService.setStr(0, bindValue);
        try {
            update("UPDATE " + token.getTableName() + " SET v = 2 WHERE " + predicate);
        } finally {
            bindVariableService.clear();
        }
        setCurrentMicros(applyTime);
        assertApplied(token, values);
    }

    /**
     * Asserts that the apply of UPDATEs with the literal, which an older build wrote to the WAL,
     * runs it as a null value: IN matches no row, NOT IN every row, and the other side of an OR
     * still decides.
     */
    private void assertLegacyLiteralAppliesAsNull(String tickExpr) throws Exception {
        assertLegacyUpdateApplied("ts IN '" + tickExpr + "'", "11111");
        assertLegacyUpdateApplied("ts NOT IN '" + tickExpr + "'", "22222");
        assertLegacyUpdateApplied("ts2 IN '" + tickExpr + "'", "11111");
        assertLegacyUpdateApplied("ts2 NOT IN '" + tickExpr + "'", "22222");
        assertLegacyUpdateApplied("dateadd('h', 1, ts) IN '" + tickExpr + "'", "11111");
        // the other side of an OR still decides
        assertLegacyUpdateApplied("ts IN '" + tickExpr + "' OR ts = '2024-01-03T09:00:00.000000Z'", "11121");
    }

    /**
     * Writes the UPDATE to the WAL of a new table without compiling it, as an older build that did
     * not reject its tick expression did, and asserts that the apply updates the rows.
     */
    private void assertLegacyUpdateApplied(String predicate, String values) throws Exception {
        final TableToken token = createTable(true);
        try (
                TableRecordMetadata metadata = sqlExecutionContext.getMetadataForWrite(token);
                WalWriter writer = engine.getWalWriter(token);
                UpdateOperation operation = new UpdateOperation(
                        token,
                        metadata.getTableId(),
                        metadata.getMetadataVersion(),
                        0,
                        new ObjList<>("v")
                )
        ) {
            operation.withSqlStatement("UPDATE " + token.getTableName() + " SET v = 2 WHERE " + predicate);
            operation.withContext(sqlExecutionContext);
            writer.apply(operation);
        }
        assertApplied(token, values);
    }

    private void assertLiteralUpdateAppliedLater(String predicate, long commitTime, long applyTime, String values) throws Exception {
        setCurrentMicros(commitTime);
        final TableToken token = createTable(true);
        update("UPDATE " + token.getTableName() + " SET v = 2 WHERE " + predicate);
        setCurrentMicros(applyTime);
        assertApplied(token, values);
    }

    private void assertRows(String tableName, String values) throws Exception {
        assertQuery("SELECT ts, v FROM " + tableName)
                .noLeakCheck()
                .expectSize()
                .timestamp("ts")
                .returns(expectedRows(values));
    }

    /**
     * Asserts the count, first and last timestamp of the rows of the table whose v is the value.
     */
    private void assertRowsWithValue(String tableName, int value, String expectedRow) throws Exception {
        assertQuery("SELECT count(), min(ts), max(ts) FROM " + tableName + " WHERE v = " + value)
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .returns("count\tmin\tmax\n" + expectedRow + "\n");
    }

    private void assertUpdateApplied(String predicate, String values) throws Exception {
        final TableToken token = createTable(true);
        update("UPDATE " + token.getTableName() + " SET v = 2 WHERE " + predicate);
        assertApplied(token, values);
    }

    /**
     * Sets s and vc of the second row of a new table to the value, and asserts that the apply of
     * the UPDATE updates the rows.
     */
    private void assertUpdateAppliedWithSecondRowValue(String rowValue, String predicate, String values) throws Exception {
        final TableToken token = createTable(true);
        final String name = token.getTableName();
        update("UPDATE " + name + " SET s = '" + rowValue + "', vc = '" + rowValue + "' WHERE ts = '" + TIMESTAMPS[1] + "'");
        drainWalQueue();
        update("UPDATE " + name + " SET v = 2 WHERE " + predicate);
        assertApplied(token, values);
    }

    /**
     * Asserts that the statement fails with the error at the position of {@code operand}, the first
     * occurrence of it after WHERE, without the space that an operand can start with to tell it
     * from the column names.
     */
    private void assertUpdateFails(String sql, String operand, String error) throws Exception {
        final int position = sql.indexOf(operand, sql.indexOf(" WHERE ")) + (operand.charAt(0) == ' ' ? 1 : 0);
        Assert.assertTrue(position > 0);
        assertExceptionNoLeakCheck(sql, position, error);
    }

    private void assertUpdateRejected(TableToken token, String predicate, String error) throws Exception {
        final String sql = "UPDATE " + token.getTableName() + " SET v = 2 WHERE " + predicate;
        final long seqTxn = engine.getTableSequencerAPI().lastTxn(token);
        assertExceptionNoLeakCheck(sql, sql.indexOf('\''), error);
        // the client saw the error, and the WAL has nothing to apply
        Assert.assertEquals(seqTxn, engine.getTableSequencerAPI().lastTxn(token));
    }

    private TableToken createTable(boolean isWal) throws Exception {
        final String name = "wt" + (++tableCount);
        execute("CREATE TABLE " + name + ' ' + COLUMNS + (isWal ? " WAL" : " BYPASS WAL"));
        execute("INSERT INTO " + name + " VALUES " + ROWS);
        if (isWal) {
            drainWalQueue();
        }
        return engine.verifyTableName(name);
    }
}
