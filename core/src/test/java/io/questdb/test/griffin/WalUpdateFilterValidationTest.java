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

import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.griffin.engine.ops.UpdateOperation;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

/**
 * Verifies that WAL and non-WAL UPDATE filters use the same wrapped INT timestamp bound.
 * Sequencing-time validation remains covered by the wider update validation suite.
 */
@RunWith(Parameterized.class)
public class WalUpdateFilterValidationTest extends AbstractCairoTest {
    private static final String ALL_ROWS_UPDATED = """
            2024-07-06T00:00:00.000000Z\t999
            2024-07-07T00:00:00.000000Z\t999
            2024-07-08T00:00:00.000000Z\t999
            2024-07-09T00:00:00.000000Z\t999
            """;
    // the reach table holds two rows, one either side of the widened bound, and marks a rewritten
    // row with 999 against the 0 every row starts at
    private static final String BOTH_REACH_ROWS_UPDATED = """
            2024-07-06T00:00:00.000000Z\t999
            2024-07-09T00:00:00.000000Z\t999
            """;
    private static final String FOUR_ROWS_UNCHANGED = """
            2024-07-06T00:00:00.000000Z\t1
            2024-07-07T00:00:00.000000Z\t2
            2024-07-08T00:00:00.000000Z\t3
            2024-07-09T00:00:00.000000Z\t4
            """;
    private static final String LAST_ROW_UPDATED = """
            2024-07-06T00:00:00.000000Z\t1
            2024-07-07T00:00:00.000000Z\t2
            2024-07-08T00:00:00.000000Z\t3
            2024-07-09T00:00:00.000000Z\t999
            """;
    private static final String NEITHER_REACH_ROW_UPDATED = """
            2024-07-06T00:00:00.000000Z\t0
            2024-07-09T00:00:00.000000Z\t0
            """;
    private static final String TAIL_REACH_ROW_UPDATED = """
            2024-07-06T00:00:00.000000Z\t0
            2024-07-09T00:00:00.000000Z\t999
            """;
    private final boolean walEnabled;

    public WalUpdateFilterValidationTest(boolean walEnabled) {
        this.walEnabled = walEnabled;
    }

    @Parameterized.Parameters(name = "wal={0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{{false}, {true}});
    }

    // The bound is INT arithmetic inside an AND chain, so intrinsic extraction still reaches it.
    @Test
    public void testIntArithmeticBoundInsideAndChainAppliesWrappedValue() throws Exception {
        assertMemoryLeak(() -> {
            createTargetTable();
            assertWrappedTimestampUpdateApplies(
                    "UPDATE t SET v = 999 WHERE v > 0 AND ts > 1720468802 * 1000000"
            );
        });
    }

    @Test
    public void testIntArithmeticBoundOnDesignatedTimestampAppliesWrappedValue() throws Exception {
        assertMemoryLeak(() -> {
            createTargetTable();
            assertWrappedTimestampUpdateApplies(
                    "UPDATE t SET v = 999 WHERE ts > 1720468802 * 1000000"
            );
        });
    }

    @Test
    public void testLegacySegmentWithIntArithmeticBoundApplies() throws Exception {
        Assume.assumeTrue(walEnabled);
        assertMemoryLeak(() -> {
            createTargetTable();
            sequenceLegacySegmentUpdate("UPDATE t SET v = 999 WHERE ts > 1720468802 * 1000000");
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("t")));
            assertQuery("SELECT ts, v FROM t")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("ts\tv\n" + ALL_ROWS_UPDATED);
        });
    }

    @Test
    public void testIntArithmeticBoundOnNonDesignatedTimestampRewritesEveryRowDivergingFrom943() throws Exception {
        assertMemoryLeak(() -> {
            createSecondTimestampTable();

            // the value the engine will compare against, printed by the statement itself before it runs
            assertQuery("UPDATE u SET v = 999 WHERE other > 1_720_468_802 * 1_000_000")
                    .noLeakCheck()
                    .assertsPlanContaining("Update table: u", "filter: -607497088<other");
            // and the preview that names the rows it will touch
            assertQuery("SELECT count() AS c FROM u WHERE other > 1_720_468_802 * 1_000_000")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("c\n4\n");

            update("UPDATE u SET v = 999 WHERE other > 1_720_468_802 * 1_000_000");
            if (walEnabled) {
                drainWalQueue();
                Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("u")));
            }
            assertQuery("SELECT ts, v FROM u")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("ts\tv\n" + ALL_ROWS_UPDATED);
        });
    }

    /**
     * The reach of the un-guarded rule, so that a later change to any one spelling reddens rather
     * than drifts. Every case here RUNS the UPDATE and reads the table back, because the decision
     * being pinned is an UPDATE-path decision: a guard added to the UPDATE compile path later has to
     * turn this test red and force the call to be taken again, and a SELECT-only inventory would
     * have stayed green through exactly that change. Each case also asserts the identical SELECT's
     * count first, because "preview it with the same SELECT" is the only remedy this release offers
     * a caller and it has to hold for every spelling, not just the headline one. Two of the cases
     * hold the wrapped bound one level below the comparison and inside a function argument, which is
     * where the finding's own escalation path ran; two more wrap at run time, in a column and in a
     * bind variable, where no compile-time fold can see them at all.
     * <p>
     * Released 9.4.3 rewrote only the tail row in every wrapping case below, for the reason given on
     * {@link #testIntArithmeticBoundOnNonDesignatedTimestampRewritesEveryRowDivergingFrom943()}. The
     * two rewritten rows asserted here are the disclosed divergence, not a correct answer.
     * <p>
     * DATE is the trap in this list and it needs its own multiplier. A DATE column holds
     * MILLISECONDS - {@code MicrosTimestampDriver#fromDate} multiplies by
     * {@code Micros.MILLI_MICROS} on the way in - so the DATE spelling of this backfill is
     * {@code d > 1720468802 * 1000}, which wraps to {@code -1813083696}, and its remedy is
     * {@code * 1000L}. The microsecond remedy {@code * 1000000L} that fixes a TIMESTAMP or a LONG
     * column matches NO row on a DATE column, and the last case asserts that: a "remedy" that
     * silently updates nothing is a worse outcome than the defect it claims to fix, so it is pinned
     * rather than left for a reader to assume.
     * <p>
     * The designated timestamp follows the same wrapped-value rule as these non-designated forms.
     */
    @Test
    public void testTheWrapReachesEveryNonDesignatedSpelling() throws Exception {
        assertMemoryLeak(() -> {
            createReachTable();

            // the headline shape, on a second TIMESTAMP column
            assertUpdateReach("other > 1_720_468_802 * 1_000_000", 2, BOTH_REACH_ROWS_UPDATED);
            // a LONG column widens the wrapped INT exactly as a TIMESTAMP does
            assertUpdateReach("l > 1_720_468_802 * 1_000_000", 2, BOTH_REACH_ROWS_UPDATED);
            // so does a DATE column, in the micros spelling and in the millis one a DATE user writes.
            // The millis spelling wraps to its own constant, and the statement's plan prints it.
            assertUpdateReach("d > 1_720_468_802 * 1_000_000", 2, BOTH_REACH_ROWS_UPDATED);
            assertQuery("UPDATE r SET v = 999 WHERE d > 1_720_468_802 * 1_000")
                    .noLeakCheck()
                    .assertsPlanContaining("Update table: r", "-1813083696<d");
            assertUpdateReach("d > 1_720_468_802 * 1_000", 2, BOTH_REACH_ROWS_UPDATED);
            // the arithmetic one level below the comparison
            assertUpdateReach("other - 1_720_468_802 * 1_000_000 > 0", 2, BOTH_REACH_ROWS_UPDATED);
            // and under a narrowing cast, inside a function argument
            assertUpdateReach("other > to_utc('1720468802'::int * 1_000_000, 'UTC')", 2, BOTH_REACH_ROWS_UPDATED);
            // epoch seconds read off a column wrap once per row, at run time, where no compile-time
            // fold can see them. This is the spelling a backfill actually has, and it is why a guard
            // that refuses only a provable wrap would not close this path.
            assertUpdateReach("other > secs * 1_000_000", 2, BOTH_REACH_ROWS_UPDATED);
            // a bind variable is the same story: the value arrives after compilation, and the
            // product wraps per row. This is the spelling a backfill script has.
            bindVariableService.setInt(0, 1_720_468_802);
            assertUpdateReach("other > $1 * 1_000_000", 2, BOTH_REACH_ROWS_UPDATED);

            // widening one operand is the remedy, in the unit of the column being bounded
            assertUpdateReach("other > 1_720_468_802 * 1_000_000L", 1, TAIL_REACH_ROW_UPDATED);
            assertUpdateReach("l > 1_720_468_802 * 1_000_000L", 1, TAIL_REACH_ROW_UPDATED);
            assertUpdateReach("other > secs * 1_000_000L", 1, TAIL_REACH_ROW_UPDATED);
            assertUpdateReach("d > 1_720_468_802 * 1_000L", 1, TAIL_REACH_ROW_UPDATED);
            // and the micros remedy is NOT the DATE remedy: it matches no row, so a caller who
            // copies it reads "0 rows" and concludes the data was already correct.
            assertUpdateReach("d > 1_720_468_802 * 1_000_000L", 0, NEITHER_REACH_ROW_UPDATED);
        });
    }

    /**
     * The join-flavoured UPDATE, the last spelling the finding's escalation reached. It is the only
     * one where the two modes differ, and the difference does not come from this PR: a non-WAL table
     * follows the wrap like every other spelling, while a WAL table refuses the statement outright
     * because {@code UPDATE ... FROM} is replicated as SQL and cannot be replayed against a second
     * table. That refusal closes this spelling on WAL for a replication reason, not a value-domain
     * one, so it is not a guard and it does not generalise - pinned here so that neither half is
     * mistaken for the other.
     */
    @Test
    public void testTheWrapReachesTheJoinFlavouredUpdate() throws Exception {
        assertMemoryLeak(() -> {
            createSecondTimestampTable();
            final String sql = "UPDATE u SET v = 999 FROM u u2"
                    + " WHERE u.ts = u2.ts AND u.other > 1_720_468_802 * 1_000_000";
            if (walEnabled) {
                assertQuery(sql)
                        .noLeakCheck()
                        .fails(0, "UPDATE statements with join are not supported yet for WAL tables");
                drainWalQueue();
                Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("u")));
                assertQuery("SELECT ts, v FROM u")
                        .noLeakCheck()
                        .expectSize()
                        .timestamp("ts")
                        .returns("ts\tv\n" + FOUR_ROWS_UNCHANGED);
            } else {
                update(sql);
                assertQuery("SELECT ts, v FROM u")
                        .noLeakCheck()
                        .expectSize()
                        .timestamp("ts")
                        .returns("ts\tv\n" + ALL_ROWS_UPDATED);
            }
        });
    }

    /**
     * CONTROL and remedy for
     * {@link #testIntArithmeticBoundOnNonDesignatedTimestampRewritesEveryRowDivergingFrom943()}:
     * widening one operand moves the arithmetic to 64 bits, and the same bound then touches only the
     * tail - which is what released 9.4.3 did without the {@code L}.
     */
    @Test
    public void testWidenedNonDesignatedBoundTouchesOnlyTheTail() throws Exception {
        assertMemoryLeak(() -> {
            createSecondTimestampTable();
            update("UPDATE u SET v = 999 WHERE other > 1_720_468_802 * 1_000_000L");
            if (walEnabled) {
                drainWalQueue();
                Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("u")));
            }
            assertQuery("SELECT ts, v FROM u")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("ts\tv\n" + LAST_ROW_UPDATED);
        });
    }

    // CONTROL: widening one operand keeps the arithmetic at 64 bits, and the statement applies.
    @Test
    public void testWidenedBoundStillApplies() throws Exception {
        assertMemoryLeak(() -> {
            createTargetTable();
            update("UPDATE t SET v = 999 WHERE ts > 1720468802 * 1000000L");
            if (walEnabled) {
                drainWalQueue();
                Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("t")));
            }
            assertQuery("SELECT ts, v FROM t")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("ts\tv\n" + LAST_ROW_UPDATED);
        });
    }

    @Override
    protected void prepareForQueryAssertion() {
        if (walEnabled) {
            drainWalQueue();
        }
    }

    /**
     * Asserts that the statement fails with the same synchronous error in both modes, that it
     * changed nothing, and - on WAL - that it never reached the sequencer and left the table
     * healthy. The sequencer transaction count is the load-bearing part: an error raised after the
     * statement was sequenced would leave the count at 2 and the table suspended.
     */
    private void assertWrappedTimestampUpdateApplies(String updateSql) throws Exception {
        update(updateSql);
        if (walEnabled) {
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("t")));
        }
        assertQuery("SELECT ts, v FROM t")
                .noLeakCheck()
                .expectSize()
                .timestamp("ts")
                .returns("ts\tv\n" + ALL_ROWS_UPDATED);
    }

    /**
     * Runs {@code UPDATE r SET v = 999 WHERE <predicate>} against the reach table and reads the
     * table back, then resets the marker column for the next case. The identical SELECT's count is
     * asserted first: an UPDATE's WHERE clause builds the same filter factory as the SELECT's, and
     * the two agreeing is what makes "preview it with a SELECT" a usable remedy, so it is asserted
     * per spelling rather than assumed once.
     */
    private void assertUpdateReach(String predicate, int previewCount, String expectedRows) throws Exception {
        assertQuery("SELECT count() AS c FROM r WHERE " + predicate)
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .returns("c\n" + previewCount + "\n");
        update("UPDATE r SET v = 999 WHERE " + predicate);
        if (walEnabled) {
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("r")));
        }
        assertQuery("SELECT ts, v FROM r")
                .noLeakCheck()
                .expectSize()
                .timestamp("ts")
                .returns("ts\tv\n" + expectedRows);
        update("UPDATE r SET v = 0");
        if (walEnabled) {
            drainWalQueue();
        }
    }

    /**
     * One row either side of the widened bound {@code 1720468802}, in every column type the wrap
     * reaches. {@code l} is microseconds and {@code d} is milliseconds, which is the whole reason
     * the DATE remedy needs a different multiplier from the other two.
     */
    private void createReachTable() throws Exception {
        execute("CREATE TABLE r (ts TIMESTAMP, other TIMESTAMP, d DATE, l LONG, secs INT, v INT) TIMESTAMP(ts) PARTITION BY DAY"
                + (walEnabled ? " WAL" : ""));
        execute("""
                INSERT INTO r VALUES
                ('2024-07-06T00:00:00.000000Z', '2024-07-06T00:00:00.000000Z', '2024-07-06T00:00:00.000Z', 1_720_224_000_000_000, 1_720_468_802, 0),
                ('2024-07-09T00:00:00.000000Z', '2024-07-09T00:00:00.000000Z', '2024-07-09T00:00:00.000Z', 1_720_483_200_000_000, 1_720_468_802, 0)""");
        if (walEnabled) {
            drainWalQueue();
        }
    }

    private void createSecondTimestampTable() throws Exception {
        execute("CREATE TABLE u (ts TIMESTAMP, other TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY"
                + (walEnabled ? " WAL" : ""));
        execute("""
                INSERT INTO u VALUES
                ('2024-07-06T00:00:00.000000Z', '2024-07-06T00:00:00.000000Z', 1),
                ('2024-07-07T00:00:00.000000Z', '2024-07-07T00:00:00.000000Z', 2),
                ('2024-07-08T00:00:00.000000Z', '2024-07-08T00:00:00.000000Z', 3),
                ('2024-07-09T00:00:00.000000Z', '2024-07-09T00:00:00.000000Z', 4)""");
        if (walEnabled) {
            drainWalQueue();
        }
    }

    private void createTargetTable() throws Exception {
        execute("CREATE TABLE t (ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY"
                + (walEnabled ? " WAL" : ""));
        execute("""
                INSERT INTO t VALUES
                ('2024-07-06T00:00:00.000000Z', 1),
                ('2024-07-07T00:00:00.000000Z', 2),
                ('2024-07-08T00:00:00.000000Z', 3),
                ('2024-07-09T00:00:00.000000Z', 4)""");
        if (walEnabled) {
            drainWalQueue();
        }
    }

    /**
     * Writes the {@code CMD_UPDATE_TABLE} SQL event an older build's {@code UpdateOperation}
     * produced, bypassing the client compiler so the apply path is exercised directly. Idiom from
     * {@code WalUpdateScalarSubqueryTest#sequenceLegacySegmentUpdate}.
     */
    private void sequenceLegacySegmentUpdate(String updateSql) throws Exception {
        final TableToken target = engine.verifyTableName("t");
        try (
                TableRecordMetadata metadata = sqlExecutionContext.getMetadataForWrite(target);
                WalWriter writer = engine.getWalWriter(target)
        ) {
            final UpdateOperation operation = new UpdateOperation(
                    target,
                    metadata.getTableId(),
                    metadata.getMetadataVersion(),
                    0,
                    new ObjList<>("v")
            );
            operation.withSqlStatement(updateSql);
            operation.withContext(sqlExecutionContext);
            writer.apply(operation);
        }
    }
}
