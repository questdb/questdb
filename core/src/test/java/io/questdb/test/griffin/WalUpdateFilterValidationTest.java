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
 * Regression pin for the rule that a WAL {@code UPDATE} which cannot compile must be refused when
 * the client compiles it, not when the WAL apply job re-compiles it.
 * <p>
 * A WAL {@code UPDATE} is compiled twice. The client compile sequences the SQL text; the apply job
 * re-compiles that text against the table and executes it. The two compiles used to disagree,
 * because the client compile has no {@link io.questdb.cairo.TableReader} - it runs against the
 * sequencer metadata so that it can succeed even when WAL apply is behind - and
 * {@code SqlCodeGenerator#generateTableQuery0} returned an empty factory as soon as it saw a null
 * reader, before {@code WhereClauseParser#extract} had looked at the WHERE clause at all. Every
 * error that lives inside intrinsic extraction was therefore unreachable at sequencing time and
 * reachable at apply time, and an apply-time failure suspends the table.
 * <p>
 * {@code UPDATE t SET v = 999 WHERE ts > 1720468802 * 1000000} is the spelling that made this
 * visible: the bound is INT arithmetic, which {@code WhereClauseParser#canCastToTimestamp} does not
 * accept as a timestamp, so extraction raises {@code Invalid date}. On a non-WAL table the caller
 * saw that error. On a WAL table the caller saw success and a row count, the statement went into the
 * WAL, and the apply job suspended the table - stopping ingestion for it - with the error visible
 * only in the log and in {@code wal_tables()}.
 * <p>
 * The cases run twice, once against a non-WAL table and once against a WAL one. Non-WAL is the
 * control: it is where the error has always been synchronous, and it shows what the WAL path is
 * expected to do. The WAL cases additionally assert that nothing was sequenced - a rejection is only
 * worth anything if it happens before the transaction is acknowledged - and that the table is not
 * suspended.
 * <p>
 * The class also pins the BOUNDARY of that rule, which is where review finding F2 landed:
 * {@code UPDATE u SET v = 999 WHERE other > 1720468802 * 1000000} over a NON-designated timestamp
 * compiles, wraps, and rewrites every row.
 * <p>
 * Released 9.4.3 rewrote only the tail there, because PR #4824 gave the INT arithmetic operators a
 * {@code getLong()} that recomputed at 64 bits. The every-row outcome the boundary cases assert is
 * therefore a characterization of a deliberate divergence, not a correct answer and not released
 * behaviour - see {@link IntWidthWrapTest} for the rule that produces it. It is the costliest
 * consequence that rule has on a mutating statement: silent, irreversible, and invisible to a caller
 * who does not run EXPLAIN or the equivalent SELECT first.
 * <p>
 * Nothing refuses it, in either mode, and that is deliberate - {@code NarrowIntArithmetic} guards
 * the three consumers that never show the value they used - a partition filter, a window frame
 * width and a SAMPLE BY interval - and an UPDATE shows it twice over: {@code EXPLAIN UPDATE ...}
 * prints the wrapped bound and the identical SELECT returns the rows the UPDATE will rewrite. Both
 * are asserted, so the justification stays checked rather than remembered.
 * <p>
 * {@link #testLegacySegmentStillSuspendsAtApply()} pins what the fix deliberately does NOT change: a
 * statement sequenced by an older build, still unapplied across an upgrade, is re-compiled at apply
 * and still suspends. Nothing at apply time can turn that into a synchronous error, because the
 * caller is long gone; and {@code ApplyWal2TableJob} refuses to skip a failed {@code UPDATE}
 * (see its {@code cmdType != CMD_UPDATE_TABLE} clause) because the statement was acknowledged when
 * it was sequenced, so skipping it would silently lose acknowledged DML.
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
    public void testIntArithmeticBoundInsideAndChainIsRefusedByTheClient() throws Exception {
        assertMemoryLeak(() -> {
            createTargetTable();
            assertRefusedBeforeSequencing(
                    "UPDATE t SET v = 999 WHERE v > 0 AND ts > 1720468802 * 1000000",
                    53
            );
        });
    }

    // The headline spelling: an epoch-second bound multiplied up to microseconds. The product wraps
    // to an INT, which is not a timestamp bound, so the statement cannot compile in either mode.
    @Test
    public void testIntArithmeticBoundOnDesignatedTimestampIsRefusedByTheClient() throws Exception {
        assertMemoryLeak(() -> {
            createTargetTable();
            assertRefusedBeforeSequencing(
                    "UPDATE t SET v = 999 WHERE ts > 1720468802 * 1000000",
                    43
            );
        });
    }

    /**
     * A segment written by a build from before the client-side rejection existed, left unapplied
     * across the upgrade. The apply job re-compiles it, fails, and suspends - which stays correct:
     * the transaction was acknowledged when it was sequenced, so it can neither be skipped nor
     * reported to its caller.
     * <p>
     * A plain {@code RESUME WAL} therefore does not recover it. {@code OperationExecutor#executeUpdate}
     * deliberately does not mark a failed UPDATE's sequencer transaction committed - the failure may
     * be transient and the statement must not be lost - so the resume retries the same transaction,
     * re-compiles the same text and suspends again. The recovery that does work is
     * {@code RESUME WAL FROM TXN}, which skips the transaction explicitly, and this test pins both
     * halves of that so the operator story stays true. (A failed ALTER behaves the other way round:
     * {@code executeAlter} does mark it committed, so a plain {@code RESUME WAL} clears it.)
     */
    @Test
    public void testLegacySegmentStillSuspendsAtApply() throws Exception {
        Assume.assumeTrue(walEnabled);
        assertMemoryLeak(() -> {
            createTargetTable();
            sequenceLegacySegmentUpdate("UPDATE t SET v = 999 WHERE ts > 1720468802 * 1000000");
            drainWalQueue();
            Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("t")));
            assertQuery("SELECT ts, v FROM t")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("ts\tv\n" + FOUR_ROWS_UNCHANGED);

            execute("ALTER TABLE t RESUME WAL");
            drainWalQueue();
            Assert.assertTrue(
                    "RESUME WAL retries the same transaction, so it suspends again",
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("t"))
            );

            // The seed INSERT is transaction 1 and the legacy UPDATE is transaction 2, so resuming
            // from 3 skips it. Ingestion then works again and the UPDATE is permanently lost - which
            // is why the client-side rejection is the fix and this is only the fallback.
            execute("ALTER TABLE t RESUME WAL FROM TXN 3");
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("t")));
            execute("INSERT INTO t VALUES ('2024-07-10T00:00:00.000000Z', 7)");
            assertQuery("SELECT ts, v FROM t")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("ts\tv\n" + FOUR_ROWS_UNCHANGED
                            + "2024-07-10T00:00:00.000000Z\t7\n");
        });
    }

    /**
     * The boundary of the rule this class pins, and the shape review finding F2 raised. An INT
     * arithmetic bound on a NON-designated timestamp column compiles, wraps, and rewrites every row
     * rather than the tail the statement meant.
     * <p>
     * Released 9.4.3 rewrote only the tail here - {@code 1, 2, 3, 999} - because PR #4824 gave the
     * INT arithmetic operators a {@code getLong()} that recomputed at 64 bits. The
     * {@code 999, 999, 999, 999} asserted below is therefore a characterization of a deliberate,
     * disclosed divergence, not a correct answer and not longstanding behaviour. It is silent,
     * irreversible, and a caller who runs neither EXPLAIN nor the equivalent SELECT gets no signal
     * at all. Whoever reddens this test by restoring 64-bit recomputation is reversing a documented
     * breaking change, not repairing a bug in the pin.
     * <p>
     * It is deliberately not guarded. {@code NarrowIntArithmetic} guards three consumers - a
     * {@code DROP / DETACH / CONVERT PARTITION ... WHERE} clause, a window frame width and a
     * {@code SAMPLE BY} interval - and it guards them because none of the three ever shows the
     * value it used. An UPDATE's bound is shown, and the first two assertions here are that
     * showing, not decoration: the statement's own plan prints the wrapped bound, and the identical
     * SELECT returns exactly the rows the UPDATE goes on to rewrite. They are what justifies leaving
     * this path un-guarded, so if either stops holding this test reddens and the decision has to be
     * taken again rather than inherited.
     * <p>
     * The remedy is {@link #testWidenedNonDesignatedBoundTouchesOnlyTheTail()}: widen an operand. On
     * a DATE column the multiplier differs, because DATE is milliseconds - see
     * {@link #testTheWrapReachesEveryNonDesignatedSpelling()}.
     */
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
     * The designated timestamp is the one spelling that does not compile at all, and
     * {@link #testIntArithmeticBoundOnDesignatedTimestampIsRefusedByTheClient()} pins it. It fails
     * for a reason that pre-dates this branch: {@code WhereClauseParser.canCastToTimestamp} does not
     * list INT, so intrinsic extraction refuses any INT-typed bound on the designated timestamp.
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
    private void assertRefusedBeforeSequencing(String updateSql, int errorPos) throws Exception {
        assertQuery(updateSql)
                .noLeakCheck()
                .fails(errorPos, "Invalid date");
        if (walEnabled) {
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("t")));
            assertQuery("SELECT suspended, writerTxn, sequencerTxn FROM wal_tables() WHERE name = 't'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            suspended\twriterTxn\tsequencerTxn
                            false\t1\t1
                            """);
        }
        assertQuery("SELECT ts, v FROM t")
                .noLeakCheck()
                .expectSize()
                .timestamp("ts")
                .returns("ts\tv\n" + FOUR_ROWS_UNCHANGED);
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
     * produced, bypassing the client compiler that now refuses the statement. Idiom from
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
