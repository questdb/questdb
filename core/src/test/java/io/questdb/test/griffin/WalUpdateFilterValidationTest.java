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
 * {@link #testLegacySegmentStillSuspendsAtApply()} pins what the fix deliberately does NOT change: a
 * statement sequenced by an older build, still unapplied across an upgrade, is re-compiled at apply
 * and still suspends. Nothing at apply time can turn that into a synchronous error, because the
 * caller is long gone; and {@code ApplyWal2TableJob} refuses to skip a failed {@code UPDATE}
 * (see its {@code cmdType != CMD_UPDATE_TABLE} clause) because the statement was acknowledged when
 * it was sequenced, so skipping it would silently lose acknowledged DML.
 */
@RunWith(Parameterized.class)
public class WalUpdateFilterValidationTest extends AbstractCairoTest {
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
