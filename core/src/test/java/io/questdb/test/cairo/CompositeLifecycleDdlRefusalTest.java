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

package io.questdb.test.cairo;

import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Invariant 6 for the SIX partition-lifecycle DDL operations: a refusal must fire at the statement
 * that caused it.
 * <p>
 * Every one of these gates lives in {@code TableWriter}, which on a WAL table is the <b>apply</b>
 * side, not the statement side. Measured 2026-08-18 for {@code DROP PARTITION}: the statement
 * SUCCEEDS, the apply job then throws, and the table is SUSPENDED —
 * <pre>
 * C ApplyWal2TableJob job failed, table suspended [table=c~1, seqTxn=2,
 *   error=... composite partitioning does not yet support DROP PARTITION [table=c]]
 * </pre>
 * — so a user who types a currently-unsupported lifecycle DDL gets a suspended table rather than an
 * error message, and the statement they typed appears to have worked.
 * <p>
 * This is the same defect class wave 0 was created to close. Wave 0 fixed {@code SET FORMAT PARQUET}
 * and the O3 purge and did not reach these six, because the closure index tracks them as
 * <i>capability</i> gaps owned by sub-project 1 rather than as invariant-6 violations. They are both.
 * <p>
 * <b>What each test asserts, and why "it threw" is not enough:</b> the async behaviour also throws —
 * later, on another thread, after suspending the table. So each test asserts the refusal AND that the
 * table is still usable afterwards. Only the second half distinguishes a synchronous refusal from a
 * suspension.
 */
public class CompositeLifecycleDdlRefusalTest extends AbstractCairoTest {

    @Test
    public void testAttachPartitionRefusesAtTheStatement() throws Exception {
        assertRefusedAtStatement("ALTER TABLE c ATTACH PARTITION LIST '2023-01-01'");
    }

    /*
     * The DETACH PARTITION refusal test that stood here is gone: sub-project 1 made DETACH cell-aware
     * once 1E unblocked it (detach calls squash internally, so it could not be reached before). A
     * composite day detaches as a container holding its cells -- see CompositeDetachAttachTest. ATTACH
     * remains refused, and its test below still guards that.
     */

    /*
     * The DROP refusal tests that stood here are gone. Whole-day DROP became supported in 1B and
     * per-cell DROP in 1C, so there is no DROP shape left for this suite to guard.
     *
     * What remains here -- ATTACH, DETACH, SQUASH -- is now the whole of it. Worth stating because
     * this suite has shrunk from six operations to three over one session, and every removal was a
     * capability landing rather than a test being weakened.
     */

    /*
     * The FORCE DROP refusal test that stood here is gone -- sub-project 1D made it cell-aware.
     *
     * Worth recording what it was FOR: when this suite was written, FORCE DROP was the only one of the
     * six lifecycle DDLs that already refused synchronously, which made it the POSITIVE CONTROL. The
     * same assertion passed for it unchanged while failing for the other five, proving those five
     * failures were real rather than an artifact of how the suite asserts. It served that purpose, and
     * the remaining tests here (ATTACH, DETACH, SQUASH) still guard the synchronous-refusal contract
     * that 1B Task 0 established for them.
     */

    /*
     * The SQUASH PARTITIONS refusal test that stood here is gone, for the same reason as SET TTL
     * below: sub-project 1E made split-fragment squash cell-aware for mid-table day groups, so the
     * statement is no longer refused on a composite table. What squash does on an ACTIVE-TAIL day
     * group is skip (logged), not refuse -- so there is no statement-level contract left to assert
     * here. The behaviour is covered by CompositeSquashTest instead.
     */

    /*
     * The SET TTL refusal test that stood here is gone: sub-project 1D made TTL eviction cell-aware,
     * so SET TTL is no longer refused on a composite table. It was the worst of the six for
     * invariant 6 -- evaluated at every COMMIT rather than at its own DDL, so a composite table
     * accepted the TTL and then suspended on the next ordinary INSERT. Both halves are now fixed: the
     * refusal became synchronous in 1B Task 0, and 1D removed the need for it entirely.
     * Eviction correctness lives in CompositeTtlAndForceDropTest.
     */

    /**
     * Issues {@code ddl} against a routed composite table and requires BOTH:
     * <ol>
     *     <li>it is refused — the statement itself raises the error;</li>
     *     <li>the table is still usable afterwards, i.e. NOT suspended.</li>
     * </ol>
     * The second is the load-bearing half. Asserting only the first would pass against today's async
     * behaviour, which also raises an error — just on the apply thread, after suspending the table.
     */
    private void assertRefusedAtStatement(String ddl) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE c (ts TIMESTAMP, exch SYMBOL, px DOUBLE)"
                    + " TIMESTAMP(ts) PARTITION BY DAY, exch LAYOUT PLAIN WAL");
            execute("INSERT INTO c VALUES ('2023-01-01T01:00:00.000000Z','E0',1.0),"
                    + "('2023-01-01T05:00:00.000000Z','E1',5.0)");
            drainWalQueue();

            boolean refused = false;
            try {
                execute(ddl);
            } catch (Throwable expected) {
                refused = true;
                TestUtilsBridge.assertMentionsComposite(expected);
            }
            // the apply job would suspend the table here if the gate is on the writer side
            drainWalQueue();

            Assert.assertTrue("statement was accepted rather than refused: " + ddl, refused);
            Assert.assertFalse("table was SUSPENDED by " + ddl + " -- the refusal fired on the apply"
                    + " thread, not at the statement (invariant 6)", isSuspended());

            // and it must still be usable: a refused statement must leave the table working
            execute("INSERT INTO c VALUES ('2023-01-01T09:00:00.000000Z','E2',9.0)");
            drainWalQueue();
            assertQuery("select count() from c").noLeakCheck().noRandomAccess().expectSize()
                    .returns("count\n3\n");
        });
    }

    private boolean isSuspended() throws Exception {
        final io.questdb.std.str.StringSink sink = new io.questdb.std.str.StringSink();
        printSql("select suspended from wal_tables() where name = 'c'", sink);
        return sink.toString().contains("true");
    }

    /**
     * Keeps the assertion about the error text in one place; the six operations use six different
     * messages but all name composite partitioning.
     */
    private static final class TestUtilsBridge {
        static void assertMentionsComposite(Throwable t) {
            final String msg = t.getMessage() == null ? "" : t.getMessage();
            Assert.assertTrue("refusal should name composite partitioning, got: " + msg,
                    msg.contains("composite partitioning"));
        }
    }
}
