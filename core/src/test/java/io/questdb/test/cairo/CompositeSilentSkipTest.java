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

import io.questdb.cairo.TableToken;
import io.questdb.griffin.SqlException;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * SP8 Task 10 — the two operations composite SKIPS silently.
 * <p>
 * Composite tables skip the INTERNAL split-fragment squash ({@code TableWriter:18229}) and the
 * INTERNAL symbol-capacity autoscale ({@code TableWriter:17567}), logging each rather than failing.
 * Note the scope: it is the internal, commit-time work that is skipped. The user-issued
 * {@code ALTER TABLE ... SQUASH PARTITIONS} is GATED instead, and its gate fires asynchronously via WAL
 * suspension -- see this file's own test for why that distinction matters. Everywhere else this
 * feature either matches its plain twin or fails loudly, so a silent skip is a deliberate exception
 * — and it is acceptable ONLY if it is provably harmless.
 * <p>
 * These tests assert exactly that: the internal skip leaves the table correct and twin-equal
 * afterwards. Note that "accepted" must be checked as NOT-SUSPENDED, not merely as "execute() did not
 * throw" -- composite gates on a WAL table fire from the WAL-apply job, long after execute() returns. What is NOT asserted is that
 * the underlying work happened — that is the whole point of a skip. Asserting the skip's internal
 * bookkeeping would pin an implementation detail; asserting the table is still right is the contract.
 * <p>
 * If either of these skips is ever implemented for real, these tests should keep passing unchanged —
 * they constrain the outcome, not the mechanism.
 */
public class CompositeSilentSkipTest extends AbstractCairoTest {

    /**
     * Symbol-capacity autoscale is skipped for composite. ALTER must still succeed, and the table
     * must still read exactly like its plain twin afterwards.
     */
    @Test
    public void testSymbolCapacityAutoscaleSkipLeavesTableCorrect() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            insertIntoBothAndDrain(
                    "('2023-01-01T00:00:00.000000Z','A',1.0),('2023-01-01T00:00:01.000000Z','B',2.0)," +
                            "('2023-01-01T00:00:02.000000Z','C',3.0)");
            assertTwinEquivalence(3);

            // Accepted, not gated: this must NOT throw. A composite table skips the autoscale work
            // internally and carries on.
            execute("alter table c alter column exch symbol capacity 1024");
            drainWalQueue();

            assertNotSuspended();
            assertTwinEquivalence(3);

            // ... and the table keeps working afterwards, including routing a brand-new cell.
            insertIntoBothAndDrain(
                    "('2023-01-01T00:00:03.000000Z','D',4.0),('2023-01-01T00:00:04.000000Z','A',5.0)");
            assertTwinEquivalence(5);
        });
    }

    /**
     * Split-fragment squash is skipped for composite. O3 traffic that would normally produce split
     * fragments must still leave the table twin-correct.
     */
    @Test
    public void testSplitFragmentSquashSkipLeavesTableCorrect() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            // Seed a day, then push out-of-order rows into it repeatedly. On a plain table this is
            // the traffic that produces split partitions and later squashes them; composite skips
            // the squash.
            insertIntoBothAndDrain("('2023-01-01T12:00:00.000000Z','A',1.0)");
            assertTwinEquivalence(1);

            insertIntoBothAndDrain("('2023-01-01T06:00:00.000000Z','A',2.0)");
            insertIntoBothAndDrain("('2023-01-01T03:00:00.000000Z','B',3.0)");
            insertIntoBothAndDrain("('2023-01-01T09:00:00.000000Z','B',4.0)");
            insertIntoBothAndDrain("('2023-01-01T01:00:00.000000Z','A',5.0)");
            assertTwinEquivalence(5);

            assertNotSuspended();

            // The INTERNAL split-fragment squash above is silently skipped. An EXPLICIT
            // ALTER TABLE ... SQUASH PARTITIONS is a different thing entirely: it is GATED, and as of
            // sub-project 1B Task 0 the gate fires SYNCHRONOUSLY, at the statement.
            //
            // This assertion has now been wrong in BOTH directions, which is why it is spelled out.
            // First it asserted the statement was "accepted" on the strength of execute() not throwing
            // -- meaningless against an async gate, and green over a suspended table. Then it asserted
            // the async suspension, which was accurate but encoded an invariant-6 violation as the
            // expected behaviour. The refusal now lands where the user typed it.
            final TableToken token = engine.verifyTableName("c");
            try {
                execute("alter table c squash partitions");
                Assert.fail("explicit SQUASH PARTITIONS must be refused at the statement");
            } catch (SqlException expected) {
                TestUtils.assertContains(expected.getFlyweightMessage(), "composite");
                TestUtils.assertContains(expected.getFlyweightMessage(), "SQUASH PARTITIONS");
            }
            drainWalQueue();

            Assert.assertFalse("a statement-time refusal must NOT suspend the table",
                    engine.getTableSequencerAPI().isSuspended(token));

            // The refusal must also be CLEAN: the table is still fully readable and still twin-equal,
            // i.e. the gate rejected before mutating anything. Reads are unaffected by suspension --
            // only WAL application is halted.
            //
            // The RESUME WAL note that used to live here is obsolete: nothing is suspended now, so
            // there is nothing to recover from. That was the whole cost of the async gate.
            assertTwinEquivalence(5);
        });
    }

    private void assertNotSuspended() {
        Assert.assertFalse(
                "composite table must not be suspended by a skipped operation",
                engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("c")));
    }

    private void assertTwinEquivalence(long expectedRows) throws Exception {
        final String expectedCount = "count\n" + expectedRows + "\n";
        assertQuery("select count() from c").noLeakCheck().noRandomAccess().expectSize().returns(expectedCount);
        assertQuery("select count(px) from c").noLeakCheck().noRandomAccess().expectSize().returns(expectedCount);
        assertSqlCursors(
                "select ts, exch, px from p order by ts, exch, px",
                "select ts, exch, px from c order by ts, exch, px");
        assertSqlCursors(
                "select exch, count() from p order by exch",
                "select exch, count() from c order by exch");
    }

    private void createTwins() throws SqlException {
        execute("create table c (ts timestamp, exch symbol, px double) timestamp(ts) partition by day, exch wal");
        execute("create table p (ts timestamp, exch symbol, px double) timestamp(ts) partition by day wal");
    }

    private void insertIntoBothAndDrain(String valuesTuples) throws SqlException {
        execute("insert into c values " + valuesTuples);
        execute("insert into p values " + valuesTuples);
        drainWalQueue();
    }
}
