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
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * CHARACTERISATION TEST -- pins a KNOWN PRODUCT GAP, not desired behaviour.
 * <p>
 * Adding a POSTING-family index to a routed composite table is accepted by the ALTER statement and
 * then BRICKS the table on a later commit: {@code TableWriter#sealPostingIndexForPartition} refuses
 * on a routed composite (Task 16), but it runs inside the WAL-apply job, so the failure arrives as a
 * SUSPENDED table rather than as an error on the statement that caused it.
 * <p>
 * That is the same defect SHAPE as the FORMAT PARQUET bug fixed earlier in this branch -- successful
 * DDL, broken table -- and it violates the same invariant 6 ("the refusal fires at the statement that
 * caused it"). Every other composite gate refuses at the statement. See
 * {@code CompositeFreshParquetGateTest#testCompositeFormatParquetRefusedAtCreate} for the fixed
 * precedent this one should eventually follow.
 * <p>
 * HOW THIS WAS FOUND: not by inspection. Enrolling {@code ADD COLUMN} in the composite differential
 * fuzz (2026-08-26) failed 5 of 24 sweep seeds, every one of them this suspension. The plan had
 * recorded ADD COLUMN as blocked only by the harness's fixed-shape INSERT; that was true but
 * incomplete, and the second blocker was the product's. See
 * {@code CompositeFuzzRunner#dropPostingIndexAddColumnOps}.
 * <p>
 * WHY IT IS NOT FIXED HERE: the natural statement-time gate needs the table's dimension count, and
 * the WAL-side metadata ({@code TableRecordMetadata}) does not expose {@code getPartitionSpec()} --
 * which is exactly why every existing composite gate opens a {@code TableReader} instead. So the fix
 * is either a reader open on the ALTER path in {@code SqlCompilerImpl#alterTableAddColumn} (covers
 * SQL only, leaving {@code TableWriterAPI#addColumn} callers exposed) or plumbing the spec to the
 * WAL writer (covers both). Choosing between those is a design call, not a drive-by edit.
 * <p>
 * <b>WHEN THE GAP IS FIXED THIS TEST WILL FAIL, BY DESIGN.</b> {@link
 * #testCompositeAddColumnWithPostingIndexSuspendsTableInsteadOfRefusing()} asserts the CURRENT bad
 * behaviour. On fixing, replace its body with the refusal assertion -- the ALTER must throw
 * "composite partitioning does not yet support ..." and the table must remain live and unsuspended.
 */
public class CompositeAddColumnPostingGateTest extends AbstractCairoTest {

    /**
     * POSITIVE CONTROL. Without it the composite assertion is vacuous: it would pass even if the
     * suspension had nothing to do with composite partitioning -- e.g. if this SQL were simply
     * invalid, or if POSTING indexes were broken generally. A plain table taking the identical
     * statements must stay live and queryable.
     */
    @Test
    public void testPlainTableAcceptsPostingIndexAddColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE p (ts TIMESTAMP, exch SYMBOL, px DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO p VALUES "
                    + "('2023-01-01T01:00:00.000000Z','BTC',1.0),"
                    + "('2023-01-01T02:00:00.000000Z','ETH',2.0)");
            drainWalQueue();

            execute("ALTER TABLE p ADD COLUMN extra SYMBOL INDEX TYPE POSTING");
            drainWalQueue();

            // an O3 row, so the commit takes the merge path that reaches the seal
            execute("INSERT INTO p VALUES ('2023-01-01T01:30:00.000000Z','BTC',3.0,'X')");
            drainWalQueue();

            final TableToken token = engine.verifyTableName("p");
            Assert.assertFalse("plain table must not suspend on a POSTING index",
                    engine.getTableSequencerAPI().isSuspended(token));
            final StringSink counted = new StringSink();
            TestUtils.printSql(engine, sqlExecutionContext, "SELECT count() FROM p", counted);
            TestUtils.assertContains(counted, "3");
        });
    }

    /**
     * The gap. ALTER returns OK; the table dies on the next merge commit.
     * <p>
     * Both halves are asserted deliberately. Asserting only the suspension would leave it ambiguous
     * whether the statement had ALSO failed -- and "the statement refused it" is precisely the
     * behaviour this test exists to record as ABSENT.
     */
    @Test
    public void testCompositeAddColumnWithPostingIndexSuspendsTableInsteadOfRefusing() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE c (ts TIMESTAMP, exch SYMBOL, px DOUBLE) TIMESTAMP(ts) "
                    + "PARTITION BY DAY, exch WAL");
            // two distinct dimension values => genuinely ROUTED, not dormant. The gate exempts
            // dormant composite tables by design, so a single-cell table would not reach it and this
            // test would be a false green.
            execute("INSERT INTO c VALUES "
                    + "('2023-01-01T01:00:00.000000Z','BTC',1.0),"
                    + "('2023-01-01T02:00:00.000000Z','ETH',2.0)");
            drainWalQueue();

            final TableToken token = engine.verifyTableName("c");
            Assert.assertFalse("precondition: table healthy before the ALTER",
                    engine.getTableSequencerAPI().isSuspended(token));

            // HALF ONE: the statement is accepted. This is the bug -- it should refuse here.
            execute("ALTER TABLE c ADD COLUMN extra SYMBOL INDEX TYPE POSTING");
            drainWalQueue();

            // HALF TWO: a later merge commit reaches the seal and suspends the table.
            execute("INSERT INTO c VALUES ('2023-01-01T01:30:00.000000Z','BTC',3.0,'X')");
            drainWalQueue();

            Assert.assertTrue(
                    "EXPECTED-FAILURE PIN: the composite table is suspended by the POSTING seal gate. "
                            + "If this assertion fails, the gap may have been FIXED -- check whether the "
                            + "ALTER now refuses at the statement, and if so replace this test body with "
                            + "the refusal assertion described in the class javadoc.",
                    engine.getTableSequencerAPI().isSuspended(token));

            final StringSink err = new StringSink();
            TestUtils.printSql(engine, sqlExecutionContext,
                    "select errorMessage from wal_tables() where name = 'c'", err);
            TestUtils.assertContains(err, "composite partitioning does not yet support a POSTING index seal");
        });
    }
}
