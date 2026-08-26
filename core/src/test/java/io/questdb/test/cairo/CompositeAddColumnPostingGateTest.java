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
 * REGRESSION LOCK. Adding a POSTING-family index to a composite table must be refused AT THE
 * STATEMENT (invariant 6), not accepted and then bricked.
 * <p>
 * MEASURED before this was fixed: the ALTER returned OK and the table SUSPENDED on the next merge
 * commit, when {@code TableWriter#sealPostingIndexForPartition} refused -- that seal is cell-blind and
 * cannot handle a routed composite's per-cell chains (Task 16). A successful DDL and a broken table:
 * the same defect SHAPE as the FORMAT PARQUET bug fixed earlier on this branch.
 * <p>
 * HOW IT WAS FOUND: not by inspection. Enrolling {@code ADD COLUMN} in the composite differential
 * fuzz surfaced it as one of the seeds that suspended the subject twin.
 * <p>
 * SCOPE, and why it is not narrower: the refusal fires for ANY composite table, not only a routed
 * one, matching what {@code CompositeFreshParquetGateTest} does for FORMAT PARQUET. A dormant
 * composite table accepting a POSTING index works only until a second cell routes, at which point it
 * bricks -- so refusing up front prevents planting the landmine rather than waiting for it to go off.
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
     * The lock. The statement must fail, and the table must remain live and queryable afterwards --
     * both halves matter, since a refusal that still damaged the table would be no better than the
     * bug it replaced.
     */
    @Test
    public void testCompositeAddColumnWithPostingIndexIsRefusedAtTheStatement() throws Exception {
        assertMemoryLeak(() -> {
            final TableToken token = createRoutedComposite();
            try {
                execute("ALTER TABLE c ADD COLUMN extra SYMBOL INDEX TYPE POSTING");
                Assert.fail("a POSTING index must be refused on a composite table");
            } catch (Exception e) {
                TestUtils.assertContains(e.getMessage(),
                        "composite partitioning does not yet support a POSTING index");
            }

            // the refusal is at the statement, so the table is untouched and still usable
            Assert.assertFalse("a refused ALTER must not suspend the table",
                    engine.getTableSequencerAPI().isSuspended(token));
            execute("INSERT INTO c VALUES ('2023-01-01T03:00:00.000000Z','BTC',4.0)");
            drainWalQueue();
            Assert.assertFalse("the table must still accept writes after the refusal",
                    engine.getTableSequencerAPI().isSuspended(token));

            final StringSink sink = new StringSink();
            TestUtils.printSql(engine, sqlExecutionContext, "SELECT count() FROM c", sink);
            TestUtils.assertContains(sink, "3");
        });
    }

    private TableToken createRoutedComposite() throws Exception {
        execute("CREATE TABLE c (ts TIMESTAMP, exch SYMBOL, px DOUBLE) TIMESTAMP(ts) "
                + "PARTITION BY DAY, exch WAL");
        // two distinct dimension values => genuinely routed, not dormant
        execute("INSERT INTO c VALUES "
                + "('2023-01-01T01:00:00.000000Z','BTC',1.0),"
                + "('2023-01-01T02:00:00.000000Z','ETH',2.0)");
        drainWalQueue();
        return engine.verifyTableName("c");
    }
}
