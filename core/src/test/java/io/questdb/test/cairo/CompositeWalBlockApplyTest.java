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

import io.questdb.std.Chars;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Composite writes applied as a BLOCK -- several WAL transactions drained in one go -- against the
 * plain twin.
 * <p>
 * This is the neighbour of the flatten defect (see {@code processO3BlockComposite}'s own doc). That
 * bug needed a WAL segment carrying more than the one transaction being applied, because what went
 * unreconciled was the disagreement between a row's position in the segment and its position in the
 * gathered O3 columns. Draining after every insert -- what almost every test in this suite does --
 * hides that class of defect. A blocked apply is also the shape a loaded server actually runs in:
 * transactions arrive faster than the apply job drains them, so they are applied in batches.
 * <p>
 * THE FINDING THAT MADE THIS CLASS WORTH KEEPING is the last test. A batch is applied as ONE commit,
 * so a batch of per-cell transactions becomes an interleaved MULTI-CELL commit -- which the branch
 * refuses on a table with a var-size column. "Split into per-cell commits", the documented workaround
 * for that refusal, therefore does not survive batching: the writer re-combines exactly what the user
 * split. It fails loudly (suspended table, named error) rather than silently, which is invariant 2
 * doing its job, but anyone relying on the workaround under load needs to know it is conditional on
 * the apply job never batching.
 * <p>
 * That last test is also what keeps the other three honest. "Several transactions applied as one
 * block" is a claim about writer internals that a twin comparison cannot see -- if the queue were
 * drained one transaction at a time the tests would still pass, proving nothing. The refusal can only
 * fire when three SINGLE-cell transactions are combined into one MULTI-cell commit, so it is direct
 * evidence that this fixture does produce a batch.
 */
public class CompositeWalBlockApplyTest extends AbstractCairoTest {

    /**
     * A batch containing an out-of-order transaction into a cell that earlier transactions in the SAME
     * batch populated. Fixed-size columns only, so the batch is not refused.
     */
    @Test
    public void testBatchEndingInAnOutOfOrderTransactionMatchesThePlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createFixedSizeTwins();
            for (String cell : new String[]{"E0", "E1", "E2"}) {
                execute("INSERT INTO c SELECT ts, exch, c_int, c_long FROM src WHERE exch = '" + cell + "'");
                execute("INSERT INTO p SELECT ts, exch, c_int, c_long FROM src WHERE exch = '" + cell + "'");
            }
            // Still no drain -- the out-of-order transaction joins the same block.
            execute("INSERT INTO c SELECT ts, exch, c_int, c_long FROM src WHERE ts < '2023-01-02' AND exch = 'E0' ORDER BY c_int");
            execute("INSERT INTO p SELECT ts, exch, c_int, c_long FROM src WHERE ts < '2023-01-02' AND exch = 'E0' ORDER BY c_int");
            drainWalQueue();

            assertTwins();
        });
    }

    /**
     * A batch applied on top of already-committed data, so the merge reconciles against rows on disk
     * rather than only against rows from within its own block.
     */
    @Test
    public void testBatchOnTopOfCommittedDataMatchesThePlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createFixedSizeTwins();
            execute("INSERT INTO c SELECT ts, exch, c_int, c_long FROM src WHERE exch = 'E0'");
            execute("INSERT INTO p SELECT ts, exch, c_int, c_long FROM src WHERE exch = 'E0'");
            drainWalQueue();

            for (String cell : new String[]{"E1", "E2"}) {
                execute("INSERT INTO c SELECT ts, exch, c_int, c_long FROM src WHERE exch = '" + cell + "'");
                execute("INSERT INTO p SELECT ts, exch, c_int, c_long FROM src WHERE exch = '" + cell + "'");
            }
            execute("INSERT INTO c SELECT ts, exch, c_int, c_long FROM src WHERE ts < '2023-01-03' AND exch = 'E0' ORDER BY c_int");
            execute("INSERT INTO p SELECT ts, exch, c_int, c_long FROM src WHERE ts < '2023-01-03' AND exch = 'E0' ORDER BY c_int");
            drainWalQueue();

            assertTwins();
        });
    }

    /**
     * Every transaction in the batch is in order and single-cell; only the batching is unusual.
     */
    @Test
    public void testInOrderBatchMatchesThePlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createFixedSizeTwins();
            // NO drain between these: they pile up in the sequencer and are applied as one block.
            for (String cell : new String[]{"E0", "E1", "E2"}) {
                execute("INSERT INTO c SELECT ts, exch, c_int, c_long FROM src WHERE exch = '" + cell + "'");
                execute("INSERT INTO p SELECT ts, exch, c_int, c_long FROM src WHERE exch = '" + cell + "'");
            }
            drainWalQueue();

            assertTwins();
        });
    }

    /**
     * The workaround's limit, pinned. Each INSERT here is single-cell -- precisely what the refusal's
     * message tells the user to do -- but they are drained together, so the writer sees one
     * interleaved multi-cell commit and refuses it. The table suspends with the named error and keeps
     * ZERO rows, against the plain twin's full 240.
     * <p>
     * Asserted rather than merely documented because the failure is invisible from the SQL that
     * produced it: nothing about three per-cell INSERTs suggests they will be recombined.
     */
    @Test
    public void testPerCellCommitsBatchedTogetherAreRefusedOnAVarSizeTable() throws Exception {
        assertMemoryLeak(() -> {
            execute(SRC);
            execute("CREATE TABLE c (ts TIMESTAMP, exch SYMBOL, c_int INT, c_varchar VARCHAR)"
                    + " TIMESTAMP(ts) PARTITION BY DAY, exch WAL");
            execute("CREATE TABLE p (ts TIMESTAMP, exch SYMBOL, c_int INT, c_varchar VARCHAR)"
                    + " TIMESTAMP(ts) PARTITION BY DAY WAL");
            for (String cell : new String[]{"E0", "E1", "E2"}) {
                execute("INSERT INTO c SELECT ts, exch, c_int, c_varchar FROM src WHERE exch = '" + cell + "'");
                execute("INSERT INTO p SELECT ts, exch, c_int, c_varchar FROM src WHERE exch = '" + cell + "'");
            }
            drainWalQueue();

            Assert.assertEquals("the plain twin takes the same batch", 240, count("SELECT count() FROM p"));
            Assert.assertEquals("composite kept rows from a refused commit", 0, count("SELECT count() FROM c"));
            assertSuspendedWith("an interleaved multi-cell commit is not yet supported for a table with a var-size column");
        });
    }

    private void assertSuspendedWith(String expectedFragment) throws Exception {
        try (RecordCursorFactory factory = select("SELECT name, suspended, errorMessage FROM wal_tables()");
             RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            while (cursor.hasNext()) {
                final CharSequence name = cursor.getRecord().getStrA(0);
                if (name != null && Chars.equals("c", name)) {
                    Assert.assertTrue("composite table should be suspended", cursor.getRecord().getBool(1));
                    final CharSequence err = cursor.getRecord().getStrA(2);
                    Assert.assertNotNull("suspended without an error message", err);
                    Assert.assertTrue(
                            "unexpected suspension reason: " + err,
                            Chars.contains(err, expectedFragment)
                    );
                    return;
                }
            }
        }
        Assert.fail("composite table not found in wal_tables()");
    }

    private void assertTwins() throws Exception {
        assertSqlCursors("SELECT * FROM p ORDER BY ts, c_int", "SELECT * FROM c ORDER BY ts, c_int");
    }

    private static long count(String sql) throws Exception {
        try (RecordCursorFactory factory = select(sql);
             RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            Assert.assertTrue(cursor.hasNext());
            return cursor.getRecord().getLong(0);
        }
    }

    private void createFixedSizeTwins() throws Exception {
        execute(SRC);
        execute("CREATE TABLE c (ts TIMESTAMP, exch SYMBOL, c_int INT, c_long LONG) TIMESTAMP(ts)"
                + " PARTITION BY DAY, exch WAL");
        execute("CREATE TABLE p (ts TIMESTAMP, exch SYMBOL, c_int INT, c_long LONG) TIMESTAMP(ts)"
                + " PARTITION BY DAY WAL");
    }

    private static final String SRC = "CREATE TABLE src AS (SELECT"
            + " timestamp_sequence(1672531200000000L, 3600000000L) ts,"
            + " rnd_symbol('E0','E1','E2') exch, rnd_int() c_int, rnd_long() c_long,"
            + " rnd_varchar(4, 12, 1) c_varchar"
            + " FROM long_sequence(240)) TIMESTAMP(ts) PARTITION BY DAY";
}
