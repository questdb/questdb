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
import org.junit.Test;

/**
 * DROP INDEX on a COMPOSITE table whose cells have DIFFERENT column tops.
 * <p>
 * {@code DropIndexOperator#executeDropIndex} was made cell-aware for the hard-link paths, but the
 * column-top read two lines below the cellKey resolution is the cellKey-BLIND 3-arg form:
 * <pre>
 *   final int cellKey = tableWriter.getPartitionCellKey(pIndex);
 *   long columnVersion = tableWriter.getColumnNameTxn(pTimestamp, cellKey, columnIndex);  // per-cell
 *   long columnTop     = tableWriter.getColumnTop(pTimestamp, columnIndex, -1);           // cell 0
 *   ...
 *   tableWriter.upsertColumnVersion(pTimestamp, cellKey, columnIndex, columnTop);         // per-cell
 * </pre>
 * The value is read for cell 0 and written into THIS cell's {@code _cv} record. Where cells have
 * different column tops, every cell's top is overwritten with cell 0's.
 * <p>
 * A column top is the row offset within a partition at which a column starts existing, so corrupting
 * it misaligns every subsequent read of that column in that cell -- values appear against the wrong
 * rows, or NULLs appear where data was written. The row COUNT stays right, which is why this test
 * asserts VALUES.
 * <p>
 * The setup below gives the two cells different tops on purpose: BTC has 3 rows and ETH has 1 when
 * ADD COLUMN runs, so BTC's top is 3 and ETH's is 1. A cell-blind read propagates 3 to both.
 */
public class CompositeDropIndexColumnTopTest extends AbstractCairoTest {

    @Test
    public void testDropIndexPreservesPerCellColumnTops() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE c (ts TIMESTAMP, exch SYMBOL, px DOUBLE) TIMESTAMP(ts) "
                    + "PARTITION BY DAY, exch WAL");
            // Same day, UNEVEN row counts per cell -- this is what makes the tops differ.
            execute("INSERT INTO c VALUES "
                    + "('2023-03-01T01:00:00.000000Z','BTC',1.0),"
                    + "('2023-03-01T02:00:00.000000Z','BTC',2.0),"
                    + "('2023-03-01T03:00:00.000000Z','BTC',3.0),"
                    + "('2023-03-01T04:00:00.000000Z','ETH',4.0)");
            drainWalQueue();

            execute("ALTER TABLE c ADD COLUMN tag SYMBOL INDEX");
            drainWalQueue();

            // Rows carrying the new column, into BOTH cells.
            execute("INSERT INTO c VALUES "
                    + "('2023-03-01T05:00:00.000000Z','BTC',5.0,'B1'),"
                    + "('2023-03-01T06:00:00.000000Z','ETH',6.0,'E1'),"
                    + "('2023-03-01T07:00:00.000000Z','ETH',7.0,'E2')");
            drainWalQueue();

            final String expected = "ts\texch\tpx\ttag\n" +
                    "2023-03-01T01:00:00.000000Z\tBTC\t1.0\t\n" +
                    "2023-03-01T02:00:00.000000Z\tBTC\t2.0\t\n" +
                    "2023-03-01T03:00:00.000000Z\tBTC\t3.0\t\n" +
                    "2023-03-01T04:00:00.000000Z\tETH\t4.0\t\n" +
                    "2023-03-01T05:00:00.000000Z\tBTC\t5.0\tB1\n" +
                    "2023-03-01T06:00:00.000000Z\tETH\t6.0\tE1\n" +
                    "2023-03-01T07:00:00.000000Z\tETH\t7.0\tE2\n";

            // Precondition: the data is right BEFORE the drop. Without this the assertion after the
            // drop could be blaming DROP INDEX for damage that was already there.
            assertQuery("select ts, exch, px, tag from c order by ts")
                    .noLeakCheck().expectSize().timestamp("ts").returns(expected);

            execute("ALTER TABLE c ALTER COLUMN tag DROP INDEX");
            drainWalQueue();

            assertQuery("select ts, exch, px, tag from c order by ts")
                    .noLeakCheck().expectSize().timestamp("ts").returns(expected);
        });
    }

    /**
     * POSITIVE CONTROL. The identical sequence on a PLAIN table must be unaffected, so a failure above
     * is attributable to the per-cell resolution rather than to DROP INDEX being broken generally or to
     * ADD COLUMN column tops being mishandled everywhere.
     */
    @Test
    public void testPlainTableDropIndexPreservesColumnTop() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE p (ts TIMESTAMP, exch SYMBOL, px DOUBLE) TIMESTAMP(ts) "
                    + "PARTITION BY DAY WAL");
            execute("INSERT INTO p VALUES "
                    + "('2023-03-01T01:00:00.000000Z','BTC',1.0),"
                    + "('2023-03-01T02:00:00.000000Z','BTC',2.0),"
                    + "('2023-03-01T03:00:00.000000Z','BTC',3.0),"
                    + "('2023-03-01T04:00:00.000000Z','ETH',4.0)");
            drainWalQueue();

            execute("ALTER TABLE p ADD COLUMN tag SYMBOL INDEX");
            drainWalQueue();
            execute("INSERT INTO p VALUES "
                    + "('2023-03-01T05:00:00.000000Z','BTC',5.0,'B1'),"
                    + "('2023-03-01T06:00:00.000000Z','ETH',6.0,'E1')");
            drainWalQueue();

            final String expected = "ts\texch\tpx\ttag\n" +
                    "2023-03-01T01:00:00.000000Z\tBTC\t1.0\t\n" +
                    "2023-03-01T02:00:00.000000Z\tBTC\t2.0\t\n" +
                    "2023-03-01T03:00:00.000000Z\tBTC\t3.0\t\n" +
                    "2023-03-01T04:00:00.000000Z\tETH\t4.0\t\n" +
                    "2023-03-01T05:00:00.000000Z\tBTC\t5.0\tB1\n" +
                    "2023-03-01T06:00:00.000000Z\tETH\t6.0\tE1\n";

            execute("ALTER TABLE p ALTER COLUMN tag DROP INDEX");
            drainWalQueue();

            assertQuery("select ts, exch, px, tag from p order by ts")
                    .noLeakCheck().expectSize().timestamp("ts").returns(expected);
        });
    }
}
