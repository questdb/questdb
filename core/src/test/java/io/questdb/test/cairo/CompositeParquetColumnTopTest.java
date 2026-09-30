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

import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * CONVERT PARTITION TO PARQUET on a composite table whose cells have DIFFERENT COLUMN TOPS.
 * <p>
 * <b>The condition that makes this visible.</b> A cell-blind column-top read only diverges from a
 * cell-aware one when the cells disagree. Every earlier composite/parquet test added no column after
 * the first insert, so every top was 0 and cell 0's blind answer was coincidentally right for every
 * cell -- they passed while the defect was live. Here BTC has 3 rows and ETH has 1 when {@code tag} is
 * added, so their tops are 3 and 1.
 * <p>
 * MEASURED before the fix, straight after CONVERT and with no other statement in between:
 * <pre>
 *   composite  ...T06 ETH          &lt;- value GONE
 *   plain      ...T06 ETH  E1
 * </pre>
 * {@code TableUtils#produceParquetFromNative} resolved
 * {@code columnVersionReader.getRecordIndex(partitionTimestamp, writerIndex)} -- the cell-BLIND 2-arg
 * form -- and derived BOTH the column top and the column name txn from it, so every cell of a day was
 * encoded using cell 0's metadata. A non-zero cell whose real top was lower had its values encoded as
 * absent, and the read path takes parquet column tops from the FILE's own chunk metadata, so the loss
 * was baked in at conversion time and no later read could recover it.
 * <p>
 * <b>This was originally mis-attributed to ALTER COLUMN TYPE</b>, because that was the statement the
 * failure was first observed after. The "before" snapshot was captured but never asserted. Asserting
 * the precondition -- the data must be correct BEFORE the operation under suspicion -- is what moved
 * the blame from ALTER TYPE to CONVERT.
 */
public class CompositeParquetColumnTopTest extends AbstractCairoTest {

    /**
     * The defect itself: CONVERT alone, nothing else.
     */
    @Test
    public void testConvertPreservesValuesInCellsWithUnevenColumnTops() throws Exception {
        assertMemoryLeak(() -> {
            createUnevenCells("c", ", exch");
            createUnevenCells("p", "");

            execute("ALTER TABLE c CONVERT PARTITION TO PARQUET LIST '2023-10-01'");
            execute("ALTER TABLE p CONVERT PARTITION TO PARQUET LIST '2023-10-01'");
            drainWalQueue();
            assertNotSuspended("c");

            assertMatchesTwin("c", "p");

            // The cells must actually BE parquet -- otherwise this passes for the wrong reason.
            final StringSink parquetCount = new StringSink();
            TestUtils.printSql(engine, sqlExecutionContext,
                    "SELECT count() FROM table_partitions('c') WHERE isParquet = true", parquetCount);
            TestUtils.assertEquals("count\n2\n", parquetCount);
        });
    }

    /**
     * And the round trip back, which must restore the same values.
     */
    @Test
    public void testConvertToParquetAndBackPreservesValues() throws Exception {
        assertMemoryLeak(() -> {
            createUnevenCells("c2", ", exch");
            createUnevenCells("p2", "");

            execute("ALTER TABLE c2 CONVERT PARTITION TO PARQUET LIST '2023-10-01'");
            execute("ALTER TABLE p2 CONVERT PARTITION TO PARQUET LIST '2023-10-01'");
            drainWalQueue();
            execute("ALTER TABLE c2 CONVERT PARTITION TO NATIVE LIST '2023-10-01'");
            execute("ALTER TABLE p2 CONVERT PARTITION TO NATIVE LIST '2023-10-01'");
            drainWalQueue();
            assertNotSuspended("c2");

            assertMatchesTwin("c2", "p2");
        });
    }

    /**
     * ALTER COLUMN TYPE over the converted cells -- the shape the defect was first seen through.
     */
    @Test
    public void testAlterColumnTypeOverParquetCellsPreservesValues() throws Exception {
        assertMemoryLeak(() -> {
            createUnevenCells("c3", ", exch");
            createUnevenCells("p3", "");

            execute("ALTER TABLE c3 CONVERT PARTITION TO PARQUET LIST '2023-10-01'");
            execute("ALTER TABLE p3 CONVERT PARTITION TO PARQUET LIST '2023-10-01'");
            drainWalQueue();

            // PRECONDITION. Without this the assertion below would blame ALTER TYPE for damage done
            // by CONVERT -- which is exactly the mis-attribution that happened the first time.
            assertMatchesTwin("c3", "p3");

            execute("ALTER TABLE c3 ALTER COLUMN tag TYPE VARCHAR");
            execute("ALTER TABLE p3 ALTER COLUMN tag TYPE VARCHAR");
            drainWalQueue();
            assertNotSuspended("c3");

            assertMatchesTwin("c3", "p3");
        });
    }

    /**
     * CONTROL: the same ALTER TYPE on composite cells that were never converted. Establishes that
     * composite ALTER COLUMN TYPE is sound on native cells, so a parquet failure is attributable to
     * the conversion rather than to composite type changes generally.
     */
    @Test
    public void testAlterColumnTypeOnNativeCompositeCells() throws Exception {
        assertMemoryLeak(() -> {
            createUnevenCells("c4", ", exch");
            createUnevenCells("p4", "");
            execute("ALTER TABLE c4 ALTER COLUMN tag TYPE VARCHAR");
            execute("ALTER TABLE p4 ALTER COLUMN tag TYPE VARCHAR");
            drainWalQueue();
            assertMatchesTwin("c4", "p4");
        });
    }

    /**
     * O3 INTO a converted cell, with UNEVEN column tops.
     * <p>
     * The cold-storage fix that made this shape work at all was verified with a table that had no
     * ADD COLUMN, i.e. every column top 0 -- the same vacuous setup that let the CONVERT data loss
     * hide behind four green tests. This re-checks that fix under the condition that actually
     * discriminates.
     */
    @Test
    public void testO3IntoConvertedCellWithUnevenColumnTops() throws Exception {
        assertMemoryLeak(() -> {
            createUnevenCells("c5", ", exch");
            createUnevenCells("p5", "");

            execute("ALTER TABLE c5 CONVERT PARTITION TO PARQUET LIST '2023-10-01'");
            execute("ALTER TABLE p5 CONVERT PARTITION TO PARQUET LIST '2023-10-01'");
            drainWalQueue();
            assertMatchesTwin("c5", "p5");   // precondition

            // O3 into the converted ETH cell, BEFORE its existing rows, carrying a tag value.
            execute("INSERT INTO c5 VALUES ('2023-10-01T00:30:00.000000Z','ETH',9.0,'E0')");
            execute("INSERT INTO p5 VALUES ('2023-10-01T00:30:00.000000Z','ETH',9.0,'E0')");
            drainWalQueue();
            assertNotSuspended("c5");

            assertMatchesTwin("c5", "p5");
            final StringSink count = new StringSink();
            TestUtils.printSql(engine, sqlExecutionContext, "SELECT count() FROM c5", count);
            TestUtils.assertContains(count, "7");
        });
    }

    /**
     * SQUASH over converted cells with uneven tops -- another shape never probed in this condition.
     */
    @Test
    public void testSquashOverParquetCellsWithUnevenColumnTops() throws Exception {
        assertMemoryLeak(() -> {
            createUnevenCells("c6", ", exch");
            createUnevenCells("p6", "");
            execute("ALTER TABLE c6 CONVERT PARTITION TO PARQUET LIST '2023-10-01'");
            execute("ALTER TABLE p6 CONVERT PARTITION TO PARQUET LIST '2023-10-01'");
            drainWalQueue();
            execute("ALTER TABLE c6 SQUASH PARTITIONS");
            execute("ALTER TABLE p6 SQUASH PARTITIONS");
            drainWalQueue();
            assertNotSuspended("c6");
            assertMatchesTwin("c6", "p6");
        });
    }

    /**
     * IN-ORDER append into a cell that has been converted to PARQUET.
     * <p>
     * Composite fast-append writes straight into a cell's NATIVE column files, bypassing the O3
     * machinery entirely. Its eligibility predicates ({@code isCompositeSingleCellFastAppendPossible},
     * {@code isCompositeMultiCellFastAppendPossible}) contain no reference to parquet anywhere -- the
     * whole family has zero parquet awareness -- because a composite table could not hold a parquet
     * cell when that code was written. CONVERT PARTITION TO PARQUET makes it possible now.
     * <p>
     * If fast-append takes a parquet cell, it appends native bytes to a partition the reader decodes
     * as parquet. This asserts the twin instead of guessing which way that fails.
     */
    @Test
    public void testInOrderAppendIntoConvertedCell() throws Exception {
        assertMemoryLeak(() -> {
            createUnevenCells("c_fa", ", exch");
            createUnevenCells("p_fa", "");
            execute("ALTER TABLE c_fa CONVERT PARTITION TO PARQUET LIST '2023-10-01'");
            execute("ALTER TABLE p_fa CONVERT PARTITION TO PARQUET LIST '2023-10-01'");
            drainWalQueue();
            assertMatchesTwin("c_fa", "p_fa");   // precondition

            // STRICTLY IN ORDER -- later than every existing row, same day, same cells. This is the
            // shape fast-append is for.
            execute("INSERT INTO c_fa VALUES ('2023-10-01T20:00:00.000000Z','BTC',20.0,'B9'),"
                    + "('2023-10-01T21:00:00.000000Z','ETH',21.0,'E9')");
            execute("INSERT INTO p_fa VALUES ('2023-10-01T20:00:00.000000Z','BTC',20.0,'B9'),"
                    + "('2023-10-01T21:00:00.000000Z','ETH',21.0,'E9')");
            drainWalQueue();
            assertNotSuspended("c_fa");

            assertMatchesTwin("c_fa", "p_fa");
        });
    }

    private void assertMatchesTwin(String composite, String plain) throws Exception {
        final StringSink c = new StringSink();
        final StringSink p = new StringSink();
        TestUtils.printSql(engine, sqlExecutionContext,
                "SELECT ts, exch, tag FROM " + composite + " ORDER BY ts", c);
        TestUtils.printSql(engine, sqlExecutionContext,
                "SELECT ts, exch, tag FROM " + plain + " ORDER BY ts", p);
        // Non-vacuous: the twin must actually carry the value that goes missing.
        TestUtils.assertContains(p, "E1");
        TestUtils.assertEquals(composite + " differs from the plain twin " + plain, p, c);
    }

    /**
     * Cells with UNEVEN row counts at the moment the column is added, so their column tops differ:
     * BTC gets 3 rows before ADD COLUMN, ETH gets 1.
     */
    private void createUnevenCells(String name, String dimension) throws Exception {
        execute("CREATE TABLE " + name + " (ts TIMESTAMP, exch SYMBOL, px DOUBLE) TIMESTAMP(ts) "
                + "PARTITION BY DAY" + dimension + " WAL");
        execute("INSERT INTO " + name + " VALUES "
                + "('2023-10-01T01:00:00.000000Z','BTC',1.0),"
                + "('2023-10-01T02:00:00.000000Z','BTC',2.0),"
                + "('2023-10-01T03:00:00.000000Z','BTC',3.0),"
                + "('2023-10-01T04:00:00.000000Z','ETH',4.0)");
        drainWalQueue();
        execute("ALTER TABLE " + name + " ADD COLUMN tag SYMBOL");
        drainWalQueue();
        execute("INSERT INTO " + name + " VALUES "
                + "('2023-10-01T05:00:00.000000Z','BTC',5.0,'B1'),"
                + "('2023-10-01T06:00:00.000000Z','ETH',6.0,'E1')");
        drainWalQueue();
    }

    private void assertNotSuspended(String table) {
        Assert.assertFalse(table + " suspended",
                engine.getTableSequencerAPI().isSuspended(engine.verifyTableName(table)));
    }
}
