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

import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Write-amplification accounting on a composite table.
 * <p>
 * {@code addPhysicallyWrittenRows} feeds two things: the {@code physically_written_rows} counter in
 * {@code table_writer_metrics()}, and the write-amplification histogram {@code ApplyWal2TableJob}
 * records per table ({@code physicalRows / logicalRows}). An O3 merge is where the two diverge --
 * rewriting a partition to insert a handful of rows is exactly what amplification is meant to
 * expose -- so a merge path that failed to report its rewritten rows would make composite tables
 * look free to anything reading those percentiles.
 * <p>
 * This is a REGRESSION LOCK, not the fix for a defect. It exists because the accounting is not
 * visible in the composite dispatch itself: {@code processO3BlockPlain} calls
 * {@code addPhysicallyWrittenRows} directly, but only in its APPEND branch, which
 * {@code processO3BlockComposite} does not have -- composite always takes the async merge path, and
 * the merge's rows are reported from the shared {@code O3PartitionJob} instead. Reading the two
 * dispatch methods side by side therefore suggests a gap that is not there, and the cheap way to
 * keep it that way is to assert the observable rather than re-derive the argument.
 */
public class CompositeWriteMetricsTest extends AbstractCairoTest {

    /**
     * An out-of-order merge into a populated cell reports the rows it physically rewrote.
     * <p>
     * The assertion is a lower bound rather than an exact figure because a composite merge rewrites
     * one CELL where the plain twin rewrites the whole day, so the two legitimately differ. The
     * bound is still discriminating: it is above the incoming row count, so reporting only the
     * inserted rows -- or nothing -- fails it.
     */
    @Test
    public void testO3MergeIntoACellReportsPhysicallyWrittenRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE src AS (SELECT timestamp_sequence(1672531200000000L, 3600000000L) ts,"
                    + " rnd_symbol('E0','E1','E2') exch, rnd_int() c_int FROM long_sequence(240))"
                    + " TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE c (ts TIMESTAMP, exch SYMBOL, c_int INT) TIMESTAMP(ts)"
                    + " PARTITION BY DAY, exch WAL");
            for (String cell : new String[]{"E0", "E1", "E2"}) {
                execute("INSERT INTO c SELECT * FROM src WHERE exch = '" + cell + "'");
                drainWalQueue();
            }

            // Derived, not hardcoded: the generator's RNG stream shifts with the column list, so the
            // number of E0 rows in the first day is not a constant across edits to this fixture.
            final long incoming = countOf("SELECT count() FROM src WHERE ts < '2023-01-02' AND exch = 'E0'");
            Assert.assertTrue("fixture produced no rows to merge", incoming > 0);

            final long before = engine.getMetrics().tableWriterMetrics().getPhysicallyWrittenRows();

            // Out of order, into a cell that already holds rows: a genuine merge, not an append.
            execute("INSERT INTO c SELECT * FROM src WHERE ts < '2023-01-02' AND exch = 'E0'"
                    + " ORDER BY c_int");
            drainWalQueue();

            final long written = engine.getMetrics().tableWriterMetrics().getPhysicallyWrittenRows() - before;

            // The merged cell holds at least the rows the commit carried, and a merge rewrites the
            // cell's existing rows alongside them -- so anything at or below the incoming row count
            // means the rewrite went unreported.
            Assert.assertTrue(
                    "composite O3 merge reported " + written + " physically written rows; a merge that"
                            + " rewrites a populated cell must report more than the " + incoming
                            + " rows it carried",
                    // Measured at the time of writing: 14 rewritten for 7 incoming -- the cell's
                    // existing rows plus the merged-in ones.
                    written > incoming
            );
        });
    }

    private static long countOf(String sql) throws Exception {
        try (RecordCursorFactory factory = select(sql);
             RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            Assert.assertTrue(cursor.hasNext());
            return cursor.getRecord().getLong(0);
        }
    }
}
