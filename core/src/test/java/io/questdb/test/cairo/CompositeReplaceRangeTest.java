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
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.std.NumericException;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.griffin.SqlException;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cairo.wal.WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE;

/**
 * REPLACE-range commits on a composite table.
 * <p>
 * A replace-range commit ({@code WAL_DEDUP_MODE_REPLACE_RANGE}) deletes every existing row inside
 * [lo, hi) and substitutes the rows carried by that commit. It is not reachable from ordinary SQL --
 * live-view and materialized-view refresh drive it through
 * {@code WalWriter#commitWithParams} -- which is why it is exercised here at that level.
 * <p>
 * <b>IMPLEMENTED 2026-08-27 (80344978d1).</b> This class used to open "PINS AN UNIMPLEMENTED
 * FEATURE" and describe, at length, what implementing REPLACE would need. It was implemented, the
 * tests below became twin comparisons, and the header was left behind -- along with a
 * {@code assertCompositeRefusedAndStillReadable} helper nothing called any more. Both are removed
 * here (2026-08-31); the implementation notes live in the commit that did the work.
 * <p>
 * What the composite side has to do beyond the plain one, kept because it is the reason these tests
 * exist: within each visited partition, EVERY EXISTING CELL must be dispatched, not only the cells
 * the incoming rows name. A cell with no replacement rows still has to lose its rows in [lo, hi), or
 * the composite table keeps rows its plain twin dropped.
 * <p>
 * <b>OPEN, unproven, found 2026-08-31 while auditing the min/max-recompute defect family.</b>
 * {@code o3ConsumePartitionUpdateSink} ASSIGNS rather than folds when {@code isFirstPartitionReplaced}:
 * {@code txWriter.minTimestamp = timestampMin}. On a composite table the update block is per CELL, so
 * {@code timestampMin} is one cell's minimum and the LAST block consumed wins. Instrumented on two
 * shapes -- min-holder at the higher cellKey, then at the lower -- and both ended CORRECT, because in
 * both the block carrying the day's minimum happened to be consumed last; the trace shows the value
 * transiently wrong in between ({@code timestampMin=11:30, existingMin=08:00} followed by
 * {@code timestampMin=08:00, existingMin=11:30}). So the outcome depends on consumption order rather
 * than on construction. No failing shape was found, so nothing was changed: this branch does not fix
 * without a red test. The fix, if a shape is found, is the one used for the drop paths -- recompute
 * across the day's cells rather than trusting a single block.
 */
public class CompositeReplaceRangeTest extends AbstractCompositeTwinTest {

    /**
     * The range covers rows in ONE cell of one day, leaving the sibling cell untouched. This is the
     * case a cell-blind implementation gets wrong in the most damaging way: deleting the range across
     * every cell rather than only where the replacement rows belong.
     */
    @Test(timeout = 120_000)
    public void testReplaceRangeWithinOneDay() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            seedBothDays();

            // Replace 02:00..04:00 on day 1 with a single new E0 row.
            replaceRangeOnBoth("2023-01-01T03:00:00.000000Z", "E0", 99.0,
                    "2023-01-01T02:00:00.000000Z", "2023-01-01T04:00:00.000000Z");

            assertTwinEqual("");
            assertTwinEqual(" WHERE exch = 'E0'");
            assertTwinEqual(" WHERE exch = 'E1'");
        });
    }

    /**
     * A range spanning BOTH days, so the replace crosses a partition boundary as well as cells.
     */
    @Test(timeout = 120_000)
    public void testReplaceRangeSpanningDays() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            seedBothDays();

            replaceRangeOnBoth("2023-01-01T12:00:00.000000Z", "E1", 77.0,
                    "2023-01-01T00:00:00.000000Z", "2023-01-02T23:59:59.999999Z");

            assertTwinEqual("");
            assertTwinEqual(" WHERE exch = 'E1'");
        });
    }

    /**
     * A replace commit carrying NO rows: the range is deleted and nothing substituted. Exercises the
     * delete half on its own.
     */
    @Test(timeout = 120_000)
    public void testReplaceRangeWithNoRowsDeletesTheRange() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            seedBothDays();

            replaceRangeOnBoth(null, null, 0.0,
                    "2023-01-01T00:00:00.000000Z", "2023-01-01T23:59:59.999999Z");

            assertTwinEqual("");
            assertTwinEqual(" WHERE exch = 'E0'");
        });
    }

    /**
     * Issues the same replace-range commit against both twins. A null {@code tsStr} commits the range
     * with no replacement rows.
     */
    private void replaceRangeOnBoth(String tsStr, String sym, double px, String rangeLo, String rangeHi)
            throws NumericException {
        replaceRangeOn("c", tsStr, sym, px, rangeLo, rangeHi);
        replaceRangeOn("p", tsStr, sym, px, rangeLo, rangeHi);
        drainWalQueue();
        engine.releaseInactive();
    }

    private void replaceRangeOn(String table, String tsStr, String sym, double px, String rangeLo, String rangeHi)
            throws NumericException {
        final TableToken token = engine.verifyTableName(table);
        try (WalWriter ww = engine.getWalWriter(token)) {
            if (tsStr != null) {
                final TableWriter.Row row = ww.newRow(MicrosTimestampDriver.floor(tsStr));
                row.putSym(1, sym);
                row.putDouble(2, px);
                row.append();
            }
            ww.commitWithParams(
                    MicrosTimestampDriver.floor(rangeLo),
                    MicrosTimestampDriver.floor(rangeHi) + 1,
                    WAL_DEDUP_MODE_REPLACE_RANGE
            );
        }
    }

    /**
     * Two days, two cells each, so a range can be scoped to one cell of one day and the untouched
     * siblings are observable.
     */
    private void seedBothDays() throws Exception {
        insertIntoBoth("('2023-01-01T01:00:00.000000Z','E0',1.0),"
                + "('2023-01-01T03:00:00.000000Z','E0',2.0),"
                + "('2023-01-01T05:00:00.000000Z','E1',3.0),"
                + "('2023-01-02T01:00:00.000000Z','E0',4.0),"
                + "('2023-01-02T03:00:00.000000Z','E1',5.0)");
        drainWalQueue();
        engine.releaseInactive();
    }
}
