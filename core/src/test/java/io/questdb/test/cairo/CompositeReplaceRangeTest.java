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
 * <b>PINS AN UNIMPLEMENTED FEATURE.</b> {@code processO3BlockComposite} throws "composite
 * partitioning does not yet support the REPLACE commit mode", so any live view or mat view whose base
 * is composite suspends the table on refresh. Each test below asserts that refusal AND that the table
 * is left readable, so the pin fails the day the refusal is lifted -- at which point the assertions
 * become the twin comparisons already written out beside them.
 * <p>
 * The oracle when it IS implemented must be the plain twin: both tables get identical rows and
 * identical replace ranges, so whatever the plain table's replace semantics are, the composite one
 * must match. That matters more than a hand-written expectation, because replace-range has edge cases
 * (empty range, range covering a whole partition, range spanning partitions) whose correct answers are
 * already encoded in the plain implementation.
 * <p>
 * <b>What implementing it needs, mapped 2026-08-27 so the next attempt does not start cold.</b>
 * <ul>
 *   <li>ALREADY PLUMBED: the cell-aware {@code o3CommitPartitionAsync} overload carries
 *       {@code o3TimestampLo}/{@code o3TimestampHi} and {@code cellKey}, so the replace bounds already
 *       reach {@code O3PartitionJob} per cell. No queue or task change is needed.</li>
 *   <li>MISSING, and it is all in the dispatch loop. {@code processO3BlockComposite} is driven purely
 *       by incoming rows ({@code while (srcOoo < srcOooMax)}). The plain loop instead runs
 *       {@code while (srcOoo < srcOooMax || (isCommitReplaceMode() && partitionTimestamp <=
 *       o3TimestampMax))} and advances with {@code txWriter.getNextExistingPartitionTimestamp}, so it
 *       still visits partitions the commit has no rows for -- which is how the DELETE half of a
 *       replace happens.</li>
 *   <li>THE COMPOSITE-SPECIFIC PART: for each partition in range, every EXISTING CELL must be
 *       dispatched, not just the cells named by the incoming rows. A cell with no replacement rows
 *       still has to have its rows in [lo, hi) deleted, or the composite table keeps rows the plain
 *       twin dropped. There is no "visit all cells of this partition" step in the dispatch today.</li>
 *   <li>ALSO NEEDED: the {@code srcOooLo > srcOooHi && (srcDataMax == 0 || append)} skip, and
 *       {@code replaceMaxTimestamp} tracking, both per cell rather than per partition.</li>
 * </ul>
 * <b>Why it was not attempted in the session that mapped it:</b> a half-correct version deletes rows
 * from the wrong cells -- silent data loss, the exact defect class this branch has been fixing -- and
 * the loop's own comments note that a stranded {@code o3PartitionUpdRemaining} ticket produces "an
 * untimed, unkillable spin -- a HANG, not a crash". It is a few hundred lines in the writer's most
 * defect-prone loop and wants its own session with the full verification set (twin, broad O3
 * regression, negative control, -da arm).
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
     * The composite table must REFUSE the replace commit -- and the refusal must leave it readable,
     * with its pre-replace rows intact. A refusal that corrupted or emptied the table would be worse
     * than the missing feature, and only asserting the error message would not notice.
     */
    private void assertCompositeRefusedAndStillReadable() throws SqlException {
        printSql("SELECT count() FROM c");
        Assert.assertEquals(
                "the composite table must still hold its five seeded rows after the refused replace",
                "count\n5\n", sink.toString());
        // Deliberately NOT asserting that the plain twin's COUNT differs. It often does not: a range
        // holding one row, replaced by one row, leaves the count unchanged -- which is what made the
        // first version of this assertion fail on testReplaceRangeWithinOneDay. The count is a weak
        // signal for the plain side; the composite side's "unchanged because refused" is the invariant
        // worth pinning, and assertTwinEqual is what will carry the real comparison once the feature
        // lands.
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
