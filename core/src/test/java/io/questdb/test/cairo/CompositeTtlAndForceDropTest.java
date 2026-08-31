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

import io.questdb.griffin.SqlException;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Sub-project 1D: do TTL eviction and {@code FORCE DROP PARTITION} already work on a composite table,
 * now that 1B made the shared removal machinery cell-correct?
 * <p>
 * Both funnel through {@code dropPartitionByExactTimestamp} — {@code enforceTtl} calls it twice,
 * {@code forceRemovePartitions} is FORCE DROP's entry point — and 1B fixed that method's two
 * cellKey-0 assumptions (the index lookup that returned -1 once cell 0 was gone, and the removal that
 * always targeted cell 0). The hypothesis is that both operations inherited the fix and need only
 * their gates removed. These tests exist to TEST that rather than assume it.
 * <p>
 * Every test carries a timeout: the loop these operations share is the one that spun 34,314,283 times
 * before 1B fixed it, and a regression there wedges CI rather than failing it.
 */
public class CompositeTtlAndForceDropTest extends AbstractCompositeTwinTest {

    /**
     * FORCE DROP of the FIRST day must recompute the table minimum across the surviving day's cells.
     * <p>
     * {@code forceRemovePartitions} is the third site of the cellKey-order defect found on 2026-08-31
     * (the others being {@code dropPartitionByExactTimestamp} and {@code removePartitionCell}). Its
     * recompute reads a SINGLE partition record, and on a composite table records are ordered
     * (timestamp ASC, cellKey ASC) while a cell's timestamps are unrelated to its cellKey -- that is
     * assigned in the order dimension VALUES first arrived. Here day 2's lowest cellKey (E0) holds
     * 20:00 while a higher one (E1) holds 08:00.
     * <p>
     * A minimum that lands too HIGH is silent: {@code cullIntervals} discards every interval below it,
     * so timestamp-FILTERED reads lose rows while counts and unfiltered scans stay correct. The twin
     * comparison with an explicit WHERE is what catches it; the unfiltered one does not.
     * <p>
     * The plain arm of that recompute is deliberately left as upstream wrote it, so on this shape the
     * plain twin is the oracle for the composite side alone.
     * <p>
     * <b>THREE cells, and that is load-bearing.</b> With two, the upstream single-record read lands on
     * index 1 -- which with only two survivors IS the sibling holding the minimum, so the defect hides.
     * Verified by mutation: the two-cell form of this test passes against the unfixed writer, the
     * three-cell form fails. Day 2's cellKeys ascend 0,1,2 while its minima DESCEND 20:00, 18:00,
     * 08:00, so neither record 0 nor record 1 is the answer.
     */
    @Test(timeout = 60_000)
    public void testForceDropFirstDayRecomputesMinAcrossSiblingCells() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            insertIntoBoth("('2023-01-01T10:00:00.000000Z','E0',1.0),"
                    + "('2023-01-01T11:00:00.000000Z','E1',2.0),"
                    + "('2023-01-01T12:00:00.000000Z','E2',3.0)");
            drainWalQueue();
            insertIntoBoth("('2023-01-02T20:00:00.000000Z','E0',4.0),"
                    + "('2023-01-02T18:00:00.000000Z','E1',5.0),"
                    + "('2023-01-02T08:00:00.000000Z','E2',6.0)");
            drainWalQueue();

            execute("ALTER TABLE c FORCE DROP PARTITION LIST '2023-01-01'");
            execute("ALTER TABLE p FORCE DROP PARTITION LIST '2023-01-01'");
            drainWalQueue();

            assertTwinEqual(" WHERE ts >= '2023-01-02T08:00:00.000000Z' AND ts < '2023-01-02T09:00:00.000000Z'");
            assertTwinEqual("");
        });
    }

    /**
     * FORCE DROP of a whole day must match the plain twin.
     */
    @Test(timeout = 60_000)
    public void testForceDropWholeDayMatchesPlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            seedThreeMultiCellDays();

            execute("ALTER TABLE c FORCE DROP PARTITION LIST '2023-01-02'");
            execute("ALTER TABLE p FORCE DROP PARTITION LIST '2023-01-02'");
            drainWalQueue();

            assertTwinEqual("");
        });
    }

    /**
     * FORCE DROP exists to bypass SAFETY CHECKS. It does not bypass the addressing rule, and this is
     * asserted rather than inherited from {@code DROP}: if FORCE DROP reaches a different parse path,
     * it needs its own guard. 1B measured that naming one cell in a {@code DROP} destroyed the whole
     * day; the same shape must not be reachable here.
     */
    @Test(timeout = 60_000)
    public void testForceDropIndividualCellIsRefused() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            seedThreeMultiCellDays();

            // MEASURED: this shape dies in the DATE PARSER, not at a composite gate --
            // "'yyyy-MM-dd' expected, found [ts=2023-01-02/E0]". So FORCE DROP's LIST parser already
            // makes a cell-qualified name unreachable, and it needs no equivalent of 1B's
            // refuseCellQualifiedPartitionName. Note the asymmetry: DROP's LIST parser ACCEPTS
            // <day>/<cell>, which is exactly why 1B had to add an explicit guard there.
            try {
                execute("ALTER TABLE c FORCE DROP PARTITION LIST '2023-01-02/E0'");
                Assert.fail("FORCE DROP must not be able to drop an individual cell either");
            } catch (SqlException expected) {
                TestUtils.assertContains(expected.getFlyweightMessage(), "'yyyy-MM-dd' expected");
            }
            drainWalQueue();

            Assert.assertEquals("a refused statement must remove nothing", 3,
                    cellCount("c", "2023-01-02"));
        });
    }

    /**
     * TTL eviction, exercised through the path that actually evicts.
     * <p>
     * TTL is evaluated at every COMMIT, not at its own DDL — that is why 1B Task 0 found {@code SET
     * TTL} suspending composite tables on an ordinary {@code INSERT}. So this test sets the TTL and
     * then INSERTS to trigger eviction. A test that only ran {@code ALTER TABLE … SET TTL} would prove
     * nothing about the evicting path.
     * <p>
     * The plain twin is the anti-vacuity control: it must actually LOSE partitions, otherwise the
     * workload never triggered eviction and any agreement between the twins is meaningless.
     */
    @Test(timeout = 60_000)
    public void testTtlEvictsWholeDaysMatchingPlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            seedThreeMultiCellDays();

            final int plainDaysBefore = dayCount("p");
            execute("ALTER TABLE c SET TTL 1 DAY");
            execute("ALTER TABLE p SET TTL 1 DAY");
            drainWalQueue();

            // eviction happens on COMMIT, not on the DDL above
            insertIntoBoth("('2023-01-05T01:00:00.000000Z','E0',50.0)");
            drainWalQueue();

            final int plainDaysAfter = dayCount("p");
            Assert.assertTrue("control failed: the plain twin evicted nothing, so this workload does"
                            + " not exercise TTL at all (before=" + plainDaysBefore
                            + ", after=" + plainDaysAfter + ')',
                    plainDaysAfter < plainDaysBefore);

            assertTwinEqual("");
        });
    }

    /**
     * Cell subdirectory count for one day of a composite table.
     */
    private int cellCount(String table, String dayDir) throws Exception {
        final java.nio.file.Path day = tableDir(table).resolve(dayDir);
        if (!java.nio.file.Files.isDirectory(day)) {
            return 0;
        }
        try (java.util.stream.Stream<java.nio.file.Path> s = java.nio.file.Files.list(day)) {
            return (int) s.filter(java.nio.file.Files::isDirectory).count();
        }
    }

    private int dayCount(String table) throws Exception {
        try (java.util.stream.Stream<java.nio.file.Path> s = java.nio.file.Files.list(tableDir(table))) {
            return (int) s.filter(java.nio.file.Files::isDirectory)
                    .filter(p -> p.getFileName().toString().startsWith("2023-"))
                    .count();
        }
    }

    private void seedThreeMultiCellDays() throws Exception {
        final StringBuilder sb = new StringBuilder();
        for (int day = 1; day <= 3; day++) {
            for (int cell = 0; cell <= 2; cell++) {
                if (sb.length() > 0) {
                    sb.append(',');
                }
                sb.append("('2023-01-0").append(day).append('T').append(String.format("%02d", 1 + cell * 4))
                        .append(":00:00.000000Z','E").append(cell).append("',")
                        .append(day * 10 + cell).append(".0)");
            }
        }
        insertIntoBoth(sb.toString());
        drainWalQueue();
    }

    private java.nio.file.Path tableDir(String table) throws Exception {
        final java.nio.file.Path root = java.nio.file.Paths.get(configuration.getDbRoot());
        try (java.util.stream.Stream<java.nio.file.Path> s = java.nio.file.Files.list(root)) {
            return s.filter(java.nio.file.Files::isDirectory)
                    .filter(p -> p.getFileName().toString().startsWith(table + "~"))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("no table directory for " + table));
        }
    }
}
