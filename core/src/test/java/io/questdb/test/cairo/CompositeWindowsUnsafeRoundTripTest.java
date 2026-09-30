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
 * Dimension values that are hazardous as Windows directory names, driven END TO END against a plain
 * twin rather than through the escaper alone.
 * <p>
 * {@code CompositeWindowsUnsafeDimensionTest} pins what {@code putPathSafe} emits. That is necessary
 * and not sufficient: the escaper is called from BOTH sides -- the writer names the directory, the
 * reader looks it up -- and a value that renders differently on the two sides would be written to one
 * directory and read from another. A unit test on one function cannot see that; only a round trip can.
 * <p>
 * The values here are the two the escaper gained handling for, and both are ordinary data rather than
 * exotica: {@code CON}/{@code NUL}/{@code AUX} are real ticker and sensor names that Windows reserves
 * as device names whatever their characters are, and a trailing space is what sloppy ingest produces
 * -- Windows silently strips it, so two distinct values would otherwise share one cell.
 * <p>
 * The plain twin is the oracle throughout: it stores these values with no notion of directories, so
 * any divergence is the composite naming and nothing else.
 * <p>
 * WHAT THIS SUITE DOES NOT PROVE, measured rather than assumed: with the reserved-name and
 * trailing-space escapes REVERTED, all three tests here still pass, while
 * {@code CompositeWindowsUnsafeDimensionTest} goes three red. That is not a weakness to fix -- it is
 * inherent. Linux accepts a directory called {@code CON} and preserves a trailing space, so the
 * hazard simply does not exist on Linux. These tests lock the writer, reader and pruning render sites
 * into AGREEMENT through whatever the escaper emits; the escaper unit test is what pins the escaping
 * itself. Anyone tempted to read this file as Windows coverage should stop here.
 * <p>
 * The actual Windows signal is the {@code macwin} CI check on the pull request. It had not run as of
 * this commit, because the PR is a draft and drafts skip CI -- so the reserved-name and trailing-space
 * behaviour is reasoned from Windows' documented rules, not observed. Marking the PR ready is what
 * turns that reasoning into evidence.
 */
public class CompositeWindowsUnsafeRoundTripTest extends AbstractCairoTest {

    private static final String COLUMNS = "(ts TIMESTAMP, exch SYMBOL, px DOUBLE)";

    /**
     * Reserved device names must round-trip and stay in cells of their own. Asserting the ROWS, not
     * the directory names: what matters to a user is that the value they wrote comes back, and that
     * two different values did not collapse into one cell.
     */
    @Test
    public void testReservedDeviceNamesRoundTripAgainstThePlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            // CONSOLE and COM10 are deliberately alongside CON and COM1: they are NOT reserved, so if
            // the escaper over-matched they would be mangled too, and the twins would still agree --
            // which is why the cell count below is asserted as well.
            insertIntoBoth("('2023-01-01T01:00:00.000000Z','CON',1.0),"
                    + "('2023-01-01T02:00:00.000000Z','NUL',2.0),"
                    + "('2023-01-01T03:00:00.000000Z','AUX',3.0),"
                    + "('2023-01-01T04:00:00.000000Z','COM1',4.0),"
                    + "('2023-01-01T05:00:00.000000Z','CONSOLE',5.0),"
                    + "('2023-01-01T06:00:00.000000Z','COM10',6.0)");

            assertTwins();
            assertQuery("SELECT count_distinct(exch) FROM c")
                    .noLeakCheck().noRandomAccess().expectSize()
                    .returns("count_distinct\n6\n");
        });
    }

    /**
     * A trailing space must not collapse two values onto one cell. {@code 'a'} and {@code 'a '} are
     * distinct SYMBOL values, and Windows strips the trailing space from a directory name -- so
     * without the escape these two would land in the same directory and one value's rows would be
     * served under the other's.
     */
    @Test
    public void testTrailingSpaceRoundTripsAgainstThePlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            insertIntoBoth("('2023-01-01T01:00:00.000000Z','a',1.0),"
                    + "('2023-01-01T02:00:00.000000Z','a ',2.0),"
                    + "('2023-01-01T03:00:00.000000Z','New York',3.0)");

            assertTwins();
            // Three distinct values, so three cells: 'a' and 'a ' must not have merged.
            assertQuery("SELECT count_distinct(exch) FROM c")
                    .noLeakCheck().noRandomAccess().expectSize()
                    .returns("count_distinct\n3\n");
            // And the value itself comes back with its space intact, not trimmed on the way through
            // a directory name.
            assertQuery("SELECT px FROM c WHERE exch = 'a '")
                    .noLeakCheck()
                    .returns("px\n2.0\n");
        });
    }

    /**
     * Cell PRUNING over these values, because pruning re-renders the segment to pick directories --
     * a second render site that has to agree with the writer's. A wrong render here reads zero rows
     * rather than failing, which is the quiet failure mode this feature exists to avoid.
     */
    @Test
    public void testPruningFindsTheEscapedCells() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            insertIntoBoth("('2023-01-01T01:00:00.000000Z','CON',1.0),"
                    + "('2023-01-01T02:00:00.000000Z','b ',2.0),"
                    + "('2023-01-01T03:00:00.000000Z','ok',3.0)");

            assertQuery("SELECT px FROM c WHERE exch = 'CON'")
                    .noLeakCheck()
                    .returns("px\n1.0\n");
            assertQuery("SELECT px FROM c WHERE exch = 'b '")
                    .noLeakCheck()
                    .returns("px\n2.0\n");
        });
    }

    private void assertTwins() throws Exception {
        assertSqlCursors("SELECT * FROM p ORDER BY ts", "SELECT * FROM c ORDER BY ts");
    }

    private void createTwins() throws Exception {
        execute("CREATE TABLE c " + COLUMNS + " TIMESTAMP(ts) PARTITION BY DAY, exch WAL");
        execute("CREATE TABLE p " + COLUMNS + " TIMESTAMP(ts) PARTITION BY DAY WAL");
    }

    private void insertIntoBoth(String values) throws Exception {
        execute("INSERT INTO c VALUES " + values);
        execute("INSERT INTO p VALUES " + values);
        drainWalQueue();
    }
}
