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
 * Interleaved MULTI-CELL commits on a table with var-size columns, against the plain twin.
 * <p>
 * This shape was refused outright until the per-cell scratch gather learned to handle a var-size
 * column ({@code buildCompositeCellGroupScratch}). The refusal was the branch's sharpest limit, and
 * worse than a missing feature: the documented workaround was to split into per-cell commits, but
 * {@code ApplyWal2TableJob} applies a batch of transactions as ONE commit, so under load it
 * recombined those per-cell commits into exactly the interleaved commit being refused and suspended
 * the table. {@link CompositeWalBlockApplyTest} covers that direction; this class covers the
 * straightforward one -- a single INSERT whose rows genuinely interleave across cells.
 * <p>
 * "Interleaved" is the load-bearing word: the rows must alternate between cells WITHIN one commit, so
 * the writer has to regroup them per cell rather than dispatch the range whole. Ordering the insert by
 * timestamp with several symbols present is what produces that; a per-cell INSERT would take the
 * single-cell path and prove nothing.
 */
public class CompositeInterleavedVarSizeTest extends AbstractCairoTest {

    private static final String COLUMNS = "(ts TIMESTAMP, exch SYMBOL, c_int INT,"
            + " c_string STRING, c_varchar VARCHAR, c_binary BINARY, c_array DOUBLE[])";

    private static final String GENERATOR = "SELECT"
            + " timestamp_sequence(1672531200000000L, 3600000000L) ts,"
            + " rnd_symbol('E0','E1','E2') exch,"
            + " rnd_int() c_int,"
            + " rnd_str(4, 12, 1) c_string,"
            + " rnd_varchar(4, 12, 1) c_varchar,"
            + " rnd_bin(4, 16, 1) c_binary,"
            + " rnd_double_array(1, 0) c_array"
            + " FROM long_sequence(240)";

    /**
     * Every var-size type in one interleaved commit. STRING, VARCHAR, BINARY and ARRAY each have their
     * own aux-entry format, which is precisely why a width-agnostic gather could not serve them and why
     * a per-driver gather has to be exercised on all four rather than one representative.
     */
    @Test
    public void testInterleavedCommitWithEveryVarSizeTypeMatchesThePlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            // ONE commit, rows in timestamp order, so consecutive rows land in different cells.
            execute("INSERT INTO c SELECT * FROM src");
            execute("INSERT INTO p SELECT * FROM src");
            drainWalQueue();

            assertTwins("after a single interleaved multi-cell commit");
        });
    }

    /**
     * An interleaved commit that MERGES into cells which already hold rows, so each group is a genuine
     * O3 merge rather than an append into an empty cell. This is the shape that exercises the gathered
     * scratch through the native merge path, where a wrong row id is silent rather than loud.
     */
    @Test
    public void testInterleavedO3MergeIntoPopulatedCellsMatchesThePlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            createTwins();
            execute("INSERT INTO c SELECT * FROM src WHERE ts >= '2023-01-04'");
            execute("INSERT INTO p SELECT * FROM src WHERE ts >= '2023-01-04'");
            drainWalQueue();

            // Out of order AND interleaved: earlier days, every cell, one commit.
            execute("INSERT INTO c SELECT * FROM src WHERE ts < '2023-01-04' ORDER BY c_int");
            execute("INSERT INTO p SELECT * FROM src WHERE ts < '2023-01-04' ORDER BY c_int");
            drainWalQueue();

            assertTwins("after an interleaved out-of-order merge into populated cells");
        });
    }

    /**
     * The same interleaved commit with DEDUP enabled, which routes even a single-cell range through the
     * scratch builder, so it is the path most likely to be reached in production by a table that dedups.
     */
    @Test
    public void testInterleavedCommitUnderDedupMatchesThePlainTwin() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE src AS (" + GENERATOR + ") TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE c " + COLUMNS + " TIMESTAMP(ts) PARTITION BY DAY, exch WAL"
                    + " DEDUP UPSERT KEYS(ts, exch)");
            execute("CREATE TABLE p " + COLUMNS + " TIMESTAMP(ts) PARTITION BY DAY WAL"
                    + " DEDUP UPSERT KEYS(ts, exch)");

            execute("INSERT INTO c SELECT * FROM src");
            execute("INSERT INTO p SELECT * FROM src");
            drainWalQueue();

            // Same rows again: every one is a duplicate on (ts, exch), so the dedup merge runs over an
            // interleaved batch.
            execute("INSERT INTO c SELECT * FROM src");
            execute("INSERT INTO p SELECT * FROM src");
            drainWalQueue();

            assertTwins("after an interleaved dedup upsert");
        });
    }

    private void assertTwins(String when) throws Exception {
        try {
            assertSqlCursors("SELECT * FROM p ORDER BY ts, c_int", "SELECT * FROM c ORDER BY ts, c_int");
        } catch (AssertionError e) {
            throw new AssertionError("composite and plain twins diverged " + when, e);
        }
    }

    private void createTwins() throws Exception {
        execute("CREATE TABLE src AS (" + GENERATOR + ") TIMESTAMP(ts) PARTITION BY DAY");
        execute("CREATE TABLE c " + COLUMNS + " TIMESTAMP(ts) PARTITION BY DAY, exch WAL");
        execute("CREATE TABLE p " + COLUMNS + " TIMESTAMP(ts) PARTITION BY DAY WAL");
    }
}
