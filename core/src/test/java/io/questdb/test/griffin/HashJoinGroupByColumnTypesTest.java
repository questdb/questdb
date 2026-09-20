/*+*****************************************************************************
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

package io.questdb.test.griffin;

import io.questdb.cairo.SqlJitMode;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

import static io.questdb.test.griffin.HashJoinGroupByQualificationTest.assertDifferential;
import static io.questdb.test.griffin.HashJoinGroupByQualificationTest.context;

/**
 * Column types the fused plan reads outside the narrow set the build's row heap copies. A probe
 * column never reaches that heap - the reducer reads it straight off the page frame - so a probe
 * column of any type may appear in a filter, a grouping key or an aggregate argument. A build
 * column does reach the heap, so it still answers to
 * {@link io.questdb.griffin.HashJoinGroupByCandidate#supportsValueType(int)}.
 * <p>
 * The LEFT JOIN in every query pins which input is which: the planner always builds the slave of
 * an outer join, so {@code p} is the probe and {@code b} the build whatever the two sizes are.
 */
public class HashJoinGroupByColumnTypesTest extends AbstractCairoTest {
    // Types the GROUP BY key sink stages but the build's row heap does not copy.
    private static final String[] WIDE_COLUMNS = {"vc", "str", "ip", "u", "l256", "g1", "g8", "dec32", "dec64", "dec128", "dec256"};

    @Test
    public void testBuildColumnsOfWideTypesKeepTheOrdinaryPlan() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            insertRows();
            try (SqlExecutionContextImpl context = context(engine, 4)) {
                context.changePageFrameSizes(1, 8);
                for (String column : WIDE_COLUMNS) {
                    // A payload the row heap cannot copy, a build filter over it and an aggregate
                    // over it all disqualify the shape until 6b widens the payload set.
                    assertDifferential("SELECT b." + column + " k, count(*) n FROM p LEFT JOIN b ON p.id=b.id ORDER BY k",
                            context, false);
                    assertDifferential("SELECT count(*) n FROM p LEFT JOIN b ON p.id=b.id WHERE b." + column + " IS NOT NULL",
                            context, false);
                    assertDifferential("SELECT count(b." + column + ") n FROM p LEFT JOIN b ON p.id=b.id", context, false);
                }
            }
        });
    }

    @Test
    public void testProbeColumnsOfWideTypesOnNativeAndParquetFrames() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            insertRows();
            try (SqlExecutionContextImpl context = context(engine, 4)) {
                context.changePageFrameSizes(1, 8);
                for (int format = 0; format < 2; format++) {
                    if (format == 1) {
                        execute("ALTER TABLE p CONVERT PARTITION TO PARQUET WHERE t < '2020-01-02'");
                        execute("ALTER TABLE b CONVERT PARTITION TO PARQUET WHERE t < '2020-01-02'");
                    }
                    for (String column : WIDE_COLUMNS) {
                        // A grouping key, which reaches the fragments' key sink.
                        assertDifferential("SELECT p." + column + " k, count(*) n, sum(b.v) bv "
                                + "FROM p LEFT JOIN b ON p.id=b.id ORDER BY k", context, true);
                        // A filter, which the reducer runs over the raw frame.
                        assertDifferential("SELECT count(*) n, sum(b.v) bv FROM p LEFT JOIN b ON p.id=b.id "
                                + "WHERE p." + column + " IS NOT NULL", context, true);
                        // An aggregate argument, which item 3's registry admits for these types.
                        assertDifferential("SELECT count(p." + column + ") n, sum(b.v) bv "
                                + "FROM p LEFT JOIN b ON p.id=b.id", context, true);
                    }
                }
            }
        });
    }

    @Test
    public void testProbeColumnsOfWideTypesUnderJitAndAStagedKey() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            insertRows();
            try (SqlExecutionContextImpl context = context(engine, 4)) {
                context.changePageFrameSizes(1, 8);
                for (boolean jit : new boolean[]{false, true}) {
                    context.setJitMode(jit ? SqlJitMode.JIT_MODE_FORCE_SCALAR : SqlJitMode.JIT_MODE_DISABLED);
                    for (String on : new String[]{"p.id=b.id", "p.id=b.id AND p.l=b.l"}) {
                        // A wide probe column as the key of the GROUP BY above a staged join key.
                        assertDifferential("SELECT p.vc k, count(*) n, sum(b.v) bv FROM p LEFT JOIN b ON " + on
                                + " WHERE p.l > 0 ORDER BY k", context, true);
                        assertDifferential("SELECT p.u k, count(p.l256) n256, count(p.dec256) n256d "
                                + "FROM p LEFT JOIN b ON " + on + " ORDER BY k", context, true);
                    }
                }
            }
        });
    }

    @Test
    public void testProbeExpressionsOverWideColumns() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            insertRows();
            try (SqlExecutionContextImpl context = context(engine, 4)) {
                context.changePageFrameSizes(1, 8);
                // An expression over a wide probe column, whose own type decides the gate.
                assertDifferential("SELECT length(p.vc) k, count(*) n FROM p LEFT JOIN b ON p.id=b.id ORDER BY k",
                        context, true);
                assertDifferential("SELECT p.str || 'x' k, count(*) n FROM p LEFT JOIN b ON p.id=b.id ORDER BY k",
                        context, true);
                assertDifferential("SELECT count(*) n FROM p LEFT JOIN b ON p.id=b.id WHERE p.vc LIKE 'v%'",
                        context, true);
                // A wide probe column compared against a build column of the same type reaches the
                // post-join filter rather than the key, and the build column disqualifies it.
                assertDifferential("SELECT count(*) n FROM p LEFT JOIN b ON p.id=b.id WHERE p.vc = b.vc",
                        context, false);
            }
        });
    }

    @Test
    public void testUnstagedProbeTypesKeepTheOrdinaryPlan() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            insertRows();
            try (SqlExecutionContextImpl context = context(engine, 4)) {
                context.changePageFrameSizes(1, 8);
                // BINARY and ARRAY are outside the GROUP BY key set, so grouping by one keeps the
                // ordinary plan instead of failing the query with a fused-only error.
                assertDifferential("SELECT p.bin k, count(*) n FROM p LEFT JOIN b ON p.id=b.id", context, false);
                assertDifferential("SELECT p.arr k, count(*) n FROM p LEFT JOIN b ON p.id=b.id", context, false);
                // A filter over either one is a BOOLEAN expression, so the shape still qualifies.
                assertDifferential("SELECT count(*) n FROM p LEFT JOIN b ON p.id=b.id WHERE p.bin IS NOT NULL",
                        context, true);
                assertDifferential("SELECT count(*) n FROM p LEFT JOIN b ON p.id=b.id WHERE array_sum(p.arr) > 0.5",
                        context, true);
            }
        });
    }

    private void createTables() throws Exception {
        for (String name : new String[]{"p", "b"}) {
            execute("CREATE TABLE " + name + " (id INT, l LONG, vc VARCHAR, str STRING, ip IPV4, u UUID, "
                    + "l256 LONG256, g1 GEOHASH(1c), g8 GEOHASH(8c), "
                    + "dec32 DECIMAL(9,2), dec64 DECIMAL(18,2), dec128 DECIMAL(38,2), dec256 DECIMAL(50,2), "
                    + "bin BINARY, arr DOUBLE[], v DOUBLE, t TIMESTAMP) TIMESTAMP(t) PARTITION BY DAY");
        }
    }

    private void insertRows() throws Exception {
        // Small overlapping domains with NULLs on both sides, spread over two partitions so that
        // the parquet run converts one of them and leaves the other native.
        for (String name : new String[]{"p", "b"}) {
            execute("INSERT INTO " + name + " (id, l, vc, str, ip, u, l256, g1, g8, "
                    + "dec32, dec64, dec128, dec256, bin, arr, v, t) "
                    + "SELECT rnd_int(0, 2, 1), rnd_int(0, 2, 1)::long, "
                    + "rnd_varchar('va', 'vb', NULL), rnd_str('sa', 'sb', NULL), "
                    + "rnd_ipv4('10.0.0.1/30', 1), "
                    + "rnd_str('11111111-1111-1111-1111-111111111111', "
                    + "'22222222-2222-2222-2222-222222222222', NULL)::uuid, "
                    + "rnd_str('0x01', '0x02', NULL)::long256, "
                    + "rnd_str('s', 'e', NULL)::geohash(1c), "
                    + "rnd_str('sp052w92', 'ezs42e44', NULL)::geohash(8c), "
                    + "rnd_int(0, 2, 1)::decimal(9,2), rnd_int(0, 2, 1)::decimal(18,2), "
                    + "rnd_int(0, 2, 1)::decimal(38,2), rnd_int(0, 2, 1)::decimal(50,2), "
                    + "rnd_bin(4, 8, 2), rnd_double_array(1, 1), "
                    + "x*0.25, timestamp_sequence('2020-01-01', 3600000000) FROM long_sequence(48)");
        }
    }
}
