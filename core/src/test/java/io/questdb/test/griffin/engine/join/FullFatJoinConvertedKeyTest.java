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

package io.questdb.test.griffin.engine.join;

import io.questdb.PropertyKey;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * A full-fat ASOF or LT join reads a right-side key column from its join map, which stores the
 * column in the key type of its key pair. Each test reads a key column whose key type differs from
 * the column type, and expects the rows of the same join over the table, which reads the column
 * from the table. A join over a UNION ALL is full-fat without forcing full-fat joins.
 */
public class FullFatJoinConvertedKeyTest extends AbstractCairoTest {
    private static final String STRING_OVER_VARCHAR_ROWS = """
            ts\tk\tk1\tlen\tis_e\tts1
            1970-01-01T00:00:01.000000Z\tx\tx\t1\tfalse\t1970-01-01T00:00:00.000000Z
            1970-01-01T00:00:03.000000Z\té\té\t1\ttrue\t1970-01-01T00:00:02.000000Z
            1970-01-01T00:00:05.000000Z\t\t\t-1\tfalse\t1970-01-01T00:00:04.000000Z
            1970-01-01T00:00:07.000000Z\t\uD834\uDD1E clef\t\uD834\uDD1E clef\t7\tfalse\t1970-01-01T00:00:06.000000Z
            1970-01-01T00:00:09.000000Z\ty\t\t-1\tfalse\t
            """;
    private static final String TIMESTAMP_OVER_NANOS_ROWS = """
            ts\tk\tk1\tk_micros\tts1
            1970-01-01T00:00:01.000000Z\t2024-01-01T00:00:00.000001000Z\t2024-01-01T00:00:00.000001Z\t1704067200000001\t1970-01-01T00:00:00.000000Z
            1970-01-01T00:00:03.000000Z\t1969-12-31T23:59:59.999999000Z\t1969-12-31T23:59:59.999999Z\t-1\t1970-01-01T00:00:02.000000Z
            1970-01-01T00:00:05.000000Z\t2024-01-03T00:00:00.000000000Z\t\tnull\t
            """;

    @Test
    public void testAsOfJoinReadsStringKeyComparedWithVarchar() throws Exception {
        assertStringKeyComparedWithVarchar("ASOF");
    }

    @Test
    public void testAsOfJoinReadsSymbolKeyOfSelfJoin() throws Exception {
        assertSymbolKeyOfSelfJoin(
                "ASOF",
                """
                        ts\ts\ts1\tts1
                        1970-01-01T00:00:01.000000Z\tx\tx\t1970-01-01T00:00:01.000000Z
                        1970-01-01T00:00:02.000000Z\té\té\t1970-01-01T00:00:02.000000Z
                        1970-01-01T00:00:03.000000Z\t\t\t1970-01-01T00:00:03.000000Z
                        1970-01-01T00:00:04.000000Z\tx\tx\t1970-01-01T00:00:04.000000Z
                        """
        );
    }

    @Test
    public void testAsOfJoinReadsTimestampKeyComparedWithNanos() throws Exception {
        assertTimestampKeyComparedWithNanos("ASOF");
    }

    @Test
    public void testAsOfJoinToleranceReadsStringKeyAfterEvacuation() throws Exception {
        // with a threshold of 1, every master row evacuates the join map into the other map
        setProperty(PropertyKey.CAIRO_SQL_ASOF_JOIN_EVACUATION_THRESHOLD, "1");
        assertMemoryLeak(() -> {
            createStringOverVarcharTables();
            final String on = " a1 ON a1.k = a0.k TOLERANCE 3s";
            assertConvertedKeyJoin(
                    STRING_OVER_VARCHAR_ROWS,
                    "SELECT a0.ts, a0.k, a1.k, length(a1.k) len, a1.k = 'é' is_e, a1.ts FROM m a0 ASOF JOIN s" + on,
                    "SELECT a0.ts, a0.k, a1.k, length(a1.k) len, a1.k = 'é' is_e, a1.ts FROM m a0 ASOF JOIN " + unionOf("s") + on
            );
        });
    }

    @Test
    public void testFilterAboveAsOfJoinReadsStringKeyComparedWithVarchar() throws Exception {
        // a2.v ties a1.c and a1.s, so the filter a1.s = a1.c runs above the ASOF join and reads a1.c,
        // a STRING key compared with VARCHAR a0.v. The row at 0s matches itself, but its s differs
        // from c, so the result is the row at 1s, which matches no row.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t2 (c STRING, v VARCHAR, s SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t1 (s SYMBOL, v SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t2 VALUES
                        ('b', 'b', 'b', '1970-01-01T00:00:00Z'),
                        ('a', NULL, NULL, '1970-01-01T00:00:01Z'),
                        (NULL, 'a', 'a', '1970-01-01T00:00:02Z')
                    """);
            execute("INSERT INTO t1 VALUES ('a', NULL, '1970-01-01T00:00:00Z')");
            final String query = """
                    SELECT a1.c, a1.v
                    FROM t2 a0
                    ASOF JOIN #RIGHT# a1 ON a1.v = a0.c AND a1.c = a0.v
                    JOIN t1 a2 ON a2.s = a0.c AND a2.v = a1.c AND a2.v = a1.s
                    """;
            final String expected = """
                    c\tv
                    \t
                    """;
            assertQuery(query.replace("#RIGHT#", "t2")).noLeakCheck().noRandomAccess().returns(expected);
            assertQuery(query.replace("#RIGHT#", unionOf("t2"))).noLeakCheck().noRandomAccess().returns(expected);
        });
    }

    @Test
    public void testLtJoinReadsStringKeyComparedWithVarchar() throws Exception {
        assertStringKeyComparedWithVarchar("LT");
    }

    @Test
    public void testLtJoinReadsSymbolKeyOfSelfJoin() throws Exception {
        assertSymbolKeyOfSelfJoin(
                "LT",
                """
                        ts\ts\ts1\tts1
                        1970-01-01T00:00:01.000000Z\tx\t\t
                        1970-01-01T00:00:02.000000Z\té\t\t
                        1970-01-01T00:00:03.000000Z\t\t\t
                        1970-01-01T00:00:04.000000Z\tx\tx\t1970-01-01T00:00:01.000000Z
                        """
        );
    }

    @Test
    public void testLtJoinReadsTimestampKeyComparedWithNanos() throws Exception {
        assertTimestampKeyComparedWithNanos("LT");
    }

    @Test
    public void testProjectionAboveAsOfJoinReadsStringKeyComparedWithVarchar() throws Exception {
        // a1.v is a STRING key compared with VARCHAR a0.c. The row at 2s matches the row at 0s, whose
        // s equals c, and joins t1, so the result projects a1.v of a matched row.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t2 (c VARCHAR, v STRING, s SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE t1 (s STRING, v STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t2 VALUES
                        ('b', 'a', 'b', '1970-01-01T00:00:00Z'),
                        (NULL, 'b', NULL, '1970-01-01T00:00:01Z'),
                        ('a', 'b', 'a', '1970-01-01T00:00:02Z')
                    """);
            execute("""
                    INSERT INTO t1 VALUES
                        ('a', 'b', '1970-01-01T00:00:00Z'),
                        (NULL, NULL, '1970-01-01T00:00:01Z'),
                        (NULL, 'b', '1970-01-01T00:00:02Z')
                    """);
            final String query = """
                    SELECT a1.c, a1.v
                    FROM t2 a0
                    ASOF JOIN #RIGHT# a1 ON a1.v = a0.c AND a1.c = a0.v
                    JOIN t1 a2 ON a2.s = a0.c AND a2.v = a1.c AND a2.v = a1.s
                    ORDER BY a1.c, a1.v
                    """;
            final String expected = """
                    c\tv
                    \t
                    b\ta
                    """;
            assertQuery(query.replace("#RIGHT#", "t2")).noLeakCheck().returns(expected);
            assertQuery(query.replace("#RIGHT#", unionOf("t2"))).noLeakCheck().returns(expected);
        });
    }

    private static String unionOf(String table) {
        return "((SELECT * FROM " + table + " UNION ALL SELECT * FROM " + table + " WHERE 1 = 0) TIMESTAMP(ts))";
    }

    private void assertConvertedKeyJoin(String expected, String tableJoin, String unionJoin) throws Exception {
        // the join over the table reads the right-side key column from the table
        assertQuery(tableJoin).noLeakCheck().noRandomAccess().timestamp("ts").expectSize().returns(expected);
        // the forced full-fat join and the join over a UNION ALL read it from the join map
        assertQuery(tableJoin).noLeakCheck().noRandomAccess().timestamp("ts").expectSize().fullFatJoins().returns(expected);
        assertQuery(unionJoin).noLeakCheck().noRandomAccess().timestamp("ts").expectSize().returns(expected);
    }

    private void assertStringKeyComparedWithVarchar(String join) throws Exception {
        assertMemoryLeak(() -> {
            createStringOverVarcharTables();
            // one key pair keys an UnorderedVarcharMap, two key pairs key an OrderedMap
            for (String on : new String[]{" a1 ON a1.k = a0.k", " a1 ON a1.k = a0.k AND a1.j = a0.j"}) {
                final String select = "SELECT a0.ts, a0.k, a1.k, length(a1.k) len, a1.k = 'é' is_e, a1.ts FROM m a0 " + join + " JOIN ";
                assertConvertedKeyJoin(STRING_OVER_VARCHAR_ROWS, select + "s" + on, select + unionOf("s") + on);
            }
        });
    }

    private void assertSymbolKeyOfSelfJoin(String join, String expected) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL, j INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO t VALUES
                        ('x', 1, '1970-01-01T00:00:01Z'),
                        ('é', 1, '1970-01-01T00:00:02Z'),
                        (NULL, 1, '1970-01-01T00:00:03Z'),
                        ('x', 1, '1970-01-01T00:00:04Z')
                    """);
            for (String on : new String[]{" a1 ON a0.s = a1.s", " a1 ON a0.s = a1.s AND a0.j = a1.j"}) {
                final String query = "SELECT a0.ts, a0.s, a1.s, a1.ts FROM t a0 " + join + " JOIN t" + on;
                assertQuery(query).noLeakCheck().noRandomAccess().timestamp("ts").expectSize().returns(expected);
                // only a self-join keys the map on the symbol key, and a self-join over the table
                // itself is full-fat only when full-fat joins are forced
                assertQuery(query).noLeakCheck().noRandomAccess().timestamp("ts").expectSize().fullFatJoins().returns(expected);
            }
        });
    }

    private void assertTimestampKeyComparedWithNanos(String join) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m (k TIMESTAMP_NS, j INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE s (k TIMESTAMP, j INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO m VALUES
                        ('2024-01-01T00:00:00.000001Z', 1, '1970-01-01T00:00:01Z'),
                        ('1969-12-31T23:59:59.999999Z', 1, '1970-01-01T00:00:03Z'),
                        ('2024-01-03T00:00:00Z', 1, '1970-01-01T00:00:05Z')
                    """);
            execute("""
                    INSERT INTO s VALUES
                        ('2024-01-01T00:00:00.000001Z', 1, '1970-01-01T00:00:00Z'),
                        ('1969-12-31T23:59:59.999999Z', 1, '1970-01-01T00:00:02Z'),
                        ('2024-01-04T00:00:00Z', 1, '1970-01-01T00:00:04Z')
                    """);
            for (String on : new String[]{" a1 ON a1.k = a0.k", " a1 ON a1.k = a0.k AND a1.j = a0.j"}) {
                final String select = "SELECT a0.ts, a0.k, a1.k, a1.k::LONG k_micros, a1.ts FROM m a0 " + join + " JOIN ";
                assertConvertedKeyJoin(TIMESTAMP_OVER_NANOS_ROWS, select + "s" + on, select + unionOf("s") + on);
            }
        });
    }

    private void createStringOverVarcharTables() throws Exception {
        execute("CREATE TABLE m (k VARCHAR, j INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("CREATE TABLE s (k STRING, j INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("""
                INSERT INTO m VALUES
                    ('x', 1, '1970-01-01T00:00:01Z'),
                    ('é', 1, '1970-01-01T00:00:03Z'),
                    (NULL, 1, '1970-01-01T00:00:05Z'),
                    ('\uD834\uDD1E clef', 1, '1970-01-01T00:00:07Z'),
                    ('y', 1, '1970-01-01T00:00:09Z')
                """);
        execute("""
                INSERT INTO s VALUES
                    ('x', 1, '1970-01-01T00:00:00Z'),
                    ('é', 1, '1970-01-01T00:00:02Z'),
                    (NULL, 1, '1970-01-01T00:00:04Z'),
                    ('\uD834\uDD1E clef', 1, '1970-01-01T00:00:06Z'),
                    ('z', 1, '1970-01-01T00:00:08Z')
                """);
    }
}
