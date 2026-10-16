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
import io.questdb.cairo.SqlJitMode;
import io.questdb.griffin.SqlException;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.BindVarTuple;
import io.questdb.test.tools.BindVariableTestSetter;
import org.junit.Assert;
import org.junit.Test;

/**
 * A WHERE conjunct on a table that runs before a RIGHT or FULL join filters after that join, where it
 * also drops the rows that the join NULL-extends. When a conjunct of the table is false on its NULL
 * row, SqlOptimiser also filters the scan of the table with a copy of every stable conjunct of the
 * table, see SqlOptimiser.addScanFilterBeforeNullingJoin(). The copies remove only rows that the
 * post-join filter drops anyway, so the rows stay the same while interval scans and JIT filters come
 * back.
 * <p>
 * Data D: t0 holds r1 (c 1, k 0, ts 2024-01-05T01:00), r2 (2, 1, 2024-01-05T02:00), r3 (NULL, 2,
 * 2024-01-05T03:00) and r4 (1, 3, 2024-01-06T01:00); t1 holds k 0, 1 and 5; t2 holds k 0, 1, 2 and 9.
 * Shape X, "t0 CROSS JOIN t1 RIGHT JOIN t2 ON t2.k = t1.k", matches every t0 row with t2 rows 0 and 1,
 * and NULL-extends t2 rows 2 and 9. Shape Y, "t0 CROSS JOIN t1 RIGHT JOIN t2 ON t2.k = t0.k", matches
 * r1, r2 and r3 three times each (once per t1 row), r4 never, and NULL-extends t2 row 9.
 * The expected counts follow by hand from the data, and agree with DuckDB with = written as
 * IS NOT DISTINCT FROM and QuestDB's != as IS DISTINCT FROM.
 */
public class RightJoinWhereScanFilterTest extends AbstractCairoTest {
    private static final String X_FULL_JOINS = "FROM t0 CROSS JOIN t1 FULL JOIN t2 ON t2.k = t1.k";
    private static final String X_JOINS = "FROM t0 CROSS JOIN t1 RIGHT JOIN t2 ON t2.k = t1.k";
    private static final String Y_JOINS = "FROM t0 CROSS JOIN t1 RIGHT JOIN t2 ON t2.k = t0.k";

    @Test
    public void testAsOfJoinMasterFilteredSlaveNot() throws Exception {
        // t0 is the master of the ASOF join, so a copy filters its scan without changing which t3
        // row each remaining t0 row matches. t3 is the slave, which the post-join rule of
        // LEFT/ASOF/LT slaves keeps unchanged. ASOF matches r1 and r2 with v 1, r3 and r4 with v 2;
        // t2 rows 0 and 1 match every t0 row through t1.
        assertMemoryLeak(() -> {
            createTables();
            execute("CREATE TABLE t3 (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t3 VALUES (1, '2024-01-05T00:30:00.000000Z'), (2, '2024-01-05T02:30:00.000000Z')");
            final String joins = "FROM t0 ASOF JOIN t3 CROSS JOIN t1 RIGHT JOIN t2 ON t2.k = t1.k";
            assertCount("SELECT count(*) " + joins + " WHERE t0.c = 1", 4);
            assertQuery("SELECT count(*) " + joins + " WHERE t0.c = 1")
                    .noLeakCheck()
                    .assertsPlanContaining("filter: c=1");
            assertCount("SELECT count(*) " + joins + " WHERE t3.v = 1", 4);
            assertQuery("SELECT count(*) " + joins + " WHERE t3.v = 1")
                    .noLeakCheck()
                    .assertsPlanNotContaining("filter: v=1");
        });
    }

    @Test
    public void testBindVariableComparisonOperandTypes() throws Exception {
        // c > $1 is false on a NULL c whatever the value, so its copy filters the scan for any bind
        // type. Over shape Y, r1 (c 1) and r2 (c 2) match three times each.
        assertMemoryLeak(() -> {
            createTables();
            final String sql = "SELECT count(*) " + Y_JOINS + " WHERE t0.c > $1";
            assertBindCounts(sql, true,
                    BindVarTuple.ok("INT MIN+1", "count\n6\n", s -> s.setInt(0, Integer.MIN_VALUE + 1)),
                    BindVarTuple.ok("INT NULL", "count\n0\n", s -> s.setInt(0, Numbers.INT_NULL))
            );
            assertBindCounts(sql, true,
                    BindVarTuple.ok("LONG max", "count\n0\n", s -> s.setLong(0, Long.MAX_VALUE)),
                    BindVarTuple.ok("LONG MIN+1", "count\n6\n", s -> s.setLong(0, Long.MIN_VALUE + 1))
            );
            assertBindCounts(sql, true,
                    BindVarTuple.ok("DOUBLE -1e300", "count\n6\n", s -> s.setDouble(0, -1e300)),
                    BindVarTuple.ok("DOUBLE NaN", "count\n0\n", s -> s.setDouble(0, Double.NaN))
            );
            assertBindCounts(sql, true,
                    BindVarTuple.ok("STRING 0", "count\n6\n", s -> s.setStr(0, "0")),
                    BindVarTuple.ok("STRING NULL", "count\n0\n", s -> s.setStr(0, null))
            );
        });
    }

    @Test
    public void testBindVariables() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // c = $1 is true on a NULL c when the value is NULL: r3 three times and the NULL row of
            // t2 row 9. A copy would turn t2 rows 0 and 1 into NULL rows too and return 6.
            assertBindCounts("SELECT count(*) " + Y_JOINS + " WHERE t0.c = $1", false,
                    BindVarTuple.ok("1", "count\n3\n", s -> s.setInt(0, 1)),
                    BindVarTuple.ok("NULL", "count\n4\n", s -> s.setInt(0, Numbers.INT_NULL))
            );
            // ts > $1 is false on a NULL ts whatever the value: r2 and r3 three times each
            assertBindCounts("SELECT count(*) " + Y_JOINS + " WHERE t0.ts > $1", true,
                    BindVarTuple.ok("01:30", "count\n6\n", s -> s.setStr(0, "2024-01-05T01:30:00.000000Z")),
                    BindVarTuple.ok("NULL", "count\n0\n", s -> s.setStr(0, null))
            );
            // ts < $2 rejects the NULL row, so ts >= $1 is copied with it: r1, r2 and r3 twice each.
            // With a NULL $1, ts >= $1 fails every real row and ts < $2 every NULL row.
            assertBindCounts("SELECT count(*) " + X_JOINS + " WHERE t0.ts >= $1 AND t0.ts < $2", true,
                    BindVarTuple.ok("day", "count\n6\n", s -> {
                        s.setStr(0, "2024-01-05");
                        s.setStr(1, "2024-01-06");
                    }),
                    BindVarTuple.ok("NULL", "count\n0\n", s -> {
                        s.setStr(0, null);
                        s.setStr(1, "2024-01-06");
                    })
            );
            // ts >= $1 alone accepts the NULL rows of t2 rows 2 and 9 for a NULL value, as NULL >= NULL
            // holds. A copy would turn all four t2 rows into NULL rows and return 4.
            assertBindCounts("SELECT count(*) " + X_JOINS + " WHERE t0.ts >= $1", false,
                    BindVarTuple.ok("day", "count\n8\n", s -> s.setStr(0, "2024-01-05")),
                    BindVarTuple.ok("NULL", "count\n2\n", s -> s.setStr(0, null))
            );
            // both bounds accept NULL for NULL values: only the NULL row of t2 row 9. A copy would
            // turn all four t2 rows into NULL rows and return 4.
            assertBindCounts("SELECT count(*) " + Y_JOINS + " WHERE t0.ts >= $1 AND t0.ts <= $2", false,
                    BindVarTuple.ok("NULL", "count\n1\n", s -> {
                        s.setStr(0, null);
                        s.setStr(1, null);
                    }),
                    BindVarTuple.ok("r1 and r2", "count\n6\n", s -> {
                        s.setStr(0, "2024-01-05");
                        s.setStr(1, "2024-01-05T02:00:00.000000Z");
                    })
            );
            // ts < $1 rejects the NULL row and takes c != 1 along: r2 and r3 three times each
            assertBindCounts("SELECT count(*) " + Y_JOINS + " WHERE t0.c != 1 AND t0.ts < $1", true,
                    BindVarTuple.ok("2024-01-06", "count\n6\n", s -> s.setStr(0, "2024-01-06")),
                    BindVarTuple.ok("NULL", "count\n0\n", s -> s.setStr(0, null))
            );
            assertBindCounts("SELECT count(*) " + Y_JOINS + " WHERE t0.ts BETWEEN $1 AND $2", true,
                    BindVarTuple.ok("day", "count\n9\n", s -> {
                        s.setStr(0, "2024-01-05");
                        s.setStr(1, "2024-01-05T23:59:00.000000Z");
                    }),
                    BindVarTuple.ok("NULL", "count\n0\n", s -> {
                        s.setStr(0, null);
                        s.setStr(1, null);
                    })
            );
        });
    }

    @Test
    public void testColumnAddedAfterData() throws Exception {
        // the rows written before ADD COLUMN read NULL in c
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (k INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t0 VALUES (0, '2024-01-05T01:00:00.000000Z'), (1, '2024-01-05T02:00:00.000000Z')");
            execute("ALTER TABLE t0 ADD COLUMN c INT");
            execute("INSERT INTO t0 VALUES (2, '2024-01-05T03:00:00.000000Z', 1)");
            createJoinedTables();
            assertScanFilteredCount("SELECT count(*) " + Y_JOINS + " WHERE t0.c = 1", 3, true);
            // k 0 and k 1 three times each, and the NULL row of t2 row 9
            assertScanFilteredCount("SELECT count(*) " + Y_JOINS + " WHERE t0.c IS NULL", 7, false);
        });
    }

    @Test
    public void testColumnTypes() throws Exception {
        // Per type, t0 holds a value row (k 0) and a NULL row (k 1). Over shape Y, each matches
        // three times, and t2 rows 2 and 9 add two NULL-extended rows, which hold the same NULL as
        // the NULL row: false for BOOLEAN and 0 for BYTE and SHORT, which have no NULL. A predicate
        // that keeps the value row and rejects NULL counts 3 and filters the scan; one true on NULL
        // counts 5 more and does not.
        assertMemoryLeak(() -> {
            assertTypeCases("INT", "1",
                    typeCase("x = 1", true, false),
                    typeCase("x != 1", false, true),
                    typeCase("x BETWEEN 0 AND 2", true, false),
                    typeCase("x IN (1, 2)", true, false),
                    typeCase("x::string = '1'", true, false),
                    typeCase("x > $1", true, false, s -> s.setInt(0, 0))
            );
            assertTypeCases("LONG", "1",
                    typeCase("x = 1", true, false),
                    typeCase("x IN (1, 2)", true, false),
                    typeCase("x > $1", true, false, s -> s.setLong(0, 0))
            );
            for (String symbolType : new String[]{"SYMBOL", "SYMBOL INDEX"}) {
                assertTypeCases(symbolType, "'a'",
                        typeCase("x = 'a'", true, false),
                        typeCase("x != 'a'", false, true),
                        typeCase("x IN ('a', 'b')", true, false),
                        typeCase("x NOT IN ('a')", false, true),
                        typeCase("x ~ 'a'", true, false),
                        typeCase("x LIKE 'a%'", true, false),
                        typeCase("x ILIKE 'A%'", true, false),
                        typeCase("x::string = 'a'", true, false),
                        typeCase("length(x) = 1", true, false),
                        typeCase("x IS NULL", false, true),
                        typeCase("x > $1", true, false, s -> s.setStr(0, ""))
                );
            }
            for (String stringType : new String[]{"VARCHAR", "STRING"}) {
                assertTypeCases(stringType, "'a'",
                        typeCase("x = 'a'", true, false),
                        typeCase("x IN ('a', 'b')", true, false),
                        typeCase("x ~ 'a'", true, false),
                        typeCase("x LIKE 'a%'", true, false),
                        typeCase("x ILIKE 'A%'", true, false),
                        typeCase("length(x) = 1", true, false),
                        typeCase("coalesce(x, 'a') = 'a'", true, true),
                        typeCase("x > $1", true, false, s -> s.setStr(0, ""))
                );
            }
            assertTypeCases("CHAR", "'a'",
                    typeCase("x = 'a'", true, false),
                    typeCase("x != 'a'", false, true),
                    typeCase("x > $1", true, false, s -> s.setChar(0, '0'))
            );
            for (String timestampType : new String[]{"TIMESTAMP", "TIMESTAMP_NS"}) {
                assertTypeCases(timestampType, "'2024-01-05T01:00:00.000000Z'",
                        typeCase("x = '2024-01-05T01:00:00.000000Z'", true, false),
                        typeCase("x IN '2024-01-05'", true, false),
                        typeCase("x BETWEEN '2024-01-05' AND '2024-01-06'", true, false),
                        typeCase("x > $1", true, false, s -> s.setStr(0, "2024-01-01"))
                );
            }
            assertTypeCases("DATE", "'2024-01-05T01:00:00.000Z'",
                    typeCase("x = '2024-01-05T01:00:00.000Z'", true, false),
                    typeCase("x > $1", true, false, s -> s.setStr(0, "2024-01-01"))
            );
            for (String decimalType : new String[]{"DECIMAL(18, 2)", "DECIMAL(38, 10)"}) {
                assertTypeCases(decimalType, "1.25m",
                        typeCase("x = 1.25m", true, false),
                        typeCase("x > 1m", true, false),
                        typeCase("x > $1", true, false, s -> s.setInt(0, 1))
                );
            }
            assertTypeCases("DOUBLE", "1.5",
                    typeCase("x = 1.5", true, false),
                    typeCase("x = 'NaN'", false, true),
                    typeCase("x > $1", true, false, s -> s.setDouble(0, 0))
            );
            assertTypeCases("FLOAT", "1.5",
                    typeCase("x = 1.5", true, false),
                    typeCase("x > $1", true, false, s -> s.setFloat(0, 0))
            );
            assertTypeCases("UUID", "'11111111-1111-1111-1111-111111111111'",
                    typeCase("x = '11111111-1111-1111-1111-111111111111'", true, false),
                    typeCase("x != '11111111-1111-1111-1111-111111111111'", false, true)
            );
            assertTypeCases("IPv4", "'1.2.3.4'",
                    typeCase("x = '1.2.3.4'", true, false),
                    typeCase("x <<= '1.2.3.0/24'", true, false),
                    typeCase("x > $1", true, false, s -> s.setStr(0, "0.0.0.1"))
            );
            assertTypeCases("LONG256", "'0x01'",
                    typeCase("x = '0x01'", true, false),
                    typeCase("x != '0x01'", false, true)
            );
            assertTypeCases("GEOHASH(5c)", "#u33d8",
                    typeCase("x = #u33d8", true, false),
                    typeCase("x WITHIN (#u33)", true, false)
            );
            // the join reads x[1] unqualified: t0.x[1] fails with "Invalid column: t0.x", as it does
            // without the join
            assertTypeCases("DOUBLE[]", "ARRAY[1.0, 2.0]",
                    typeCase("x[1] = 1.0", true, false)
            );
            // BOOLEAN, BYTE and SHORT have no NULL: their NULL row holds false or 0
            assertTypeCases("BOOLEAN", "true",
                    typeCase("x = true", true, false),
                    typeCase("x = false", false, true)
            );
            for (String shortType : new String[]{"BYTE", "SHORT"}) {
                assertTypeCases(shortType, "1",
                        typeCase("x = 1", true, false),
                        typeCase("x = 0", false, true),
                        typeCase("x::int BETWEEN 1 AND 2", true, false),
                        // false on the NULL row for a value of 0, but true for a value of -1
                        new TypeCase("x > $1", true, false, false, s -> s.setInt(0, 0))
                );
            }
        });
    }

    @Test
    public void testConstantPredicates() throws Exception {
        // over shape Y; whether the scan carries a copy decides the count only when the copy is
        // wrong, so each case asserts both
        assertMemoryLeak(() -> {
            createTables();
            assertScanFilteredCount("SELECT count(*) " + Y_JOINS + " WHERE t0.c = 1", 3, true);
            // NULL != 1 is true in QuestDB: r2, r3 three times each and the NULL row. A copy would
            // remove r1, and t2 row 0 would become a NULL row that passes: 8.
            assertScanFilteredCount("SELECT count(*) " + Y_JOINS + " WHERE t0.c != 1", 7, false);
            // r3 three times and the NULL row. A copy would make t2 rows 0 and 1 NULL rows: 6.
            assertScanFilteredCount("SELECT count(*) " + Y_JOINS + " WHERE t0.c IS NULL", 4, false);
            assertScanFilteredCount("SELECT count(*) " + Y_JOINS + " WHERE t0.c IS NOT NULL", 6, true);
            assertScanFilteredCount("SELECT count(*) " + Y_JOINS + " WHERE t0.c = NULL", 4, false);
            // r1, r3 three times each, and the NULL row
            assertScanFilteredCount("SELECT count(*) " + Y_JOINS + " WHERE coalesce(t0.c, 1) = 1", 7, false);
            // the smallest INT value, MIN+1, keeps every non-NULL c
            assertScanFilteredCount("SELECT count(*) " + Y_JOINS + " WHERE t0.c > -2_147_483_647", 6, true);
            assertScanFilteredCount("SELECT count(*) " + Y_JOINS + " WHERE t0.c IN (1, 2)", 6, true);
            assertScanFilteredCount("SELECT count(*) " + Y_JOINS + " WHERE t0.c BETWEEN 1 AND 2", 6, true);
            assertScanFilteredCount("SELECT count(*) " + Y_JOINS + " WHERE t0.c::string ~ '^1$'", 3, true);
            assertScanFilteredCount("SELECT count(*) " + Y_JOINS + " WHERE NOT (t0.c = 1)", 7, false);
            assertScanFilteredCount("SELECT count(*) " + Y_JOINS + " WHERE t0.c = 1 + 0", 3, true);
            // the other operand of < and > may be a column of the table: r1 (1 > 0) and r2 (2 > 1)
            assertScanFilteredCount("SELECT count(*) " + Y_JOINS + " WHERE t0.c > t0.k", 6, true);
            assertScanFilteredCount("SELECT count(*) " + Y_JOINS + " WHERE t0.k < t0.c", 6, true);
            // the other operand of < and > may come first
            assertScanFilteredCount("SELECT count(*) " + Y_JOINS + " WHERE -1 < t0.c", 6, true);
            // a function of the column with more arguments: a day after r1, r2 and r3 is past 02:30
            assertScanFilteredCount("SELECT count(*) " + Y_JOINS + " WHERE dateadd('d', 1, t0.ts) > '2024-01-05T02:30'", 9, true);
            // a conjunct that reads two columns and is not < or > is not checked: r1 three times
            assertScanFilteredCount("SELECT count(*) " + Y_JOINS + " WHERE coalesce(t0.c, t0.k, 5) = 1", 3, false);
            // a conjunct that reads two columns and is not < or > is not checked: r1 (1 - 0) and r2
            // (2 - 1) three times each
            assertScanFilteredCount("SELECT count(*) " + Y_JOINS + " WHERE t0.c - t0.k = 1", 6, false);
            // a sub-query is evaluated once more per scan, so it gets no copy
            assertScanFilteredCount("SELECT count(*) " + Y_JOINS + " WHERE t0.s IN (SELECT 'a'::varchar FROM long_sequence(1))", 3, false);
            assertScanFilteredCount("SELECT count(*) " + Y_JOINS + " WHERE t0.c = (SELECT 1 FROM long_sequence(1))", 3, false);
        });
    }

    @Test
    public void testCrossJoinPrefixSubQueryAndCte() throws Exception {
        // the outer WHERE moves into the join level after its join pass, through
        // moveWhereInsideSubQueries: r1, r2 and r3 twice each
        assertMemoryLeak(() -> {
            createTables();
            final String subQuery = "SELECT count(*) FROM (SELECT t0.c, t0.ts " + X_JOINS + ") WHERE ts IN '2024-01-05'";
            assertQuery(subQuery)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .withPlanContaining("Interval forward scan on: t0", "Filter filter: t0.ts in [1704412800000000,1704499199999999]")
                    .returns("count\n6\n");
            assertQuery("WITH j AS (SELECT t0.c, t0.ts " + X_JOINS + ") SELECT count(*) FROM j WHERE ts IN '2024-01-05'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .withPlanContaining("Interval forward scan on: t0")
                    .returns("count\n6\n");
            // ts > $1 is false on the NULL row whatever the value: r2, r3 and r4 twice each
            bindVariableService.clear();
            bindVariableService.setStr(0, "2024-01-05T01:30:00.000000Z");
            final String bindSubQuery = "SELECT count(*) FROM (SELECT t0.c, t0.ts " + X_JOINS + ") WHERE ts > $1";
            assertQuery(bindSubQuery)
                    .noLeakCheck()
                    .assertsPlanContaining("Interval forward scan on: t0");
            assertQuery(bindSubQuery)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .assertBinds(cases(
                            BindVarTuple.ok("01:30", "count\n6\n", s -> s.setStr(0, "2024-01-05T01:30:00.000000Z")),
                            BindVarTuple.ok("NULL", "count\n0\n", s -> s.setStr(0, null))
                    ));
            // c != 1 alone is true on NULL, so the sub-query form copies nothing either:
            // r2, r3 twice each, and the NULL rows of t2 rows 2 and 9
            assertQuery("SELECT count(*) FROM (SELECT t0.c, t0.ts " + X_JOINS + ") WHERE c != 1")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .withPlanNotContaining("filter: c!=1")
                    .returns("count\n6\n");
            // a stable conjunct on a sub-query column whose expression is computed stays at the
            // sub-query: r1 twice
            assertQuery("SELECT count(*) FROM (SELECT t0.c + 1 c1, t0.ts " + X_JOINS + ") WHERE c1 = 2 AND ts IN '2024-01-05'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .withPlanContaining("Interval forward scan on: t0")
                    .returns("count\n2\n");
        });
    }

    @Test
    public void testDataShapes() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // the interval sits on the partition boundary: r4 twice
            assertIntervalScanCount("SELECT count(*) " + X_JOINS + " WHERE t0.ts IN '2024-01-06'", 2);
            // r3 and r4, twice each
            assertIntervalScanCount("SELECT count(*) " + X_JOINS + " WHERE t0.ts BETWEEN '2024-01-05T03:00' AND '2024-01-06T01:00'", 4);
            // an empty t2: the RIGHT join returns nothing, the FULL join the prefix rows that pass,
            // r1, r2 and r3 three times each
            execute("TRUNCATE TABLE t2");
            assertIntervalScanCount("SELECT count(*) " + X_JOINS + " WHERE t0.ts IN '2024-01-05'", 0);
            assertIntervalScanCount("SELECT count(*) " + X_FULL_JOINS + " WHERE t0.ts IN '2024-01-05'", 9);
            execute("INSERT INTO t2 VALUES (0), (1), (2), (9)");
            // an all-NULL c
            execute("UPDATE t0 SET c = NULL");
            assertScanFilteredCount("SELECT count(*) " + Y_JOINS + " WHERE t0.c = 1", 0, true);
            // a single row in t0, r1: matched with t2 rows 0 and 1
            execute("TRUNCATE TABLE t0");
            execute("INSERT INTO t0 VALUES (1, 0, 'a', '2024-01-05T01:00:00.000000Z')");
            assertIntervalScanCount("SELECT count(*) " + X_JOINS + " WHERE t0.ts IN '2024-01-05'", 2);
            // an empty t0: every t2 row becomes a NULL row, which the filter drops
            execute("TRUNCATE TABLE t0");
            assertIntervalScanCount("SELECT count(*) " + X_JOINS + " WHERE t0.ts IN '2024-01-05'", 0);
            assertScanFilteredCount("SELECT count(*) " + Y_JOINS + " WHERE t0.c = 1", 0, true);
        });
    }

    @Test
    public void testDoubleInfinityAgainstNaNBindVariable() throws Exception {
        // c > 0 rejects the NULL row and takes d >= $1 along. Java compares DOUBLE NULL as equal to
        // any other NULL, and +Infinity reads as NULL, so +Infinity >= NaN holds. With JIT on, the
        // scan copy decides as the plain WHERE under JIT does, and the post-join filter keeps what
        // it keeps. Rows: (c 1, d +Infinity, k 0), (c 1, d 1.0, k 1) and (NULL, NULL, k 2).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (c INT, d DOUBLE, k INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t0 VALUES (1, 1.0 / 0, 0, '2024-01-05T01:00:00.000000Z'), (1, 1.0, 1, '2024-01-05T02:00:00.000000Z'),"
                    + " (NULL, NULL, 2, '2024-01-05T03:00:00.000000Z')");
            createJoinedTables();
            final String sql = "SELECT count(*) " + Y_JOINS + " WHERE t0.c > 0 AND t0.d >= $1";
            final String plainSql = "SELECT count(*) FROM t0 WHERE c > 0 AND d >= $1";
            final int callerJitMode = sqlExecutionContext.getJitMode();
            try {
                sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
                // the +Infinity row three times; 1.0 >= NaN is false and NULL > 0 is false
                assertBindCounts(sql, true, BindVarTuple.ok("NaN", "count\n3\n", s -> s.setDouble(0, Double.NaN)));
                sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
                bindVariableService.clear();
                bindVariableService.setDouble(0, Double.NaN);
                sink.clear();
                printSql(plainSql);
                final long plainCount = Long.parseLong(sink.toString().substring("count\n".length()).trim());
                assertBindCounts(sql, true, BindVarTuple.ok("NaN", "count\n" + (3 * plainCount) + "\n", s -> s.setDouble(0, Double.NaN)));
            } finally {
                sqlExecutionContext.setJitMode(callerJitMode);
            }
        });
    }

    @Test
    public void testErrors() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // 'abc' is no INT, so c = 'abc' is false on every row, the NULL row included, and the scan
            // of t0 becomes empty
            assertQuery("SELECT count(*) " + Y_JOINS + " WHERE t0.c = 'abc'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .withPlanContaining("Empty table", "Filter filter: false")
                    .returns("count\n0\n");
            // the regex does not compile, so no copy is made and the error is the one that the
            // post-join filter reports, at the pattern
            final String badRegex = "SELECT count(*) " + Y_JOINS + " WHERE t0.s ~ '['";
            assertException(badRegex, badRegex.indexOf("'['") + 1, "Unclosed character class");
            // a function that does not exist fails at its position, whether the copy is made or not
            final String unknownFunction = "SELECT count(*) " + Y_JOINS + " WHERE t0.c = undefined_function(1)";
            assertException(unknownFunction, unknownFunction.indexOf("undefined_function"), "unknown function name: undefined_function");
            // BETWEEN written as a function with two arguments fails in the parser
            final String betweenCall = "SELECT count(*) " + Y_JOINS + " WHERE between(t0.c, 1)";
            assertException(betweenCall, betweenCall.indexOf("between"), "too few arguments for 'between' [found=2,expected=3]");
            // the copy converts the bind value at the scan, as a plain WHERE c > $1 does
            final String bindSql = "SELECT count(*) " + Y_JOINS + " WHERE t0.c > $1";
            bindVariableService.clear();
            bindVariableService.setStr(0, "abc");
            assertException(bindSql, 0, "inconvertible value: `abc` [STRING -> INT]");
            // with an empty t2 the RIGHT join returns no row, yet the scan copy still converts the
            // value; this is the one case in which the copy reports an error that the post-join
            // filter alone did not
            execute("TRUNCATE TABLE t2");
            bindVariableService.clear();
            bindVariableService.setStr(0, "abc");
            assertException(bindSql, 0, "inconvertible value: `abc` [STRING -> INT]");
            execute("INSERT INTO t2 VALUES (0), (1), (2), (9)");
            // the same compiled query after TRUNCATE and INSERT
            assertQuery("SELECT count(*) " + Y_JOINS + " WHERE t0.c = 1")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .mutateWith("TRUNCATE TABLE t0")
                    .returns("count\n3\n", "count\n0\n");
            assertQuery("SELECT count(*) " + Y_JOINS + " WHERE t0.c = 1")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .mutateWith("INSERT INTO t0 VALUES (1, 0, 'a', '2024-01-05T01:00:00.000000Z'), (1, 1, 'b', '2024-01-05T02:00:00.000000Z')")
                    .returns("count\n0\n", "count\n6\n");
            // a dropped column fails at its position
            execute("ALTER TABLE t0 DROP COLUMN c");
            final String dropped = "SELECT count(*) " + Y_JOINS + " WHERE t0.c = 1";
            assertException(dropped, dropped.indexOf("t0.c"), "Invalid column: t0.c");
            // the table still answers a plain SELECT
            assertQuery("SELECT k, s FROM t0")
                    .noLeakCheck()
                    .expectSize()
                    .returns("k\ts\n0\ta\n1\tb\n");
        });
    }

    @Test
    public void testExecutionModes() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final int callerJitMode = sqlExecutionContext.getJitMode();
            try {
                for (int jitMode : new int[]{SqlJitMode.JIT_MODE_DISABLED, SqlJitMode.JIT_MODE_ENABLED}) {
                    for (boolean isParallel : new boolean[]{false, true}) {
                        sqlExecutionContext.setJitMode(jitMode);
                        sqlExecutionContext.setParallelFilterEnabled(isParallel);
                        assertIntervalScanCount("SELECT count(*) " + X_JOINS + " WHERE t0.ts IN '2024-01-05'", 6);
                        assertIntervalScanCount("SELECT count(*) " + X_FULL_JOINS + " WHERE t0.ts IN '2024-01-05'", 9);
                        bindVariableService.clear();
                        bindVariableService.setStr(0, "2024-01-05T01:30:00.000000Z");
                        assertIntervalScanCount("SELECT count(*) " + Y_JOINS + " WHERE t0.ts > $1", 6);
                        final String filterPlan = !isParallel
                                ? "Filter filter: c=1"
                                : jitMode == SqlJitMode.JIT_MODE_ENABLED ? "Async JIT Filter" : "Async Filter";
                        assertQuery("SELECT count(*) " + Y_JOINS + " WHERE t0.c = 1")
                                .noLeakCheck()
                                .noRandomAccess()
                                .expectSize()
                                .withPlanContaining(filterPlan, "filter: c=1", "Filter filter: t0.c=1")
                                .returns("count\n3\n");
                    }
                }
            } finally {
                sqlExecutionContext.setJitMode(callerJitMode);
                sqlExecutionContext.setParallelFilterEnabled(configuration.isSqlParallelFilterEnabled());
            }
        });
    }

    @Test
    public void testFloatToleranceBindVariable() throws Exception {
        // JIT compares FLOAT with FLOAT_EPSILON, wider than the Java filter's DOUBLE_TOLERANCE, so
        // JIT reads 1e-10 > 0 as false and Java as true. With JIT on, a row survives only when the
        // JIT scan copy and the Java post-join filter both keep it, so the count follows the plain
        // WHERE under JIT. Rows: (f 1e-10, k 0), (f 5, k 1) and (NULL, k 2).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (f FLOAT, k INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t0 VALUES (1e-10, 0, '2024-01-05T01:00:00.000000Z'), (5, 1, '2024-01-05T02:00:00.000000Z'),"
                    + " (NULL, 2, '2024-01-05T03:00:00.000000Z')");
            createJoinedTables();
            final String sql = "SELECT count(*) " + Y_JOINS + " WHERE t0.f > $1";
            final int callerJitMode = sqlExecutionContext.getJitMode();
            try {
                sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
                // f 1e-10 and f 5, three times each
                assertBindCounts(sql, true, BindVarTuple.ok("0", "count\n6\n", s -> s.setFloat(0, 0)));
                sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
                // the plain WHERE under JIT keeps only f 5
                assertQuery("SELECT count(*) FROM t0 WHERE f > $1")
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .assertBinds(cases(BindVarTuple.ok("0", "count\n1\n", s -> s.setFloat(0, 0))));
                assertBindCounts(sql, true, BindVarTuple.ok("0", "count\n3\n", s -> s.setFloat(0, 0)));
            } finally {
                sqlExecutionContext.setJitMode(callerJitMode);
            }
        });
    }

    @Test
    public void testFullJoinIntervalScan() throws Exception {
        // t0 x t1 rows with t1 row 5 match no t2 row and stay once each: r1, r2 and r3 with t2
        // rows 0 and 1, and with t1 row 5
        assertMemoryLeak(() -> {
            createTables();
            assertIntervalScanCount("SELECT count(*) " + X_FULL_JOINS + " WHERE t0.ts IN '2024-01-05'", 9);
            // r1 with t1 rows 0, 1 and 5, as in the RIGHT join plus t1 row 5
            assertScanFilteredCount("SELECT count(*) FROM t0 CROSS JOIN t1 FULL JOIN t2 ON t2.k = t1.k WHERE t0.k = 0", 3, true);
            assertScanFilteredCount("SELECT count(*) FROM t0 CROSS JOIN t1 FULL JOIN t2 ON t2.k = t1.k WHERE t0.c != 1", 8, false);
        });
    }

    @Test
    public void testGroupByOrderByLimitAndWindow() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String where = " WHERE t0.ts IN '2024-01-05'";
            assertQuery("SELECT t2.k, count(*) " + X_JOINS + where + " GROUP BY t2.k ORDER BY t2.k")
                    .noLeakCheck()
                    .expectSize()
                    .withPlanContaining("Interval forward scan on: t0")
                    .returns("k\tcount\n0\t3\n1\t3\n");
            // NULL sorts first among INT values
            assertQuery("SELECT t0.c, t2.k " + X_JOINS + where + " ORDER BY t2.k, t0.c LIMIT 2")
                    .noLeakCheck()
                    .withPlanContaining("Interval forward scan on: t0")
                    .returns("c\tk\nnull\t0\n1\t0\n");
            assertQuery("SELECT t0.c, t2.k " + X_JOINS + where + " ORDER BY t2.k, t0.c LIMIT -2")
                    .noLeakCheck()
                    .expectSize()
                    .withPlanContaining("Interval forward scan on: t0")
                    .returns("c\tk\n1\t1\n2\t1\n");
            assertQuery("SELECT max(rn) FROM (SELECT row_number() OVER () rn " + X_JOINS + where + ")")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .withPlanContaining("Interval forward scan on: t0")
                    .returns("max\n6\n");
        });
    }

    @Test
    public void testIndexedSymbol() throws Exception {
        // s = 'a' keeps r1 (k 0, matched three times) and r4 (k 3, unmatched)
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (c INT, k INT, s SYMBOL INDEX, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            insertT0Rows();
            createJoinedTables();
            assertQuery("SELECT count(*) " + Y_JOINS + " WHERE t0.s = 'a'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .withPlanContaining("Index forward scan on: s")
                    .returns("count\n3\n");
            // ts > $2 rejects the NULL row and takes s = $1 along, which alone accepts NULL for a
            // NULL value
            bindVariableService.clear();
            bindVariableService.setStr(0, "a");
            bindVariableService.setStr(1, "2024-01-05");
            final String sql = "SELECT count(*) " + Y_JOINS + " WHERE t0.s = $1 AND t0.ts > $2";
            assertQuery(sql)
                    .noLeakCheck()
                    .assertsPlanContaining("Index forward scan on: s");
            assertQuery(sql)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .assertBinds(cases(
                            BindVarTuple.ok("a", "count\n3\n", s -> {
                                s.setStr(0, "a");
                                s.setStr(1, "2024-01-05");
                            }),
                            // r3 holds NULL in s; NULL = NULL holds
                            BindVarTuple.ok("NULL", "count\n3\n", s -> {
                                s.setStr(0, null);
                                s.setStr(1, "2024-01-05");
                            })
                    ));
        });
    }

    @Test
    public void testIntervalScan() throws Exception {
        // r1, r2 and r3 match t2 rows 0 and 1 through t1; t2 rows 2 and 9 become NULL rows, which
        // the post-join filter drops
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT count(*) " + X_JOINS + " WHERE t0.ts IN '2024-01-05'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .withPlan("""
                            Count
                                Filter filter: t0.ts in [1704412800000000,1704499199999999]
                                    Hash Right Outer Join Light
                                      condition: t2.k=t1.k
                                        Cross Join
                                            PageFrame
                                                Row forward scan
                                                Interval forward scan on: t0
                                                  intervals: [("2024-01-05T00:00:00.000000Z","2024-01-05T23:59:59.999999Z")]
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: t1
                                        Hash
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: t2
                            """)
                    .returns("count\n6\n");
        });
    }

    @Test
    public void testLateral() throws Exception {
        // per t2 row o, the inner join rows whose t2.k is o.k: three each for 0 and 1, none for the
        // NULL rows of 2 and 9
        assertMemoryLeak(() -> {
            createTables();
            final String sql = "SELECT o.k, c FROM t2 o, LATERAL (SELECT count(*) c " + X_JOINS
                    + " WHERE t0.ts IN '2024-01-05' AND t2.k = o.k) ORDER BY o.k";
            assertQuery(sql)
                    .noLeakCheck()
                    .returns("k\tc\n0\t3\n1\t3\n2\t0\n9\t0\n");
            assertSqlCursors(
                    "SELECT o.k, c FROM t2 o, LATERAL (SELECT count(*) c FROM (SELECT t0.ts, t2.k k2 " + X_JOINS
                            + " LIMIT 1_000_000_000) WHERE ts IN '2024-01-05' AND k2 = o.k) ORDER BY o.k",
                    sql
            );
        });
    }

    @Test
    public void testLatestBy() throws Exception {
        // the deprecated LATEST BY puts LATEST ON on t0 itself, as the trailing form does
        assertMemoryLeak(() -> {
            createLatestOnTables();
            for (String join : new String[]{"RIGHT", "FULL"}) {
                final String sql = "SELECT count(*) FROM t0 LATEST BY k CROSS JOIN t1 " + join + " JOIN t2 ON t2.k = t1.k WHERE t0.c = 1";
                assertLatestOnFilteredAfterLatestBy(sql, 2);
            }
        });
    }

    @Test
    public void testLatestOnSubQuery() throws Exception {
        // LATEST ON in a sub-query keeps the copy above it, at the sub-query, below the CROSS join
        assertMemoryLeak(() -> {
            createLatestOnTables();
            for (String join : new String[]{"RIGHT", "FULL"}) {
                final String sql = "SELECT count(*) FROM (SELECT * FROM t0 LATEST ON ts PARTITION BY k) t0 CROSS JOIN t1 "
                        + join + " JOIN t2 ON t2.k = t1.k WHERE t0.c = 1";
                assertQuery(sql)
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .withPlanContaining("Filter filter: t0.c=1", "filter: c=1")
                        .returns("count\n2\n");
            }
            // the WHERE on the LATEST ON key moves into the join level, where its copy filters the rows
            // that LATEST ON picked: b, matched with t2 rows 0 and 1
            assertQuery("SELECT count(*) FROM (SELECT t0.k, t0.c FROM t0 CROSS JOIN t1 RIGHT JOIN t2 ON t2.k = t1.k"
                    + " LATEST ON ts PARTITION BY k) WHERE k = 'b'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .withPlanContaining("Filter filter: k='b'\n")
                    .returns("count\n2\n");
            execute("CREATE TABLE t0a AS (SELECT * FROM t0 WHERE k = 'a') TIMESTAMP(ts) PARTITION BY DAY");
            for (String join : new String[]{"RIGHT", "FULL"}) {
                assertCount("SELECT count(*) FROM (SELECT * FROM t0a LATEST ON ts PARTITION BY k) t0 CROSS JOIN t1 "
                        + join + " JOIN t2 ON t2.k = t1.k WHERE t0.c = 1", 0);
            }
        });
    }

    @Test
    public void testMaterializedView() throws Exception {
        // c IN (1, 2) keeps r1 and r2 on 2024-01-05 and r4 on 2024-01-06, each matched with t2 rows 0 and
        // 1; the incremental refresh after a new 2024-01-06 row with c 2 adds two more
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (c INT, k INT, s SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            insertT0Rows();
            createJoinedTables();
            execute("CREATE MATERIALIZED VIEW mv WITH BASE t0 AS (SELECT t0.ts, count(*) cnt " + X_JOINS
                    + " WHERE t0.c IN (1, 2) SAMPLE BY 1d)");
            drainWalAndMatViewQueues();
            assertQuery("SELECT ts, cnt FROM mv ORDER BY ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\tcnt\n2024-01-05T00:00:00.000000Z\t4\n2024-01-06T00:00:00.000000Z\t2\n");
            execute("INSERT INTO t0 VALUES (2, 4, 'b', '2024-01-06T05:00:00.000000Z')");
            drainWalAndMatViewQueues();
            assertQuery("SELECT ts, cnt FROM mv ORDER BY ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\tcnt\n2024-01-05T00:00:00.000000Z\t4\n2024-01-06T00:00:00.000000Z\t4\n");
        });
    }

    @Test
    public void testMatchesLimitFencedReference() throws Exception {
        // LIMIT keeps the WHERE of the reference above the join, so the reference takes no copy
        assertMemoryLeak(() -> {
            createTables();
            final String[] predicates = {
                    "t0.c = 1", "t0.c != 1", "t0.c IS NULL", "t0.c IS NOT NULL", "t0.c IN (1, 2)", "t0.c BETWEEN 1 AND 2",
                    "t0.ts IN '2024-01-05'", "t0.ts > '2024-01-05T01:30'", "t0.s = 'a'", "t0.s != 'a'", "t0.s IN ('a', 'b')",
                    "t0.s NOT IN ('a')", "t0.s LIKE 'a%'", "t0.s ~ 'b'", "t0.c != 1 AND t0.ts < '2024-01-06'",
                    "coalesce(t0.c, 1) = 1 AND t0.ts IN '2024-01-05'", "t0.c > t0.k", "t0.c = 1 OR t0.s = 'b'"
            };
            for (String joins : new String[]{X_JOINS, X_FULL_JOINS, Y_JOINS, "FROM t0 JOIN t1 ON t1.k > t0.k RIGHT JOIN t2 ON t2.k = t1.k"}) {
                for (String predicate : predicates) {
                    assertSqlCursors(
                            "SELECT count(*) FROM (SELECT t0.c, t0.k, t0.s, t0.ts " + joins + " LIMIT 1_000_000_000) WHERE "
                                    + predicate.replace("t0.", ""),
                            "SELECT count(*) " + joins + " WHERE " + predicate
                    );
                }
            }
        });
    }

    @Test
    public void testNowStableAndRandomUnstable() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // now() is stable within an execution: 2024-01-06T00:30 minus a day keeps r1, r2 and r3
            setCurrentMicros(1_704_501_000_000_000L);
            assertScanFilteredCount("SELECT count(*) " + Y_JOINS + " WHERE t0.ts > dateadd('d', -1, now())", 9, true);
            // rnd_int() and systimestamp() change per call, so the scan never evaluates them, on
            // either side of the comparison or as a bound of BETWEEN
            assertQuery("SELECT count(*) " + Y_JOINS + " WHERE t0.c > rnd_int(0, 2, 0)")
                    .noLeakCheck()
                    .assertsPlanNotContaining("filter: c>rnd_int");
            assertScanFiltered("SELECT count(*) " + Y_JOINS + " WHERE t0.c > rnd_int(0, 2, 0)", false);
            assertScanFiltered("SELECT count(*) " + Y_JOINS + " WHERE t0.ts > systimestamp()", false);
            assertScanFiltered("SELECT count(*) " + Y_JOINS + " WHERE rnd_int(0, 2, 0) < t0.c", false);
            assertScanFiltered("SELECT count(*) " + Y_JOINS + " WHERE t0.c BETWEEN rnd_int(0, 1, 0) AND 2", false);
            assertScanFiltered("SELECT count(*) FROM (SELECT t0.c, t0.ts " + X_JOINS + ") WHERE c > rnd_int(0, 2, 0)", false);
            // an unstable conjunct gets no copy even when another conjunct rejects NULL
            assertScanFiltered("SELECT count(*) " + Y_JOINS + " WHERE t0.c = 1 AND t0.k < rnd_int(0, 2, 0)", true);
            assertQuery("SELECT count(*) " + Y_JOINS + " WHERE t0.c = 1 AND t0.k < rnd_int(0, 2, 0)")
                    .noLeakCheck()
                    .assertsPlanNotContaining("filter: c=1 and k<rnd_int");
        });
    }

    @Test
    public void testOtherPrefixTables() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // non-equi prefix: r1 pairs with t1 rows 1 and 5, r2, r3 and r4 with 5; t2 row 1 matches
            // (r1, 1), and the other t2 rows become NULL rows
            assertIntervalScanCount("SELECT count(*) FROM t0 JOIN t1 ON t1.k > t0.k RIGHT JOIN t2 ON t2.k = t1.k WHERE t0.ts IN '2024-01-05'", 1);
            // keyed prefix: r1 and r2 join t1; t2 row 0 matches r1
            assertQuery("SELECT count(*) FROM t0 JOIN t1 ON t1.k = t0.k RIGHT JOIN t2 ON t2.k = t0.k WHERE t0.c = 1")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .withPlanContaining("filter: c=1", "Filter filter: t0.c=1")
                    .returns("count\n1\n");
            // a WHERE on t1 filters the t1 scan: r1, r2 and r3 once each with t1 row 1
            assertQuery("SELECT count(*) " + Y_JOINS + " WHERE t1.k = 1")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .withPlanContaining("filter: k=1", "Filter filter: t1.k=1")
                    .returns("count\n3\n");
        });
    }

    @Test
    public void testPageFrames() throws Exception {
        // more t0 rows than one page frame; a conjunct false on NULL drops every NULL row, so the
        // RIGHT join returns the rows of the INNER join
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 16);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 AS (SELECT CASE WHEN x % 7 = 0 THEN NULL ELSE (x % 5)::int END c, (x % 10)::int k,"
                    + " timestamp_sequence('2024-01-04', 600_000_000) ts FROM long_sequence(1_000)) TIMESTAMP(ts) PARTITION BY DAY");
            createJoinedTables();
            for (String predicate : new String[]{"t0.c = 1", "t0.ts IN '2024-01-05'", "t0.c > 2 AND t0.ts < '2024-01-06'"}) {
                assertSqlCursors(
                        "SELECT count(*) FROM t0 CROSS JOIN t1 JOIN t2 ON t2.k = t0.k WHERE " + predicate,
                        "SELECT count(*) " + Y_JOINS + " WHERE " + predicate
                );
                assertScanFiltered("SELECT count(*) " + Y_JOINS + " WHERE " + predicate, true);
            }
        });
    }

    @Test
    public void testParquetPartition() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("ALTER TABLE t0 CONVERT PARTITION TO PARQUET LIST '2024-01-05'");
            assertIntervalScanCount("SELECT count(*) " + X_JOINS + " WHERE t0.ts IN '2024-01-05'", 6);
            assertIntervalScanCount("SELECT count(*) " + X_FULL_JOINS + " WHERE t0.ts IN '2024-01-05'", 9);
        });
    }

    @Test
    public void testSetOperations() throws Exception {
        // distinct t2.k over the rows of the interval: 0 and 1
        assertMemoryLeak(() -> {
            createTables();
            final String rows = "SELECT t2.k " + X_JOINS + " WHERE t0.ts IN '2024-01-05'";
            assertQuery("SELECT count(*) FROM (" + rows + " UNION ALL " + rows + ")")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .withPlanContaining("Interval forward scan on: t0")
                    .returns("count\n12\n");
            assertQuery(rows + " EXCEPT SELECT 0")
                    .noLeakCheck()
                    .noRandomAccess()
                    .withPlanContaining("Interval forward scan on: t0")
                    .returns("k\n1\n");
            assertQuery(rows + " INTERSECT SELECT 1")
                    .noLeakCheck()
                    .noRandomAccess()
                    .withPlanContaining("Interval forward scan on: t0")
                    .returns("k\n1\n");
        });
    }

    @Test
    public void testSpliceJoinNotCopied() throws Exception {
        // SPLICE pairs each row with the prevailing row of the other side, so a filter on the scan of
        // sa would change which sa row prevails
        assertMemoryLeak(() -> {
            execute("CREATE TABLE sa (c INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO sa VALUES (1, '2024-01-05T00:00:01.000000Z'), (2, '2024-01-05T00:00:02.000000Z')");
            execute("CREATE TABLE sb (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO sb VALUES (10, '2024-01-05T00:00:01.500000Z'), (20, '2024-01-05T00:00:02.500000Z')");
            final String sql = "SELECT sa.c, sb.v FROM sa SPLICE JOIN sb WHERE sa.c = 1";
            assertSqlCursors("SELECT * FROM (SELECT sa.c, sb.v FROM sa SPLICE JOIN sb LIMIT 1_000_000_000) WHERE c = 1", sql);
            assertQuery(sql)
                    .noLeakCheck()
                    .assertsPlanNotContaining("filter: c=1");
        });
    }

    @Test
    public void testTrailingLatestOn() throws Exception {
        // LATEST ON on t0 itself picks a -> (c 2) and b -> (c 1); the copy filters those rows, not the
        // scan below LATEST ON, which would pick the older a row (c 1) and count 4
        assertMemoryLeak(() -> {
            createLatestOnTables();
            for (String join : new String[]{"RIGHT", "FULL"}) {
                assertLatestOnFilteredAfterLatestBy("SELECT count(*) FROM t0 CROSS JOIN t1 " + join
                        + " JOIN t2 ON t2.k = t1.k WHERE t0.c = 1 LATEST ON ts PARTITION BY k", 2);
            }
            // with only the a rows, the latest one fails c = 1
            execute("CREATE TABLE t0a AS (SELECT * FROM t0 WHERE k = 'a') TIMESTAMP(ts) PARTITION BY DAY");
            for (String join : new String[]{"RIGHT", "FULL"}) {
                assertCount("SELECT count(*) FROM t0a t0 CROSS JOIN t1 " + join
                        + " JOIN t2 ON t2.k = t1.k WHERE t0.c = 1 LATEST ON ts PARTITION BY k", 0);
                assertCount("SELECT count(*) FROM t0a t0 LATEST BY k CROSS JOIN t1 " + join
                        + " JOIN t2 ON t2.k = t1.k WHERE t0.c = 1", 0);
            }
        });
    }

    @Test
    public void testView() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("CREATE VIEW v AS (SELECT t0.c, t0.ts, t2.k k2 " + X_JOINS + ")");
            assertQuery("SELECT count(*) FROM v WHERE ts IN '2024-01-05'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .withPlanContaining("Interval forward scan on: t0")
                    .returns("count\n6\n");
        });
    }

    @Test
    public void testWalTableAndNonPartitionedTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t0 (c INT, k INT, s SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            insertT0Rows();
            drainWalQueue();
            createJoinedTables();
            assertIntervalScanCount("SELECT count(*) " + X_JOINS + " WHERE t0.ts IN '2024-01-05'", 6);
            execute("CREATE TABLE np (c INT, k INT)");
            execute("INSERT INTO np VALUES (1, 0), (2, 1), (NULL, 2), (1, 3)");
            assertScanFilteredCount("SELECT count(*) FROM np t0 CROSS JOIN t1 RIGHT JOIN t2 ON t2.k = t0.k WHERE t0.c = 1", 3, true);
        });
    }

    private void assertBindCounts(String sql, boolean isScanFiltered, BindVarTuple... cases) throws Exception {
        final ObjList<BindVarTuple> list = cases(cases);
        bindVariableService.clear();
        cases[0].getBinds().assignBindVariables(bindVariableService);
        assertScanFiltered(sql, isScanFiltered);
        assertQuery(sql)
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .assertBinds(list);
    }

    private void assertCount(String sql, int expectedCount) throws Exception {
        assertQuery(sql)
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .returns("count\n" + expectedCount + "\n");
    }

    private void assertIntervalScanCount(String sql, int expectedCount) throws Exception {
        assertQuery(sql)
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .withPlanContaining("Interval forward scan on: t0")
                .returns("count\n" + expectedCount + "\n");
    }

    private void assertLatestOnFilteredAfterLatestBy(String sql, int expectedCount) throws Exception {
        assertQuery(sql)
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .withPlanContaining(
                        """
                                            Cross Join
                                                Filter filter: c=1
                                                    LatestByDeferredListValuesFiltered
                                                        Frame backward scan on: t0
                                """,
                        "Filter filter: t0.c=1"
                )
                .returns("count\n" + expectedCount + "\n");
    }

    // The scan of a table before the RIGHT/FULL join carries a filter when the plan below the CROSS
    // join, which holds the scans of t0 and t1, filters or narrows to intervals
    private static void assertScanFiltered(String sql, boolean isScanFiltered) throws SqlException {
        sink.clear();
        printSql("EXPLAIN " + sql);
        final String plan = sink.toString();
        final int crossJoin = plan.indexOf("Cross Join");
        Assert.assertTrue(plan, crossJoin > 0);
        final String belowJoin = plan.substring(crossJoin);
        Assert.assertEquals(sql + '\n' + plan, isScanFiltered, belowJoin.contains("ilter") || belowJoin.contains("Interval"));
    }

    private void assertScanFilteredCount(String sql, int expectedCount, boolean isScanFiltered) throws Exception {
        assertCount(sql, expectedCount);
        assertScanFiltered(sql, isScanFiltered);
    }

    private void assertTypeCases(String type, String value, TypeCase... typeCases) throws Exception {
        execute("DROP TABLE IF EXISTS t0");
        execute("CREATE TABLE t0 (x " + type + ", k INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("INSERT INTO t0 VALUES (" + value + ", 0, '2024-01-05T01:00:00.000000Z'), (NULL, 1, '2024-01-05T02:00:00.000000Z')");
        execute("DROP TABLE IF EXISTS t1");
        execute("DROP TABLE IF EXISTS t2");
        createJoinedTables();
        for (TypeCase typeCase : typeCases) {
            final String description = type + ": " + typeCase.predicate;
            // the plain WHERE confirms the hand-derived truth on the value and the NULL row
            final int plainCount = (typeCase.isValueKept ? 1 : 0) + (typeCase.isNullKept ? 1 : 0);
            final int joinCount = (typeCase.isValueKept ? 3 : 0) + (typeCase.isNullKept ? 5 : 0);
            final String plainSql = "SELECT count(*) FROM t0 WHERE " + typeCase.predicate;
            final String joinSql = "SELECT count(*) " + Y_JOINS + " WHERE " + typeCase.predicate.replaceAll("\\bx\\b(?!\\[)", "t0.x");
            try {
                bindVariableService.clear();
                typeCase.binds.assignBindVariables(bindVariableService);
                assertQuery(plainSql)
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .assertBinds(cases(BindVarTuple.ok(description, "count\n" + plainCount + "\n", typeCase.binds)));
                assertBindCounts(joinSql, typeCase.isScanFiltered, BindVarTuple.ok(description, "count\n" + joinCount + "\n", typeCase.binds));
            } catch (AssertionError | SqlException e) {
                throw new AssertionError(description + ": " + e.getMessage(), e);
            }
        }
    }

    private static ObjList<BindVarTuple> cases(BindVarTuple... cases) {
        final ObjList<BindVarTuple> list = new ObjList<>(cases.length);
        for (BindVarTuple c : cases) {
            list.add(c);
        }
        return list;
    }

    private static void createJoinedTables() throws SqlException {
        execute("CREATE TABLE t1 (k INT)");
        execute("INSERT INTO t1 VALUES (0), (1), (5)");
        execute("CREATE TABLE t2 (k INT)");
        execute("INSERT INTO t2 VALUES (0), (1), (2), (9)");
    }

    private static void createLatestOnTables() throws SqlException {
        // the two a rows: the older one passes c = 1, the newer one fails it
        execute("CREATE TABLE t0 (c INT, k SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("INSERT INTO t0 VALUES (1, 'a', '2024-01-05T01:00:00.000000Z'), (2, 'a', '2024-01-05T02:00:00.000000Z'),"
                + " (1, 'b', '2024-01-05T03:00:00.000000Z')");
        execute("CREATE TABLE t1 (k INT)");
        execute("INSERT INTO t1 VALUES (0), (1)");
        execute("CREATE TABLE t2 (k INT)");
        execute("INSERT INTO t2 VALUES (0), (1), (2)");
    }

    private static void createTables() throws SqlException {
        execute("CREATE TABLE t0 (c INT, k INT, s SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        insertT0Rows();
        createJoinedTables();
    }

    private static void insertT0Rows() throws SqlException {
        execute("""
                INSERT INTO t0 VALUES
                (1, 0, 'a', '2024-01-05T01:00:00.000000Z'),
                (2, 1, 'b', '2024-01-05T02:00:00.000000Z'),
                (NULL, 2, NULL, '2024-01-05T03:00:00.000000Z'),
                (1, 3, 'a', '2024-01-06T01:00:00.000000Z')
                """);
    }

    private static TypeCase typeCase(String predicate, boolean isValueKept, boolean isNullKept) {
        return new TypeCase(predicate, isValueKept, isNullKept, !isNullKept, s -> {
        });
    }

    private static TypeCase typeCase(String predicate, boolean isValueKept, boolean isNullKept, BindVariableTestSetter binds) {
        return new TypeCase(predicate, isValueKept, isNullKept, !isNullKept, binds);
    }

    private record TypeCase(
            String predicate,
            boolean isValueKept,
            boolean isNullKept,
            boolean isScanFiltered,
            BindVariableTestSetter binds
    ) {
    }
}
