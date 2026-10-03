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

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * A WHERE or ON conjunct that fails to bind raises its error when its filter is generated: column resolution
 * errors come first, then designated-timestamp interval errors, then the residual conjuncts from last to first.
 */
public class DeferredPredicateErrorTest extends AbstractCairoTest {
    private static final String ABS_ERROR = "there is no matching function `abs` with the argument types: (INT, INT)";
    private static final String EQ_ERROR = "there is no matching operator `=` with the argument types: INT = BOOLEAN";
    private static final String MISMATCH_ERROR = "expression type mismatch, expected: BOOLEAN, actual: INT";

    @Test
    public void testColumnResolutionPrecedes() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT * FROM x WHERE v = true AND nocol = 1").noLeakCheck().fails(35, "Invalid column: nocol");
            assertQuery("SELECT * FROM x WHERE ts = 'abc' AND nocol = 1").noLeakCheck().fails(37, "Invalid column: nocol");
            assertQuery("SELECT nocol FROM x WHERE v = true").noLeakCheck().fails(7, "Invalid column: nocol");
            assertQuery("SELECT nocol FROM x WHERE v").noLeakCheck().fails(7, "Invalid column: nocol");
            assertQuery("SELECT nocol FROM x WHERE v + 1").noLeakCheck().fails(7, "Invalid column: nocol");
            assertQuery("SELECT * FROM x WHERE v = true AND v IN (SELECT nocol FROM x)").noLeakCheck().fails(48, "Invalid column: nocol");
            assertQuery("SELECT * FROM x WHERE x.v = true AND y.v = 1").noLeakCheck().fails(37, "Invalid table name or alias");
            assertQuery("SELECT * FROM x a JOIN x b ON a.v = b.v WHERE a.v = true AND nocol = 1").noLeakCheck().fails(61, "Invalid column: nocol");
        });
    }

    @Test
    public void testConjunctOrder() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT * FROM x WHERE v = true AND abs(v, 1) = 1").noLeakCheck().fails(35, ABS_ERROR);
            assertQuery("SELECT * FROM x WHERE abs(v, 1) = 1 AND v = true").noLeakCheck().fails(42, EQ_ERROR);
            assertQuery("SELECT * FROM x WHERE nofunc(v) = 1 AND v = 0x123").noLeakCheck().fails(44, "invalid constant: 0x123");
            assertQuery("SELECT * FROM x WHERE v = 0x123 AND nofunc(v) = 1").noLeakCheck().fails(36, "unknown function name: nofunc(INT)");
            assertQuery("SELECT * FROM x WHERE v = true AND abs(v, 1) = 1 AND nofunc(v) = 1").noLeakCheck().fails(53, "unknown function name: nofunc(INT)");
            assertQuery("SELECT * FROM x WHERE (v = true AND abs(v, 1) = 1) AND (nofunc(v) = 1 AND v = 0x12)").noLeakCheck().fails(56, "unknown function name: nofunc(INT)");
            assertQuery("SELECT * FROM z WHERE substring(s, 1, -1) = 'a' AND v = true").noLeakCheck().fails(54, EQ_ERROR);
            assertQuery("SELECT * FROM z WHERE v = true AND substring(s, 1, -1) = 'a'").noLeakCheck().fails(35, "negative substring length is not allowed");
            assertQuery("SELECT * FROM xd WHERE d = 'abc' AND v = true").noLeakCheck().fails(39, EQ_ERROR);
            assertQuery("SELECT * FROM xd WHERE v = true AND d = 'abc'").noLeakCheck().fails(40, "Invalid date [str=abc]");
            assertQuery("SELECT * FROM x WHERE v = true AND v = true").noLeakCheck().fails(37, EQ_ERROR);
        });
    }

    @Test
    public void testConstantFalseKeepsError() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT * FROM x WHERE v = true AND false").noLeakCheck().fails(24, EQ_ERROR);
            assertQuery("SELECT * FROM x WHERE false AND v = true").noLeakCheck().fails(34, EQ_ERROR);
            assertQuery("SELECT * FROM x WHERE 1 = 2 AND v = true").noLeakCheck().fails(34, EQ_ERROR);
            assertQuery("SELECT * FROM x a JOIN x b ON a.v = b.v WHERE false AND a.v = true").noLeakCheck().fails(60, EQ_ERROR);
            assertQuery("SELECT * FROM x a JOIN x b ON a.v = b.v WHERE a.v = true AND false").noLeakCheck().fails(50, EQ_ERROR);
        });
    }

    @Test
    public void testIntervalErrorsPrecede() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT * FROM x WHERE v = true AND ts = 'abc' AND abs(v, 1) = 1").noLeakCheck().fails(40, "invalid timestamp");
            assertQuery("SELECT * FROM x WHERE abs(v, 1) = 1 AND ts = 'abc' AND v = true").noLeakCheck().fails(45, "invalid timestamp");
            assertQuery("SELECT * FROM x WHERE v = true AND ts IN ('2014', 'abc')").noLeakCheck().fails(50, "Invalid date");
            assertQuery("SELECT * FROM x WHERE abs(1, 1) = 1 AND ts = 'abc'").noLeakCheck().fails(45, "invalid timestamp");
            assertQuery("SELECT * FROM x WHERE v AND ts = 'abc'").noLeakCheck().fails(33, "invalid timestamp");
            assertQuery("SELECT * FROM x WHERE v + 1 AND ts = 'abc'").noLeakCheck().fails(37, "invalid timestamp");
            assertQuery("SELECT * FROM x WHERE ts = 'abc' AND v IN (1, true)").noLeakCheck().fails(27, "invalid timestamp");
            assertQuery("SELECT * FROM x WHERE ts = 'abc' AND v IN (SELECT v FROM x)").noLeakCheck().fails(27, "invalid timestamp");
            assertQuery("SELECT * FROM x WHERE v IN (SELECT v FROM x) AND ts = 'abc'").noLeakCheck().fails(54, "invalid timestamp");
            assertQuery("SELECT * FROM x WHERE count() > 1 AND ts = 'abc'").noLeakCheck().fails(43, "invalid timestamp");
            assertQuery("SELECT * FROM xn WHERE v = true AND ts = 'abc'").noLeakCheck().fails(41, "invalid timestamp");
            assertQuery("SELECT * FROM xd WHERE d = 'abc' AND ts = 'def'").noLeakCheck().fails(42, "invalid timestamp");
            assertQuery("SELECT * FROM xd WHERE ts = 'abc' AND d = 'def' AND v = true").noLeakCheck().fails(28, "invalid timestamp");
            assertQuery("SELECT count() FROM x WHERE v = true AND ts = 'abc' SAMPLE BY 1h").noLeakCheck().fails(46, "invalid timestamp");
            assertQuery("SELECT * FROM x WHERE v = true AND ts = 'abc' LATEST ON ts PARTITION BY v").noLeakCheck().fails(40, "invalid timestamp");
            assertQuery("SELECT v, row_number() OVER () FROM x WHERE v = true AND ts = 'abc'").noLeakCheck().fails(62, "invalid timestamp");
        });
    }

    @Test
    public void testJoinErrors() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT * FROM x a JOIN x b ON a.v = b.v WHERE a.v = true AND b.ts = 'abc'").noLeakCheck().fails(50, EQ_ERROR);
            assertQuery("SELECT * FROM x a JOIN x b ON a.v = b.v WHERE b.v = true AND a.ts = 'abc'").noLeakCheck().fails(68, "invalid timestamp");
            assertQuery("SELECT * FROM x a JOIN x b ON a.v = b.v WHERE abs(b.v, 1) = 1 AND a.v = true").noLeakCheck().fails(70, EQ_ERROR);
            assertQuery("SELECT * FROM x a JOIN x b ON a.v = b.v WHERE a.v + b.v = true AND a.ts = 'abc'").noLeakCheck().fails(74, "invalid timestamp");
            assertQuery("SELECT * FROM x a JOIN x b ON a.v = b.v WHERE b.v = 0x123 AND a.v + b.v = true").noLeakCheck().fails(52, "invalid constant: 0x123");
            assertQuery("SELECT * FROM x a JOIN x b ON a.v = b.v AND b.v = true WHERE a.ts = 'abc'").noLeakCheck().fails(68, "invalid timestamp");
            assertQuery("SELECT * FROM x a JOIN x b ON a.v = b.v AND a.v = true WHERE b.ts = 'abc'").noLeakCheck().fails(48, EQ_ERROR);
            assertQuery("SELECT * FROM x a JOIN x b ON a.v = b.v AND abs(a.v, 1) = 1 AND b.v = true").noLeakCheck().fails(44, ABS_ERROR);
            assertQuery("SELECT * FROM x a JOIN x b ON a.v = b.v WHERE b.v IN (SELECT ts FROM x) AND a.v = true").noLeakCheck().fails(80, EQ_ERROR);
            assertQuery("SELECT * FROM x a CROSS JOIN x b WHERE b.v = true AND a.ts = 'abc'").noLeakCheck().fails(61, "invalid timestamp");
            assertQuery("SELECT * FROM x a JOIN x b ON a.v = b.v JOIN x c ON b.v = c.v WHERE c.v = true AND b.ts = 'abc'").noLeakCheck().fails(90, "invalid timestamp");
            assertQuery("SELECT * FROM x a JOIN x b ON a.v = b.v JOIN x c ON b.v = c.v WHERE b.v = true AND c.ts = 'abc'").noLeakCheck().fails(72, EQ_ERROR);
            assertQuery("SELECT * FROM x a JOIN x b ON a.v = b.v JOIN x c ON b.v = c.v WHERE c.v = 0x123 AND b.v = true").noLeakCheck().fails(88, EQ_ERROR);
            assertQuery("SELECT * FROM x a JOIN (SELECT * FROM x WHERE v = true) b ON a.v = b.v WHERE a.ts = 'abc'").noLeakCheck().fails(84, "invalid timestamp");
            assertQuery("SELECT * FROM x a JOIN (SELECT * FROM x WHERE ts = 'abc') b ON a.v = b.v WHERE a.v = true").noLeakCheck().fails(83, EQ_ERROR);
            assertQuery("SELECT abs(a.v, 1) FROM x a JOIN x b ON a.v = b.v WHERE b.v = true").noLeakCheck().fails(60, EQ_ERROR);
        });
    }

    @Test
    public void testNestedFilters() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT * FROM (SELECT * FROM x WHERE v = true) WHERE ts = 'abc'").noLeakCheck().fails(58, "invalid timestamp");
            assertQuery("SELECT * FROM (SELECT * FROM x WHERE ts = 'abc') WHERE v = true").noLeakCheck().fails(42, "invalid timestamp");
            assertQuery("SELECT * FROM (SELECT * FROM x WHERE v = true) WHERE abs(v, 1) = 1").noLeakCheck().fails(53, ABS_ERROR);
            assertQuery("SELECT * FROM (SELECT * FROM x WHERE abs(v, 1) = 1) WHERE v = true").noLeakCheck().fails(60, EQ_ERROR);
            assertQuery("SELECT * FROM (SELECT * FROM x WHERE v = true LIMIT 1) WHERE ts = 'abc'").noLeakCheck().fails(39, EQ_ERROR);
            assertQuery("SELECT * FROM (SELECT * FROM x WHERE v = true ORDER BY v) WHERE ts = 'abc'").noLeakCheck().fails(69, "invalid timestamp");
            assertQuery("WITH c AS (SELECT * FROM x WHERE v = true) SELECT * FROM c WHERE ts = 'abc'").noLeakCheck().fails(70, "invalid timestamp");
            assertQuery("SELECT * FROM x WHERE ts = 'abc' UNION ALL SELECT * FROM x WHERE v = true").noLeakCheck().fails(27, "invalid timestamp");
            assertQuery("SELECT * FROM (SELECT * FROM x UNION ALL SELECT * FROM x WHERE abs(v, 1) = 1) WHERE ts = 'abc'").noLeakCheck().fails(89, "invalid timestamp");
            assertQuery("SELECT abs(v, 1) FROM x WHERE v = true").noLeakCheck().fails(32, EQ_ERROR);
            assertQuery("SELECT abs(v, 1) FROM x WHERE v + 1").noLeakCheck().fails(32, "boolean expression expected");
        });
    }

    @Test
    public void testNonBooleanSourceConjuncts() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT * FROM x a JOIN x b ON a.v = b.v AND a.v").noLeakCheck().fails(0, "boolean expression expected");
            assertQuery("SELECT * FROM x a JOIN x b ON a.v = b.v AND b.v").noLeakCheck().fails(0, "boolean expression expected");
            assertQuery("SELECT * FROM x a JOIN x b ON a.v = b.v WHERE a.v").noLeakCheck().fails(0, "boolean expression expected");
            assertQuery("SELECT * FROM x a JOIN x b ON a.v = b.v WHERE a.v AND b.ts = 'abc'").noLeakCheck().fails(0, "boolean expression expected");
            assertQuery("SELECT * FROM x a JOIN x b ON a.v = b.v WHERE a.v AND a.ts = 'abc'").noLeakCheck().fails(61, "invalid timestamp");
            assertQuery("SELECT * FROM x a JOIN x b ON a.v = b.v WHERE a.v AND a.v + 1").noLeakCheck().fails(0, MISMATCH_ERROR);
            assertQuery("SELECT * FROM x a JOIN x b ON a.v = b.v WHERE a.v + 1 AND a.v - 1").noLeakCheck().fails(50, MISMATCH_ERROR);
            assertQuery("SELECT * FROM x a JOIN x b ON a.v = b.v WHERE a.v + 1 AND b.v = true").noLeakCheck().fails(50, "boolean expression expected");
            assertQuery("SELECT * FROM x a JOIN x b ON a.v = b.v WHERE a.v + 1 AND b.ts = 'abc'").noLeakCheck().fails(50, "boolean expression expected");
            assertQuery("SELECT * FROM x a JOIN x b ON a.v = b.v WHERE b.ts = 'abc' AND a.v + 1").noLeakCheck().fails(67, "boolean expression expected");
            assertQuery("SELECT * FROM x a JOIN x b ON a.v = b.v WHERE b.v + 1 AND a.ts = 'abc'").noLeakCheck().fails(65, "invalid timestamp");
            assertQuery("SELECT * FROM x a JOIN x b ON a.v = b.v AND a.v + 1 WHERE b.ts = 'abc'").noLeakCheck().fails(48, "boolean expression expected");
            assertQuery("SELECT * FROM x a JOIN x b ON a.v = b.v AND a.v WHERE b.v = true").noLeakCheck().fails(0, "boolean expression expected");
            assertQuery("SELECT * FROM x a LEFT JOIN x b ON a.v = b.v AND b.v").noLeakCheck().fails(49, "boolean expression expected");
            assertQuery("SELECT * FROM x a LEFT JOIN x b ON a.v = b.v WHERE a.v").noLeakCheck().fails(0, "boolean expression expected");
            assertQuery("SELECT * FROM x a LEFT JOIN x b ON a.v = b.v WHERE b.v").noLeakCheck().fails(51, "boolean expression expected");
            assertQuery("SELECT * FROM x a LEFT JOIN x b ON a.v = b.v WHERE a.v + 1 AND b.ts = 'abc'").noLeakCheck().fails(55, "boolean expression expected");
            assertQuery("SELECT * FROM x a CROSS JOIN x b WHERE a.v + 1 AND b.ts = 'abc'").noLeakCheck().fails(43, "boolean expression expected");
            assertQuery("SELECT * FROM x a ASOF JOIN x b WHERE b.v").noLeakCheck().fails(38, "boolean expression expected");
            assertQuery("SELECT * FROM x a ASOF JOIN x b WHERE a.v + 1 AND b.ts = 'abc'").noLeakCheck().fails(42, "boolean expression expected");
            assertQuery("SELECT * FROM x a JOIN x b ON a.v = b.v JOIN x c ON b.v = c.v WHERE b.v + 1 AND c.ts = 'abc'").noLeakCheck().fails(72, "boolean expression expected");
            assertQuery("SELECT * FROM x a JOIN (SELECT * FROM x) b ON a.v = b.v WHERE b.v").noLeakCheck().fails(62, "boolean expression expected");
            assertQuery("SELECT * FROM x a WHERE a.v").noLeakCheck().fails(0, "boolean expression expected");
            assertQuery("SELECT * FROM x WHERE v").noLeakCheck().fails(22, "boolean expression expected");
            assertQuery("SELECT * FROM (SELECT * FROM x) WHERE v").noLeakCheck().fails(38, "boolean expression expected");
            assertExceptionNoLeakCheck("UPDATE x SET v = 1 FROM x b WHERE x.v = b.v AND x.v", 0, "boolean expression expected");
        });
    }

    @Test
    public void testOuterAndTemporalJoinErrors() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT * FROM x a LEFT JOIN x b ON a.v = b.v AND b.v = true WHERE a.ts = 'abc'").noLeakCheck().fails(73, "invalid timestamp");
            assertQuery("SELECT * FROM x a LEFT JOIN x b ON a.v = b.v AND a.v = true WHERE b.ts = 'abc'").noLeakCheck().fails(53, EQ_ERROR);
            assertQuery("SELECT * FROM x a LEFT JOIN x b ON a.v = b.v WHERE b.v = true AND a.ts = 'abc'").noLeakCheck().fails(73, "invalid timestamp");
            assertQuery("SELECT * FROM x a LEFT JOIN x b ON a.v = b.v WHERE a.v = true AND b.ts = 'abc'").noLeakCheck().fails(55, EQ_ERROR);
            assertQuery("SELECT * FROM x a LEFT JOIN x b ON a.v = b.v AND b.v = 0x123 AND a.v + b.v = true").noLeakCheck().fails(75, EQ_ERROR);
            assertQuery("SELECT * FROM x a ASOF JOIN x b WHERE b.v = true AND a.ts = 'abc'").noLeakCheck().fails(60, "invalid timestamp");
            assertQuery("SELECT * FROM x a ASOF JOIN x b WHERE a.v = true AND b.ts = 'abc'").noLeakCheck().fails(42, EQ_ERROR);
            assertQuery("SELECT * FROM x a ASOF JOIN x b ON (v) WHERE a.v = true AND abs(b.v, 1) = 1").noLeakCheck().fails(49, EQ_ERROR);
            assertQuery("SELECT * FROM x a ASOF JOIN (SELECT * FROM x WHERE v = true) b WHERE a.ts = 'abc'").noLeakCheck().fails(76, "invalid timestamp");
            assertQuery("SELECT * FROM x a LT JOIN x b ON (v) WHERE b.v = true AND a.v = 0x123").noLeakCheck().fails(64, "invalid constant: 0x123");
            assertQuery("SELECT * FROM x a SPLICE JOIN x b WHERE a.v = true AND b.ts = 'abc'").noLeakCheck().fails(62, "Invalid date [str=abc]");
        });
    }

    @Test
    public void testSubqueryErrors() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT * FROM x WHERE v IN (SELECT ts FROM x) AND abs(v, 1) = 1").noLeakCheck().fails(50, ABS_ERROR);
            assertQuery("SELECT * FROM x WHERE abs(v, 1) = 1 AND v IN (SELECT ts FROM x)").noLeakCheck().fails(46, "cannot compare LONG with type CURSOR");
            assertQuery("SELECT * FROM x WHERE v = (SELECT ts FROM x) AND ts = 'abc'").noLeakCheck().fails(54, "invalid timestamp");
            assertQuery("SELECT * FROM x WHERE v = true AND v IN (SELECT ts FROM x WHERE ts = 'abc')").noLeakCheck().fails(69, "invalid timestamp");
            assertQuery("SELECT * FROM x WHERE v = true AND v IN (SELECT v FROM x WHERE nocol = 1)").noLeakCheck().fails(63, "Invalid column: nocol");
        });
    }

    @Test
    public void testTimestampComparisonErrors() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT * FROM x WHERE ts = 'abc' AND timestamp_floor('xx', ts) = ts").noLeakCheck().fails(53, "invalid unit 'xx'");
            assertQuery("SELECT * FROM x WHERE timestamp_floor('xx', ts) = ts AND v = true").noLeakCheck().fails(38, "invalid unit 'xx'");
            assertQuery("SELECT * FROM x WHERE v = true AND timestamp_floor('xx', ts) = ts").noLeakCheck().fails(51, "invalid unit 'xx'");
            assertQuery("SELECT * FROM x WHERE v = true AND ts > '2014'").noLeakCheck().fails(24, EQ_ERROR);
            assertQuery("SELECT * FROM x WHERE ts > '2015' AND ts < '2014' AND v = true").noLeakCheck().fails(56, EQ_ERROR);
            assertQuery("EXPLAIN SELECT * FROM x WHERE v = true AND ts > '2014'").noLeakCheck().fails(32, EQ_ERROR);
        });
    }

    @Test
    public void testTypeMismatchOrder() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT * FROM x a JOIN x b ON a.v + b.v AND a.v - b.v").noLeakCheck().fails(34, MISMATCH_ERROR);
            assertQuery("SELECT * FROM x a LEFT JOIN x b ON a.v + b.v AND a.v - b.v").noLeakCheck().fails(39, MISMATCH_ERROR);
            assertQuery("SELECT * FROM x a JOIN x b ON a.ts = b.ts AND a.v - b.v AND b.v - a.v").noLeakCheck().fails(50, MISMATCH_ERROR);
            assertQuery("SELECT * FROM x a JOIN x b ON a.v - b.v WHERE a.ts = 'abc'").noLeakCheck().fails(53, "invalid timestamp");
            assertQuery("SELECT * FROM x a JOIN x b ON a.ts = b.ts AND a.v - b.v WHERE a.ts = 'abc'").noLeakCheck().fails(69, "invalid timestamp");
            assertQuery("SELECT * FROM x a JOIN x b ON a.v = b.v AND a.v = true AND a.v + 1").noLeakCheck().fails(48, EQ_ERROR);
            assertQuery("SELECT * FROM x a JOIN x b ON a.v = b.v WHERE a.v + b.v AND a.v - b.v").noLeakCheck().fails(50, MISMATCH_ERROR);
            assertQuery("SELECT * FROM x a JOIN x b ON a.v = b.v JOIN x c ON b.v = c.v WHERE c.v - b.v AND a.ts = 'abc'").noLeakCheck().fails(89, "invalid timestamp");
            assertQuery("SELECT * FROM x WHERE v + 1 AND v - 1").noLeakCheck().fails(24, MISMATCH_ERROR);
            assertQuery("SELECT * FROM x WHERE v - 1 AND ts = 'abc'").noLeakCheck().fails(37, "invalid timestamp");
            assertQuery("SELECT * FROM x WHERE v = true AND v - 1").noLeakCheck().fails(24, EQ_ERROR);
            assertQuery("SELECT * FROM x WHERE v - 1 AND v = true").noLeakCheck().fails(34, EQ_ERROR);
        });
    }

    @Test
    public void testUpdateErrors() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertExceptionNoLeakCheck("UPDATE x SET v = 1 WHERE v = true AND ts = 'abc'", 43, "invalid timestamp");
            assertExceptionNoLeakCheck("UPDATE x SET v = 1 WHERE ts = 'abc' AND abs(v, 1) = 1", 30, "invalid timestamp");
            assertExceptionNoLeakCheck("UPDATE x SET v = 1 WHERE v = true AND abs(v, 1) = 1", 38, ABS_ERROR);
            assertExceptionNoLeakCheck("UPDATE x SET v = abs(v, 1) WHERE v = true", 35, EQ_ERROR);
            assertExceptionNoLeakCheck("UPDATE x SET v = 1 WHERE v = true AND nocol = 1", 38, "Invalid column: nocol");
            assertExceptionNoLeakCheck("UPDATE x SET v = 1 WHERE v = true AND false", 27, EQ_ERROR);
            assertExceptionNoLeakCheck("UPDATE x SET v = 1 FROM x b WHERE x.v = b.v AND b.v = true AND x.ts = 'abc'", 70, "invalid timestamp");
            assertExceptionNoLeakCheck("UPDATE x SET v = 1 FROM x b WHERE x.v = b.v AND x.v = true AND b.ts = 'abc'", 52, EQ_ERROR);
        });
    }

    private static void createTables() throws Exception {
        execute("CREATE TABLE x (ts TIMESTAMP, ts2 TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY");
        execute("CREATE TABLE xn (ts TIMESTAMP_NS, ts2 TIMESTAMP_NS, v INT) TIMESTAMP(ts) PARTITION BY DAY");
        execute("CREATE TABLE xd (d DATE, ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY");
        execute("CREATE TABLE z (ts TIMESTAMP, v INT, s STRING, sym SYMBOL)");
        execute("INSERT INTO x VALUES ('2014-01-02T12:30:00.000Z', '2014-01-02T12:30:00.000Z', 1)");
        execute("INSERT INTO xn VALUES ('2014-01-02T12:30:00.000Z', '2014-01-02T12:30:00.000Z', 1)");
        execute("INSERT INTO xd VALUES ('2014-01-02T12:30:00.000Z', '2014-01-02T12:30:00.000Z', 1)");
        execute("INSERT INTO z VALUES ('2014-01-02T12:30:00.000Z', 1, 'a', 'b')");
    }
}
