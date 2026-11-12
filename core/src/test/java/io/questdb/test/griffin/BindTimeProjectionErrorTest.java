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
 * A select-list, grouping-key, aggregate, window, FILL or ORDER BY expression that fails to bind fails the statement
 * where the binder binds it, in binder order, before any interval or generation error, whether or not the optimiser
 * prunes its column.
 */
public class BindTimeProjectionErrorTest extends AbstractCairoTest {
    private static final String ABS_ERROR = "there is no matching function `abs` with the argument types: (BOOLEAN)";
    private static final String COUNT_ERROR = "there is no matching function `count` with the argument types: (BOOLEAN, INT)";
    private static final String ROW = """
            ts
            2014-01-02T12:30:00.000000Z
            """;

    @Test
    public void testAggregateErrors() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            assertQuery("SELECT ts, count(true, 1) FROM x WHERE ts = 'abc'").noLeakCheck().fails(44, "invalid timestamp");
            assertQuery("SELECT ts, count(true, 1) FROM x").noLeakCheck().fails(11, COUNT_ERROR);
            assertQuery("SELECT count(true, 1), nocol FROM x").noLeakCheck().fails(23, "Invalid column: nocol");
            assertQuery("SELECT c FROM (SELECT ts, count(true, 1) c FROM x)").noLeakCheck().fails(26, COUNT_ERROR);
            assertQuery("SELECT ts FROM (SELECT ts, abs(true) k, count() FROM x)").noLeakCheck().fails(27, ABS_ERROR);
            assertQuery("SELECT ts, sum(abs(true)) FROM x WHERE ts = 'abc'").noLeakCheck().fails(44, "invalid timestamp");
            assertQuery("SELECT ts, count() + abs(true) FROM x WHERE ts = 'abc'").noLeakCheck().fails(49, "invalid timestamp");
            assertQuery("SELECT ts, count() FROM x WHERE ts = 'abc' GROUP BY ts, abs(true)").noLeakCheck().fails(37, "invalid timestamp");
            assertQuery("SELECT ts, count() FROM x GROUP BY ts, abs(true)").noLeakCheck().fails(39, ABS_ERROR);
            assertQuery("SELECT abs(true), count() FROM x WHERE ts = 'abc'").noLeakCheck().fails(44, "invalid timestamp");
            assertQuery("SELECT ts, count(true, 1) FROM x WHERE ts = 'abc' SAMPLE BY 1h").noLeakCheck().fails(44, "invalid timestamp");
            assertQuery("SELECT ts, count(true, 1) c FROM x WHERE ts = 'abc' ORDER BY c").noLeakCheck().fails(46, "invalid timestamp");
            assertQuery("SELECT ts, count() c FROM x WHERE ts = 'abc' ORDER BY abs(true)").noLeakCheck().fails(39, "invalid timestamp");
            assertQuery("SELECT DISTINCT abs(true) FROM x WHERE ts = 'abc'").noLeakCheck().fails(44, "invalid timestamp");
            assertQuery("SELECT ts, abs(true), count(), nocol FROM x").noLeakCheck().fails(11, ABS_ERROR);
            assertQuery("SELECT ts, count() FROM x GROUP BY abs(true)").noLeakCheck().fails(35, ABS_ERROR);
            assertQuery("SELECT count() FROM (SELECT ts, count(true, 1) c FROM x)").noLeakCheck().fails(32, COUNT_ERROR);
            assertQuery("SELECT count(true, 1) FROM x WHERE ts = 'abc'").noLeakCheck().fails(40, "invalid timestamp");
            assertQuery("SELECT 1 FROM (SELECT count(true, 1) c FROM x)").noLeakCheck().fails(22, COUNT_ERROR);
            assertQuery("SELECT 1 FROM (SELECT count() d, count(true, 1) c FROM x)").noLeakCheck().fails(33, COUNT_ERROR);
            assertQuery("SELECT ts, avg(abs(true)) FROM x WHERE ts = 'abc' SAMPLE BY 1h FILL(NULL)").noLeakCheck().fails(44, "invalid timestamp");
            assertQuery("SELECT ts FROM (SELECT ts, count(true, 1) c FROM x)").noLeakCheck().fails(27, COUNT_ERROR);
            assertQuery("SELECT ts FROM (SELECT ts, count() + abs(true) c FROM x)").noLeakCheck().fails(37, ABS_ERROR);
            assertQuery("SELECT ts FROM (SELECT ts, count(true, 1) c FROM x SAMPLE BY 1h)").noLeakCheck().fails(27, COUNT_ERROR);
            assertQuery("SELECT ts, d FROM (SELECT ts, count(true, 1) c, count() d FROM x)").noLeakCheck().fails(30, COUNT_ERROR);
            assertQuery("SELECT ts FROM (SELECT ts, avg(abs(true)) a FROM x SAMPLE BY 1h FILL(NULL))").noLeakCheck().fails(31, ABS_ERROR);
        });
    }

    @Test
    public void testAggregateErrorOrder() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            assertQuery("SELECT ts, count(true, 1), timestamp_floor('xx', ts) FROM x").noLeakCheck().fails(43, "invalid unit 'xx'");
            assertQuery("SELECT ts, count(true, 1), abs(true) FROM x").noLeakCheck().fails(27, ABS_ERROR);
            assertQuery("SELECT ts, abs(true), count(true, 1) FROM x").noLeakCheck().fails(11, ABS_ERROR);
            assertQuery("SELECT ts, count(true, 1) FROM x GROUP BY ts, abs(true)").noLeakCheck().fails(46, ABS_ERROR);
            assertQuery("SELECT 1 FROM (SELECT count(true, 1) c, count() d FROM x)").noLeakCheck().fails(22, COUNT_ERROR);
            assertQuery("SELECT 1 FROM (SELECT ts, count(true, 1) c FROM x)").noLeakCheck().fails(26, COUNT_ERROR);
            assertQuery("SELECT count() FROM (SELECT ts, count(true, 1) c, count() d FROM x)").noLeakCheck().fails(32, COUNT_ERROR);
            assertQuery("SELECT d FROM (SELECT count(true, 1) c, count() d FROM x)").noLeakCheck().fails(22, COUNT_ERROR);
        });
    }

    @Test
    public void testAggregateErrorWithFillValues() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            assertQuery("SELECT ts, avg(abs(true)), sum(v) FROM x WHERE ts = 'abc' SAMPLE BY 1h FILL(NULL, 0)").noLeakCheck().fails(52, "invalid timestamp");
            assertQuery("SELECT ts, sum(v) s, avg(abs(true)) a FROM x SAMPLE BY 1h FILL(0, NULL)").noLeakCheck().fails(25, ABS_ERROR);
            assertQuery("SELECT ts, avg(abs(true)) a, sum(v) s FROM x SAMPLE BY 1h FILL(PREV(s), 0)").noLeakCheck().fails(15, ABS_ERROR);
            assertQuery("SELECT ts FROM (SELECT ts, avg(abs(true)) a, sum(v) s FROM x SAMPLE BY 1h FILL(NULL, 0))").noLeakCheck().fails(31, ABS_ERROR);
            assertQuery("SELECT ts, s FROM (SELECT ts, sum(v) s, avg(abs(true)) a FROM x SAMPLE BY 1h FILL(7, NULL))").noLeakCheck().fails(44, ABS_ERROR);
        });
    }

    @Test
    public void testConsumedColumnFailsWhereBound() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            assertQuery("SELECT b FROM (SELECT ts, abs(true) b FROM x)").noLeakCheck().fails(26, ABS_ERROR);
            assertQuery("SELECT * FROM (SELECT ts, abs(true) b FROM x)").noLeakCheck().fails(26, ABS_ERROR);
            assertQuery("SELECT b + 1 FROM (SELECT ts, abs(true) b FROM x)").noLeakCheck().fails(30, ABS_ERROR);
            assertQuery("SELECT ts FROM (SELECT ts, abs(true) b FROM x) WHERE b = 1").noLeakCheck().fails(27, ABS_ERROR);
            assertQuery("SELECT ts FROM (SELECT ts, abs(true) b FROM x) ORDER BY b").noLeakCheck().fails(27, ABS_ERROR);
            assertQuery("SELECT abs(true) b, b + 1 c FROM x").noLeakCheck().fails(7, ABS_ERROR);
            assertQuery("SELECT a.b FROM (SELECT ts, abs(true) b FROM x) a JOIN x c ON a.ts = c.ts").noLeakCheck().fails(28, ABS_ERROR);
            assertQuery("SELECT a.ts FROM (SELECT ts, abs(true) b FROM x) a JOIN x c ON a.b = c.v").noLeakCheck().fails(29, ABS_ERROR);
            assertQuery("SELECT b, count() FROM (SELECT ts, abs(true) b FROM x)").noLeakCheck().fails(35, ABS_ERROR);
            assertQuery("SELECT ts, row_number() OVER (PARTITION BY b) FROM (SELECT ts, abs(true) b FROM x)").noLeakCheck().fails(63, ABS_ERROR);
            assertQuery("SELECT ts FROM (SELECT DISTINCT ts, abs(true) b FROM x)").noLeakCheck().fails(36, ABS_ERROR);
            assertQuery("SELECT count() FROM (SELECT ts, abs(true) b FROM x)").noLeakCheck().fails(32, ABS_ERROR);
            assertQuery("SELECT * FROM (SELECT ts, abs(true) b FROM x UNION ALL SELECT ts, 1 FROM x)").noLeakCheck().fails(26, ABS_ERROR);
            assertQuery("WITH c AS (SELECT ts, abs(true) b FROM x) SELECT b FROM c").noLeakCheck().fails(22, ABS_ERROR);
            assertQuery("SELECT ts FROM (SELECT ts, v FROM x ORDER BY abs(true))").noLeakCheck().fails(45, ABS_ERROR);
            assertQuery("SELECT c FROM (SELECT ts, abs(true) b, timestamp_floor('xx', ts) c FROM x)").noLeakCheck().fails(26, ABS_ERROR);
            assertExceptionNoLeakCheck("INSERT INTO x SELECT ts, ts2, abs(true) FROM x", 30, ABS_ERROR);
            assertExceptionNoLeakCheck("CREATE TABLE z AS (SELECT ts, abs(true) b FROM x)", 30, ABS_ERROR);
        });
    }

    @Test
    public void testErrorsFollowBinderOrder() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            assertQuery("SELECT abs(true), nocol FROM x").noLeakCheck().fails(7, ABS_ERROR);
            assertQuery("SELECT abs(true), timestamp_floor('xx', ts) FROM x").noLeakCheck().fails(7, ABS_ERROR);
            assertQuery("SELECT timestamp_floor('xx', ts), abs(true) FROM x").noLeakCheck().fails(23, "invalid unit 'xx'");
            assertQuery("SELECT abs(true) FROM x WHERE ts = 'abc'").noLeakCheck().fails(35, "invalid timestamp");
            assertQuery("SELECT nofunc(v) FROM x WHERE ts = 'abc'").noLeakCheck().fails(35, "invalid timestamp");
            assertQuery("SELECT foo(abs(true), nocol) FROM x").noLeakCheck().fails(22, "Invalid column: nocol");
            assertQuery("SELECT abs(true, nocol) FROM x").noLeakCheck().fails(17, "Invalid column: nocol");
            assertQuery("SELECT ts, b FROM (SELECT ts, abs(true) b FROM x) WHERE ts = 'abc'").noLeakCheck().fails(30, ABS_ERROR);
            assertQuery("SELECT ts, abs(true) b FROM x WHERE ts = 'abc' ORDER BY b").noLeakCheck().fails(41, "invalid timestamp");
            assertQuery("SELECT * FROM x WHERE ts = 'abc' ORDER BY abs(true)").noLeakCheck().fails(27, "invalid timestamp");
            assertQuery("SELECT * FROM x ORDER BY abs(true)").noLeakCheck().fails(25, ABS_ERROR);
            assertQuery("SELECT ts FROM x ORDER BY abs(true), nocol").noLeakCheck().fails(26, ABS_ERROR);
            assertQuery("SELECT abs(true) FROM x ORDER BY abs(false)").noLeakCheck().fails(7, ABS_ERROR);
            assertQuery("SELECT ts, abs(true) FROM x WHERE nofunc(v) = 1").noLeakCheck().fails(34, "unknown function name: nofunc(INT)");
            assertQuery("SELECT abs(true) FROM x a JOIN x b ON a.ts = b.ts WHERE b.ts = 'abc'").noLeakCheck().fails(63, "invalid timestamp");
            assertQuery("EXPLAIN SELECT abs(true) FROM x").noLeakCheck().fails(15, ABS_ERROR);
        });
    }

    @Test
    public void testPrunedColumnFails() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            assertQuery("SELECT ts FROM (SELECT ts, timestamp_floor('xx', ts) b FROM x)").noLeakCheck().fails(43, "invalid unit 'xx'");
            assertQuery("SELECT ts FROM (SELECT ts, abs(true) b FROM x)").noLeakCheck().fails(27, ABS_ERROR);
            assertQuery("SELECT ts FROM (SELECT ts, unknown_fn() b FROM x)").noLeakCheck().fails(27, "unknown function name: unknown_fn()");
            assertQuery("SELECT ts FROM (SELECT ts, unknown_fn(v) b FROM x)").noLeakCheck().fails(27, "unknown function name: unknown_fn(INT)");
            assertQuery("SELECT ts FROM (SELECT ts, v::uuid b FROM x)").noLeakCheck().fails(28, "there is no matching function `cast` with the argument types: (INT, UUID)");
            assertQuery("SELECT ts FROM (SELECT ts, ts2 = 'abc' b FROM x)").noLeakCheck().fails(33, "invalid timestamp");
            assertQuery("SELECT ts FROM (SELECT ts, b FROM (SELECT ts, abs(true) b FROM x))").noLeakCheck().fails(46, ABS_ERROR);
            assertQuery("SELECT ts FROM (SELECT * FROM (SELECT ts, abs(true) b FROM x))").noLeakCheck().fails(42, ABS_ERROR);
            assertQuery("SELECT ts FROM (SELECT ts, abs(true) b, timestamp_floor('xx', ts) c FROM x)").noLeakCheck().fails(27, ABS_ERROR);
            assertQuery("SELECT ts FROM (SELECT ts, abs(true) b FROM x LIMIT 1)").noLeakCheck().fails(27, ABS_ERROR);
            assertQuery("WITH c AS (SELECT ts, abs(true) b FROM x) SELECT ts FROM c").noLeakCheck().fails(22, ABS_ERROR);
            assertQuery("SELECT a.ts FROM (SELECT ts, abs(true) b FROM x) a JOIN x c ON a.ts = c.ts").noLeakCheck().fails(29, ABS_ERROR);
            assertQuery("SELECT ts FROM (SELECT ts, abs(true) b FROM x UNION ALL SELECT ts, 1 FROM x)").noLeakCheck().fails(27, ABS_ERROR);
            assertQuery("SELECT ts, count() FROM (SELECT ts, abs(true) b FROM x)").noLeakCheck().fails(36, ABS_ERROR);
            assertQuery("SELECT c FROM (SELECT ts, count() c FROM x GROUP BY ts, abs(true))").noLeakCheck().fails(56, ABS_ERROR);
            assertExceptionNoLeakCheck("INSERT INTO x SELECT ts, ts2, v FROM (SELECT ts, ts2, v, abs(true) b FROM x)", 57, ABS_ERROR);
            assertExceptionNoLeakCheck("CREATE TABLE z AS (SELECT ts FROM (SELECT ts, abs(true) b FROM x))", 46, ABS_ERROR);
            assertExceptionNoLeakCheck("CREATE VIEW vw AS (SELECT ts FROM (SELECT ts, abs(true) b FROM x))", 46, ABS_ERROR);
            assertQuery("SELECT ts FROM (SELECT ts, abs(v) b FROM x)").noLeakCheck().expectSize().timestamp("ts").returns(ROW);
        });
    }

    @Test
    public void testWindowErrors() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            assertQuery("SELECT ts, row_number() OVER (PARTITION BY abs(true)) FROM x WHERE ts = 'abc'").noLeakCheck().fails(72, "invalid timestamp");
            assertQuery("SELECT ts, sum(abs(true)) OVER () FROM x WHERE ts = 'abc'").noLeakCheck().fails(52, "invalid timestamp");
            assertQuery("SELECT ts, sum(abs(true)) OVER () FROM x").noLeakCheck().fails(15, ABS_ERROR);
            assertQuery("SELECT ts, row_number() OVER (), abs(true) FROM x WHERE ts = 'abc'").noLeakCheck().fails(61, "invalid timestamp");
            assertQuery("SELECT ts, abs(true), row_number() OVER (), nocol FROM x").noLeakCheck().fails(11, ABS_ERROR);
            assertQuery("SELECT ts, nofn() OVER () FROM x WHERE ts = 'abc'").noLeakCheck().fails(44, "invalid timestamp");
            assertQuery("SELECT ts, nofn() OVER () FROM x").noLeakCheck().fails(11, "unknown function name: nofn()");
            assertQuery("SELECT ts, row_number() OVER () + abs(true) FROM x WHERE ts = 'abc'").noLeakCheck().fails(62, "invalid timestamp");
            assertQuery("SELECT ts, sum(v) OVER (PARTITION BY timestamp_floor('xx', ts)) FROM x WHERE ts = 'abc'").noLeakCheck().fails(82, "invalid timestamp");
            assertQuery("SELECT ts, row_number() OVER (PARTITION BY nocol) FROM x WHERE ts = 'abc'").noLeakCheck().fails(68, "invalid timestamp");
            assertQuery("SELECT ts, sum(nocol) OVER () FROM x WHERE ts = 'abc'").noLeakCheck().fails(48, "invalid timestamp");
            assertQuery("SELECT * FROM (SELECT ts, row_number() OVER (PARTITION BY abs(true)) r FROM x)").noLeakCheck().fails(58, ABS_ERROR);
            assertQuery("SELECT ts FROM (SELECT ts, sum(abs(true)) OVER () s FROM x) WHERE s > 0").noLeakCheck().fails(31, ABS_ERROR);
            assertQuery("SELECT ts FROM (SELECT ts, row_number() OVER (PARTITION BY abs(true)) r FROM x)").noLeakCheck().fails(59, ABS_ERROR);
            assertQuery("SELECT ts FROM (SELECT ts, sum(abs(true)) OVER () s FROM x)").noLeakCheck().fails(31, ABS_ERROR);
            assertQuery("SELECT ts FROM (SELECT ts, row_number() OVER () r, abs(true) a FROM x)").noLeakCheck().fails(51, ABS_ERROR);
            assertQuery("SELECT ts FROM (SELECT ts, nofn() OVER () w FROM x)").noLeakCheck().fails(27, "unknown function name: nofn()");
            assertQuery("SELECT ts, b FROM (SELECT ts, sum(abs(true)) OVER () a, row_number() OVER () b FROM x)").noLeakCheck().fails(34, ABS_ERROR);
        });
    }

    @Test
    public void testWindowOrdersByColumnsOnly() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            assertQuery("SELECT ts, row_number() OVER (ORDER BY abs(v)) FROM x").noLeakCheck().fails(39, "Invalid column: abs");
            assertQuery("SELECT ts, row_number() OVER (ORDER BY v + 1) FROM x").noLeakCheck().fails(41, "Invalid column: +");
            assertQuery("SELECT ts, row_number() OVER (ORDER BY v::string) FROM x").noLeakCheck().fails(40, "Invalid column: cast");
            assertQuery("SELECT ts, row_number() OVER (ORDER BY 1) FROM x").noLeakCheck().fails(39, "Invalid column: 1");
            assertQuery("SELECT ts, row_number() OVER (ORDER BY sum(v)) FROM x").noLeakCheck().fails(39, "Invalid column: sum");
            assertQuery("SELECT ts, row_number() OVER (ORDER BY abs(nocol)) FROM x").noLeakCheck().fails(39, "Invalid column: abs");
            assertQuery("SELECT ts, row_number() OVER (ORDER BY abs(v)) FROM x WHERE ts = 'abc'").noLeakCheck().fails(65, "invalid timestamp");
            assertQuery("SELECT ts, row_number() OVER (ORDER BY abs(v)), nocol FROM x").noLeakCheck().fails(48, "Invalid column: nocol");
            assertQuery("SELECT ts FROM (SELECT ts, row_number() OVER (ORDER BY abs(v)) r FROM x)").noLeakCheck().fails(55, "Invalid column: abs");
            assertQuery("SELECT ts, row_number() OVER (ORDER BY (v)) FROM x").noLeakCheck().expectSize().timestamp("ts").returns("""
                    ts\trow_number
                    2014-01-02T12:30:00.000000Z\t1
                    """);
        });
    }

    private static void createTable() throws Exception {
        execute("CREATE TABLE x (ts TIMESTAMP, ts2 TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY");
        execute("INSERT INTO x VALUES ('2014-01-02T12:30:00.000Z', '2014-01-02T12:30:00.000Z', 1)");
    }
}
