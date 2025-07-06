/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.test.griffin.engine.groupby;

import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class CountTest extends AbstractCairoTest {

    @Test
    public void testColumnAlias() throws Exception {
        assertQuery(
                "cnt\n" +
                        "20\n",
                "select count() cnt from x",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_float(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " rnd_double(0)*100 c," +
                        " abs(rnd_int()) d," +
                        " rnd_byte(2, 50) e," +
                        " abs(rnd_short()) f," +
                        " abs(rnd_long()) g," +
                        " timestamp_sequence(0, 0) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by NONE",
                null,
                "insert into x select * from (" +
                        "select" +
                        " rnd_float(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " rnd_double(0)*100 c," +
                        " abs(rnd_int()) d," +
                        " rnd_byte(2, 50) e," +
                        " abs(rnd_short()) f," +
                        " abs(rnd_long()) g," +
                        " timestamp_sequence(0, 0) k" +
                        " from" +
                        " long_sequence(5)" +
                        ") timestamp(k)",
                "cnt\n" +
                        "25\n",
                false,
                true,
                false
        );
    }

    @Test
    public void testCountOverCursorThrows() throws Exception {
        assertMemoryLeak(() -> {
            try {
                assertQueryNoLeakCheck(
                        "cnt_1\tcnt_42\n" +
                                "20\t20\n",
                        "count(select distinct s from x where right(s, 1)='/')",
                        "create table x (s string, ts timestamp) timestamp(ts) partition by day",
                        null,
                        false,
                        true
                );
                Assert.fail();
            } catch (SqlException ignore) {
            }
        });
    }

    @Test
    public void testInterpolation() throws Exception {
        execute("create table x (ts timestamp, d double, f float, ip ipv4, i int, l256 long256, l long, s string, sym symbol, vch varchar) timestamp(ts);");
        execute("insert into x values " +
                "('2000-01-01T00:00', 1, 1, '192.168.1.1', 1, '0x42', 1, 'foo', 'foo', 'foo'), " +
                "('2000-01-01T04:30', 2, 2, '192.168.1.2', 2, '0x43', 2, 'bar', 'bar', 'bar'), " +
                "('2000-01-01T05:30', 2, 2, '192.168.1.2', 2, '0x43', 2, 'bar', 'bar', 'bar'), " +
                "('2000-01-03T00:00', 1, 1, '192.168.1.1', 1, '0x42', 1, 'foo', 'foo', 'foo');"
        );

        String expected = "ts\tcount\n" +
                "2000-01-01T00:00:00.000000Z\t3\n" +
                "2000-01-02T00:00:00.000000Z\t2\n" +
                "2000-01-03T00:00:00.000000Z\t1\n";

        // double
        assertQuery(
                expected,
                "select ts, count(d) from x sample by 1d fill(linear)",
                "ts",
                true,
                true
        );

        // float
        assertQuery(
                expected,
                "select ts, count(f) from x sample by 1d fill(linear)",
                "ts",
                true,
                true
        );

        // ipv4
        assertQuery(
                expected,
                "select ts, count(ip) from x sample by 1d fill(linear)",
                "ts",
                true,
                true
        );

        // int
        assertQuery(
                expected,
                "select ts, count(i) from x sample by 1d fill(linear)",
                "ts",
                true,
                true
        );

        // long256
        assertQuery(
                expected,
                "select ts, count(l256) from x sample by 1d fill(linear)",
                "ts",
                true,
                true
        );

        // long
        assertQuery(
                expected,
                "select ts, count(l) from x sample by 1d fill(linear)",
                "ts",
                true,
                true
        );

        // string
        assertQuery(
                expected,
                "select ts, count(s) from x sample by 1d fill(linear)",
                "ts",
                true,
                true
        );

        // symbol
        assertQuery(
                expected,
                "select ts, count(sym) from x sample by 1d fill(linear)",
                "ts",
                true,
                true
        );

        // varchar
        assertQuery(
                expected,
                "select ts, count(vch) from x sample by 1d fill(linear)",
                "ts",
                true,
                true
        );
    }

    @Test
    public void testKnownSize() throws Exception {
        assertQuery(
                "count\n" +
                        "20\n",
                "select count() from x",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_float(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " rnd_double(0)*100 c," +
                        " abs(rnd_int()) d," +
                        " rnd_byte(2, 50) e," +
                        " abs(rnd_short()) f," +
                        " abs(rnd_long()) g," +
                        " timestamp_sequence(0, 0) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by NONE",
                null,
                "insert into x select * from (" +
                        "select" +
                        " rnd_float(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " rnd_double(0)*100 c," +
                        " abs(rnd_int()) d," +
                        " rnd_byte(2, 50) e," +
                        " abs(rnd_short()) f," +
                        " abs(rnd_long()) g," +
                        " timestamp_sequence(0, 0) k" +
                        " from" +
                        " long_sequence(5)" +
                        ") timestamp(k)",
                "count\n" +
                        "25\n",
                false,
                true,
                false
        );
    }

    @Test
    public void testLongConst() throws Exception {
        assertQuery(
                "cnt_1\tcnt_42\n" +
                        "20\t20\n",
                "select count(1) cnt_1, count(42) cnt_42 from x",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_float(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " rnd_double(0)*100 c," +
                        " timestamp_sequence(0, 0) k" +
                        " from" +
                        " long_sequence(20)" +
                        ") timestamp(k) partition by NONE",
                null,
                "insert into x select * from (" +
                        "select" +
                        " rnd_float(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " rnd_double(0)*100 c," +
                        " timestamp_sequence(0, 0) k" +
                        " from" +
                        " long_sequence(5)" +
                        ") timestamp(k)",
                "cnt_1\tcnt_42\n" +
                        "25\t25\n",
                false,
                true,
                false
        );
    }

    @Test
    public void testUnknownSize() throws Exception {
        assertQuery(
                "count\n" +
                        "4919\n",
                "select count() from x where g > 0",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_float(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " rnd_double(0)*100 c," +
                        " abs(rnd_int()) d," +
                        " rnd_byte(2, 50) e," +
                        " abs(rnd_short()) f," +
                        " rnd_long() g," +
                        " timestamp_sequence(0, 0) k" +
                        " from" +
                        " long_sequence(10000)" +
                        ") timestamp(k) partition by NONE",
                null,
                "insert into x select * from (" +
                        "select" +
                        " rnd_float(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " rnd_double(0)*100 c," +
                        " abs(rnd_int()) d," +
                        " rnd_byte(2, 50) e," +
                        " abs(rnd_short()) f," +
                        " rnd_long() g," +
                        " timestamp_sequence(0, 0) k" +
                        " from" +
                        " long_sequence(800)" +
                        ") timestamp(k)",
                "count\n" +
                        "5319\n",
                false,
                true,
                false
        );
    }
}
