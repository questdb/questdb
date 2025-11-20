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
    public void testCountDecimal() throws Exception {
        assertQuery(
                "key\tc8\tc16\tc32\tc64\tc128\tc256\n" +
                        "19\t412\t412\t418\t413\t411\t423\n" +
                        "18\t414\t409\t414\t410\t429\t417\n" +
                        "17\t405\t424\t423\t416\t418\t419\n" +
                        "16\t411\t412\t425\t411\t415\t415\n" +
                        "15\t420\t423\t414\t409\t404\t423\n" +
                        "14\t416\t421\t401\t422\t413\t419\n" +
                        "13\t414\t406\t418\t424\t415\t410\n" +
                        "12\t414\t416\t410\t404\t415\t408\n" +
                        "11\t419\t426\t410\t408\t407\t415\n" +
                        "10\t426\t402\t410\t437\t421\t414\n" +
                        "9\t422\t424\t428\t420\t404\t414\n" +
                        "8\t405\t417\t421\t420\t408\t423\n" +
                        "7\t411\t418\t421\t425\t434\t423\n" +
                        "6\t413\t412\t432\t410\t420\t421\n" +
                        "5\t423\t403\t387\t413\t438\t412\n" +
                        "4\t418\t425\t415\t421\t429\t430\n" +
                        "3\t402\t410\t429\t416\t422\t435\n" +
                        "2\t415\t402\t411\t418\t414\t432\n" +
                        "1\t432\t424\t421\t404\t406\t418\n" +
                        "0\t406\t424\t411\t404\t425\t410\n",
                "select id%20 key, count(d8) c8, count(d16) c16, count(d32) c32, " +
                        "count(d64) c64, count(d128) c128, count(d256) c256 " +
                        "from x " +
                        "order by key desc",
                "create table x as (" +
                        "select" +
                        " x id," +
                        " rnd_decimal(2,0,2) d8," +
                        " rnd_decimal(4,1,2) d16," +
                        " rnd_decimal(7,1,2) d32," +
                        " rnd_decimal(15,2,2) d64," +
                        " rnd_decimal(32,3,2) d128," +
                        " rnd_decimal(70,5,2) d256," +
                        " timestamp_sequence(0, 1000) ts" +
                        " from long_sequence(10000)" +
                        ") timestamp(ts) partition by NONE",
                null,
                true,
                true
        );
    }

    @Test
    public void testCountLong256() throws Exception {
        assertQuery(
                "count\tfirst\tlast\n" +
                        "5000\t8260188555232587029\t6825995141825164433\n",
                "select count(l256), first(l256), last(l256) from x where id % 2 = 0",
                "create table x as (" +
                        "select" +
                        " x id," +
                        " rnd_long256() l256," +
                        " timestamp_sequence(0, 1000) ts" +
                        " from long_sequence(10000)" +
                        ") timestamp(ts) partition by NONE",
                null,
                false,
                true
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
    public void testCountUuid() throws Exception {
        assertQuery(
                "count\tfirst\tlast\n" +
                        "5000\t9f9b2131-d49f-4d1d-ab81-39815c50d341\te0b69f12-4df8-4e24-b321-f0a37ccbad0a\n",
                "select count(uuid), first(uuid), last(uuid) from x where id % 2 = 0",
                "create table x as (" +
                        "select" +
                        " x id," +
                        " rnd_uuid4() uuid," +
                        " timestamp_sequence(0, 1000) ts" +
                        " from long_sequence(10000)" +
                        ") timestamp(ts) partition by NONE",
                null,
                false,
                true
        );
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
