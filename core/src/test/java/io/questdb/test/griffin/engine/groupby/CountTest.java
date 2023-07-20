/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.test.AbstractGriffinTest;
import org.junit.Test;

public class CountTest extends AbstractGriffinTest {

    @Test
    public void testColumnAlias() throws Exception {
        assertQuery13("cnt\n" +
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
                true
        );
    }

    @Test(expected = SqlException.class)
    public void testConstNullThrows() throws Exception {
        assertQuery("cnt_1\tcnt_42\n" +
                        "20\t20\n",
                "select count(NULL) from x",
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
                false,
                true
        );
    }

    @Test(expected = SqlException.class)
    public void testCountOverCursorThrows() throws Exception {
        assertQuery("cnt_1\tcnt_42\n" +
                        "20\t20\n",
                "count(select distinct s from x where right(s, 1)='/')",
                "create table x (s string, ts timestamp) timestamp(ts) partition by day",
                null,
                false,
                true
        );
    }

    @Test
    public void testKnownSize() throws Exception {
        assertQuery13("count\n" +
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
                true
        );
    }

    @Test
    public void testLongConst() throws Exception {
        assertQuery13("cnt_1\tcnt_42\n" +
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
                true
        );
    }

    @Test
    public void testUnknownSize() throws Exception {
        assertQuery13("count\n" +
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
                true
        );
    }
}
